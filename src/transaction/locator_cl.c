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
 * locator_cl.c - Transaction object locator (at client)
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <assert.h>

#include "db.h"
#include "environment_variable.h"
#include "porting.h"
#include "locator_cl.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "work_space.h"
#include "object_representation.h"
#include "transform_cl.h"
#include "class_object.h"
#include "schema_manager.h"
#include "server_interface.h"
#include "locator.h"
#include "boot_cl.h"
#include "memory_hash.h"
#include "system_parameter.h"
#include "dbi.h"
#include "repl_log.h"
#include "transaction_cl.h"
#include "network_interface_cl.h"
#include "execute_statement.h"

#define WS_SET_FOUND_DELETED(mop) WS_SET_DELETED(mop)
#define MAX_FETCH_SIZE 64

/* Mflush structures */
typedef struct locator_mflush_temp_oid LOCATOR_MFLUSH_TEMP_OID;
struct locator_mflush_temp_oid
{				/* Keep temporarily OIDs when flushing */
  MOP mop;			/* Mop with temporarily OID */
  int obj;			/* The mflush object number */
  LOCATOR_MFLUSH_TEMP_OID *next;	/* Next                     */
};

typedef struct locator_mflush_cache LOCATOR_MFLUSH_CACHE;
struct locator_mflush_cache
{				/* Description of mflushing block structure */
  LC_COPYAREA *copy_area;	/* Area where mflush objects are
				 * placed
				 */
  LC_COPYAREA_MANYOBJS *mobjs;	/* Structure which describes mflush
				 * objects
				 */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe one object               */
  LOCATOR_MFLUSH_TEMP_OID *mop_toids;	/* List of objects with temp. OIDs   */
  MOP mop_tail_toid;
  MOP class_mop;		/* Class_mop of last mflush object   */
  MOBJ class_obj;		/* The class of last mflush object   */
  HFID *hfid;			/* Instance heap of last mflush obj  */
  RECDES recdes;		/* Record descriptor                 */
  bool decache;			/* true, if objects are decached
				 * after they are mflushed.
				 */
  bool isone_mflush;		/* true, if we are not doing a
				 * massive flushing of objects
				 */
};

typedef struct locator_cache_lock LOCATOR_CACHE_LOCK;
struct locator_cache_lock
{
  OID *oid;			/* Fetched object                       */
  OID *class_oid;		/* Class of object                      */
  LOCK lock;			/* Lock acquired for fetched object     */
  LOCK class_lock;		/* Lock acquired for class              */
  LOCK implicit_lock;		/* Lock acquired for prefetched objects */
};

typedef struct locator_list_nested_mops LOCATOR_LIST_NESTED_MOPS;
struct locator_list_nested_mops
{
  LIST_MOPS *list;		/* The nested list of mops */
};

typedef struct locator_list_keep_mops LOCATOR_LIST_KEEP_MOPS;
struct locator_list_keep_mops
{
  int (*fun) (MOBJ class_obj);	/* Function to call to decide if this
				 * a class that it is kept
				 */
  LOCK lock;			/* The lock to cache */
  LIST_MOPS *list;		/* The list of mops  */
};

static volatile sig_atomic_t lc_Is_siginterrupt = false;

#if defined(RYE_DEBUG)
static void locator_dump_mflush (FILE * out_fp,
				 LOCATOR_MFLUSH_CACHE * mflush);
#endif /* RYE_DEBUG */
static void locator_cache_lock (MOP mop, MOBJ ignore_notgiven_object,
				void *xcache_lock);
static void locator_cache_lock_set (MOP mop, MOBJ ignore_notgiven_object,
				    void *xlockset);
static int locator_lock (MOP mop, LOCK lock);
static int locator_lock_set (int num_mops, MOP * vector_mop,
			     LOCK reqobj_class_lock, int quit_on_errors);
static int
locator_get_rest_objects_classes (LC_LOCKSET * lockset,
				  MOP class_mop, MOBJ class_obj);
static int
locator_cache_object_class (MOP mop, LC_COPYAREA_ONEOBJ * obj,
			    MOBJ * object_p, RECDES * recdes_p,
			    bool * call_fun);
static int
locator_cache_object_instance (MOP mop, MOP class_mop,
			       MOP * hint_class_mop_p, MOBJ * hint_class_p,
			       LC_COPYAREA_ONEOBJ * obj, MOBJ * object_p,
			       RECDES * recdes_p, bool * call_fun);
static int
locator_cache_have_object (MOP * mop_p, MOBJ * object_p, RECDES * recdes_p,
			   MOP * hint_class_mop_p, MOBJ * hint_class_p,
			   bool * call_fun, LC_COPYAREA_ONEOBJ * obj);
static int locator_cache (LC_COPYAREA * copy_area, MOP hint_class_mop,
			  MOBJ hint_class,
			  void (*fun) (MOP mop, MOBJ object, void *args),
			  void *args);
static int locator_mflush (MOP mop, void *mf);
static int locator_mflush_initialize (LOCATOR_MFLUSH_CACHE * mflush,
				      MOP class_mop, MOBJ class, HFID * hfid,
				      bool decache, bool isone_mflush);
static void locator_mflush_reset (LOCATOR_MFLUSH_CACHE * mflush);
static int locator_mflush_reallocate_copy_area (LOCATOR_MFLUSH_CACHE * mflush,
						int minsize);

static int locator_repl_mflush (LOCATOR_MFLUSH_CACHE * mflush);
static int locator_repl_mflush_force (LOCATOR_MFLUSH_CACHE * mflush);
static void locator_repl_mflush_check_error (LC_COPYAREA * mflush);

static void locator_mflush_end (LOCATOR_MFLUSH_CACHE * mflush);
static int locator_mflush_force (LOCATOR_MFLUSH_CACHE * mflush);
static int
locator_class_to_disk (LOCATOR_MFLUSH_CACHE * mflush, MOBJ object,
		       int *round_length_p, WS_MAP_STATUS * map_status);
static int
locator_mem_to_disk (LOCATOR_MFLUSH_CACHE * mflush, MOBJ object,
		     int *round_length_p, WS_MAP_STATUS * map_status);
#if defined (ENABLE_UNUSED_FUNCTION)
static void locator_mflush_set_dirty (MOP mop, MOBJ ignore_object,
				      void *ignore_argument);
#endif
static void locator_keep_mops (MOP mop, MOBJ object, void *kmops);
#if defined (ENABLE_UNUSED_FUNCTION)
static int locator_save_nested_mops (LC_LOCKSET * lockset, void *save_mops);
#endif
static LC_FIND_CLASSNAME
locator_find_class_by_oid (MOP * class_mop, const char *classname,
			   OID * class_oid, LOCK lock);
static LIST_MOPS *locator_fun_get_all_mops (MOP class_mop, LOCK lock,
					    int (*fun) (MOBJ class_obj));
#if defined (ENABLE_UNUSED_FUNCTION)
static int locator_add_to_oidset_when_temp_oid (MOP mop, void *data);
#endif
static LC_FIND_CLASSNAME locator_reserve_class_name (const char *class_name,
						     OID * class_oid);
/*
 * locator_reserve_class_name () -
 *    return:
 *  class_name(in):
 *  class_oid(in):
 */
LC_FIND_CLASSNAME
locator_reserve_class_name (const char *class_name, OID * class_oid)
{
  return locator_reserve_class_names (1, &class_name, class_oid);
}

/*
 * locator_set_sig_interrupt () -
 *
 * return:
 *   set(in):
 *
 * Note:
 */
void
locator_set_sig_interrupt (int set)
{
  if (set != false || lc_Is_siginterrupt == true)
    {
      lc_Is_siginterrupt = set;
      log_set_interrupt (set);
    }

}

/*
 * locator_is_root () - Is mop the root mop?
 *
 * return:
 *   mop(in): Memory Object pointer
 *
 * Note: Find out if the passed mop is the root mop.
 */
bool
locator_is_root (MOP mop)
{
  if (mop == sm_Root_class_mop
      || ws_mop_compare (mop, sm_Root_class_mop) == 0)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * locator_is_class () - Is mop a class mop?
 *
 * return:
 *
 *   mop(in): Memory Object pointer
 *   hint_purpose(in): Fetch purpose: Valid ones for this function
 *                     DB_FETCH_READ
 *                     DB_FETCH_WRITE
 *
 * Note: Find out if the object associated with the given mop is a
 *              class object. If the object does not exist, the function
 *              returns that the object is not a class.
 */
bool
locator_is_class (MOP mop)
{
  MOP class_mop;
  MOBJ object;

  class_mop = ws_class_mop (mop);
  if (class_mop == NULL)
    {
      /*
       * The class identifier of the object associated with the mop is stored
       * along with the object on the disk representation. The class mop is not
       * stored with the object since the object is not cached, fetch the object
       * and cache it into the workspace
       */

      if (locator_lock (mop, S_LOCK) != NO_ERROR)
	{
	  return false;
	}

      if (ws_find (mop, &object) == WS_FIND_MOP_DELETED)
	{
	  return false;
	}

      if (object == NULL)
	{
	  return false;		/* Object does not exist, so it is not a class */
	}

      class_mop = ws_class_mop (mop);
    }

  return locator_is_root (class_mop);
}

/*
 * locator_cache_lock () -
 *
 * return: nothing
 *
 *   mop(in): Memory Object pointer
 *   ignore_notgiven_object(in): The object is not passed... ignored
 *   xcache_lock(in): The lock to cache
 *
 * Note: Cache the lock for the given object MOP. The cached lock type
 *              is included in the cache_lock structure, and it depends upon
 *              the object that is passed.. that is, the requested object, the
 *              class of the requested object, or a prefetched object.
 */
static void
locator_cache_lock (MOP mop, UNUSED_ARG MOBJ ignore_notgiven_object,
		    void *xcache_lock)
{
  LOCATOR_CACHE_LOCK *cache_lock;
  OID *oid;
  LOCK lock;

  cache_lock = (LOCATOR_CACHE_LOCK *) xcache_lock;
  oid = ws_oid (mop);

  /*
   * The cached lock depends upon the object that we are dealing, Is the
   * object the requested one, is the class of the requested object,
   * or is a prefetched object
   */

  if (OID_EQ (oid, cache_lock->oid))
    {
      lock = cache_lock->lock;
    }
  else if (cache_lock->class_oid && OID_EQ (oid, cache_lock->class_oid))
    {
      lock = cache_lock->class_lock;
    }
  else
    {
      assert (cache_lock->implicit_lock >= NULL_LOCK
	      && ws_get_lock (mop) >= NULL_LOCK);
      lock = lock_Conv[cache_lock->implicit_lock][ws_get_lock (mop)];
      assert (lock != NA_LOCK);
    }

  /*
   * If the lock is IS_LOCK, IX_LOCK, the object must be a class. Otherwise,
   * we call the server with the wrong lock, the server should have fixed
   * the lock by now.
   */

  ws_set_lock (mop, lock);
}

/* Lock for prefetched instances of the same class */
static LOCK
locator_to_prefected_lock (LOCK class_lock)
{
  if (class_lock == S_LOCK)
    {
      return S_LOCK;
    }
  else if (IS_WRITE_EXCLUSIVE_LOCK (class_lock))
    {
      return X_LOCK;
    }
  else
    {
      return NULL_LOCK;
    }
}

/*
 * locator_cache_lock_set () - Cache a lock for the fetched object
 *
 * return: nothing
 *
 *   mop(in): Memory Object pointer
 *   ignore_notgiven_object(in): The object is not passed... ignored
 *   xlockset(in): Request structure of requested objects to lock
 *                 and fetch
 *
 * Note: Cache the lock for the given object MOP. The lock mode cached
 *       depends if the object is part of the requested object, part of
 *       the classes of the requested objects, or a prefetched object.
 */
static void
locator_cache_lock_set (MOP mop, UNUSED_ARG MOBJ ignore_notgiven_object,
			void *xlockset)
{
  LC_LOCKSET *lockset;		/* The area of requested objects             */
  OID *oid;			/* Oid of the object being cached            */
  MOP class_mop;		/* The class mop of the object being cached  */
  LOCK lock = NULL_LOCK;	/* Lock to be set on the object being cached */
  bool found = false;
  int stopidx_class;
  int stopidx_reqobj;
  int i;

  lockset = (LC_LOCKSET *) xlockset;

  oid = ws_oid (mop);
  class_mop = ws_class_mop (mop);

  stopidx_class = lockset->num_classes_of_reqobjs;
  stopidx_reqobj = lockset->num_reqobjs;

  while (true)
    {
      /*
       * Is the object part of the classes of the requested objects ?
       */
      for (i = lockset->last_classof_reqobjs_cached + 1; i < stopidx_class;
	   i++)
	{
	  if (OID_EQ (oid, &lockset->classes[i].oid))
	    {
	      assert (ws_get_lock (mop) >= NULL_LOCK);
	      lock = lock_Conv[lock][ws_get_lock (mop)];
	      assert (lock != NA_LOCK);
	      found = true;
	      /*
	       * Cache the location of the current on for future initialization of
	       * the search. The objects are cached in the same order as they are
	       * requested. The classes of the requested objects are sent before
	       * the actual requested objects
	       */
	      lockset->last_classof_reqobjs_cached = i;
	      break;
	    }
	}

      /*
       * Is the object part of the requested objects ?
       */
      for (i = lockset->last_reqobj_cached + 1;
	   found == false && i < stopidx_reqobj; i++)
	{
	  if (OID_EQ (oid, &lockset->objects[i].oid))
	    {
	      /* The object was requested */
	      /* Is the object a class ?.. */
	      if (class_mop != NULL && locator_is_root (class_mop))
		{
		  lock = lockset->reqobj_class_lock;
		}
	      else
		{
		  lock = NULL_LOCK;
		}

	      assert (lock >= NULL_LOCK && ws_get_lock (mop) >= NULL_LOCK);
	      lock = lock_Conv[lock][ws_get_lock (mop)];
	      assert (lock != NA_LOCK);
	      found = true;
	      lockset->last_reqobj_cached = i;
	      /*
	       * Likely, we have finished all the classes by now.
	       */
	      lockset->last_classof_reqobjs_cached =
		lockset->num_classes_of_reqobjs;
	      break;
	    }
	}

      /*
       * If were not able to find the object. We need to start looking from
       * the very beginning of the lists, and stop the searching one object
       * before where the current search stoped.
       *
       * If we have already search both lists from the very beginning stop.
       */

      if (found == true)
	{
	  break;
	}

      if (lockset->last_classof_reqobjs_cached != -1
	  || lockset->last_reqobj_cached != -1)
	{
	  /*
	   * Try the portion of the list that we have not looked
	   */
	  stopidx_class = lockset->last_classof_reqobjs_cached - 1;
	  stopidx_reqobj = lockset->last_reqobj_cached - 1;

	  lockset->last_classof_reqobjs_cached = -1;
	  lockset->last_reqobj_cached = -1;
	}
      else
	{
	  /*
	   * Leave the hints the way they were..
	   */
	  lockset->last_classof_reqobjs_cached = stopidx_class + 1;
	  lockset->last_reqobj_cached = stopidx_reqobj;
	  break;
	}
    }				/* while */

  if (found == false && class_mop != NULL)
    {
      /*
       * This is a prefetched object
       */
      lock = ws_get_lock (class_mop);
      lock = locator_to_prefected_lock (lock);

      assert (lock >= NULL_LOCK && ws_get_lock (mop) >= NULL_LOCK);
      lock = lock_Conv[lock][ws_get_lock (mop)];
      assert (lock != NA_LOCK);

      /*
       * If a prefetch a class somehow.. I don't have any lock on the root
       * set, the lowest lock on it
       */
      if (lock == NULL_LOCK && class_mop == sm_Root_class_mop)
	{
	  lock = S_LOCK;
	}
      found = true;
    }

  if (found == true)
    {
      ws_set_lock (mop, lock);
    }
}

/*
 * locator_lock () - Lock an object
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mop(in): Mop of the object to lock
 *   lock(in): Lock to acquire
 *
 * Note: The object associated with the given MOP is locked with the
 *              desired lock. The object locator on the server is not invoked
 *              if the object is actually cached with the desired lock or with
 *              a more powerful lock. In any other case, the object locator in
 *              the server is invoked to acquire the desired lock and possibly
 *              to bring the desired object along with some other objects that
 *              may be prefetched.
 */
static int
locator_lock (MOP mop, LOCK lock)
{
  LOCATOR_CACHE_LOCK cache_lock;	/* Cache the lock */
  OID *oid;			/* OID of object to lock                  */
  LOCK current_lock;		/* Current lock cached for desired object */
  MOBJ object;			/* The desired object                     */
  MOP class_mop;		/* Class mop of object to lock            */
  OID *class_oid;		/* Class identifier of object to lock     */
  MOBJ class_obj;		/* The class of the desired object        */
  LC_COPYAREA *fetch_area;	/* Area where objects are received        */
  int error_code = NO_ERROR;
  bool is_prefetch;

  assert (lock >= NULL_LOCK);

  oid = ws_oid (mop);

  if (ws_find (mop, &object) == WS_FIND_MOP_DELETED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
	      oid->volid, oid->pageid, oid->slotid);
      error_code = ER_FAILED;
      goto end;
    }

  /*
   * Invoke the transaction object locator on the server either:
   * a) if the object is not cached
   * b) the current lock acquired on the object is less powerful
   *    than the requested lock.
   */

  class_mop = ws_class_mop (mop);

  current_lock = ws_get_lock (mop);
  assert (current_lock >= NULL_LOCK);

  if (object == NULL || current_lock == NULL_LOCK
      || ((lock = lock_Conv[lock][current_lock]) != current_lock
	  && !OID_ISTEMP (oid)))
    {
      /* We must invoke the transaction object locator on the server */
      assert (lock != NA_LOCK);

      cache_lock.oid = oid;
      cache_lock.lock = lock;

      if (object == NULL && WS_ISMARK_DELETED (mop))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
		  oid->volid, oid->pageid, oid->slotid);
	  error_code = ER_FAILED;
	  goto end;
	}

      /*
       * Get the class information for the desired object, just in case we need
       * to bring it from the server
       */

      if (class_mop == NULL)
	{
	  /* Don't know the class. Server must figure it out */
	  class_oid = NULL;
	  class_obj = NULL;

	  cache_lock.class_oid = class_oid;
	  cache_lock.class_lock = NULL_LOCK;
	  cache_lock.implicit_lock = NULL_LOCK;
	}
      else
	{
	  class_oid = ws_oid (class_mop);
	  if (ws_find (class_mop, &class_obj) == WS_FIND_MOP_DELETED)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_CLASS_OF_INSTANCE, 3,
		      oid->volid, oid->pageid, oid->slotid);
	      error_code = ER_FAILED;
	      goto end;
	    }
	  cache_lock.class_oid = class_oid;
	  if (lock == NULL_LOCK)
	    {
	      cache_lock.class_lock = ws_get_lock (class_mop);
	    }
	  else
	    {
	      cache_lock.class_lock = S_LOCK;

	      assert (ws_get_lock (class_mop) >= NULL_LOCK);
	      cache_lock.class_lock =
		lock_Conv[cache_lock.class_lock][ws_get_lock (class_mop)];
	      assert (cache_lock.class_lock != NA_LOCK);
	    }
	  /* Lock for prefetched instances of the same class */
	  cache_lock.implicit_lock =
	    locator_to_prefected_lock (cache_lock.class_lock);
	}

      /* Now acquire the lock and fetch the object if needed */
      if (cache_lock.implicit_lock != NULL_LOCK)
	{
	  is_prefetch = true;
	}
      else
	{
	  is_prefetch = false;
	}
      if (locator_fetch (oid, lock, class_oid, is_prefetch,
			 &fetch_area) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
      /* We were able to acquire the lock. Was the cached object valid ? */

      if (fetch_area != NULL)
	{
	  /*
	   * Cache the objects that were brought from the server
	   */
	  if (current_lock > NULL_LOCK)
	    {
	      /* already cached
	       */
	      assert (object != NULL);
	    }
	  else
	    {
	      error_code = locator_cache (fetch_area, class_mop, class_obj,
					  locator_cache_lock, &cache_lock);
	    }
	  locator_free_copy_area (fetch_area);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      /*
       * Cache the lock for the object and its class.
       * We need to do this since we don't know if the object was received in
       * the fetch area
       */
      locator_cache_lock (mop, NULL, &cache_lock);

      if (class_mop != NULL)
	{
	  locator_cache_lock (class_mop, NULL, &cache_lock);
	}
    }

end:
  return error_code;

error:
  /* There was a failure. Was the transaction aborted ? */
  if (er_errid () == ER_LK_UNILATERALLY_ABORTED)
    {
      tran_abort_only_client (false);
    }

  return error_code;
}

/*
 * locator_lock_set () - Lock a set of objects
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_mops(in): Number of mops to lock
 *   vector_mop(in): A vector of mops to lock
 *   reqobj_inst_lock(in): Lock to acquire for requested objects that are
 *                      instances.
 *   reqobj_class_lock(in): Lock to acquire for requested objects that are
                        classes
 *   quit_on_errors(in): Quit when an error is found such as cannot lock all
 *                      nested objects.
 *
 * Note: The objects associated with the MOPs in the given vector_mop
 *              area are locked with the desired lock. The object locator on
 *              the server is not invoked if all objects are actually cached
 *              with the desired lock or with a more powerful lock. In any
 *              other case, the object locator in the server is invoked to
 *              acquire the desired lock and possibly to bring the desired
 *              objects along with some other objects that may be prefetched
 *              such as the classes of the objects.
 *              The function does not quit when an error is found if the value
 *              of request->quit_on_errors is false. In this case the
 *              object with the error is not locked/fetched. The function
 *              tries to lock all the objects at once, however if this fails
 *              and the function is allowed to continue when errors are
 *              detected, the objects are locked individually.
 */
static int
locator_lock_set (int num_mops, MOP * vector_mop,
		  LOCK reqobj_class_lock, int quit_on_errors)
{
  LC_LOCKSET *lockset;		/* Area to object to be requested   */
  LC_LOCKSET_REQOBJ *reqobjs;	/* Description of requested objects */
  LC_LOCKSET_CLASSOF *reqclasses;	/* Description of classes of
					 * requested objects
					 */
  MOP mop;			/* mop of the object in question    */
  OID *oid;			/* OID of MOP object to lock        */
  LOCK lock;			/* The desired lock                 */
  LOCK current_lock;		/* Current lock cached for desired
				 * object
				 */
  MOBJ object;			/* The desired object               */
  MOP class_mop = NULL;		/* Class mop of object to lock      */
  OID *class_oid;		/* Class id of object to lock       */
  MOBJ class_obj = NULL;	/* The class of the desired object  */
  int error_code = NO_ERROR;
  int i, j;
  MHT_TABLE *htbl = NULL;	/* Hash table of already found oids */

  if (num_mops <= 0)
    {
      return NO_ERROR;
    }

  lockset = locator_allocate_lockset (num_mops, reqobj_class_lock,
				      quit_on_errors);
  if (lockset == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  reqobjs = lockset->objects;
  reqclasses = lockset->classes;

  /*
   * If there were requested more than 30 objects, set a memory hash table
   * to check for duplicates.
   */

  if (num_mops > 30)
    {
      htbl = mht_create ("Memory hash locator_lock_set", num_mops, oid_hash,
			 oid_compare_equals);
    }

  for (i = 0; i < num_mops; i++)
    {
      mop = vector_mop[i];
      if (mop == NULL)
	{
	  continue;
	}
      class_mop = ws_class_mop (mop);
      oid = ws_oid (mop);

      /*
       * Make sure that it is not duplicated. This is needed since our API does
       * not enforce uniqueness in sequences and so on.
       *
       * We may need to sort the list to speed up, removal of duplications or
       * build a special kind of hash table.
       */

      if (htbl != NULL)
	{
	  /*
	   * Check for duplicates by looking into the hash table
	   */
	  if (mht_get (htbl, oid) == NULL)
	    {
	      /*
	       * The object has not been processed
	       */
	      if (mht_put (htbl, oid, mop) != mop)
		{
		  mht_destroy (htbl);
		  htbl = NULL;
		}
	      j = lockset->num_reqobjs;
	    }
	  else
	    {
	      /*
	       * These object has been processed. The object is duplicated in the
	       * list of requested objects.
	       */
	      j = 0;
	    }
	}
      else
	{
	  /*
	   * We do not have a hash table to check for duplicates, we must do a
	   * sequential scan.
	   */
	  for (j = 0; j < lockset->num_reqobjs; j++)
	    {
	      if (OID_EQ (oid, &lockset->objects[j].oid))
		{
		  break;	/* The object is already in the request list */
		}
	    }
	}

      if (j < lockset->num_reqobjs)
	{
	  continue;
	}

      /* Is mop a class ? ... simple comparison, don't use locator_is_root */
      if (class_mop == sm_Root_class_mop)
	{
	  lock = reqobj_class_lock;
	}
      else
	{
	  lock = NULL_LOCK;
	}

      if (ws_find (mop, &object) == WS_FIND_MOP_DELETED)
	{
	  if (quit_on_errors == false)
	    {
	      continue;
	    }
	  else
	    {
	      /* The object has been deleted */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	      error_code = ER_HEAP_UNKNOWN_OBJECT;
	      break;
	    }
	}

#if defined (SA_MODE) && !defined (RYE_DEBUG)
      if (object != NULL)
	{
	  /* The object is cached */
	  assert (lock >= NULL_LOCK && ws_get_lock (class_mop) >= NULL_LOCK);
	  lock = lock_Conv[lock][ws_get_lock (mop)];
	  assert (lock != NA_LOCK);
	  ws_set_lock (mop, lock);
	  continue;
	}
#endif /* SA_MODE && !RYE_DEBUG */

      /*
       * Invoke the transaction object locator on the server either:
       * a) if the object is not cached
       * b) the current lock acquired on the object is less powerful
       *    than the requested lock.
       */

      current_lock = ws_get_lock (mop);
      assert (lock >= NULL_LOCK && current_lock >= NULL_LOCK);
      lock = lock_Conv[lock][current_lock];
      assert (lock != NA_LOCK);

      if (object == NULL || current_lock == NULL_LOCK
	  || (lock != current_lock && !OID_ISTEMP (oid)))
	{

	  /*
	   * We must invoke the transaction object locator on the server for this
	   * object.
	   */

	  /* Find the cache coherency numbers for fetching purposes */

	  if (object == NULL && WS_ISMARK_DELETED (mop))
	    {
	      if (quit_on_errors == false)
		{
		  continue;
		}
	      else
		{
		  /* The object has been deleted */
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
			  oid->slotid);
		  error_code = ER_HEAP_UNKNOWN_OBJECT;
		  break;
		}
	    }

	  COPY_OID (&reqobjs->oid, oid);

	  /*
	   * Get the class information for the desired object, just in case we
	   * need to bring it from the server
	   */

	  if (class_mop == NULL)
	    {
	      /* Don't know the class. Server must figure it out */
	      reqobjs->class_index = -1;
	    }
	  else
	    {
	      class_oid = ws_oid (class_mop);
	      if (ws_find (class_mop, &class_obj) == WS_FIND_MOP_DELETED)
		{
		  if (quit_on_errors == false)
		    {
		      continue;
		    }
		  else
		    {
		      /* The class has been deleted */
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_HEAP_UNKNOWN_CLASS_OF_INSTANCE, 3,
			      oid->volid, oid->pageid, oid->slotid);
		      error_code = ER_HEAP_UNKNOWN_CLASS_OF_INSTANCE;
		      break;
		    }
		}

	      COPY_OID (&reqclasses->oid, class_oid);

	      /* Check for duplication in list of classes of requested
	       * objects */
	      for (j = 0; j < lockset->num_classes_of_reqobjs; j++)
		{
		  if (OID_EQ (class_oid, &lockset->classes[j].oid))
		    {
		      break;	/* The class is already in the class array */
		    }
		}

	      if (j >= lockset->num_classes_of_reqobjs)
		{
		  /* Class is not in the list */
		  reqobjs->class_index = lockset->num_classes_of_reqobjs;
		  lockset->num_classes_of_reqobjs++;
		  reqclasses++;
		}
	      else
		{
		  /* Class is already in the list */
		  reqobjs->class_index = j;
		}
	    }
	  lockset->num_reqobjs++;
	  reqobjs++;
	}
    }

  /*
   * We do not need the hash table any longer
   */
  if (htbl != NULL)
    {
      mht_destroy (htbl);
      htbl = NULL;
    }

  /*
   * Now acquire the locks and fetch the desired objects when needed
   */

  if (error_code == NO_ERROR && lockset != NULL && lockset->num_reqobjs > 0)
    {
      error_code = locator_get_rest_objects_classes (lockset, class_mop,
						     class_obj);
      if (error_code == NO_ERROR)
	{
	  /*
	   * Cache the lock for the requested objects and their classes.
	   */
	  for (i = 0; i < lockset->num_classes_of_reqobjs; i++)
	    {
	      if ((!OID_ISNULL (&lockset->classes[i].oid))
		  && (mop = ws_mop (&lockset->classes[i].oid,
				    sm_Root_class_mop)) != NULL)
		{
		  /*
		   * The following statement was added as safety after the C/S stub
		   * optimization of locator_fetch_lockset...which does not bring back
		   * the lock lockset array
		   */
		  if (ws_find (mop, &object) != WS_FIND_MOP_DELETED
		      && object != NULL)
		    {
		      locator_cache_lock_set (mop, NULL, lockset);
		    }
		}
	      else if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
		{
		  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
		}
	    }

	  if (error_code == NO_ERROR)
	    {
	      for (i = 0; i < lockset->num_reqobjs; i++)
		{
		  if ((!OID_ISNULL (&lockset->objects[i].oid))
		      && (mop = ws_mop (&lockset->objects[i].oid,
					NULL)) != NULL)
		    {
		      /*
		       * The following statement was added as safety after the
		       * C/S stub optimization of locator_fetch_lockset...which does
		       * not bring back the lock lockset array
		       */
		      if (ws_find (mop, &object) != WS_FIND_MOP_DELETED
			  && object != NULL)
			{
			  locator_cache_lock_set (mop, NULL, lockset);
			}
		    }
		  else if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
		    {
		      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
		    }
		}
	    }
	}

      if (quit_on_errors == false)
	{
	  /* Make sure that there was not an error in the interested object */
	  mop = vector_mop[0];
	  if (ws_find (mop, &object) == WS_FIND_MOP_DELETED)
	    {
	      /* The object has been deleted */
	      oid = ws_oid (mop);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	      error_code = ER_HEAP_UNKNOWN_OBJECT;
	      goto error;
	    }
	  /* The only way to find out if there was an error, is by looking to
	   * acquired lock
	   */
	  class_mop = ws_class_mop (mop);
	  if (class_mop == sm_Root_class_mop)
	    {
	      lock = reqobj_class_lock;
	    }
	  else
	    {
	      lock = NULL_LOCK;
	    }

	  current_lock = ws_get_lock (mop);
	  assert (lock >= NULL_LOCK && current_lock >= NULL_LOCK);
	  lock = lock_Conv[lock][current_lock];
	  assert (lock != NA_LOCK);

	  if (current_lock == NULL_LOCK || lock != current_lock)
	    {
	      error_code = ER_FAILED;
	      if (er_errid () == 0)
		{
		  oid = ws_oid (mop);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LC_LOCK_CACHE_ERROR, 3, oid->volid,
			  oid->pageid, oid->slotid);
		  error_code = ER_LC_LOCK_CACHE_ERROR;
		  goto error;
		}
	    }
	}
    }

error:
  if (lockset != NULL)
    {
      locator_free_lockset (lockset);
    }

  return error_code;
}

/*
 * locator_get_rest_objects_classes:
 *
 * return : error code
 *
 *    lockset(in/out):
 *    class_mop(in/out):
 *    class_obj(in/out):
 *
 * Note : Now get the rest of the objects and classes
 */
static int
locator_get_rest_objects_classes (LC_LOCKSET * lockset,
				  MOP class_mop, MOBJ class_obj)
{
  int error_code = NO_ERROR;
  int i, idx = 0;
  LC_COPYAREA *fetch_copyarea[MAX_FETCH_SIZE];
  LC_COPYAREA **fetch_ptr = fetch_copyarea;

  if (MAX (lockset->num_classes_of_reqobjs, lockset->num_reqobjs) >
      MAX_FETCH_SIZE)
    {
      fetch_ptr =
	(LC_COPYAREA **) malloc (sizeof (LC_COPYAREA *) *
				 MAX (lockset->num_classes_of_reqobjs,
				      lockset->num_reqobjs));

      if (fetch_ptr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1,
		  sizeof (LC_COPYAREA *) *
		  MAX (lockset->num_classes_of_reqobjs,
		       lockset->num_reqobjs));

	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }

  while (lockset->num_classes_of_reqobjs
	 > lockset->num_classes_of_reqobjs_processed
	 || lockset->num_reqobjs > lockset->num_reqobjs_processed)
    {
      fetch_ptr[idx] = NULL;
      if (locator_fetch_lockset (lockset, &fetch_ptr[idx]) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  break;
	}

      idx++;
    }

  for (i = 0; i < idx; i++)
    {
      if (fetch_ptr[i] != NULL)
	{
	  int ret = locator_cache (fetch_ptr[i], class_mop, class_obj, NULL,
				   NULL);
	  if (ret != NO_ERROR && error_code != NO_ERROR)
	    {
	      error_code = ret;
	    }

	  locator_free_copy_area (fetch_ptr[i]);
	}
    }

  if (fetch_ptr != fetch_copyarea)
    {
      free_and_init (fetch_ptr);
    }

  return error_code;
}

/*
 * locator_fetch_class () - Fetch a class
 *
 * return: MOBJ
 *
 *   class_mop(in):Memory Object Pointer of class to fetch
 *   purpose(in): Fetch purpose: Valid ones:
 *                                    DB_FETCH_READ
 *                                    DB_FETCH_WRITE
 *                                    DB_FETCH_DIRTY
 *                                    DB_FETCH_CLREAD_INSTWRITE
 *                                    DB_FETCH_CLREAD_INSTREAD
 *                                    DB_FETCH_QUERY_READ
 *                                    DB_FETCH_QUERY_WRITE
 *
 * Note: Fetch the class associated with the given mop for the given
 *              purpose
 *
 *              Currently, this function is very simple. More complexity will
 *              be added to support long duration transaction with the notion
 *              of membership transactions, hard, soft, and broken locks.
 *              (See report on "Long Transaction Support")
 */
MOBJ
locator_fetch_class (MOP class_mop, LOCK lock)
{
  MOBJ class_obj;		/* The desired class                     */

  if (class_mop == NULL)
    {
      return NULL;
    }

  if (locator_lock (class_mop, lock) != NO_ERROR)
    {
      return NULL;
    }

  if (ws_find (class_mop, &class_obj) == WS_FIND_MOP_DELETED)
    {
      return NULL;
    }

  return class_obj;
}

/*
 * locator_fetch_instance () - Fetch an instance
 *
 * return: MOBJ
 *
 *   mop(in): Memory Object Pointer of class to fetch
 *   purpose(in): Fetch purpose: Valid ones:
 *                                    DB_FETCH_READ
 *                                    DB_FETCH_WRITE
 *                                    DB_FETCH_DIRTY
 *
 * Note: Fetch the instance associated with the given mop for the given
 *              purpose
 *
 * Note:        Currently, this function is very simple. More complexity will
 *              be added to support long duration transaction with the notion
 *              of membership transactions, hard, soft, and broken locks.
 *              (See report on "Long Transaction Support")
 */
MOBJ
locator_fetch_instance (MOP mop)
{
  MOBJ inst;			/* The desired instance                  */

  inst = NULL;
  if (locator_lock (mop, NULL_LOCK) != NO_ERROR)
    {
      return NULL;
    }
  if (ws_find (mop, &inst) == WS_FIND_MOP_DELETED)
    {
      return NULL;
    }

  return inst;
}

/*
 * locator_fetch_set () - Fetch a set of objects (both instances and classes)
 *
 * return: MOBJ of the first MOP or NULL
 *
 *   num_mops(in): Number of mops to to fetch
 *   mop_set(in): A vector of mops to fetch
 *   inst_purpose(in): Fetch purpose for requested objects that are instances:
 *                  Valid ones:
 *                                    DB_FETCH_READ
 *                                    DB_FETCH_WRITE
 *                                    DB_FETCH_DIRTY
 *   class_purpose(in): Fetch purpose for requested objects that are classes:
 *                  Valid ones:
 *                                    DB_FETCH_READ
 *                                    DB_FETCH_WRITE
 *                                    DB_FETCH_DIRTY
 *                                    DB_FETCH_CLREAD_INSTWRITE
 *                                    DB_FETCH_CLREAD_INSTREAD
 *                                    DB_FETCH_QUERY_READ
 *                                    DB_FETCH_QUERY_WRITE
 *                                    DB_FETCH_QUERY_WRITE
 *   quit_on_errors(in): Wheater to continue fetching in case an error, such as
 *                  object does not exist, is found.
 *
 * Note:Fetch the objects associated with the given mops for the given
 *              purpose. The function does not quit when an error is found if
 *              the value of quit_on_errors is false. In this case the object
 *              with the error is not locked/fetched. The function tries to
 *              lock all the objects at once, however if this fails and the
 *              function is allowed to continue when errors are detected, the
 *              objects are locked individually.
 */
MOBJ
locator_fetch_set (int num_mops, MOP * mop_set, LOCK lock, int quit_on_errors)
{
  MOBJ object;			/* The desired object of the first mop */

  if (num_mops <= 0)
    {
      return NULL;
    }

  if (num_mops == 1)
    {
      MOP first = mop_set[0];

      /* Execute a simple fetch */
      if (ws_class_mop (first) == sm_Root_class_mop)
	{
	  return locator_fetch_class (first, lock);
	}
      else
	{
	  return locator_fetch_instance (first);
	}
    }

  object = NULL;
  if (locator_lock_set (num_mops, mop_set, lock, quit_on_errors) != NO_ERROR)
    {
      return NULL;
    }
  if (ws_find (*mop_set, &object) == WS_FIND_MOP_DELETED)
    {
      return NULL;
    }

  return object;
}

/*
 * locator_keep_mops () - Append mop to list of mops
 *
 * return: LIST_MOPS * (Must be deallocated by caller)
 *
 *   mop(in): Mop of an object
 *   object(in): The object .. ignored
 *   kmops(in): The keep most list of mops (set as a side effect)
 *
 * Note:Append the mop to the list of mops.
 */
static void
locator_keep_mops (MOP mop, MOBJ object, void *kmops)
{
  LOCATOR_LIST_KEEP_MOPS *keep_mops;
  LOCK lock;

  keep_mops = (LOCATOR_LIST_KEEP_MOPS *) kmops;

  assert (keep_mops->lock >= NULL_LOCK && ws_get_lock (mop) >= NULL_LOCK);
  lock = lock_Conv[keep_mops->lock][ws_get_lock (mop)];
  assert (lock != NA_LOCK);

  ws_set_lock (mop, lock);
  if (keep_mops->fun != NULL && object != NULL)
    {
      if (((*keep_mops->fun) (object)) == false)
	{
	  return;
	}
    }
  (keep_mops->list->mops)[keep_mops->list->num++] = mop;
}

/*
 * locator_fun_get_all_mops () - Get all instance mops of the given class. return only
 *                         the ones that satisfy the client function
 *
 * return: LIST_MOPS * (Must be deallocated by caller)
 *
 *   class_mop(in): Class mop of the instances
 *   lock(in):
 *   fun(in): Function to call on each object of class. If the function
 *                 returns false, the object is not returned to the caller.
 *
 * Note: Find out all the instances (mops) of a given class. The
 *              instances of the class are prefetched for future references.
 *              The list of mops is returned to the caller.
 */
static LIST_MOPS *
locator_fun_get_all_mops (MOP class_mop,
			  LOCK lock, int (*fun) (MOBJ class_obj))
{
  LOCATOR_LIST_KEEP_MOPS keep_mops;
  LC_COPYAREA *fetch_area;	/* Area where objects are received */
  HFID *hfid;
  OID *class_oid;
  OID last_oid;
  MOBJ class_obj;
  size_t size;
  INT64 nobjects;
  INT64 estimate_nobjects;
  INT64 nfetched;
  int error_code = NO_ERROR;

#if 1				/* TODO -trace */
  assert (lock == S_LOCK);
#endif

  /* Get the class */
  class_oid = ws_oid (class_mop);
  class_obj = locator_fetch_class (class_mop, lock);
  if (class_obj == NULL)
    {
      /* Unable to fetch class to find out its instances */
      return NULL;
    }

  /* Find the desired lock on the class */
  if (lock == NULL_LOCK)
    {
      lock = ws_get_lock (class_mop);
    }
  else
    {
      assert (lock >= NULL_LOCK);
      assert (ws_get_lock (class_mop) >= NULL_LOCK);
      lock = lock_Conv[lock][ws_get_lock (class_mop)];
      assert (lock != NA_LOCK);
    }

  /*
   * Find the implicit lock to be acquired by the instances
   */

  keep_mops.fun = fun;
  keep_mops.lock = locator_to_prefected_lock (lock);
  keep_mops.list = NULL;

  /* Find the heap where the instances are stored */
  hfid = sm_heap (class_obj);
  if (hfid->vfid.fileid == NULL_FILEID)
    {
      return NULL;
    }

  /* Flush all the instances */

  if (locator_flush_all_instances (class_mop, DONT_DECACHE) != NO_ERROR)
    {
      return NULL;
    }

  nobjects = 0;
  nfetched = -1;
  estimate_nobjects = -1;
  OID_SET_NULL (&last_oid);

  /* Now start fetching all the instances and build a list of the mops */

  while (nobjects != nfetched)
    {
      /*
       * Note that the number of object and the number of fetched objects are
       * updated by the locator_fetch_all function on the server
       */
      error_code = locator_fetch_all (hfid, &lock, class_oid, &nobjects,
				      &nfetched, &last_oid, &fetch_area);
      if (error_code != NO_ERROR)
	{
	  /* There was a failure. Was the transaction aborted ? */
	  if (er_errid () == ER_LK_UNILATERALLY_ABORTED)
	    {
	      (void) tran_abort_only_client (false);
	    }
	  if (fetch_area != NULL)
	    {
	      locator_free_copy_area (fetch_area);
	    }
	  if (keep_mops.list != NULL)
	    {
	      locator_free_list_mops (keep_mops.list);
	      keep_mops.list = NULL;
	    }
	  break;
	}

      /*
       * Cache the objects, that were brought from the server
       */
      if (fetch_area == NULL)
	{
	  /* No more objects */
	  break;
	}

      /*
       * If the list of mops is NULL, this is the first time.. allocate the
       * list and continue retrieving the objects
       */
      if (estimate_nobjects < nobjects)
	{
	  estimate_nobjects = nobjects;
	  size = sizeof (*keep_mops.list) + (nobjects * sizeof (MOP *));
	  if (keep_mops.list == NULL)
	    {
	      keep_mops.list = (LIST_MOPS *) malloc (size);
	      if (keep_mops.list == NULL)
		{
		  locator_free_copy_area (fetch_area);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
		  break;
		}
	      keep_mops.list->num = 0;
	    }
	  else
	    {
	      keep_mops.list = (LIST_MOPS *) realloc (keep_mops.list, size);
	      if (keep_mops.list == NULL)
		{
		  locator_free_copy_area (fetch_area);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
		  break;
		}
	    }
	}
      error_code = locator_cache (fetch_area, class_mop, class_obj,
				  locator_keep_mops, &keep_mops);
      locator_free_copy_area (fetch_area);
    }				/* while */

  if (keep_mops.list != NULL && keep_mops.lock == NULL_LOCK
      && locator_is_root (class_mop) && (lock == S_LOCK))
    {
      if (locator_lock_set (keep_mops.list->num, keep_mops.list->mops,
			    lock, true) != NO_ERROR)
	{
	  locator_free_list_mops (keep_mops.list);
	  keep_mops.list = NULL;
	}
    }

  return keep_mops.list;
}

/*
 * locator_get_all_mops () - Get all instance mops
 *
 * return: LIST_MOPS * (Must be deallocated by caller)
 *
 *   class_mop(in): Class mop of the instances
 *   purpose(in): Fetch purpose: Valid ones:
 *                                    DB_FETCH_DIRTY (Will not lock)
 *                                    DB_FETCH_QUERY_READ
 *                                    DB_FETCH_QUERY_WRITE
 *
 * Note: Find out all the instances (mops) of a given class. The
 *              instances of the class are prefetched for future references.
 *              The list of mops is returned to the caller.
 */
LIST_MOPS *
locator_get_all_mops (MOP class_mop, LOCK lock)
{
  return locator_fun_get_all_mops (class_mop, lock, NULL);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * locator_get_all_class_mops () - Return all class mops that satisfy the client
 *                          function
 *
 * return: LIST_MOPS * (Must be deallocated by caller)
 *
 *   purpose(in): Fetch purpose: Valid ones:
 *                                    DB_FETCH_DIRTY (Will not lock)
 *                                    DB_FETCH_QUERY_READ
 *                                    DB_FETCH_QUERY_WRITE
 *   fun(in): Function to call on each object of class. If the function
 *                 returns false, the object is not returned to the caller.
 *
 * Note: Find out all the classes that satisfy the given client
 *              function.
 */
LIST_MOPS *
locator_get_all_class_mops (LOCK lock, int (*fun) (MOBJ class_obj))
{
  return locator_fun_get_all_mops (sm_Root_class_mop, lock, fun);
}

/*
 * locator_save_nested_mops () - Construct list of nested references
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   lockset(in): Request of the desired object and its nested references
 *   save_mops(in):
 *
 * Note:Construct a list of all nested references including the given
 *              object.
 */
static int
locator_save_nested_mops (LC_LOCKSET * lockset, void *save_mops)
{
  int i;
  LOCATOR_LIST_NESTED_MOPS *nested = (LOCATOR_LIST_NESTED_MOPS *) save_mops;
  size_t size = sizeof (*nested->list)
    + (lockset->num_reqobjs * sizeof (MOP *));

  if (lockset->num_reqobjs <= 0)
    {
      nested->list = NULL;
      return NO_ERROR;
    }

  nested->list = (LIST_MOPS *) malloc (size);
  if (nested->list == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, size);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  nested->list->num = 0;
  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      if (!OID_ISNULL (&lockset->objects[i].oid))
	{
	  (nested->list->mops)[nested->list->num++] =
	    ws_mop (&lockset->objects[i].oid, NULL);
	}
    }

  return NO_ERROR;
}

/*
 * locator_get_all_nested_mops () - Get all nested mops of the given mop object
 *
 * return: LIST_MOPS * (Must be deallocated by caller)
 *
 *   mop(in): Memory Object Pointer of desired object (the graph root of
 *               references)
 *   prune_level(in): Get nested references upto this level. If the value is <= 0
 *                 means upto an infinite level (i.e., no pruning).
 *   inst_purpose(in):
 *
 * Note: Traverse the given object finding all direct and indirect
 *              references upto the given prune level. A negative prune level
 *              means infnite (i.e., find all nested references).
 *
 *             This function can be used to store the references of an object
 *             into another object, this will allow future used of the
 *             locator_fetch_set function wich is more efficient than the
 *             locator_fetch_nested function since the finding of the references is
 *             skipped.
 */
LIST_MOPS *
locator_get_all_nested_mops (MOP mop, int prune_level,
			     DB_FETCH_MODE inst_purpose)
{
  LOCATOR_LIST_NESTED_MOPS nested;
  LOCK lock;			/* Lock to acquire for the above purpose */

#if defined(RYE_DEBUG)
  if (ws_class_mop (mop) != NULL)
    {
      if (locator_is_root (ws_class_mop (mop)))
	{
	  OID *oid;

	  oid = ws_oid (mop);
	  er_log_debug (ARG_FILE_LINE,
			"locator_get_all_nested_mops: SYSTEM ERROR"
			" Incorrect use of function.\n Object OID %d|%d|%d"
			" associated with argument mop is not an instance.\n"
			" Calling locator_fetch_class instead..\n",
			oid->volid, oid->pageid, oid->slotid);
	  return NULL;
	}
    }
#endif /* RYE_DEBUG */

  lock = locator_fetch_mode_to_lock (inst_purpose, LC_INSTANCE);
  nested.list = NULL;
  if (locator_lock_nested (mop, lock, prune_level, true,
			   locator_save_nested_mops, &nested) != NO_ERROR)
    {
      if (nested.list != NULL)
	{
	  locator_free_list_mops (nested.list);
	  nested.list = NULL;
	}
    }

  return nested.list;
}
#endif

/*
 * locator_free_list_mops () - Free the list of all instance mops
 *
 * return: nothing
 *
 *   mops(in): Structure of mops(See function locator_get_all_mops)
 *
 * Note: Free the LIST_MOPS.
 */
void
locator_free_list_mops (LIST_MOPS * mops)
{
  int i;

  /*
   * before freeing the array, NULL out all the MOP pointers so we don't
   * become a GC root for all of those MOPs.
   */
  if (mops != NULL)
    {
      for (i = 0; i < mops->num; i++)
	{
	  mops->mops[i] = NULL;
	}
      free_and_init (mops);
    }
}

static LC_FIND_CLASSNAME
locator_find_class_by_oid (MOP * class_mop, const char *classname,
			   OID * class_oid, LOCK lock)
{
  LC_FIND_CLASSNAME found;
  int error_code;

  assert (classname != NULL);

  /* Need to check the classname to oid in the server */
  *class_mop = NULL;
  found = locator_find_class_oid (classname, class_oid, lock);
  switch (found)
    {
    case LC_CLASSNAME_EXIST:
      *class_mop = ws_mop (class_oid, sm_Root_class_mop);
      if (*class_mop == NULL || WS_ISMARK_DELETED (*class_mop))
	{
	  *class_mop = NULL;
	  if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      found = LC_CLASSNAME_ERROR;
	    }
	  else
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LC_UNKNOWN_CLASSNAME, 1, classname);
	    }

	  return found;
	}

      error_code = locator_lock (*class_mop, lock);
      if (error_code != NO_ERROR)
	{
	  /*
	   * Fetch the class object so that it gets properly interned in
	   * the workspace class table.  If we don't do that we can go
	   * through here a zillion times until somebody actually *looks*
	   * at the class object (not just its oid).
	   */
	  *class_mop = NULL;
	  found = LC_CLASSNAME_ERROR;
	}
      break;

    case LC_CLASSNAME_DELETED:
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_LC_UNKNOWN_CLASSNAME, 1, classname);
      break;

    case LC_CLASSNAME_ERROR:
      if (er_errid () == ER_LK_UNILATERALLY_ABORTED)
	{
	  (void) tran_abort_only_client (false);
	}
      break;

    default:
      break;
    }

  return found;
}

/*
 * locator_find_class () - Find mop of a class
 *
 * return: MOP
 *
 *   classname(in): Name of class to search
 *
 * Note: Find the mop of the class with the given classname. The class
 *              object may be brought to the client for future references.
 */
MOP
locator_find_class (const char *classname, LOCK lock)
{
  MOP class_mop;
  OID class_oid;
  LOCK current_lock;
  LC_FIND_CLASSNAME found;

  if (classname == NULL)
    {
      return NULL;
    }

  OID_SET_NULL (&class_oid);

  /*
   * Check if the classname to OID entry is cached. Trust the cache only if
   * there is a lock on the class
   */
  class_mop = ws_find_class (classname);
  if (class_mop == NULL)
    {
      found = locator_find_class_by_oid (&class_mop, classname,
					 &class_oid, lock);
      goto end;
    }

  current_lock = ws_get_lock (class_mop);
  if (current_lock == NULL_LOCK)
    {
      found = locator_find_class_by_oid (&class_mop, classname,
					 &class_oid, lock);
      goto end;
    }

  if (WS_ISMARK_DELETED (class_mop))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_LC_UNKNOWN_CLASSNAME, 1, classname);
      found = LC_CLASSNAME_DELETED;
    }
  else
    {
      if (locator_lock (class_mop, lock) == NO_ERROR)
	{
	  found = LC_CLASSNAME_EXIST;
	}
      else
	{
	  found = LC_CLASSNAME_ERROR;
	}
    }

end:
  if (found == LC_CLASSNAME_EXIST)
    {
      return class_mop;
    }
  else
    {
      return NULL;
    }
}

/*
 * locator_cache_object_class () -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mop(in/out):
 *   obj(in/out):
 *   object_p(in/out):
 *   recdes_p(in/out):
 *   call_fun(in/out):
 *
 * Note:
 */
static int
locator_cache_object_class (MOP mop, LC_COPYAREA_ONEOBJ * obj,
			    MOBJ * object_p, RECDES * recdes_p,
			    UNUSED_ARG bool * call_fun)
{
  int error_code = NO_ERROR;

  switch (obj->operation)
    {
    case LC_FETCH:
      if (mop->object != NULL && ws_get_lock (mop) > NULL_LOCK)
	{
	  /* already cached
	   */
	  assert (false);

	  break;
	}

      *object_p = tf_disk_to_class (&obj->oid, recdes_p);
      if (*object_p == NULL)
	{
	  error_code = ER_FAILED;
	  if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  break;
	}

      ws_cache (*object_p, mop, sm_Root_class_mop);
      break;

    default:
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "locator_cache: ** SYSTEM ERROR unknown"
		    " fetch state operation for object "
		    "= %d|%d|%d", obj->oid.volid,
		    obj->oid.pageid, obj->oid.slotid);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_GENERIC_ERROR, 1, "");
#endif /* RYE_DEBUG */
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * locator_cache_object_instance () -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mop(in/out):
 *   class_mop(in/out):
 *   hint_class_mop_p(in/out):
 *   hint_class_p(in/out):
 *   obj(in/out):
 *   object_p(in/out):
 *   recdes_p(in/out):
 *   call_fun(in/out):
 *
 * Note:
 */
static int
locator_cache_object_instance (MOP mop, MOP class_mop,
			       MOP * hint_class_mop_p, MOBJ * hint_class_p,
			       LC_COPYAREA_ONEOBJ * obj, MOBJ * object_p,
			       RECDES * recdes_p, UNUSED_ARG bool * call_fun)
{
  int error_code = NO_ERROR;
  int ignore;

  switch (obj->operation)
    {
    case LC_FETCH:
      if (mop->object != NULL && ws_get_lock (mop) > NULL_LOCK)
	{
	  /* already cached
	   */
	  assert (false);

	  break;
	}

      if (class_mop != *hint_class_mop_p)
	{
	  *hint_class_p = locator_fetch_class (class_mop, S_LOCK);
	  if (*hint_class_p == NULL)
	    {
	      error_code = ER_FAILED;
	      if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
		{
		  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
		}
	      break;
	    }
	  *hint_class_mop_p = class_mop;
	}

      /* Transform the object and cache it */
      *object_p = tf_disk_to_mem (*hint_class_p, recdes_p, &ignore);
      if (*object_p == NULL)
	{
	  /* an error should have been set */
	  error_code = ER_FAILED;
	  break;
	}

      ws_cache (*object_p, mop, class_mop);
      break;

    default:
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "locator_cache: ** SYSTEM ERROR unknown"
		    " fetch state operation for object "
		    "= %d|%d|%d", obj->oid.volid,
		    obj->oid.pageid, obj->oid.slotid);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_GENERIC_ERROR, 1, "");
#endif /* RYE_DEBUG */
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * locator_cache_have_object () -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mop_p(in/out):
 *   object_p(in/out):
 *   recdes_p(in/out):
 *   hint_class_mop_p(in/out):
 *   hint_class_p(in/out):
 *   call_fun(in/out):
 *   obj(in/out):
 *
 * Note:
 */
static int
locator_cache_have_object (MOP * mop_p, MOBJ * object_p, RECDES * recdes_p,
			   MOP * hint_class_mop_p, MOBJ * hint_class_p,
			   bool * call_fun, LC_COPYAREA_ONEOBJ * obj)
{
  MOP class_mop;		/* The class mop of object described by obj */
  int error_code = NO_ERROR;

  if (OID_IS_ROOTOID (&obj->class_oid))
    {
      /* Object is a class */
      *mop_p = ws_mop (&obj->oid, sm_Root_class_mop);
      if (*mop_p == NULL)
	{
#if defined(RYE_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"locator_cache: ** SYSTEM ERROR unable to "
			"create mop for object = %d|%d|%d",
			obj->oid.volid, obj->oid.pageid, obj->oid.slotid);
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_GENERIC_ERROR, 1, "");
#endif /* RYE_DEBUG */
	  error_code = ER_FAILED;
	  if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  return error_code;
	}

      /*
       * Don't need to transform the object, when the object is cached
       * and has a valid state (same chn)
       */
      if ((ws_find (*mop_p, object_p) != WS_FIND_MOP_DELETED
	   && (*object_p == NULL || !WS_ISDIRTY (*mop_p))))
	{
	  if (*object_p != NULL && ws_get_lock (*mop_p) > NULL_LOCK)
	    {
	      /* already cached
	       */
	      assert ((*mop_p)->object != NULL);
	    }
	  else
	    {
	      error_code = locator_cache_object_class (*mop_p, obj, object_p,
						       recdes_p, call_fun);
	      if (error_code != NO_ERROR)
		{
		  return error_code;
		}
	    }
	}
      /*
       * Assume that this class is going to be needed to transform other
       * objects in the copy area, so remember the class
       */
      *hint_class_mop_p = *mop_p;
      *hint_class_p = *object_p;
    }
  else
    {
      /* Object is an instance */
      class_mop = ws_mop (&obj->class_oid, sm_Root_class_mop);
      if (class_mop == NULL)
	{
	  error_code = ER_FAILED;
	  if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  return error_code;
	}
      *mop_p = ws_mop (&obj->oid, class_mop);
      if (*mop_p == NULL)
	{
#if defined(RYE_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"locator_cache: ** SYSTEM ERROR unable to "
			"create mop for object = %d|%d|%d",
			obj->oid.volid, obj->oid.pageid, obj->oid.slotid);
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_GENERIC_ERROR, 1, "");
#endif /* RYE_DEBUG */
	  error_code = ER_FAILED;
	  if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  return error_code;
	}

      /*
       * Don't need to transform the object, when the object is cached and
       * has a valid state (same chn)
       */
      if ((ws_find (*mop_p, object_p) != WS_FIND_MOP_DELETED
	   && (*object_p == NULL || !WS_ISDIRTY (*mop_p))))
	{
	  if (*object_p != NULL && ws_get_lock (*mop_p) > NULL_LOCK)
	    {
	      /* already cached
	       */
	      assert ((*mop_p)->object != NULL);
	    }
	  else
	    {
	      error_code = locator_cache_object_instance (*mop_p, class_mop,
							  hint_class_mop_p,
							  hint_class_p,
							  obj, object_p,
							  recdes_p, call_fun);
	      if (error_code != NO_ERROR)
		{
		  return error_code;
		}
	    }
	}
    }

  return error_code;
}

/*
 * locator_cache () - Cache several objects
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   copy_area(in): Copy area where objects are placed
 *   hint_class_mop(in): The class mop of probably the objects placed in copy_area
 *   hint_class(in): The class object of the hinted class
 *   fun(in): Function to call with mop, object, and args
 *   args(in): Arguments to be passed to function
 *
 * Note: Cache the objects stored on the given area.  If the
 *   caching fails for any object, then return error code.
 */
static int
locator_cache (LC_COPYAREA * copy_area, MOP hint_class_mop, MOBJ hint_class,
	       void (*fun) (MOP mop, MOBJ object, void *args), void *args)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in area  */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe an object in area */
  MOP mop;			/* Mop of the object described by obj */
  MOBJ object;			/* The object described by obj */
  RECDES recdes;		/* record descriptor for transformations */
  int i;
  bool call_fun;
  int error_code = NO_ERROR;

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (copy_area);
  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
  obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);

  if (hint_class_mop && hint_class == NULL)
    {
      hint_class_mop = NULL;
    }

  /* Cache one object at a time */
  for (i = 0; i < mobjs->num_objs; i++)
    {
      call_fun = true;
      obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
      LC_RECDES_TO_GET_ONEOBJ (copy_area, obj, &recdes);
      object = NULL;
      mop = NULL;

      if (recdes.length <= 0)
	{
	  assert (false);
	  continue;
	}

      error_code = locator_cache_have_object (&mop, &object, &recdes,
					      &hint_class_mop,
					      &hint_class, &call_fun, obj);
      if (error_code != NO_ERROR)
	{
	  if (error_code == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      return error_code;
	    }
	  continue;
	}

      /* Call the given function to do additional tasks */
      if (call_fun == true)
	{
	  if (fun != NULL)
	    {
	      (*fun) (mop, object, args);
	    }
	  else
	    {
	      if (mop != NULL && ws_class_mop (mop) == sm_Root_class_mop
		  && ws_get_lock (mop) == NULL_LOCK)
		{
		  ws_set_lock (mop, S_LOCK);
		}
	    }
	}
    }

  return error_code;
}

/*
 * locator_mflush_initialize () - Initialize the mflush area
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mflush(in): Structure which describes objects to flush
 *   class_mop(in): Mop of the class of last instance mflushed. This is a hint
 *                 to avoid a lot of class fetching during transformations
 *   class(in): The class object of the hinted class mop
 *   hfid(in): The heap of instances of the hinted class
 *   decache(in): true if objects must be decached after they are flushed
 *   isone_mflush(in): true if process stops after one set of objects
 *                 (i.e., one area) has been flushed to page buffer pool
 *                 (server).
 *
 * Note:Initialize the mflush structure which describes the objects in
 *              disk format to flush. A copy area of one page is defined to
 *              place the objects.
 */
static int
locator_mflush_initialize (LOCATOR_MFLUSH_CACHE * mflush, MOP class_mop,
			   MOBJ class_obj, HFID * hfid, bool decache,
			   bool isone_mflush)
{
  int error_code;

  assert (mflush != NULL);

  /* Guess that only one page is needed */
  mflush->copy_area = NULL;
  error_code = locator_mflush_reallocate_copy_area (mflush, DB_PAGESIZE);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  mflush->class_mop = class_mop;
  mflush->class_obj = class_obj;
  mflush->hfid = hfid;
  mflush->decache = decache;
  mflush->isone_mflush = isone_mflush;

  return error_code;
}

/*
 * locator_mflush_reset () - Reset the mflush area
 *
 * return: nothing
 *
 *   mflush(in): Structure which describes objects to flush
 *
 * Note: Reset the mflush structure which describes objects in disk
 *              format to flush to server. This function is used after a
 *              an flush area has been forced.
 */
static void
locator_mflush_reset (LOCATOR_MFLUSH_CACHE * mflush)
{
  assert (mflush != NULL);

  mflush->mop_toids = NULL;
  mflush->mop_tail_toid = NULL;
  mflush->mobjs->num_objs = 0;
  mflush->obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mflush->mobjs);
  LC_RECDES_IN_COPYAREA (mflush->copy_area, &mflush->recdes);
}

/*
 * locator_mflush_reallocate_copy_area () - Reallocate copy area and reset
 *                                          flush area
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mflush(in): Structure which describes objects to flush
 *   minsize(in): Minimal size of flushing copy area
 *
 * Note: Reset the mflush structure which describes objects in disk
 *              format to flush.
 */
static int
locator_mflush_reallocate_copy_area (LOCATOR_MFLUSH_CACHE * mflush,
				     int minsize)
{
  assert (mflush != NULL);

  if (mflush->copy_area != NULL)
    {
      locator_free_copy_area (mflush->copy_area);
    }

  mflush->copy_area = locator_allocate_copy_area_by_length (minsize);
  if (mflush->copy_area == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  mflush->mop_toids = NULL;
  mflush->mop_tail_toid = NULL;
  mflush->mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (mflush->copy_area);
  mflush->mobjs->num_objs = 0;
  mflush->obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mflush->mobjs);
  LC_RECDES_IN_COPYAREA (mflush->copy_area, &mflush->recdes);

  return NO_ERROR;
}

/*
 * locator_mflush_end - End the mflush area
 *
 * return: nothing
 *
 *   mflush(in): Structure which describes objects to flush
 *
 * Note: The mflush area is terminated. The copy_area is deallocated.
 */
static void
locator_mflush_end (LOCATOR_MFLUSH_CACHE * mflush)
{
  assert (mflush != NULL);

  if (mflush->copy_area != NULL)
    {
      locator_free_copy_area (mflush->copy_area);
    }
}

#if defined(RYE_DEBUG)
/*
 * locator_dump_mflush () - Dump the mflush structure
 *
 * return: nothing
 *
 *   mflush(in): Structure which describe objects to flush
 *
 * Note: Dump the mflush area
 *              This function is used for DEBUGGING PURPOSES.
 */
static void
locator_dump_mflush (FILE * out_fp, LOCATOR_MFLUSH_CACHE * mflush)
{
  fprintf (out_fp, "\n***Dumping mflush area ***\n");

  fprintf (out_fp,
	   "Num_objects = %d, Area = %p, Area Size = %d, "
	   "Available_area_at = %p, Available area size = %d\n",
	   mflush->mobjs->num_objs, (void *) (mflush->copy_area->mem),
	   (int) mflush->copy_area->length, mflush->recdes.data,
	   mflush->recdes.area_size);

  locator_dump_copy_area (out_fp, mflush->copy_area, false);

  if (mflush->recdes.area_size >
      ((mflush->copy_area->length - sizeof (LC_COPYAREA_MANYOBJS) -
	mflush->mobjs->num_objs * sizeof (LC_COPYAREA_ONEOBJ))))
    {
      fprintf (stdout, "Bad mflush structure");
    }
}
#endif /* RYE_DEBUG */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * locator_mflush_set_dirty () - Set object dirty/used when mflush failed
 *
 * return: nothing
 *
 *   mop(in): Mop of object to recover
 *   ignore_object(in): The object that has been chached
 *   ignore_argument(in):
 *
 * Note: Set the given object as dirty. This function is used when
 *              mflush failed
 */
static void
locator_mflush_set_dirty (MOP mop, UNUSED_ARG MOBJ ignore_object,
			  UNUSED_ARG void *ignore_argument)
{
  ws_dirty (mop);
}
#endif

/*
 * locator_repl_mflush_force () - Force the mflush area
 *
 * return:
 *
 *   mflush(in): Structure which describes to objects to flush
 *
 * Note: The repl objects placed on the mflush area are forced to the
 *              server (page buffer pool).
 */
static int
locator_repl_mflush_force (LOCATOR_MFLUSH_CACHE * mflush)
{
  LC_COPYAREA *reply_copy_area = NULL;
  int error_code = NO_ERROR;

  assert (mflush != NULL);

  /* Force the objects stored in area */
  if (mflush->mobjs->num_objs > 0)
    {
      error_code = locator_repl_force (mflush->copy_area, &reply_copy_area);

      /* If the force failed and the system is down.. finish */
      if (error_code == ER_LC_PARTIALLY_FAILED_TO_FLUSH)
	{
	  locator_repl_mflush_check_error (reply_copy_area);
	}
    }

  if (reply_copy_area != NULL)
    {
      locator_free_copy_area (reply_copy_area);
    }

  /* Now reset the flushing area... and continue flushing */
  locator_mflush_reset (mflush);

  return error_code;
}

/*
 * locator_repl_mflush_check_error () - save error info for later use
 *
 * return: void
 *
 *   reply_copyarea(in):
 */
static void
locator_repl_mflush_check_error (LC_COPYAREA * reply_copyarea)
{
  LC_COPYAREA_MANYOBJS *mobjs;
  LC_COPYAREA_ONEOBJ *obj;
  char *content_ptr;
  int i;

  if (reply_copyarea == NULL)
    {
      return;			/* give up */
    }

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (reply_copyarea);

  for (i = 0; i < mobjs->num_objs; i++)
    {
      obj = LC_FIND_ONEOBJ_PTR_IN_COPYAREA (mobjs, i);
      content_ptr = reply_copyarea->mem + obj->offset;

      ws_set_repl_error_into_error_link (obj, content_ptr);
    }

  return;
}

/*
 * locator_mflush_force () - Force the mflush area
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   mflush(in): Structure which describes to objects to flush
 *
 * Note: The disk objects placed on the mflush area are forced to the
 *              server (page buffer pool).
 */
static int
locator_mflush_force (LOCATOR_MFLUSH_CACHE * mflush)
{
  LOCATOR_MFLUSH_TEMP_OID *mop_toid;
  LOCATOR_MFLUSH_TEMP_OID *next_mop_toid;
  LC_COPYAREA_ONEOBJ *obj;	/* Describe one object in copy area */
  OID *oid;
  int error_code = NO_ERROR;
//  int i;
  int shard_groupid = NULL_GROUPID;
  RECDES recdes = RECDES_INITIALIZER;	/* Record descriptor for object      */

  assert (mflush != NULL);

  /* Force the objects stored in area */
  if (mflush->mobjs->num_objs >= 0)
    {
      /*
       * If there are objects with temporarily OIDs, make sure that they still
       * have temporarily OIDs. For those that do not have temporarily OIDs any
       * longer, change the flushing area to reflect the change. A situation
       * like this can happen when an object being placed in the flushing area
       * reference a new object which is already been placed in the flushing
       * area.
       */

      mop_toid = mflush->mop_toids;
      while (mop_toid != NULL)
	{
	  oid = ws_oid (mop_toid->mop);
	  if (!OID_ISTEMP (oid))
	    {
	      /* The OID of the object has already been assigned */
	      obj = LC_FIND_ONEOBJ_PTR_IN_COPYAREA (mflush->mobjs,
						    mop_toid->obj);

	      /* keep out shard table to enter
	       */
	      LC_RECDES_TO_GET_ONEOBJ (mflush->copy_area, obj, &recdes);
	      shard_groupid = or_grp_id (&recdes);
	      if (shard_groupid == GLOBAL_GROUPID
		  && shard_groupid == oid->groupid)
		{
		  ;		/* is not shard table; go ahead */
		}
	      else
		{
		  assert (false);
		  error_code = ER_FAILED;	/* TODO - set invalid shard error */
		  return error_code;
		}

	      COPY_OID (&obj->oid, oid);
	      /* TODO: see if you need to look for partitions here */
	      obj->operation = LC_FLUSH_UPDATE;
	      mop_toid->mop = NULL;
	    }
	  mop_toid = mop_toid->next;
	}

      /* Force the flushing area */
      error_code = locator_force (mflush->copy_area);

      assert (error_code != ER_LC_PARTIALLY_FAILED_TO_FLUSH);

      /* If the force failed and the system is down.. finish */
      if (error_code == ER_LK_UNILATERALLY_ABORTED
	  || (error_code != NO_ERROR && !BOOT_IS_CLIENT_RESTARTED ()))
	{
	  /* Free the memory ... and finish */
	  mop_toid = mflush->mop_toids;
	  while (mop_toid != NULL)
	    {
	      next_mop_toid = mop_toid->next;
	      /*
	       * Set mop to NULL before freeing the structure, so that it does not
	       * become a GC root for this mop..
	       */
	      mop_toid->mop = NULL;
	      free_and_init (mop_toid);
	      mop_toid = next_mop_toid;
	    }

	  return error_code;
	}

      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      /*
       * Notify the workspace module of OIDs for new objects. The MOPs must
       * reflect the new OID.. and not the temporarily OID
       */

      mop_toid = mflush->mop_toids;
      while (mop_toid != NULL)
	{
	  if (mop_toid->mop != NULL)
	    {
	      obj = LC_FIND_ONEOBJ_PTR_IN_COPYAREA (mflush->mobjs,
						    mop_toid->obj);
	      if (!OID_ISNULL (&obj->oid) && !(OID_ISTEMP (&obj->oid)))
		{
		  ws_perm_oid_and_class (mop_toid->mop, &obj->oid,
					 &obj->class_oid);
		}
	    }
	  next_mop_toid = mop_toid->next;
	  /*
	   * Set mop to NULL before freeing the structure, so that it does not
	   * become a GC root for this mop..
	   */
	  mop_toid->mop = NULL;
	  free_and_init (mop_toid);
	  mop_toid = next_mop_toid;
	}
    }

  /* Now reset the flushing area... and continue flushing */
  locator_mflush_reset (mflush);

  assert (error_code == NO_ERROR);

  return error_code;
}

/*
 * locator_class_to_disk () -
 *
 * return: error code
 *
 *   mflush(in):
 *   object(in):
 *   round_length_p(out):
 *   map_status(out):
 *
 * Note: Place the object on the current remaining flushing area. If the
 *       object does not fit. Force the area and try again
 */
static int
locator_class_to_disk (LOCATOR_MFLUSH_CACHE * mflush, MOBJ object,
		       int *round_length_p, WS_MAP_STATUS * map_status)
{
  int error_code = NO_ERROR;
  TF_STATUS tfstatus;
  bool isalone;
  bool enable_class_to_disk;

  tfstatus = tf_class_to_disk (object, &mflush->recdes);
  if (tfstatus != TF_SUCCESS)
    {
      if (mflush->mobjs->num_objs == 0)
	{
	  isalone = true;
	}
      else
	{
	  isalone = false;
	}

      enable_class_to_disk = false;
      if (tfstatus != TF_ERROR)
	{
	  if (isalone == true)
	    {
	      enable_class_to_disk = true;
	    }
	  else
	    {
	      error_code = locator_mflush_force (mflush);
	      if (error_code == NO_ERROR)
		{
		  enable_class_to_disk = true;
		}
	    }
	}

      if (enable_class_to_disk)
	{
	  /*
	   * Quit after the above force. If only one flush is
	   * desired and and we have flushed. stop
	   */
	  if (isalone == false && mflush->isone_mflush)
	    {			/* Don't do anything to current object */
	      *map_status = WS_MAP_STOP;
	      return ER_FAILED;
	    }

	  /* Try again */
	  do
	    {
	      if (tfstatus == TF_ERROR)
		{
		  /* There is an error of some sort. Stop.... */
		  *map_status = WS_MAP_FAIL;
		  return ER_FAILED;
		}
	      /*
	       * The object does not fit on flushing copy area.
	       * Increase the size of the flushing area,
	       * and try again.
	       */

	      *round_length_p = -mflush->recdes.length;

	      /*
	       * If this is the only object in the flushing copy
	       * area and does not fit even when the copy area seems
	       * to be large enough, increase the copy area by at
	       * least one page size.
	       * This is done only for security purposes, since the
	       * transformation class may not be given us the
	       * correct length, somehow.
	       */

	      if (*round_length_p <= mflush->copy_area->length
		  && isalone == true)
		{
		  *round_length_p = mflush->copy_area->length + DB_PAGESIZE;
		}

	      isalone = true;

	      if (*round_length_p > mflush->copy_area->length
		  && locator_mflush_reallocate_copy_area (mflush,
							  *round_length_p)
		  != NO_ERROR)
		{
		  /* Out of memory space */
		  *map_status = WS_MAP_FAIL;
		  return ER_FAILED;
		}

	      tfstatus = tf_class_to_disk (object, &mflush->recdes);
	    }
	  while (tfstatus != TF_SUCCESS);
	}
      else
	{
	  *map_status = WS_MAP_FAIL;
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * locator_mem_to_disk () -
 *
 * return: error code
 *
 *   mflush(in):
 *   object(in):
 *   round_length_p(out):
 *   map_status(out):
 *
 * Note: Place the object on the current remaining flushing area. If the
 *       object does not fit. Force the area and try again
 */
static int
locator_mem_to_disk (LOCATOR_MFLUSH_CACHE * mflush, MOBJ object,
		     int *round_length_p, WS_MAP_STATUS * map_status)
{
  int error_code = NO_ERROR;
  TF_STATUS tfstatus;
  bool isalone;
  bool enable_mem_to_disk;

  tfstatus = tf_mem_to_disk (mflush->class_mop, mflush->class_obj,
			     object, &mflush->recdes);
  if (tfstatus != TF_SUCCESS)
    {
      isalone = (mflush->mobjs->num_objs == 0) ? true : false;

      enable_mem_to_disk = false;
      if (tfstatus != TF_ERROR)
	{
	  if (isalone == true)
	    {
	      enable_mem_to_disk = true;
	    }
	  else
	    {
	      error_code = locator_mflush_force (mflush);
	      if (error_code == NO_ERROR)
		{
		  enable_mem_to_disk = true;
		}
	    }
	}

      if (enable_mem_to_disk)
	{
	  /*
	   * Quit after the above force. If only one flush is
	   * desired and and we have flushed. stop
	   */
	  if (isalone == false && mflush->isone_mflush)
	    {			/* Don't do anything to current object */
	      *map_status = WS_MAP_STOP;
	      return ER_FAILED;
	    }

	  /* Try again */
	  do
	    {
	      if (tfstatus == TF_ERROR)
		{
		  /* There is an error of some sort. Stop.... */
		  *map_status = WS_MAP_FAIL;
		  return ER_FAILED;
		}
	      /*
	       * The object does not fit on flushing copy area.
	       * Increase the size of the flushing area,
	       * and try again.
	       */

	      *round_length_p = -mflush->recdes.length;

	      /*
	       * If this is the only object in the flushing copy
	       * area and does not fit even when the copy area seems
	       * to be large enough, increase the copy area by at
	       * least one page size.
	       * This is done only for security purposes, since the
	       * transformation class may not be given us the
	       * correct length, somehow.
	       */

	      if (*round_length_p <= mflush->copy_area->length
		  && isalone == true)
		{
		  *round_length_p = mflush->copy_area->length + DB_PAGESIZE;
		}

	      isalone = true;

	      if (*round_length_p > mflush->copy_area->length
		  && locator_mflush_reallocate_copy_area (mflush,
							  *round_length_p) !=
		  NO_ERROR)
		{
		  /* Out of memory space */
		  *map_status = WS_MAP_FAIL;
		  return ER_FAILED;
		}

	      tfstatus = tf_mem_to_disk (mflush->class_mop, mflush->class_obj,
					 object, &mflush->recdes);
	    }
	  while (tfstatus != TF_SUCCESS);
	}
      else
	{
	  *map_status = WS_MAP_FAIL;
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * locator_mflush () - Prepare object for flushing
 *
 * return: either of WS_MAP_CONTINUE, WS_MAP_FAIL, or WS_MAP_STOP
 *
 *   mop(in): Memory Object pointer of object to flush
 *   mf(in): Multiple flush structure
 *
 * Note: Prepare the flushing of the object associated with the given
 *              mop. The object is not currently flushed, instead it is placed
 *              in a flushing area. When the flush area is full, then the area
 *              is forced to server (the page buffer pool).
 */
static int
locator_mflush (MOP mop, void *mf)
{
  int error_code = NO_ERROR;
  LOCATOR_MFLUSH_CACHE *mflush;	/* Structure which describes objects to flush */
  HFID *hfid;			/* Heap where the object is stored        */
  OID *oid;			/* Object identifier of object to flush   */
  MOBJ object;			/* The object to flush                    */
  MOP class_mop;		/* The mop of the class of object to flush */
  int round_length;		/* The length of the object in disk format
				 * rounded to alignments of size(int) */
  LC_COPYAREA_OPERATION operation;	/* Flush operation to be executed:
					 * insert, update, delete, etc. */
  int status;
  bool decache;
  WS_MAP_STATUS map_status;
  int wasted_length;

  mflush = (LOCATOR_MFLUSH_CACHE *) mf;

  /* Flush the instance only if it is dirty */
  if (!WS_ISDIRTY (mop))
    {
      return WS_MAP_CONTINUE;
    }

  oid = ws_oid (mop);

#if defined(RYE_DEBUG)
  if (OID_ISNULL (oid))
    {
      er_log_debug (ARG_FILE_LINE,
		    "locator_mflush: SYSTEM ERROR OID %d|%d|%d in"
		    " the workspace is a NULL_OID. It cannot be...\n",
		    oid->volid, oid->pageid, oid->slotid);
      return WS_MAP_FAIL;
    }
#endif /* RYE_DEBUG */

  class_mop = ws_class_mop (mop);
  if (class_mop == NULL || class_mop->object == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_HEAP_UNKNOWN_CLASS_OF_INSTANCE, 3, oid->volid, oid->pageid,
	      oid->slotid);
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "locator_mflush: SYSTEM ERROR Unable to flush.\n"
		    " Workspace does not know class_mop for object "
		    "OID %d|%d|%d\n", oid->volid, oid->pageid, oid->slotid);
#endif /* RYE_DEBUG */
      return WS_MAP_FAIL;
    }

  if (WS_ISDIRTY (class_mop) && class_mop != mop)
    {
      /*
       * Make sure that the class is not decached.. otherwise, we may have
       * problems
       */
      decache = mflush->decache;
      mflush->decache = false;
      if (WS_ISMARK_DELETED (class_mop))
	{
	  status = locator_mflush (class_mop, mf);
	  mflush->decache = decache;
	  return status;
	}
      else
	{
	  status = locator_mflush (class_mop, mf);
	  if (status != WS_MAP_CONTINUE)
	    {
	      mflush->decache = decache;
	      return status;
	    }
	  mflush->decache = decache;
	}
    }

  if (class_mop->ws_lock < X_LOCK)
    {
      /* place correct lock on class object, we might not have it yet */
      if (locator_fetch_class (class_mop, S_LOCK) == NULL)
	{
	  return WS_MAP_FAIL;
	}
    }

  if (ws_find (mop, &object) == WS_FIND_MOP_DELETED)
    {
      /* Delete operation */

      /*
       * if this is a new object (i.e., it has not been flushed), we only
       * need to decache the object
       */

      if (OID_ISTEMP (oid))
	{
	  return WS_MAP_CONTINUE;
	}

      operation = LC_FLUSH_DELETE;
      mflush->recdes.length = 0;

      /* Find the heap where the object is stored */
      /* Is the object a class ? */
      if (locator_is_root (class_mop))
	{
	  hfid = sm_Root_class_hfid;
	}
      else
	{
	  /* The object is an instance */
	  if (class_mop != mflush->class_mop)
	    {
	      /* Find the class for the current object */
	      mflush->class_obj = locator_fetch_class (class_mop, S_LOCK);
	      if (mflush->class_obj == NULL)
		{
		  mflush->class_mop = NULL;
		  return WS_MAP_FAIL;
		}

	      /* Cache this information for future flushes */
	      mflush->class_mop = class_mop;
	      mflush->hfid = sm_heap (mflush->class_obj);
	    }
	  hfid = mflush->hfid;
	}
    }
  else if (object == NULL)
    {
      /* We have the object. This is an insertion or an update operation */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "locator_mflush: SYSTEM ERROR, The MOP of"
		    " object OID %d|%d|%d is dirty, is not marked as\n"
		    " deleted and does not have the object\n",
		    oid->volid, oid->pageid, oid->slotid);
#endif /* RYE_DEBUG */
      return WS_MAP_FAIL;
    }
  else
    {
      if (OID_ISTEMP (oid))
	{
	  operation = LC_FLUSH_INSERT;
	}
      else
	{
	  operation = LC_FLUSH_UPDATE;
	}

      /* Is the object a class ? */
      if (locator_is_root (class_mop))
	{
	  if (locator_class_to_disk (mflush, object,
				     &round_length, &map_status) != NO_ERROR)
	    {
	      return map_status;
	    }
	  hfid = sm_Root_class_hfid;
	}
      else
	{
	  /* The object is an instance */
	  /* Find the class of the current instance */

	  if (class_mop != mflush->class_mop)
	    {
	      /* Find the class for the current object */
	      mflush->class_obj = locator_fetch_class (class_mop, S_LOCK);
	      if (mflush->class_obj == NULL)
		{
		  mflush->class_mop = NULL;
		  return WS_MAP_FAIL;
		}
	      /* Cache this information for future flushes */
	      mflush->class_mop = class_mop;
	      mflush->hfid = sm_heap (mflush->class_obj);
	    }

	  if (locator_mem_to_disk (mflush, object, &round_length,
				   &map_status) != NO_ERROR)
	    {
	      return map_status;
	    }
	  hfid = mflush->hfid;
	}

      /* check iff is not shard tabe
       */
      oid->groupid = or_grp_id (&(mflush->recdes));
      assert (oid->groupid == GLOBAL_GROUPID);
    }

  ws_clean (mop);

  /* Now update the mflush structure */

  if (operation == LC_FLUSH_INSERT)
    {
      /*
       * For new objects, make sure that its OID is still a temporary
       * one. If it is not, a permanent OID was assigned during the
       * transformation process, likely the object points to itself
       */
      if (OID_ISTEMP (ws_oid (mop)))
	{
	  LOCATOR_MFLUSH_TEMP_OID *mop_toid;

	  mop_toid = (LOCATOR_MFLUSH_TEMP_OID *) malloc (sizeof (*mop_toid));
	  if (mop_toid == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*mop_toid));
	      return WS_MAP_FAIL;
	    }

	  assert (mflush->mop_tail_toid != mop);

	  if (mflush->mop_tail_toid == NULL)
	    {
	      mflush->mop_tail_toid = mop;
	    }

	  mop_toid->mop = mop;
	  mop_toid->obj = mflush->mobjs->num_objs;
	  mop_toid->next = mflush->mop_toids;
	  mflush->mop_toids = mop_toid;
	}
      else
	{
	  operation = LC_FLUSH_UPDATE;

	  oid = ws_oid (mop);
	}
    }

  if (HFID_IS_NULL (hfid))
    {
      /*
       * There is not place to store the object. This is an error, the heap
       * should have been allocated when the object was created
       */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_NOHEAP, 3,
	      oid->volid, oid->pageid, oid->slotid);
      return WS_MAP_FAIL;
    }

  mflush->mobjs->num_objs++;
  mflush->obj->operation = operation;
  HFID_COPY (&mflush->obj->hfid, hfid);
  COPY_OID (&mflush->obj->class_oid, ws_oid (class_mop));
  COPY_OID (&mflush->obj->oid, oid);
  if (operation == LC_FLUSH_DELETE)
    {
      mflush->obj->length = -1;
      mflush->obj->offset = -1;
      round_length = 0;
    }
  else
    {
      round_length = mflush->recdes.length;
      mflush->obj->length = mflush->recdes.length;
      mflush->obj->offset =
	CAST_BUFLEN (mflush->recdes.data - mflush->copy_area->mem);
    }

  mflush->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (mflush->obj);

  /*
   * Round the length of the object, so that new placement of objects
   * start at alignment of sizeof(int)
   */

  wasted_length = DB_WASTED_ALIGN (round_length, MAX_ALIGNMENT);
#if !defined(NDEBUG)
  /* suppress valgrind UMW error */
  memset (mflush->recdes.data + round_length, 0,
	  MIN (wasted_length, mflush->recdes.area_size - round_length));
#endif
  round_length = round_length + wasted_length;
  mflush->recdes.data += round_length;
  mflush->recdes.area_size -= round_length + sizeof (*(mflush->obj));

  /* If there is not any more area, force the area */
  if (mflush->recdes.area_size <= 0)
    {
      /* Force the mflush area */
      error_code = locator_mflush_force (mflush);
      if (error_code != NO_ERROR)
	{
	  return WS_MAP_FAIL;
	}
    }

  return WS_MAP_CONTINUE;
}

/*
 * locator_repl_mflush () - place repl objects
 *                         into LOCATOR_MFLUSH_CACHE
 *
 * return: error code
 *
 *   mflush(in/out): copy area contents and descriptors
 */
static int
locator_repl_mflush (LOCATOR_MFLUSH_CACHE * mflush)
{
  int error = NO_ERROR;
  WS_REPL_OBJ *repl_obj;
  int required_length = 0;
  int key_length, round_length, wasted_length;
  char *ptr, *obj_start_p;

  while (true)
    {
      repl_obj = ws_get_repl_obj_from_list ();
      if (repl_obj == NULL)
	{
	  break;
	}

      required_length += OR_IDXKEY_ALIGNED_SIZE (&repl_obj->key);
      required_length += or_packed_string_length (repl_obj->class_name, NULL);
      if (repl_obj->operation != LC_FLUSH_DELETE)
	{
	  assert (repl_obj->recdes != NULL);
	  assert (repl_obj->recdes->data != NULL);
	  required_length += repl_obj->recdes->length + MAX_ALIGNMENT;
	}

      while (mflush->recdes.area_size < required_length)
	{
	  if (mflush->mobjs->num_objs == 0)
	    {
	      error =
		locator_mflush_reallocate_copy_area (mflush,
						     required_length +
						     DB_SIZEOF
						     (LC_COPYAREA_MANYOBJS));
	      if (error != NO_ERROR)
		{
		  return error;
		}
	    }
	  else
	    {
	      error = locator_repl_mflush_force (mflush);
	      if (error != NO_ERROR
		  && error != ER_LC_PARTIALLY_FAILED_TO_FLUSH)
		{
		  return error;
		}
	    }
	}

      /* put packed key_value and recdes in copy_area */

      /* put packed key_value first */
      obj_start_p = ptr = mflush->recdes.data;
      ptr = or_pack_db_idxkey (ptr, &repl_obj->key);
      if (ptr == NULL)
	{
	  assert (false);
	  error = ER_FAILED;	/* TODO - */
	  return error;
	}

      /* put packed class_name second */
      ptr = or_pack_string (ptr, repl_obj->class_name);
      if (ptr == NULL)
	{
	  assert (false);
	  error = ER_FAILED;	/* TODO - */

	  return error;
	}

      key_length = ptr - obj_start_p;

      mflush->recdes.data = ptr;

      if (repl_obj->operation == LC_FLUSH_DELETE)
	{
	  assert (repl_obj->recdes == NULL);

	  mflush->recdes.length = 0;
	}
      else
	{
	  assert (repl_obj->recdes->data != NULL);

	  memcpy (mflush->recdes.data, repl_obj->recdes->data,
		  repl_obj->recdes->length);
	  mflush->recdes.length = repl_obj->recdes->length;

	  assert (or_grp_id (&mflush->recdes) >= GLOBAL_GROUPID);
	}

      mflush->mobjs->num_objs++;
      mflush->obj->operation = repl_obj->operation;

      OID_SET_NULL (&mflush->obj->class_oid);
      HFID_SET_NULL (&mflush->obj->hfid);
      OID_SET_NULL (&mflush->obj->oid);

      mflush->obj->length = mflush->recdes.length + key_length;
      mflush->obj->offset =
	CAST_BUFLEN (obj_start_p - mflush->copy_area->mem);

      wasted_length = DB_WASTED_ALIGN (mflush->obj->length, MAX_ALIGNMENT);
#if !defined(NDEBUG)
      /* suppress valgrind UMW error */
      memset (obj_start_p + mflush->obj->length, 0,
	      MIN (wasted_length,
		   mflush->recdes.area_size - mflush->obj->length));
#endif
      round_length = mflush->obj->length + wasted_length;
      mflush->recdes.data = obj_start_p + round_length;
      mflush->recdes.area_size -= round_length + sizeof (*(mflush->obj));

      mflush->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (mflush->obj);
      ws_free_repl_obj (repl_obj);
    }

  return error;
}

/*
 * locator_flush_class () - Flush a dirty class
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   class_mop(in): Mop of class to flush
 *
 * Note: The class associated with the given mop is flushed to the page
 *              buffer pool (server). Other dirty objects may be flushed along
 *              with the class. Generally, a flushing area (page) of dirty
 *              objects is sent to the server.
 */
int
locator_flush_class (MOP class_mop)
{
  LOCATOR_MFLUSH_CACHE mflush;	/* Structure which describes objects to
				 * flush */
  MOBJ class_obj;
  int error_code = NO_ERROR;
  int map_status = WS_MAP_FAIL;

  if (class_mop == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (WS_ISDIRTY (class_mop)
      && (ws_find (class_mop, &class_obj) == WS_FIND_MOP_DELETED
	  || class_obj != NULL))
    {
      /*
       * Prepare the area for flushing... only one force area
       * Flush class and preflush other dirty objects to the flushing area
       */
      error_code = locator_mflush_initialize (&mflush, NULL, NULL, NULL,
					      DONT_DECACHE, ONE_MFLUSH);
      if (error_code == NO_ERROR)
	{
	  /* current class mop flush */
	  map_status = locator_mflush (class_mop, &mflush);
	  if (map_status == WS_MAP_CONTINUE)
	    {
	      map_status = ws_map_dirty (locator_mflush, &mflush);
	      if (map_status == WS_MAP_SUCCESS)
		{
		  if (mflush.mobjs->num_objs != 0)
		    {
		      error_code = locator_mflush_force (&mflush);
		    }
		}
	    }
	  if (map_status == WS_MAP_FAIL)
	    {
	      error_code = ER_FAILED;
	    }
	  locator_mflush_end (&mflush);
	}
    }

  if (error_code != NO_ERROR && er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
    }

  return error_code;
}

/*
 * locator_flush_all_instances () - Flush dirty instances of a class
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   class_mop(in): The class mop of the instances to flush
 *   decache(in): true, if instances must be decached after they are
 *                 flushed.
 *
 * Note: Flush all dirty instances of the class associated with the
 *              given class_mop to the page buffer pool (server). In addition,
 *              if the value of decache is true, all instances (whether or
 *              not they are dirty) of the class are decached.
 */
int
locator_flush_all_instances (MOP class_mop, bool decache)
{
  LOCATOR_MFLUSH_CACHE mflush;	/* Structure which describes objects to
				 * flush */
  MOBJ class_obj;		/* The class object */
  HFID *hfid;			/* Heap where the instances of class_mop
				 * are stored */
  int error_code = NO_ERROR;
  int map_status;
  DB_OBJLIST class_list;
  DB_OBJLIST *obj = NULL;

  if (class_mop == NULL)
    {
      return ER_FAILED;
    }

  class_obj = locator_fetch_class (class_mop, S_LOCK);
  if (class_obj == NULL)
    {
      return ER_FAILED;
    }

  class_list.op = class_mop;
  class_list.next = NULL;

  hfid = sm_heap (class_obj);
  error_code = locator_mflush_initialize (&mflush, class_mop, class_obj,
					  hfid, decache, MANY_MFLUSHES);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* Iterate through classes and flush only those which have been loaded
   * into the workspace.
   */
  for (obj = &class_list; obj != NULL && error_code == NO_ERROR;
       obj = obj->next)
    {
      if (obj->op == NULL || obj->op->object == NULL)
	{
	  /* This class is not in the workspace, skip it */
	  continue;
	}

      if (decache)
	{
	  /* decache all instances of this class */
	  map_status = ws_map_class (obj->op, locator_mflush, &mflush);
	}
      else
	{
	  /* flush all dirty instances of this class */
	  map_status = ws_map_class_dirty (obj->op, locator_mflush, &mflush);
	}

      if (map_status == WS_MAP_FAIL)
	{
	  error_code = ER_FAILED;
	}
    }

  if (mflush.mobjs->num_objs != 0)
    {
      error_code = locator_mflush_force (&mflush);
    }

  locator_mflush_end (&mflush);

  if (error_code != NO_ERROR && error_code != ER_LC_PARTIALLY_FAILED_TO_FLUSH)
    {
      return error_code;
    }

  if (decache)
    {
      for (obj = &class_list; obj != NULL; obj = obj->next)
	{
	  ws_disconnect_deleted_instances (obj->op);
	}
    }

  return error_code;
}

/*
 * locator_all_flush () - Flush all dirty objects
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * Note: Form to flush all dirty objects to the page buffer pool (server).
 */
int
locator_all_flush (void)
{
  LOCATOR_MFLUSH_CACHE mflush;	/* Structure which describes objects to
				 * flush */
  int error_code;
  int map_status;

  /* flush all dirty objects */
  error_code = locator_mflush_initialize (&mflush, NULL, NULL, NULL,
					  DONT_DECACHE, MANY_MFLUSHES);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  map_status = ws_map_dirty (locator_mflush, &mflush);
  if (map_status == WS_MAP_FAIL)
    {
      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  error_code = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
	}
    }
  else if (map_status == WS_MAP_SUCCESS)
    {
      if (mflush.mobjs->num_objs != 0)
	{
	  error_code = locator_mflush_force (&mflush);
	}
    }

  locator_mflush_end (&mflush);

  return error_code;
}

/*
 * locator_repl_flush_all () - flush all repl objects
 *
 * return: error code
 *
 * num_error(out): number of error
 */
int
locator_repl_flush_all (int *num_error)
{
  LOCATOR_MFLUSH_CACHE mflush;
  int error;
  bool continued_on_error = false;

  assert (num_error != NULL);

  *num_error = 0;

  error = locator_mflush_initialize (&mflush, NULL, NULL, NULL,
				     DONT_DECACHE, MANY_MFLUSHES);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = locator_repl_mflush (&mflush);
  if (error == ER_LC_PARTIALLY_FAILED_TO_FLUSH)
    {
      continued_on_error = true;
    }
  else if (error != NO_ERROR)
    {
      return error;
    }

  error = locator_repl_mflush_force (&mflush);
  if (error == NO_ERROR && continued_on_error == true)
    {
      error = ER_LC_PARTIALLY_FAILED_TO_FLUSH;
    }

  locator_mflush_end (&mflush);

  *num_error = ws_clear_all_repl_errors_of_error_link ();

  return error;
}

/*
 * locator_add_root () - Insert root
 *
 * return:MOP
 *
 *   root_oid(in): Root oid
 *   class_root(in): Root object
 *
 * Note: Add the root class. Used only when the database is created.
 */
MOP
locator_add_root (OID * root_oid, MOBJ class_root)
{
  MOP root_mop;			/* Mop of the root */

  /*
   * Insert the root class, set it dirty and cache the lock.. we need to cache
   * the lock since it was not acquired directly. Actually, it has not been
   * requested. It is set when the root class is flushed
   */

  /* Find a mop */
  root_mop = ws_mop (root_oid, NULL);
  if (root_mop == NULL)
    {
      return NULL;
    }

  ws_cache (class_root, root_mop, root_mop);
  ws_dirty (root_mop);
  ws_set_lock (root_mop, X_LOCK);

  sm_Root_class_mop = root_mop;
  oid_Root_class_oid = ws_oid (root_mop);

  /* Reserve the class name */
  if (locator_reserve_class_name (ROOTCLASS_NAME, oid_Root_class_oid)
      != LC_CLASSNAME_RESERVED || locator_flush_class (root_mop) != NO_ERROR)
    {
      root_mop = NULL;
    }

  sm_mark_system_class (sm_Root_class_mop, 1);

  return root_mop;
}

/*
 * locator_add_class () - Insert a class
 *
 * return: MOP
 *
 *   class(in): Class object to add onto the database
 *   classname(in): Name of the class
 *
 * Note: Add a class onto the database. Neither the permanent OID for
 *              the newly created class nor a lock on the class are assigned
 *              at this moment. Both the lock and its OID are acquired when
 *              the class is flushed to the server (page buffer pool)
 *              Only an IX lock is acquired on the root class.
 */
MOP
locator_add_class (MOBJ class_obj, const char *classname)
{
  OID class_temp_oid;		/* A temporarily OID for the newly created
				 * class
				 */
  MOP class_mop;		/* The Mop of the newly created class */
  LC_FIND_CLASSNAME reserved;
  LOCK lock;

  if (classname == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");

      return NULL;
    }

  class_mop = ws_find_class (classname);
  if (class_mop != NULL && ws_get_lock (class_mop) != NULL_LOCK)
    {
      if (!WS_ISMARK_DELETED (class_mop))
	{
	  /* The class already exist.. since it is cached */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_CLASSNAME_EXIST, 1,
		  classname);
	  return NULL;
	}

      /*
       * Flush the deleted class so we do not have problems with the
       * classname to oid entry during commit
       */
      if (locator_flush_class (class_mop) != NO_ERROR)
	{
	  return NULL;
	}
    }

  /*
   * Assign a temporarily OID. If the assigned OID is NULL, we need to flush to
   * recycle the temporarily OIDs.
   */

  OID_ASSIGN_TEMPOID (&class_temp_oid);
  if (OID_ISNULL (&class_temp_oid))
    {
      if (locator_all_flush () != NO_ERROR)
	{
	  return NULL;
	}

      OID_INIT_TEMPID ();
      OID_ASSIGN_TEMPOID (&class_temp_oid);
    }

  /* Reserve the name for the class */

  reserved = locator_reserve_class_name (classname, &class_temp_oid);
  if (reserved != LC_CLASSNAME_RESERVED)
    {
      if (reserved == LC_CLASSNAME_EXIST)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_CLASSNAME_EXIST, 1,
		  classname);
	}
      return NULL;
    }


  /*
   * SCH_M_LOCK and IX_LOCK locks were indirectly acquired on the newly
   * created class and the root class using the locator_reserve_class_name
   * function.
   */

  /*
   * If there is any lock on the sm_Root_class_mop, its lock is converted to
   * reflect the IX_LOCK. Otherwise, the root class is fetched to synchronize
   * the root
   */

  lock = ws_get_lock (sm_Root_class_mop);
  if (lock != NULL_LOCK)
    {
      assert (lock >= NULL_LOCK);
      lock = lock_Conv[lock][S_LOCK];
      assert (lock != NA_LOCK);

      ws_set_lock (sm_Root_class_mop, lock);
    }
  else
    {
      /* Fetch the rootclass object */
      if (locator_lock (sm_Root_class_mop, S_LOCK) != NO_ERROR)
	{
	  /* Unable to lock the Rootclass. Undo the reserve of classname */
	  (void) locator_delete_class_name (classname);
	  return NULL;
	}
    }

  class_mop =
    ws_cache_with_oid (class_obj, &class_temp_oid, sm_Root_class_mop);
  if (class_mop != NULL)
    {
      ws_dirty (class_mop);
      ws_set_lock (class_mop, X_LOCK);
    }

  return class_mop;
}

/*
 * locator_create_heap_if_needed () - Make sure that a heap has been assigned
 *
 * return: classobject or NULL (in case of error)
 *
 *   class_mop(in):
 *
 * Note: If a heap has not been assigned to store the instances of the
 *       given class, one is assigned at this moment.
 */
MOBJ
locator_create_heap_if_needed (MOP class_mop)
{
  MOBJ class_obj;		/* The class object */
  HFID *hfid;			/* Heap where instance will be placed */

  /*
   * Get the class for the instance.
   * Assume that we are updating, inserting, deleting instances
   */

  class_obj = locator_fetch_class (class_mop, S_LOCK);
  if (class_obj == NULL)
    {
      return NULL;
    }

  /*
   * Make sure that there is a heap for the instance. We cannot postpone
   * the creation of the heap since the class must be updated
   */

  hfid = sm_heap (class_obj);
  if (HFID_IS_NULL (hfid))
    {
      OID *oid;

      /* Need to update the class, must fetch it again with write purpose */
      class_obj = locator_fetch_class (class_mop, X_LOCK);
      if (class_obj == NULL)
	{
	  return NULL;
	}

      oid = ws_oid (class_mop);
      if (OID_ISTEMP (oid))
	{
	  if (locator_flush_class (class_mop) != NO_ERROR)
	    {
	      return NULL;
	    }
	  oid = ws_oid (class_mop);
	}

      if (heap_create (hfid, oid) != NO_ERROR)
	{
	  return NULL;
	}

      ws_dirty (class_mop);
    }

  return class_obj;
}

/*
 * locator_has_heap () - Make sure that a heap has been assigned
 *
 * return: classobject or NULL (in case of error)
 *
 *   class_mop(in):
 *
 * Note: If a heap has not been assigned to store the instances of the
 *       given class, one is assigned at this moment.
 *       If the class is a reusable OID class call
 *       locator_create_heap_if_needed () instead of locator_has_heap ()
 */
MOBJ
locator_has_heap (MOP class_mop)
{
  return locator_create_heap_if_needed (class_mop);
}

/*
 * locator_add_instance () - Insert an instance
 *
 * return: MOP
 *
 *   instance(in): Instance object to add
 *   class_mop(in): Mop of class which will hold the instance
 *
 * Note: Add a new object as an instance of the class associated with
 *              the given class_mop. Neither the permanent OID for the new
 *              instance nor a lock on the new instance are assigned at this
 *              moment. The lock and OID are actually acquired when the
 *              instance is flushed to the page buffer pool (server).
 *              Only an IX lock is acquired on the class.
 */
MOP
locator_add_instance (MOBJ instance, MOP class_mop)
{
  MOP mop;			/* Mop of newly created instance */
  OID temp_oid;			/* A temporarily OID for the newly created
				 * instance */

  /*
   * Make sure that there is a heap for the instance. We cannot postpone
   * the creation of the heap since the class must be updated
   */

  if (locator_create_heap_if_needed (class_mop) == NULL)
    {
      return NULL;
    }

  /*
   * Assign a temporarily OID. If the assigned OID is NULL, we need to flush to
   * recycle the temporarily OIDs.
   */

  OID_ASSIGN_TEMPOID (&temp_oid);
  if (OID_ISNULL (&temp_oid))
    {
      if (locator_all_flush () != NO_ERROR)
	{
	  return NULL;
	}

      OID_INIT_TEMPID ();
      OID_ASSIGN_TEMPOID (&temp_oid);
    }

  /*
   * Insert the instance, set it dirty and cache the lock.. we need to cache
   * the lock since it was not acquired directly. Actually, it has not been
   * requested. It is set when the instance is flushed
   */

  mop = ws_cache_with_oid (instance, &temp_oid, class_mop);
  if (mop != NULL)
    {
      ws_dirty (mop);
      ws_set_lock (mop, X_LOCK);
    }

  return mop;
}

/*
 * locator_remove_class () - Remove a class
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   class_mop(in): Mop of class to delete
 *
 * Note: Delete a class. The deletion of the heap (i.e., all its
 *              instances), and indices are deferred after commit time.
 */
int
locator_remove_class (MOP class_mop)
{
  MOBJ class_obj;		/* The class object */
  const char *classname;	/* The classname */
  HFID *insts_hfid;		/* Heap of instances of the class */
  int error_code = NO_ERROR;

  class_obj = locator_fetch_class (class_mop, X_LOCK);
  if (class_obj == NULL)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /* What should happen to the heap */
  insts_hfid = sm_heap (class_obj);
  if (insts_hfid->vfid.fileid != NULL_FILEID)
    {
      error_code = heap_destroy (insts_hfid);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  /* Delete the class name */
  classname = sm_classobj_name (class_obj);
  if (locator_delete_class_name (classname) == LC_CLASSNAME_DELETED
      || BOOT_IS_CLIENT_RESTARTED ())
    {
      ws_dirty (class_mop);
      ws_mark_deleted (class_mop);
      /*
       * Flush the deleted class so we do not have problems with the classname
       * to oid entry at a later point.
       */
      error_code = locator_flush_class (class_mop);
    }

error:
  return error_code;
}

/*
 * locator_remove_instance () - Remove an instance
 *
 * return: nothing
 *
 *   mop(in): Mop of instance to delete
 *
 * Note: Delete an instance. The instance is marked as deleted in the
 *              workspace. The deletion of the instance on disk is deferred
 *              until commit time.
 */
void
locator_remove_instance (MOP mop)
{
  ws_mark_deleted (mop);
}

/*
 * locator_update_class () - Prepare a class for update
 *
 * return: MOBJ
 *
 *   mop(in): Mop of class that it is going to be updated
 *
 * Note: Prepare a class for update. The class is fetched for exclusive
 *              mode and it is set dirty. Note that it is very important that
 *              the class is set dirty before it is actually updated,
 *              otherwise, the workspace may remain with a corrupted class if
 *              a failure happens.
 *
 *              This function should be called before the class is actually
 *              updated.
 */
MOBJ
locator_update_class (MOP mop)
{
  MOBJ class_obj;		/* The class object */

  assert (ws_get_lock (mop) == X_LOCK || ws_get_lock (mop) == U_LOCK);

  class_obj = locator_fetch_class (mop, mop->ws_lock);
  if (class_obj != NULL)
    {
      ws_dirty (mop);
    }

  return class_obj;
}

/*
 * locator_prepare_rename_class () - Prepare a class for class rename
 *
 * return: The class or NULL
 *
 *   class_mop(in): Mop of class that it is going to be renamed
 *   old_classname(in): Oldname of class
 *   new_classname(in): Newname of class
 *
 * Note: Prepare a class for a modification of its name from oldname to
 *              newname. If the new_classname already exist, the value
 *              LC_CLASSNAME_EXIST is returned. The value
 *              LC_CLASSNAME_RESERVED_RENAME is returned when the operation
 *              was successful. If the old_classname was previously removed,
 *              the value LC_CLASSNAME_DELETED is returned. If the class is
 *              not available or it does not exist, LC_CLASSNAME_ERROR is
 *              returned.
 *              The class is fetched in exclusive mode and it is set dirty for
 *              its update. Note that it is very important that the class is
 *              set dirty before it is actually updated, otherwise, the
 *              workspace may remain with a corrupted class if a failure
 *              happens.
 */
MOBJ
locator_prepare_rename_class (MOP class_mop, const char *old_classname,
			      const char *new_classname)
{
  MOBJ class_obj;
  MOP tmp_class_mop;
  LC_FIND_CLASSNAME renamed;

  /* Do we know about new name ? */
  if (new_classname == NULL)
    {
      return NULL;
    }

  tmp_class_mop = ws_find_class (new_classname);
  if (new_classname != NULL
      && tmp_class_mop != NULL
      && tmp_class_mop != class_mop
      && ws_get_lock (tmp_class_mop) != NULL_LOCK
      && !WS_ISMARK_DELETED (tmp_class_mop))
    {
      /* The class already exist.. since it is cached */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_CLASSNAME_EXIST, 1,
	      new_classname);
      return NULL;
    }

  class_obj = locator_fetch_class (class_mop, X_LOCK);
  if (class_obj != NULL)
    {
      renamed = locator_rename_class_name (old_classname, new_classname,
					   ws_oid (class_mop));
      if (renamed != LC_CLASSNAME_RESERVED_RENAME)
	{
	  if (renamed == LC_CLASSNAME_EXIST)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_CLASSNAME_EXIST,
		      1, new_classname);
	    }
	  return NULL;
	}

      /* Invalidate old classname to MOP entry */
      ws_drop_classname (class_obj);
      ws_add_classname (class_obj, class_mop, new_classname);
      ws_dirty (class_mop);
    }

  return class_obj;
}

/*
 * locator_update_instance () -  Prepare an instance for update
 *
 * return: MOBJ
 *
 *   mop(in): Mop of object that it is going to be updated
 *
 * Note:Prepare an instance for update. The instance is fetched for
 *              exclusive mode and it is set dirty. Note that it is very
 *              important the the instance is set dirty before it is actually
 *              updated, otherwise, the workspace may remain with a corrupted
 *              instance if a failure happens.
 *
 *              This function should be called before the instance is actually
 *              updated.
 */
MOBJ
locator_update_instance (MOP mop)
{
  MOBJ object;			/* The instance object */

  object = locator_fetch_instance (mop);
  if (object != NULL)
    {
      ws_dirty (mop);
    }

  return object;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * locator_update_tree_classes () - Prepare a tree of classes for update
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   classes_mop_set(in): An array of Mops
 *   num_classes(in): Number of classes
 *
 * Note: Prepare a tree of classes (usually a class and its subclasses)
 *              for updates. This statement must be executed during schema
 *              changes that will affect a tree of classes.
 *              This function should be called before the classes are actually
 *              updated.
 */
int
locator_update_tree_classes (MOP * classes_mop_set, int num_classes)
{
  return locator_lock_set (num_classes, classes_mop_set, X_LOCK, SCH_M_LOCK,
			   true);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * locator_assign_permanent_oid () -  Assign a permanent_oid
 *
 * return:  OID *
 *
 *   mop(in): Mop of object with temporal oid
 *
 * Note: Assign a permanent oid to the object associated with the given mop.
 * This function is needed during flushing of new objects with circular
 * dependencies (For example, an object points to itself). Otherwise, OIDs for
 * new objects are assigned automatically when the objects are placed
 * on the heap.
 */
OID *
locator_assign_permanent_oid (MOP mop)
{
  MOBJ object;			/* The object */
  int expected_length;		/* Expected length of disk object */
  OID perm_oid;			/* Permanent OID of object. Assigned as a side
				 * effect */
  MOP class_mop;		/* The class mop */
  MOBJ class_obj;		/* The class object */
  const char *name;
  HFID *hfid;			/* Heap where the object is going to be stored */

  /* Find the expected length of the object */

  class_mop = ws_class_mop (mop);
  if (class_mop == NULL
      || (class_obj = locator_fetch_class (class_mop, S_LOCK)) == NULL)
    {
      /* Could not assign a permanent OID */
      return NULL;
    }

  /* Get the object */
  if (ws_find (mop, &object) == WS_FIND_MOP_DELETED)
    {
      OID *oid;

      oid = ws_oid (mop);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
	      oid->volid, oid->pageid, oid->slotid);
      return NULL;
    }

  /* Get an approximation for the expected size */
  if (object != NULL && class_obj != NULL)
    {
      expected_length = tf_object_size (class_obj, object);
      if (expected_length < (int) sizeof (OID))
	{
	  expected_length = (int) sizeof (OID);
	}
    }
  else
    {
      expected_length = (int) sizeof (OID);
    }

  /* Find the heap where the object will be stored */

  name = NULL;
  if (locator_is_root (class_mop))
    {
      /* Object is a class */
      hfid = sm_Root_class_hfid;
      if (object != NULL)
	{
	  name = sm_classobj_name (object);
	}
    }
  else
    {
      hfid = sm_heap (class_obj);
    }

  /* Assign an address */

  if (locator_assign_oid (hfid, &perm_oid, expected_length,
			  ws_oid (class_mop), name) != NO_ERROR)
    {
      if (er_errid () == ER_LK_UNILATERALLY_ABORTED)
	{
	  (void) tran_abort_only_client (false);
	}

      return NULL;
    }

  /* Reset the OID of the mop */
  ws_perm_oid (mop, &perm_oid);

  return ws_oid (mop);
}

/*
 * locator_cache_lock_lockhint_classes :
 *
 * return:
 *
 *    lockhint(in):
 *
 * NOTE : Cache the lock for the desired classes.
 *      We need to do this since we don't know if the classes were received in
 *      the fetch areas. That is, they may have not been sent since the cached
 *      class is upto date.
 *
 */
static void
locator_cache_lock_lockhint_classes (LC_LOCKHINT * lockhint)
{
  int i;
  MOP class_mop = NULL;		/* The mop of a class                       */
  MOBJ class_obj;		/* The class object of above mop            */
  LOCK lock;			/* The lock granted to above class          */
  WS_FIND_MOP_STATUS status;

  for (i = 0; i < lockhint->num_classes; i++)
    {
      if (!OID_ISNULL (&lockhint->classes[i].oid))
	{
	  class_mop = ws_mop (&lockhint->classes[i].oid, sm_Root_class_mop);
	  if (class_mop != NULL)
	    {
	      status = ws_find (class_mop, &class_obj);
	      if (status != WS_FIND_MOP_DELETED && class_obj != NULL)
		{
		  lock = ws_get_lock (class_mop);
		  assert (lockhint->classes[i].lock >= NULL_LOCK
			  && lock >= NULL_LOCK);
		  lock = lock_Conv[lockhint->classes[i].lock][lock];
		  assert (lock != NA_LOCK);

		  ws_set_lock (class_mop, lock);
		}
	    }
	}
    }
}

/*
 * locator_lockhint_classes () - The given classes should be prelocked and prefetched
 *                          since they are likely to be needed
 *
 * return: LC_FIND_CLASSNAME
 *                        (either of LC_CLASSNAME_EXIST,
 *                                   LC_CLASSNAME_DELETED,
 *                                   LC_CLASSNAME_ERROR)
 *
 *   num_classes(in): Number of needed classes
 *   many_classnames(in): Name of the classes
 *   many_locks(in): The desired lock for each class
 *   quit_on_errors(in): Wheater to continue finding the classes in case of an
 *                     error, such as a class does not exist or locks on some
 *                     of the classes may not be granted.
 *
 */
LC_FIND_CLASSNAME
locator_lockhint_classes (int num_classes, const char **many_classnames,
			  LOCK * many_locks, int quit_on_errors)
{
  MOP class_mop = NULL;		/* The mop of a class                       */
  MOBJ class_obj = NULL;	/* The class object of above mop            */
  LOCK current_lock;		/* The lock granted to above class          */
  LC_LOCKHINT *lockhint = NULL;	/* Description of hinted classes to
				 * lock and fetch */
  LC_COPYAREA *fetch_area;	/* Area where objects are received         */
  LC_FIND_CLASSNAME all_found;	/* Result of search                        */
  bool need_call_server;	/* Do we need to invoke the server to find
				 * the classes ? */
  bool need_flush;
  int error_code = NO_ERROR;
  int i;
  OID *guessmany_class_oids = NULL;
  LOCK conv_lock;

  all_found = LC_CLASSNAME_EXIST;
  need_call_server = need_flush = false;

  /*
   * Check if we need to call the server
   */

  for (i = 0;
       i < num_classes && (need_call_server == false || need_flush == false);
       i++)
    {
      if (many_classnames[i])
	{
	  /*
	   * If we go to the server, let us flush any new class (temp OID or
	   * small cache coherance number) or a class that has been deleted
	   */
	  class_mop = ws_find_class (many_classnames[i]);
	  if (class_mop != NULL
	      && WS_ISDIRTY (class_mop)
	      && (OID_ISTEMP (ws_oid (class_mop))
		  || ws_find (class_mop, &class_obj) == WS_FIND_MOP_DELETED
		  || class_obj != NULL))
	    {
#if defined(CS_MODE)
	      assert (false);
#endif
	      need_flush = true;
	    }

	  if (need_call_server == true)
	    {
	      continue;
	    }

	  /*
	   * Check if the classname to OID entry is cached. Trust the cache only
	   * if there is a lock on the class
	   */

	  if (class_mop != NULL)
	    {
	      current_lock = ws_get_lock (class_mop);
	      assert (many_locks[i] >= NULL_LOCK
		      && current_lock >= NULL_LOCK);
	      conv_lock = lock_Conv[many_locks[i]][current_lock];
	      assert (conv_lock != NA_LOCK);
	    }

	  if (class_mop == NULL || current_lock == NULL_LOCK
	      || current_lock != conv_lock)
	    {
	      need_call_server = true;
	      continue;
	    }
	}
    }

  /*
   * Do we Need to find out the classnames to oids in the server?
   */

  if (!need_call_server)
    {
      goto error;
    }

  guessmany_class_oids = (OID *)
    malloc (sizeof (*guessmany_class_oids) * num_classes);
  if (guessmany_class_oids == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (*guessmany_class_oids) * num_classes);
      return LC_CLASSNAME_ERROR;
    }

  for (i = 0; i < num_classes; i++)
    {
      if (many_classnames[i]
	  && (class_mop = ws_find_class (many_classnames[i])) != NULL)
	{
	  /*
	   * Flush the class when the class has never been flushed and/or
	   * the class has been deleted.
	   */
	  if (need_flush == true)
	    {
	      /*
	       * May be, we should flush in a set (ala mflush)
	       */
	      if (WS_ISDIRTY (class_mop)
		  && (OID_ISTEMP (ws_oid (class_mop))
		      || ws_find (class_mop,
				  &class_obj) == WS_FIND_MOP_DELETED
		      || class_obj != NULL))
		{
#if defined(CS_MODE)
		  assert (false);
#endif
		  (void) locator_flush_class (class_mop);
		}
	    }

	  if (guessmany_class_oids != NULL)
	    {
	      if (ws_find (class_mop, &class_obj) != WS_FIND_MOP_DELETED
		  && class_obj != NULL)
		{
		  /*
		   * The class is cached
		   */
		  COPY_OID (&guessmany_class_oids[i], ws_oid (class_mop));
		}
	      else
		{
		  OID_SET_NULL (&guessmany_class_oids[i]);
		}
	    }
	}
      else
	{
	  if (guessmany_class_oids != NULL)
	    {
	      OID_SET_NULL (&guessmany_class_oids[i]);
	    }
	}
    }

  all_found = locator_find_lockhint_class_oids (num_classes, many_classnames,
						many_locks,
						guessmany_class_oids,
						quit_on_errors, &lockhint,
						&fetch_area);

  if (guessmany_class_oids != NULL)
    {
      free_and_init (guessmany_class_oids);
    }

  if (lockhint != NULL
      && lockhint->num_classes > lockhint->num_classes_processed)
    {
      /*
       * Rest the cache coherence numbers to avoid receiving classes with the
       * right state (chn) in the workspace.
       * We could have started with the number of classes processed, however,
       * we start from zero to set to NULL_OID any class that are deleted in
       * the workspace.
       */
      for (i = 0; i < lockhint->num_classes; i++)
	{
	  if (!OID_ISNULL (&lockhint->classes[i].oid)
	      && ((class_mop = ws_mop (&lockhint->classes[i].oid,
				       sm_Root_class_mop)) == NULL
		  || ws_find (class_mop, &class_obj) == WS_FIND_MOP_DELETED))
	    {
	      OID_SET_NULL (&lockhint->classes[i].oid);
	    }
	}
    }

  /*
   * If we received any classes, cache them
   */

  if (fetch_area != NULL)
    {
      /* Cache the classes that were brought from the server */
      if (locator_cache (fetch_area, sm_Root_class_mop,
			 NULL, NULL, NULL) != NO_ERROR)
	{
	  all_found = LC_CLASSNAME_ERROR;
	}
      locator_free_copy_area (fetch_area);
    }

  if (all_found == LC_CLASSNAME_ERROR
      && er_errid () == ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_only_client (false);
      quit_on_errors = true;
    }

  /*
   * Now get the rest of the objects and classes
   */

  if (lockhint != NULL
      && (all_found == LC_CLASSNAME_EXIST || quit_on_errors == false))
    {
      int i, idx = 0;
      LC_COPYAREA *fetch_copyarea[MAX_FETCH_SIZE];
      LC_COPYAREA **fetch_ptr = fetch_copyarea;

      if (lockhint->num_classes > MAX_FETCH_SIZE)
	{
	  fetch_ptr =
	    (LC_COPYAREA **) malloc (sizeof (LC_COPYAREA *) *
				     lockhint->num_classes);

	  if (fetch_ptr == NULL)
	    {
	      return LC_CLASSNAME_ERROR;
	    }
	}

      error_code = NO_ERROR;
      while (error_code == NO_ERROR
	     && lockhint->num_classes > lockhint->num_classes_processed)
	{
	  fetch_ptr[idx] = NULL;
	  error_code =
	    locator_fetch_lockhint_classes (lockhint, &fetch_ptr[idx]);
	  if (error_code != NO_ERROR)
	    {
	      if (fetch_ptr[idx] != NULL)
		{
		  locator_free_copy_area (fetch_ptr[idx]);
		  fetch_ptr[idx] = NULL;
		}
	    }

	  idx++;
	}

      for (i = 0; i < idx; i++)
	{
	  if (fetch_ptr[i] != NULL)
	    {
	      locator_cache (fetch_ptr[i], sm_Root_class_mop, NULL,
			     NULL, NULL);
	      locator_free_copy_area (fetch_ptr[i]);
	    }
	}

      if (fetch_ptr != fetch_copyarea)
	{
	  free_and_init (fetch_ptr);
	}
    }

  /*
   * Cache the lock of the hinted classes
   */

  if (lockhint != NULL
      && (all_found == LC_CLASSNAME_EXIST || quit_on_errors == false))
    {
      locator_cache_lock_lockhint_classes (lockhint);
    }

  if (lockhint != NULL)
    {
      locator_free_lockhint (lockhint);
    }

error:
  return all_found;
}


/*
 * Client oidset processing
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * Most of the LC_OIDSET code is in locator.c as it in theory could be used
 * by either the client or server and the packing/unpacking code in fact
 * has to be shared.
 *
 * In practice though, the construction of an LC_OIDSET from scratch is
 * only done by the client who will then send it to the server for processing.
 * When the oidset comes back, we then have to take care to update the
 * workspace MOPs for the changes in OIDs.  This is an operation that
 * the client side locator should do as it requires access to workspace
 * internals.
 *
 * The usual pattern for the client is this:
 *
 * 	locator_make_oid_set		      begin a structure
 * 	locator_add_oidset_object	      populate it with entries
 * 	...
 * 	locator_assign_oidset	      assign the OIDs and update the workspace
 *
 */

#if defined (ENABLE_UNUSED_FUNCTION)
static int
locator_check_object_and_get_class (MOP obj_mop, MOP * out_class_mop)
{
  int error_code = NO_ERROR;
  MOP class_mop;

  if (obj_mop == NULL || obj_mop->object == NULL)
    {
      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
      goto error;
    }

  class_mop = ws_class_mop (obj_mop);
  if (class_mop == NULL || class_mop->object == NULL)
    {
      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
      goto error;
    }

  if (!OID_ISTEMP (ws_oid (obj_mop)))
    {
      error_code = ER_LC_UNEXPECTED_PERM_OID;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error;
    }

  /*
   * Ensure that the class has been flushed at this point.
   * HFID can't be NULL at this point since we've got an instance for
   * this class.  Could use locator_has_heap to make sure.
   */

  if (OID_ISTEMP (ws_oid (class_mop)))
    {
      error_code = locator_flush_class (class_mop);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  *out_class_mop = class_mop;

error:
  return error_code;
}
#endif

/*
 * locator_add_oidset_object () - Add object to oidset
 *
 * return: oidmap structure for this object
 *
 *   oidset(in): oidset to extend
 *   obj_mop(in): object to add
 */
LC_OIDMAP *
locator_add_oidset_object (LC_OIDSET * oidset, MOP obj_mop)
{
  MOP class_mop;
  LC_OIDMAP *oid_map_p;

  if (locator_check_object_and_get_class (obj_mop, &class_mop) != NO_ERROR)
    {
      return NULL;
    }

  oid_map_p =
    locator_add_oid_set (NULL, oidset, sm_heap ((MOBJ) class_mop->object),
			 WS_OID (class_mop), WS_OID (obj_mop));
  if (oid_map_p == NULL)
    {
      return NULL;
    }

  /* remember the object handle so it can be updated later */
  if (oid_map_p->mop == NULL)
    {
      oid_map_p->mop = (void *) obj_mop;

      /*
       * Since this is the first time we've been here, compute the estimated
       * storage size.  This could be rather expensive, may want to just
       * keep an approxomate size guess in the class rather than walking
       * over the object.  If this turns out to be an expensive operation
       * (which should be unlikely relative to the cost of a server call), we can
       * just put -1 here and the heap manager will use some internal statistics
       * to make a good guess.
       */
      oid_map_p->est_size =
	tf_object_size ((MOBJ) (obj_mop->class_mop->object),
			(MOBJ) (obj_mop->object));
    }

  return oid_map_p;
}

/*
 * locator_assign_oidset () -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   oidset(in): oidset to process
 *   callback(in): Callback function to do extra work
 *
 * Note:This is the primary client side function for processing an
 *    LC_OIDSET and updating the workspace.
 *    The oidset is expected to have been populated by locator_add_oidset_object
 *    so that the o->mop fields will point directly to the workspace
 *    handles for fast updating.
 *
 *    Callback function if passed will be called for each LC_OIDMAP entry
 *    so the caller can do something extra with the client_data state for
 *    each object.
 *    The callback function cannot do anything that would result in an
 *    error so be careful.
 */
int
locator_assign_oidset (LC_OIDSET * oidset, LC_OIDMAP_CALLBACK callback)
{
  int error_code = NO_ERROR;
  LC_CLASS_OIDSET *class_oidset;
  LC_OIDMAP *oid;
  int status;

  if (oidset != NULL && oidset->total_oids > 0)
    {
      /*
       * Note:, it is currently defined that if the server returns a
       * failure here that it will have "rolled back" any partial results
       * it may have obtained, this means that we don't have to worry about
       * updating the workspace here for the permanent OID's that might
       * have been assigned before an error was encountered.
       */
      status = locator_assign_oid_batch (oidset);
      if (status != NO_ERROR)
	{
	  /* make sure we faithfully return whatever error the server sent back */
	  error_code = er_errid ();
	}
      else
	{
	  /* Map through the oidset and update the workspace */
	  for (class_oidset = oidset->classes; class_oidset != NULL;
	       class_oidset = class_oidset->next)
	    {
	      for (oid = class_oidset->oids; oid != NULL; oid = oid->next)
		{
		  if (oid->mop != NULL)
		    {
		      ws_perm_oid ((MOP) oid->mop, &oid->oid);
		    }

		  /* let the callback function do further processing if
		   * necessary */
		  if (callback != NULL)
		    {
		      (*callback) (oid);
		    }
		}
	    }
	}
    }

  return error_code;
}

/*
 * locator_add_to_oidset_when_temp_oid () -
 *
 * return: WS_MAP_ status code
 *
 *   mop(in):  object to examine
 *   data(in): pointer to the LC_OIDSET we're populating
 *
 * Note:This is a mapping function passed to ws_map_dirty by
 *    locator_assign_all_permanent_oids.  Here we check to see if the object
 *    will eventually be flushed and if so, we call tf_find_temporary_oids
 *    to walk over the object and add all the temporary OIDs that object
 *    contains to the LC_OIDSET.
 *    After walking over the object, we check to see if we've exceeded
 *    OID_BATCH_SIZE and if so, we assign the OIDs currently in the
 *    oidset, clear the oidset, and continue with the next batch.
 */
static int
locator_add_to_oidset_when_temp_oid (MOP mop, void *data)
{
  LC_OIDSET *oidset = (LC_OIDSET *) data;
  OID *oid;
  int map_status;
  MOBJ object;

  map_status = WS_MAP_CONTINUE;

  assert (ws_class_mop (mop) != NULL);

  oid = ws_oid (mop);

  if (OID_ISTEMP (oid) && ws_find (mop, &object) != WS_FIND_MOP_DELETED)
    {
      if (locator_add_oidset_object (oidset, mop) == NULL)
	{
	  return WS_MAP_FAIL;
	}

      /*
       * If we've gone over our threshold, flush the ones we have so far,
       * and clear out the oidset for more.  We may want to make this
       * part of locator_add_oidset_object rather than doing it out here.
       */
      if (oidset->total_oids > OID_BATCH_SIZE)
	{
	  if (locator_assign_oidset (oidset, NULL) != NO_ERROR)
	    {
	      return WS_MAP_FAIL;
	    }

	  locator_clear_oid_set (NULL, oidset);
	}
    }

  return map_status;
}

/*
 * locator_assign_all_permanent_oids () -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * Note:This function will turn all of the temporary OIDs in the workspace
 *    into permanent OIDs.  This will be done by issuing one or more
 *    calls to locator_assign_oidset using an LC_OIDSET that has been populated
 *    by walking over every dirty object in the workspace.
 *
 *    This is intended to be called during transaction commit processing
 *    before we start flushing objects.  It will ensure that all object
 *    references will be promoted to permanent OIDs which will reduce
 *    the number of server calls we have to make while the objects
 *    are being flushed.
 *
 *    Note: It is not GUARANTEED that all temporary OIDs will have been
 *    promoted after this function is complete.  We will try to promote
 *    all of them, and in practice, that will be the usual case.  The caller
 *    should not however rely on this behavior and later processing must
 *    be prepared to encounter temporary OIDs and handle them in the usual
 *    way.  This function is intended as a potential optimization only,
 *    it cannot be relied upon to assign permanent OIDs.
 */
int
locator_assign_all_permanent_oids (void)
{
  int error_code = NO_ERROR, map_status;
  LC_OIDSET *oidset;

  oidset = locator_make_oid_set ();
  if (oidset == NULL)
    {
      return ER_FAILED;
    }

  map_status = ws_map_dirty (locator_add_to_oidset_when_temp_oid, oidset);
  if (map_status == WS_MAP_FAIL)
    {
      error_code = ER_FAILED;
      goto error;
    }

  error_code = locator_assign_oidset (oidset, NULL);

error:
  locator_free_oid_set (NULL, oidset);
  return error_code;
}
#endif

/*
 * locator_flush_replication_info () -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   repl_info(in):
 */
int
locator_flush_replication_info (REPL_INFO * repl_info)
{
  return repl_set_info (repl_info);
}

/*
 * locator_get_append_lsa () -
 *
 * return:NO_ERROR if all OK, ER status otherwise
 *
 *   lsa(in):
 */
int
locator_get_eof_lsa (LOG_LSA * lsa)
{
  return repl_log_get_eof_lsa (lsa);
}
