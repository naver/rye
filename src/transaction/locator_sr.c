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
 * locator_sr.c : Transaction object locator (at server)
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <assert.h>

#include "locator_sr.h"
#include "locator.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "oid.h"
#include "memory_hash.h"
#include "error_manager.h"
#include "xserver_interface.h"
#include "list_file.h"
#include "query_manager.h"
#include "slotted_page.h"
#include "extendible_hash.h"
#include "btree.h"
#include "btree_load.h"
#include "heap_file.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "log_manager.h"
#include "lock_manager.h"
#include "system_catalog.h"
#include "repl_log.h"
#include "critical_section.h"
#if defined(SERVER_MODE)
#include "connection_error.h"
#include "thread.h"
#endif /* SERVER_MODE */
#include "object_print.h"
#include "object_primitive.h"
#include "object_domain.h"
#include "system_parameter.h"
#include "log_impl.h"
#include "transaction_sr.h"
#include "boot_sr.h"
#include "transform.h"
#include "perf_monitor.h"

#include "db.h"

#if defined(DMALLOC)
#include "dmalloc.h"
#endif /* DMALLOC */

#if !defined(SERVER_MODE)

#define heap_classrepr_unlock_class(oid)

#endif /* not SERVER_MODE */

/* TODO : remove */
extern bool catcls_Enable;

static const int LOCATOR_GUESS_NUM_NESTED_REFERENCES = 100;
#define LOCATOR_GUESS_HT_SIZE    LOCATOR_GUESS_NUM_NESTED_REFERENCES * 2

#define MAX_CLASSNAME_CACHE_ENTRIES     ONE_K
#define CLASSNAME_CACHE_SIZE            ONE_K

extern int catcls_insert_catalog_classes (THREAD_ENTRY * thread_p,
					  RECDES * record);
extern int catcls_delete_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, OID * class_oid);
extern int catcls_update_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, RECDES * record);
extern int catcls_remove_entry (THREAD_ENTRY * thread_p, OID * class_oid);

typedef struct locator_tmp_classname_entry LOCATOR_TMP_CLASSNAME_ENTRY;
struct locator_tmp_classname_entry
{
  char *name;			/* Name of the class */
  int tran_index;		/* Transaction of entry */
  LC_FIND_CLASSNAME action;	/* The transient operation, delete or reserve
				 * name
				 */
  OID oid;			/* The class identifier of classname */
};

typedef struct locator_tmp_desired_classname_entries
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES;
struct locator_tmp_desired_classname_entries
{
  int tran_index;
  LOG_LSA *savep_lsa;
};

typedef struct locator_return_nxobj LOCATOR_RETURN_NXOBJ;
struct locator_return_nxobj
{				/* Location of next object to return in
				 * communication (fetch) area
				 */
  LC_COPYAREA *comm_area;	/* Communication area where objects are
				 * returned and described
				 */
  LC_COPYAREA_MANYOBJS *mobjs;	/* Location in the communication area
				 * where all objects to be returned are
				 * described.
				 */
  LC_COPYAREA_ONEOBJ *obj;	/* Location in the communication area
				 * where the next object to return is
				 * described.
				 */
  HEAP_SCANCACHE *ptr_scancache;	/* Scan cache used for fetching
					 * purposes
					 */
  HEAP_SCANCACHE area_scancache;	/* Scan cache used for fetching
					 * purposes
					 */
  RECDES recdes;		/* Location in the communication area
				 * where the content of the next object
				 * to return is placed.
				 */
  int area_offset;		/* Relative offset to recdes->data in the
				 * communication area
				 */
};

static EHID locator_Classnames_table;
static EHID *locator_Eht_classnames = &locator_Classnames_table;
static MHT_TABLE *locator_Mht_classnames = NULL;

static const HFID NULL_HFID = NULL_HFID_INITIALIZER;

static void locator_permoid_class_name (THREAD_ENTRY * thread_p,
					const char *classname,
					OID * class_oid);
static int locator_force_drop_class_name_entry (const void *name, void *ent,
						void *rm);
static int locator_drop_class_name_entry (const void *name, void *ent,
					  void *rm);
static int locator_decache_class_name_entries (void);
static int locator_decache_class_name_entry (const void *name, void *ent,
					     void *dc);
static int locator_print_class_name (FILE * outfp, const void *key,
				     void *ent, void *ignore);
#if defined (ENABLE_UNUSED_FUNCTION)
static int locator_check_class_on_heap (THREAD_ENTRY * thread_p,
					void *classname, void *classoid,
					void *xvalid);
#endif
static SCAN_CODE locator_return_object (THREAD_ENTRY * thread_p,
					LOCATOR_RETURN_NXOBJ * assign,
					OID * class_oid, OID * oid);
static int locator_find_lockset_missing_class_oids (THREAD_ENTRY * thread_p,
						    LC_LOCKSET * lockset);
static SCAN_CODE locator_return_object_assign (THREAD_ENTRY * thread_p,
					       LOCATOR_RETURN_NXOBJ * assign,
					       OID * class_oid, OID * oid,
					       SCAN_CODE scan);
#if defined(ENABLE_UNUSED_FUNCTION)
static DISK_ISVALID locator_check_class (THREAD_ENTRY * thread_p,
					 OID * class_oid, RECDES * peek,
					 HFID * class_hfid, bool repair);
#endif
static int locator_repl_prepare_force (THREAD_ENTRY * thread_p,
				       int *shard_group_id,
				       LC_COPYAREA_ONEOBJ * obj,
				       RECDES * old_recdes, RECDES * recdes,
				       DB_IDXKEY * key,
				       HEAP_SCANCACHE * force_scancache);
static int locator_repl_get_key_value (DB_IDXKEY * key,
				       const char **class_name,
				       LC_COPYAREA * force_area,
				       LC_COPYAREA_ONEOBJ * obj);
static void locator_repl_add_error_to_copyarea (LC_COPYAREA ** copy_area,
						RECDES * recdes,
						LC_COPYAREA_ONEOBJ * obj,
						DB_IDXKEY * key, int err_code,
						const char *err_msg);
static int locator_insert_force (THREAD_ENTRY * thread_p, HFID * hfid,
				 OID * class_oid, OID * oid, RECDES * recdes,
				 HEAP_SCANCACHE * scan_cache,
				 int *force_count);
static int locator_delete_force (THREAD_ENTRY * thread_p, HFID * hfid,
				 OID * oid, HEAP_SCANCACHE * scan_cache,
				 int *force_count);
static int locator_update_force (THREAD_ENTRY * thread_p, HFID * hfid,
				 OID * class_oid, OID * oid,
				 RECDES * ikdrecdes, RECDES * recdes,
				 ATTR_ID * att_id, int n_att_id,
				 HEAP_SCANCACHE * scan_cache,
				 int *force_count);

#if defined(ENABLE_UNUSED_FUNCTION)
static void locator_increase_catalog_count (THREAD_ENTRY * thread_p,
					    OID * cls_oid);
static void locator_decrease_catalog_count (THREAD_ENTRY * thread_p,
					    OID * cls_oid);
#endif

static LC_FIND_CLASSNAME xlocator_reserve_class_name (THREAD_ENTRY * thread_p,
						      const char *classname,
						      const OID * class_oid);

/*
 * locator_initialize () - Initialize the locator on the server
 *
 * return: EHID *(classname_table on success or NULL on failure)
 *
 *   classname_table(in): Classname_to_OID permanent hash file
 *
 * Note: Initialize the server transaction object locator. Currently,
 *              only the classname memory hash table for transient entries is
 *              initialized.
 */
EHID *
locator_initialize (UNUSED_ARG THREAD_ENTRY * thread_p,
		    EHID * classname_table)
{
  assert (classname_table != NULL);

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /* Some kind of failure. We must notify the error to the caller. */
      return NULL;
    }

  VFID_COPY (&locator_Eht_classnames->vfid, &classname_table->vfid);
  locator_Eht_classnames->pageid = classname_table->pageid;
  if (locator_Mht_classnames != NULL)
    {
      mht_clear (locator_Mht_classnames, NULL, NULL);
    }
  else
    {
      locator_Mht_classnames = mht_create ("Memory hash Classname to OID",
					   CLASSNAME_CACHE_SIZE,
					   mht_1strhash,
					   mht_compare_strings_are_equal);
    }

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  if (locator_Mht_classnames == NULL)
    {
      return NULL;
    }
  else
    {
      return classname_table;
    }
}

/*
 * locator_finalize () - Terminates the locator on the server
 *
 * return: nothing
 *
 * Note:Terminate the object locator on the server. Currently, only
 *              the classname memory hash table is removed.
 */
void
locator_finalize (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /* Some kind of failure. We will leak resources. */
      return;
    }

  if (locator_Mht_classnames == NULL)
    {
      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      return;
    }

  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      return;
    }

  (void) mht_map (locator_Mht_classnames,
		  locator_force_drop_class_name_entry, NULL);
  mht_destroy (locator_Mht_classnames);
  locator_Mht_classnames = NULL;

  csect_exit (CSECT_CT_OID_TABLE);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
}

/*
 * xlocator_reserve_class_names () - Reserve several classnames
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_RESERVED,
 *                                  LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   num_classes(in): Number of classes
 *   classnames(in): Names of the classes
 *   class_oids(in): Object identifiers of the classes
 */
LC_FIND_CLASSNAME
xlocator_reserve_class_names (THREAD_ENTRY * thread_p, const int num_classes,
			      const char **classnames, const OID * class_oids)
{
  int i = 0;
  LC_FIND_CLASSNAME result = LC_CLASSNAME_RESERVED;

  assert (num_classes == 1);

  for (i = 0; i < num_classes; i++)
    {
      assert (classnames[i] != NULL);

      result = xlocator_reserve_class_name (thread_p, classnames[i],
					    &class_oids[i]);
      if (result != LC_CLASSNAME_RESERVED)
	{
	  /* We could potentially revert the reservation but the transient
	     entries should be properly cleaned up by the rollback so we don't
	     really need to do this here. */
	  break;
	}
    }

  return result;
}

/*
 * xlocator_reserve_class_name () - Reserve a classname
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_RESERVED,
 *                                  LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   classname(in): Name of class
 *   class_oid(in): Object identifier of the class
 *
 * Note: Reserve the name of a class.
 *              If there is an entry on the transient/memory classname table,
 *              we can proceed if the entry belongs to the current
 *              transaction, otherwise, we must wait until the transaction
 *              holding the entry terminates since the fate of the classname
 *              entry cannot be predicted. If the transient entry belongs to
 *              the current transaction, we can reserve the name only if the
 *              entry indicates that a class with such a name has been
 *              deleted or reserved. If there is not a transient entry with
 *              such a name the permanent classname to OID table is consulted
 *              and depending on the existence of an entry, the classname is
 *              reserved or an error is returned. The classname_to_OID entry
 *              that is created is a transient entry in main memory, the entry
 *              is added onto the permanent hash when the class is stored in
 *              the page buffer pool/database.
 */
static LC_FIND_CLASSNAME
xlocator_reserve_class_name (THREAD_ENTRY * thread_p, const char *classname,
			     const OID * class_oid)
{
  EH_SEARCH search;
  OID oid;
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LC_FIND_CLASSNAME reserve = LC_CLASSNAME_RESERVED;
  DB_VALUE lock_val;

  if (classname == NULL)
    {
      return LC_CLASSNAME_ERROR;
    }

#if 0
start:
#endif
  reserve = LC_CLASSNAME_RESERVED;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  /* Is there any transient entries on the classname hash table ? */
  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);
  if (entry != NULL && entry->action != LC_CLASSNAME_EXIST)
    {
      /*
       * We can only proceed if the entry belongs to the current transaction,
       * otherwise, we must lock the class associated with the classname and
       * retry the operation once the lock is granted.
       */
      if (entry->tran_index == logtb_get_current_tran_index (thread_p))
	{
	  /*
	   * The name can be reserved only if it has been deleted or
	   * previously reserved. We allow double reservations in order for
	   * multiple table renaming to properly reserve all the names
	   * involved.
	   */
	  if (entry->action == LC_CLASSNAME_DELETED
	      || entry->action == LC_CLASSNAME_DELETED_RENAME
	      || entry->action == LC_CLASSNAME_RESERVED)
	    {
	      entry->action = LC_CLASSNAME_RESERVED;
	      COPY_OID (&entry->oid, class_oid);
	    }
	  else
	    {
	      reserve = LC_CLASSNAME_EXIST;
	    }
	}
      else
	{
	  assert (false);	/* is impossbile */

	  reserve = LC_CLASSNAME_ERROR;
	}
    }
  else if (entry != NULL)
    {
      /* There is a class with such a name on the classname cache. */
      reserve = LC_CLASSNAME_EXIST;
    }
  else
    {
      /*
       * Is there a class with such a name on the permanent classname hash
       * table ?
       *
       * It is too dangerous to call the extendible hash table while holding
       * the critical section. We do not know what the extendible hash will
       * do. It may be blocked. Instead, we do the following:
       *
       *    Exit critical section
       *    execute ehash_search
       *    if not found,
       *       enter critical section
       *       double check to make sure that there is not a new entry on
       *              this name. if there is one, return immediately with
       *              classname exist, since I was not the one that add
       *              the entry.
       *       reserver name
       *       exit critical section
       */

      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      search = ehash_search (thread_p, locator_Eht_classnames,
			     classname, &oid);
      if (search == EH_ERROR_OCCURRED)
	{
	  /*
	   * Some kind of failure. We must notify the error to the caller.
	   */
	  return LC_CLASSNAME_ERROR;
	}
      if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT)
	  != NO_ERROR)
	{
	  return LC_CLASSNAME_ERROR;
	}

      /* Double check */
      entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						       classname);
      if (entry != NULL)
	{
	  reserve = LC_CLASSNAME_EXIST;
	}
      else
	{
	  if (search == EH_KEY_NOTFOUND)
	    {
	      entry =
		(LOCATOR_TMP_CLASSNAME_ENTRY *) malloc (sizeof (*entry));

	      if (entry == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*entry));
		  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		  return LC_CLASSNAME_ERROR;
		}

	      entry->name = strdup (classname);
	      if (entry->name == NULL)
		{
		  free_and_init (entry);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  strlen (classname) + 1);
		  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		  return LC_CLASSNAME_ERROR;
		}

	      entry->tran_index = logtb_get_current_tran_index (thread_p);
	      entry->action = LC_CLASSNAME_RESERVED;
	      COPY_OID (&entry->oid, class_oid);

	      log_increase_num_transient_classnames (entry->tran_index);
	      (void) mht_put (locator_Mht_classnames, entry->name, entry);
	    }
	  else
	    {
	      /* We can cache this class but don't cache it. */
	      reserve = LC_CLASSNAME_EXIST;
	    }
	}
    }

  /*
   * Note that the index has not been made permanently into the database.
   *      That is, it has not been inserted onto extendible hash.
   */

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  /*
   * Get the lock on the class if we were able to reserve the name
   */
  if (reserve == LC_CLASSNAME_RESERVED && entry != NULL)
    {
      DB_MAKE_OID (&lock_val, class_oid);
      if (lock_object (thread_p, &lock_val,
		       X_LOCK, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  assert (false);	/* is impossible */

	  /*
	   * Something wrong. Remove the entry from hash table.
	   */
	  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
			   INF_WAIT) != NO_ERROR)
	    {
	      return LC_CLASSNAME_ERROR;
	    }

	  log_decrease_num_transient_classnames (entry->tran_index);
	  (void) mht_rem (locator_Mht_classnames, entry->name, NULL, NULL);
	  free_and_init (entry->name);
	  free_and_init (entry);

	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	  reserve = LC_CLASSNAME_ERROR;
	}
    }

  return reserve;
}

/*
 * xlocator_delete_class_name () - Remove a classname
 *
 * return: LC_FIND_CLASSNAME (either of LC_CLASSNAME_DELETED,
 *                                      LC_CLASSNAME_ERROR)
 *
 *   classname(in): Name of the class to delete
 *
 * Note: Indicate that a classname has been deleted. A transient
 *              classname to OID entry is created in memory to indicate the
 *              deletion. The permanent classname to OID entry is deleted from
 *              permanent classname to OID hash table when the class is
 *              removed from the database.
 *              If there is an entry on the transient/memory classname table,
 *              we can proceed if the entry belongs to the current
 *              transaction, otherwise, we must wait until the transaction
 *              holding the entry terminates since the fate of the classname
 *              entry cannot be predicted. If the transient entry belongs to
 *              the current transaction, we can delete the name only if the
 *              entry indicates that a class with such a name has been
 *              reserved. If there is not a transient entry with such a name
 *              the permanent classname to OID table is consulted and
 *              depending on the existence of an entry, the deleted class is
 *              locked and a transient entry is created informing of the
 *              deletion.
 */
LC_FIND_CLASSNAME
xlocator_delete_class_name (THREAD_ENTRY * thread_p, const char *classname)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LC_FIND_CLASSNAME classname_delete = LC_CLASSNAME_DELETED;
  EH_SEARCH search;
  OID tmp_classoid;
  DB_VALUE lock_val;

  if (classname == NULL)
    {
      return LC_CLASSNAME_ERROR;
    }

start:
  classname_delete = LC_CLASSNAME_DELETED;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);
  if (entry != NULL && entry->action != LC_CLASSNAME_EXIST)
    {
      /*
       * We can only proceed if the entry belongs to the current transaction,
       * otherwise, we must lock the class associated with the classname and
       * retry the operation once the lock is granted.
       */
      if (entry->tran_index == logtb_get_current_tran_index (thread_p))
	{
#if 1				/* TODO - */
	  assert (entry->action == LC_CLASSNAME_DELETED);
#endif
	  /*
	   * The name can be deleted only if it has been reserved by current
	   * transaction
	   */
	  if (entry->action == LC_CLASSNAME_DELETED
	      || entry->action == LC_CLASSNAME_DELETED_RENAME)
	    {
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }

#if 1				/* TODO - */
	  assert (false);
	  classname_delete = LC_CLASSNAME_ERROR;
	  goto error;
#endif
	}
      else
	{
	  assert (false);	/* is impossible */

	  /*
	   * Do not know the fate of this entry until the transaction holding
	   * this entry either commits or aborts. Get the lock and try again.
	   */
	  COPY_OID (&tmp_classoid, &entry->oid);

	  /*
	   * Exit from critical section since we are going to be suspended and
	   * then retry again.
	   */

	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	  DB_MAKE_OID (&lock_val, &tmp_classoid);
	  if (lock_object (thread_p, &lock_val, X_LOCK,
			   LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      /*
	       * Unable to acquired lock
	       */
	      return LC_CLASSNAME_ERROR;
	    }
	  else
	    {
	      /*
	       * Try again
	       * Remove the lock.. since the above was a dirty read
	       */
	      lock_unlock_object (thread_p, &lock_val, LK_UNLOCK_TYPE_NORMAL);
	      goto start;
	    }
	}
    }
  else if (entry != NULL)
    {
      /* There is a class with such a name on the classname cache.
       * We should convert it to transient one.
       */
      entry->tran_index = logtb_get_current_tran_index (thread_p);
      entry->action = LC_CLASSNAME_DELETED;
      log_increase_num_transient_classnames (entry->tran_index);
    }
  else
    {
      OID class_oid;
      /*
       * Is there a class with such a name on the permanent classname hash
       * table ?
       */

      /*
       * Now check the permanent classname hash table.
       *
       * It is too dangerous to call the extendible hash table while holding
       * the critical section. We do not know what the extendible hash will
       * do. It may be blocked. Instead, we do the following:
       *
       *    Exit critical section
       *    execute ehash_search
       *    if not found,
       *       enter critical section
       *       double check to make sure that there is not a new entry on
       *              this name. if there is one, return immediately with
       *              classname exist, since I was not the one that add
       *              the entry.
       *       reserver name
       *       exit critical section
       */

      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

      search = ehash_search (thread_p, locator_Eht_classnames,
			     classname, &class_oid);

      if (search == EH_ERROR_OCCURRED)
	{
	  /*
	   * Some kind of failure. We must notify the error to the caller.
	   */
	  return LC_CLASSNAME_ERROR;
	}
      if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT)
	  != NO_ERROR)
	{
	  return LC_CLASSNAME_ERROR;
	}

      /* Double check */
      entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						       classname);
      if (entry != NULL)
	{
	  if (entry->action != LC_CLASSNAME_EXIST)
	    {
	      /* Transient classname by other transaction exists. */
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }

	  entry->tran_index = logtb_get_current_tran_index (thread_p);
	  entry->action = LC_CLASSNAME_DELETED;
	  COPY_OID (&entry->oid, &class_oid);
	  log_increase_num_transient_classnames (entry->tran_index);
	}
      else
	{
	  entry =
	    (LOCATOR_TMP_CLASSNAME_ENTRY *)
	    malloc (sizeof (LOCATOR_TMP_CLASSNAME_ENTRY));
	  if (entry == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      sizeof (LOCATOR_TMP_CLASSNAME_ENTRY));
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }
	  entry->name = strdup (classname);
	  if (entry->name == NULL)
	    {
	      free_and_init (entry);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (classname) + 1);
	      classname_delete = LC_CLASSNAME_ERROR;
	      goto error;
	    }
	  entry->tran_index = logtb_get_current_tran_index (thread_p);
	  entry->action = LC_CLASSNAME_DELETED;
	  COPY_OID (&entry->oid, &class_oid);
	  log_increase_num_transient_classnames (entry->tran_index);
	  (void) mht_put (locator_Mht_classnames, entry->name, entry);
	}
    }

error:
  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  /*
   * We do not need to lock the entry->oid since it has already been locked
   * in exclusive mode when the class was deleted or renamed. Avoid duplicate
   * calls.
   */

  /* Note that the index has not been dropped permanently from the database */
  return classname_delete;
}

/*
 * xlocator_rename_class_name () - Rename a classname
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_RESERVED_RENAME,
 *                                  LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   oldname(in): Oldname of class
 *   newname(in): Newname of class
 *   class_oid(in): Object identifier of the class
 *
 * Note: Rename a class in transient form.
 */
LC_FIND_CLASSNAME
xlocator_rename_class_name (THREAD_ENTRY * thread_p, const char *oldname,
			    const char *newname, const OID * class_oid)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LC_FIND_CLASSNAME renamed;

  if (oldname == NULL || newname == NULL)
    {
      return LC_CLASSNAME_ERROR;
    }

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return LC_CLASSNAME_ERROR;
    }

  renamed = xlocator_reserve_class_name (thread_p, newname, class_oid);
  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   newname);
  if (renamed == LC_CLASSNAME_RESERVED && entry != NULL)
    {
      entry->action = LC_CLASSNAME_RESERVED_RENAME;
      renamed = xlocator_delete_class_name (thread_p, oldname);
      entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						       oldname);
      if (renamed == LC_CLASSNAME_DELETED && entry != NULL)
	{
	  entry->action = LC_CLASSNAME_DELETED_RENAME;
	  renamed = LC_CLASSNAME_RESERVED_RENAME;
	}
      else
	{
	  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
		   mht_get (locator_Mht_classnames, newname));

	  if (entry == NULL
	      || csect_enter (thread_p, CSECT_CT_OID_TABLE,
			      INF_WAIT) != NO_ERROR)
	    {
	      renamed = LC_CLASSNAME_ERROR;
	      goto error;
	    }
	  (void) locator_drop_class_name_entry (newname, entry, NULL);
	  csect_exit (CSECT_CT_OID_TABLE);
	}
    }

error:

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return renamed;
}

/*
 * xlocator_find_class_oid () - Find oid of a classname
 *
 * return: LC_FIND_CLASSNAME
 *                       (either of LC_CLASSNAME_EXIST,
 *                                  LC_CLASSNAME_DELETED,
 *                                  LC_CLASSNAME_ERROR)
 *
 *   classname(in): Name of class to find
 *   class_oid(in): Set as a side effect
 *   lock(in): Lock to acquire for the class
 *
 * Note: Find the class identifier of the given class name and lock the
 *              class with the given mode.
 *              If there is an entry on the transient/memory classname table,
 *              we can proceed if the entry belongs to the current
 *              transaction, otherwise, we must wait until the transaction
 *              holding the entry terminates since the fate of the classname
 *              entry cannot be predicted.
 */
LC_FIND_CLASSNAME
xlocator_find_class_oid (THREAD_ENTRY * thread_p, const char *classname,
			 OID * class_oid, LOCK lock)
{
  EH_SEARCH search;
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  LOCK tmp_lock;
  LC_FIND_CLASSNAME find = LC_CLASSNAME_EXIST;
  DB_VALUE lock_val;

  /* init parameter */
  OID_SET_NULL (class_oid);

start:
  find = LC_CLASSNAME_EXIST;

  if (csect_enter_as_reader (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
			     INF_WAIT) != NO_ERROR)
    {
      return LC_CLASSNAME_ERROR;
    }

  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);

  if (entry != NULL)
    {
      /*
       * We can only proceed if the entry belongs to the current transaction,
       * otherwise, we must lock the class associated with the classname and
       * retry the operation once the lock is granted.
       */
      COPY_OID (class_oid, &entry->oid);
      if (entry->tran_index == logtb_get_current_tran_index (thread_p))
	{
	  if (entry->action == LC_CLASSNAME_DELETED
	      || entry->action == LC_CLASSNAME_DELETED_RENAME)
	    {
	      OID_SET_NULL (class_oid);
	      find = LC_CLASSNAME_DELETED;
	    }
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	}
      else if (entry->action == LC_CLASSNAME_EXIST)
	{
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	}
      else
	{
#if 1
	  assert (false);	/* is impossible */
#endif

	  /*
	   * Do not know the fate of this entry until the transaction is
	   * committed or aborted. Get the lock and try again.
	   */
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	  if (lock != NULL_LOCK)
	    {
	      tmp_lock = lock;
	    }
	  else
	    {
	      tmp_lock = S_LOCK;
	    }

	  DB_MAKE_OID (&lock_val, class_oid);
	  if (lock_object (thread_p, &lock_val, tmp_lock,
			   LK_UNCOND_LOCK) != LK_GRANTED)
	    {
	      /*
	       * Unable to acquired lock
	       */
	      OID_SET_NULL (class_oid);
	      find = LC_CLASSNAME_ERROR;
	    }
	  else
	    {
	      /*
	       * Try again
	       * Remove the lock.. since the above was a dirty read
	       */
	      lock_unlock_object (thread_p, &lock_val, LK_UNLOCK_TYPE_NORMAL);
	      goto start;
	    }
	}
    }
  else
    {
      /*
       * Is there a class with such a name on the permanent classname hash
       * table ?
       */
      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      search = ehash_search (thread_p, locator_Eht_classnames,
			     classname, class_oid);
      if (search != EH_KEY_FOUND)
	{
	  if (search == EH_KEY_NOTFOUND)
	    {
	      find = LC_CLASSNAME_DELETED;
	    }
	  else
	    {
	      find = LC_CLASSNAME_ERROR;
	    }
	}
      else
	{
	  if (csect_enter
	      (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
	       INF_WAIT) != NO_ERROR)
	    {
	      return LC_CLASSNAME_ERROR;
	    }
	  /* Double check */
	  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
		   mht_get (locator_Mht_classnames, classname));
	  if (entry == NULL)
	    {
	      if ((int) mht_count (locator_Mht_classnames) <
		  MAX_CLASSNAME_CACHE_ENTRIES
		  || locator_decache_class_name_entries () == NO_ERROR)
		{
		  entry =
		    (LOCATOR_TMP_CLASSNAME_ENTRY *) malloc (sizeof (*entry));
		  if (entry == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (*entry));
		      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		      return LC_CLASSNAME_ERROR;

		    }

		  entry->name = strdup (classname);
		  if (entry->name == NULL)
		    {
		      free_and_init (entry);
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      strlen (classname));
		      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		      return LC_CLASSNAME_ERROR;
		    }

		  entry->tran_index = NULL_TRAN_INDEX;
		  entry->action = LC_CLASSNAME_EXIST;
		  COPY_OID (&entry->oid, class_oid);
		  (void) mht_put (locator_Mht_classnames, entry->name, entry);
		}
	    }
	  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
	}
    }

  if (lock != NULL_LOCK && find == LC_CLASSNAME_EXIST)
    {
      /* Now acquired the desired lock */
      DB_MAKE_OID (&lock_val, class_oid);
      if (lock_object (thread_p, &lock_val,
		       lock, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  OID_SET_NULL (class_oid);
	  find = LC_CLASSNAME_ERROR;
	}
    }

  return find;
}

/*
 * locator_permoid_class_name () - Change reserve name with permanent oid
 *
 * return:
 *
 *   classname(in): Name of class
 *   class_oid(in):  New OID
 *
 * Note: Update the transient entry for the given classname with the
 *              given class identifier. The transient entry must belong to the
 *              current transaction.
 */
static void
locator_permoid_class_name (THREAD_ENTRY * thread_p, const char *classname,
			    OID * class_oid)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;

  /* Is there any transient entries on the classname hash table ? */
  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      return;
    }

  entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) mht_get (locator_Mht_classnames,
						   classname);

  if (entry != NULL && entry->action != LC_CLASSNAME_EXIST
      && entry->tran_index == logtb_get_current_tran_index (thread_p))
    {
#if 1				/* TODO - */
      assert (entry->action == LC_CLASSNAME_RESERVED);
#endif

      COPY_OID (&entry->oid, class_oid);
    }

#if 0
error:
#endif
  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
  return;
}

/*
 * locator_drop_transient_class_name_entries () - Drop transient classname entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   tran_index(in): Transaction index
 *   savep_lsa(in): up to given LSA
 *
 * Note: Remove all the classname transient entries of the given
 *              transaction up to the given savepoint.
 *              This is done when the transaction terminates and
 *              the permanent hash table has been updated with the correct
 *              entries.
 */
int
locator_drop_transient_class_name_entries (UNUSED_ARG THREAD_ENTRY * thread_p,
					   int tran_index,
					   LOG_LSA * savep_lsa)
{
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES rm;
  int error_code = NO_ERROR;

  if (tran_index != NULL_TRAN_INDEX)
    {
      if (log_get_num_transient_classnames (tran_index) <= 0)
	{
	  return error_code;
	}
    }

  rm.tran_index = tran_index;
  rm.savep_lsa = savep_lsa;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return ER_FAILED;
    }

/* TODO: SystemCatalog: 1st Phase: 2002/06/20: */
  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
      return ER_FAILED;
    }

  error_code = mht_map (locator_Mht_classnames,
			locator_drop_class_name_entry, &rm);
  if (error_code != NO_ERROR)
    {
      error_code = ER_FAILED;
    }

/* TODO: SystemCatalog: 1st Phase: 2002/06/20: */
  csect_exit (CSECT_CT_OID_TABLE);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return error_code;
}

/*
 * locator_drop_class_name_entry () - Remove one transient entry
 *
 * return: NO_ERROR or error code
 *
 *   name(in): The classname (key)
 *   ent(in): The entry (data)
 *   rm(in): Structure that indicates what to remove or NULL.
 *
 * Note: Remove transient entry if it belongs to current transaction.
 */
static int
locator_drop_class_name_entry (const void *name, void *ent, void *rm)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *drop;
  char *classname;
  OID class_oid;
  THREAD_ENTRY *thread_p;

  drop = (LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *) rm;
  thread_p = thread_get_thread_entry_info ();

  COPY_OID (&class_oid, &entry->oid);

  classname = entry->name;
  if ((entry->action != LC_CLASSNAME_EXIST)
      && (drop == NULL || drop->tran_index == NULL_TRAN_INDEX
	  || drop->tran_index == entry->tran_index))
    {
      log_decrease_num_transient_classnames (entry->tran_index);
      (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);

      (void) catcls_remove_entry (thread_p, &class_oid);

      free_and_init (ent);
      free_and_init (classname);
    }

  return NO_ERROR;
}

/*
 * locator_force_drop_class_name_entry () -
 *
 * return:
 *
 *   name(in):
 *   ent(in):
 *   rm(in):
 */
static int
locator_force_drop_class_name_entry (const void *name, void *ent, void *rm)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  UNUSED_VAR LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *drop;
  char *classname;
  OID class_oid;
  THREAD_ENTRY *thread_p;

  drop = (LOCATOR_TMP_DESIRED_CLASSNAME_ENTRIES *) rm;
  thread_p = thread_get_thread_entry_info ();

  COPY_OID (&class_oid, &entry->oid);

  classname = entry->name;

  (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);

  (void) catcls_remove_entry (thread_p, &class_oid);

  free_and_init (ent);
  free_and_init (classname);

  return NO_ERROR;
}

/*
 * locator_decache_class_name_entries () -
 *
 * return:
 */
static int
locator_decache_class_name_entries (void)
{
  int decache_count = 0;

  /* You are already in the critical section CSECT_LOCATOR_SR_CLASSNAME_TABLE.
   * So you don't need to enter CSECT_LOCATOR_SR_CLASSNAME_TABLE.
   */

  (void) mht_map (locator_Mht_classnames, locator_decache_class_name_entry,
		  &decache_count);

  /* You are in the critical section CSECT_LOCATOR_SR_CLASSNAME_TABLE yet.
   * So you should not exit CSECT_LOCATOR_SR_CLASSNAME_TABLE.
   */

  return NO_ERROR;
}

/*
 * locator_decache_class_name_entry  () -
 *
 * return: NO_ERROR or error code
 *
 *   name(in):
 *   ent(in):
 *   dc(in):
 */
static int
locator_decache_class_name_entry (const void *name, void *ent, void *dc)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  int *decache_count;
  char *classname;

  decache_count = (int *) dc;
  classname = entry->name;

  if (entry->action == LC_CLASSNAME_EXIST)
    {
      (void) mht_rem (locator_Mht_classnames, name, NULL, NULL);
      free_and_init (ent);
      free_and_init (classname);

      *decache_count += 1;
      if (*decache_count >= MAX_CLASSNAME_CACHE_ENTRIES * 0.1)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * locator_print_class_name () - Print an entry of classname memory hash table
 *
 * return: always return true
 *
 *   outfp(in): FILE stream where to dump the entry
 *   key(in): Classname
 *   ent(in): The entry associated with classname
 *   ignore(in):
 *
 * Note:Print an entry of the classname memory hash table.
 */
static int
locator_print_class_name (FILE * outfp, const void *key, void *ent,
			  UNUSED_ARG void *ignore)
{
  LOCATOR_TMP_CLASSNAME_ENTRY *entry = (LOCATOR_TMP_CLASSNAME_ENTRY *) ent;
  const char *str_action;

  fprintf (outfp, "Classname = %s, TRAN_INDEX = %d,\n",
	   (const char *) key, entry->tran_index);

  switch (entry->action)
    {
    case LC_CLASSNAME_RESERVED:
      str_action = "CLASSNAME_RESERVE";
      break;
    case LC_CLASSNAME_RESERVED_RENAME:
      str_action = "LC_CLASSNAME_RESERVED_RENAME";
      break;
    case LC_CLASSNAME_DELETED:
      str_action = "LC_CLASSNAME_DELETED";
      break;
    case LC_CLASSNAME_DELETED_RENAME:
      str_action = "LC_CLASSNAME_DELETED_RENAME";
      break;
    case LC_CLASSNAME_EXIST:
      str_action = "LC_CLASSNAME_EXIST";
      break;
    default:
      assert (false);
      str_action = "UNKNOWN";
      break;
    }

  fprintf (outfp,
	   "     action = %s, OID = %d|%d|%d\n",
	   str_action, entry->oid.volid, entry->oid.pageid,
	   entry->oid.slotid);

  return (true);
}

/*
 * locator_dump_class_names () - Dump all classname entries
 *
 * return:
 *
 *    out_fp(in): output file
 *
 * Note:Dump all names of classes and their corresponding OIDs.
 *              This function is used for debugging purposes.
 */
void
locator_dump_class_names (THREAD_ENTRY * thread_p, FILE * out_fp)
{
  if (csect_enter_as_reader (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
			     INF_WAIT) != NO_ERROR)
    {
      return;
    }
  (void) mht_dump (out_fp, locator_Mht_classnames, false,
		   locator_print_class_name, NULL);

  ehash_dump (thread_p, out_fp, locator_Eht_classnames);

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * locator_check_class_on_heap () - Check the classname on the heap object
 *
 * return: NO_ERROR continue checking, error code stop checking, bad error
 *
 *   classname(in): The expected class name
 *   classoid(in): The class object identifier
 *   xvalid(in): Could be set as a side effect to either: DISK_INVALID,
 *                 DISK_ERROR when an inconsistency is found. Otherwise, it is
 *                 left in touch. The caller should initialize it to DISK_VALID
 *
 * Note: Check if the classname of the class associated with classoid
 *              is the same as the given class name.
 *              If class does not exist, or its name is different from the
 *              given one, xvalid is set to DISK_INVALID. In the case of other
 *              kind of error, xvalid is set to DISK_ERROR.
 *              If xvalid is set to DISK_ERROR, we return false to stop
 *              the map hash, otheriwse, we return true to continue.
 */
static int
locator_check_class_on_heap (THREAD_ENTRY * thread_p, void *classname,
			     void *classoid, void *xvalid)
{
  DISK_ISVALID *isvalid = (DISK_ISVALID *) xvalid;
  const char *class_name;
  char *heap_class_name;
  OID *class_oid;

  class_name = (char *) classname;
  class_oid = (OID *) classoid;

  heap_class_name =
    heap_get_class_name_alloc_if_diff (thread_p, class_oid,
				       (char *) classname);
  if (heap_class_name == NULL)
    {
      if (er_errid () == ER_HEAP_UNKNOWN_OBJECT)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LC_INCONSISTENT_CLASSNAME_TYPE4, 4,
		  class_name, class_oid->volid, class_oid->pageid,
		  class_oid->slotid);
	  *isvalid = DISK_INVALID;
	}
      else
	{
	  *isvalid = DISK_ERROR;
	}

      goto error;
    }
  /*
   * Compare the classname pointers. If the same pointers classes are the
   * same since the class was no malloc
   */
  if (heap_class_name != classname)
    {
      /*
       * Different names
       */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LC_INCONSISTENT_CLASSNAME_TYPE1, 5,
	      class_oid->volid, class_oid->pageid, class_oid->slotid,
	      class_name, heap_class_name);
      *isvalid = DISK_INVALID;
      free_and_init (heap_class_name);
    }

error:
  if (*isvalid == DISK_ERROR)
    {
      return ER_FAILED;
    }
  else
    {
      return NO_ERROR;
    }
}

/*
 * locator_check_class_names () - Check classname consistency
 *
 * return: DISK_ISVALID
 *
 * Note: Check the consistency of the classname_to_oid and the heap of
 *              classes and vice versa.
 */
DISK_ISVALID
locator_check_class_names (THREAD_ENTRY * thread_p)
{
  DISK_ISVALID isvalid;
  RECDES peek;			/* Record descriptor for peeking object */
  HFID *root_hfid;
  OID class_oid;
  char *class_name = NULL;
  OID class_oid2;
  HEAP_SCANCACHE scan_cache;
  EH_SEARCH search;

  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      /*
       * Some kind of failure. We must notify the error to the caller.
       */
      return DISK_ERROR;
    }

  /*
   * CHECK 1: Each class that is found by scanning the heap of classes, must
   *          be part of the permanent classname_to_oid table.
   */

  /* Find the heap for the classes */

  /*
   * Find every single class
   */

  root_hfid = boot_find_root_heap ();
  if (root_hfid == NULL)
    {
      goto error;
    }
  if (heap_scancache_start (thread_p, &scan_cache, root_hfid,
			    oid_Root_class_oid, true) != NO_ERROR)
    {
      goto error;
    }

  class_oid.volid = root_hfid->vfid.volid;
  class_oid.pageid = NULL_PAGEID;
  class_oid.slotid = NULL_SLOTID;

  isvalid = DISK_VALID;
  while (heap_next (thread_p, root_hfid, oid_Root_class_oid, &class_oid,
		    &peek, &scan_cache, PEEK) == S_SUCCESS)
    {
      class_name = or_class_name (&peek);
      /*
       * Make sure that this class exists in classname_to_OID table and that
       * the OIDS matches
       */
      search = ehash_search (thread_p, locator_Eht_classnames,
			     (void *) class_name, &class_oid2);
      if (search != EH_KEY_FOUND)
	{
	  if (search == EH_ERROR_OCCURRED)
	    {
	      isvalid = DISK_ERROR;
	      break;
	    }
	  else
	    {
	      isvalid = DISK_INVALID;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LC_INCONSISTENT_CLASSNAME_TYPE3, 4,
		      class_name, class_oid.volid, class_oid.pageid,
		      class_oid.slotid);
	    }
	}
      else
	{
	  /* Are OIDs the same ? */
	  if (!OID_EQ (&class_oid, &class_oid2))
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LC_INCONSISTENT_CLASSNAME_TYPE2, 7,
		      class_name, class_oid2.volid, class_oid2.pageid,
		      class_oid2.slotid, class_oid.volid, class_oid.pageid,
		      class_oid.slotid);
	      isvalid = DISK_INVALID;
	    }
	}
    }

  /* End the scan cursor */
  if (heap_scancache_end (thread_p, &scan_cache) != NO_ERROR)
    {
      isvalid = DISK_ERROR;
    }

  /*
   * CHECK 2: Same that check1 but from classname_to_OID to existance of class
   */

  if (ehash_map (thread_p, locator_Eht_classnames,
		 locator_check_class_on_heap, &isvalid) != NO_ERROR)
    {
      isvalid = DISK_ERROR;
    }

  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return isvalid;

error:
  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

  return DISK_ERROR;
}
#endif

/*
 * Functions related to fetching and flushing
 */

/*
 * xlocator_assign_oid () - Assign a permanent oid
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object will be stored
 *   perm_oid(in/out): Object identifier.. (set as a side effect)
 *   expected_length(in): Expected length of the object
 *   class_oid(in): The class of the instance
 *   classname(in): Optional... classname for classes
 *
 * Note: A permanent oid is assigned, the object associated with that
 *              OID is locked through the new OID. If the object is a class
 *              the transient classname to OID entry is updated to reflect the
 *              newly assigned OID.
 */
int
xlocator_assign_oid (THREAD_ENTRY * thread_p, const HFID * hfid,
		     OID * perm_oid, int expected_length, OID * class_oid,
		     const char *classname)
{
  if (heap_assign_address_with_class_oid (thread_p, hfid, class_oid, perm_oid,
					  expected_length) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (classname != NULL)
    {
      locator_permoid_class_name (thread_p, classname, perm_oid);
    }

  return NO_ERROR;
}

/*
 * locator_find_lockset_missing_class_oids () - Find missing classoids
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockset(in): Request for finding mising classes
 *
 * Note: Find missing classoids in requested area.
 *              The function does not quit when an error is found if the value
 *              of lockset->quit_on_errors is false. In this case the
 *              object with the error is set to a NULL_OID. For example, when
 *              a class of an object does not exist.
 * Note: There must be enough space in the lockset area to define all
 *              missing classes.
 */
static int
locator_find_lockset_missing_class_oids (THREAD_ENTRY * thread_p,
					 LC_LOCKSET * lockset)
{
  LC_LOCKSET_REQOBJ *reqobjs;	/* Description of one instance to fetch */
  LC_LOCKSET_CLASSOF *reqclasses;	/* Description of one class of a
					 * requested object */
  OID class_oid;		/* Uses to hold the class_oid when
				 * it is unknown */
  int i, j;
  int error_code = NO_ERROR;

  /* Locate array of objects and array of classes */

  reqobjs = lockset->objects;
  reqclasses = lockset->classes;

#if defined(RYE_DEBUG)
  i = (sizeof (*lockset)
       + (lockset->num_reqobjs * (sizeof (*reqclasses) + sizeof (*reqobjs))));

  if (lockset->length < i
      || lockset->classes
      != ((LC_LOCKSET_CLASSOF *) (lockset->mem + sizeof (*lockset)))
      || lockset->objects
      < ((LC_LOCKSET_REQOBJ *) (lockset->classes + lockset->num_reqobjs)))
    {
      er_log_debug (ARG_FILE_LINE,
		    "locator_find_lockset_missing_class_oids: "
		    " *** SYSTEM ERROR. Requesting area is incorrect,\n"
		    " either area is too small %d (expect at least %d),\n"
		    " pointer to classes %p (expected %p), or\n"
		    " pointer to objects %p (expected >= %p) are incorrect\n",
		    lockset->length, i, lockset->classes,
		    ((LC_LOCKSET_CLASSOF *) (lockset->mem +
					     sizeof (*lockset))),
		    lockset->objects,
		    ((LC_LOCKSET_REQOBJ *) (lockset->classes +
					    lockset->num_reqobjs)));
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");

      error_code = ER_GENERIC_ERROR;
      goto error;
    }
#endif /* RYE_DEBUG */


  /*
   * All class identifiers of requested objects must be known. Find the ones
   * that the caller is unaware
   */

  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      if (reqobjs[i].class_index != -1 || OID_ISNULL (&reqobjs[i].oid))
	{
	  continue;
	}
      /*
       * Caller does not know the class identifier of the requested object.
       * Get the class identifier from disk
       */
      if (heap_get_class_oid (thread_p, &class_oid, &reqobjs[i].oid) == NULL)
	{
	  /*
	   * Unable to find the class of the object. Remove the object from
	   * the list of requested objects.
	   */
	  OID_SET_NULL (&reqobjs[i].oid);
	  if (lockset->quit_on_errors != false)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	  continue;
	}

      /*
       * Insert this class in the list of classes of requested objects.
       * Make sure that the class is not already present in the list.
       */

      for (j = 0; j < lockset->num_classes_of_reqobjs; j++)
	{
	  if (OID_EQ (&class_oid, &reqclasses[j].oid))
	    {
	      /* OID is already in the list */
	      reqobjs[i].class_index = j;
	      break;
	    }
	}
      if (j >= lockset->num_classes_of_reqobjs)
	{
	  /* OID is not in the list */
	  COPY_OID (&reqclasses[lockset->num_classes_of_reqobjs].oid,
		    &class_oid);
	  reqobjs[i].class_index = lockset->num_classes_of_reqobjs;
	  lockset->num_classes_of_reqobjs++;
	}
    }

error:
  return error_code;
}

static SCAN_CODE
locator_return_object_assign (UNUSED_ARG THREAD_ENTRY * thread_p,
			      LOCATOR_RETURN_NXOBJ * assign,
			      OID * class_oid, OID * oid, SCAN_CODE scan)
{
  int round_length;		/* Length of object rounded to integer alignment */

  switch (scan)
    {
    case S_SUCCESS:
      /*
       * The cached object was obsolete.
       */
      round_length = DB_ALIGN (assign->recdes.length, MAX_ALIGNMENT);
      if (assign->recdes.area_size <
	  (round_length + (int) sizeof (*assign->obj)))
	{
	  assign->recdes.area_size -= (round_length + sizeof (*assign->obj));
	  scan = S_DOESNT_FIT;
	}
      else
	{
	  assign->mobjs->num_objs++;

	  COPY_OID (&assign->obj->class_oid, class_oid);
	  COPY_OID (&assign->obj->oid, oid);
	  assign->obj->hfid = NULL_HFID;
	  assign->obj->length = assign->recdes.length;
	  assign->obj->offset = assign->area_offset;
	  assign->obj->operation = LC_FETCH;
	  assign->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (assign->obj);

#if !defined(NDEBUG)
	  /* suppress valgrind UMW error */
	  memset (assign->recdes.data + assign->recdes.length, 0,
		  MIN (round_length - assign->recdes.length,
		       assign->recdes.area_size - assign->recdes.length));
#endif
	  assign->recdes.length = round_length;
	  assign->area_offset += round_length;
	  assign->recdes.data += round_length;
	  assign->recdes.area_size -= round_length + sizeof (*assign->obj);
	}

      break;

    case S_DOESNT_EXIST:
      /*
       * The object does not exist
       */
      if (assign->recdes.area_size < (int) sizeof (*assign->obj))
	{
	  assign->recdes.area_size -= sizeof (*assign->obj);
	  scan = S_DOESNT_FIT;
	}
      else
	{
	  assign->mobjs->num_objs++;

	  /* Indicate to the caller that the object does not exist any longer */
	  COPY_OID (&assign->obj->class_oid, class_oid);
	  COPY_OID (&assign->obj->oid, oid);
	  assign->obj->hfid = NULL_HFID;
	  assign->obj->length = -1;
	  assign->obj->offset = -1;
	  assign->obj->operation = LC_FETCH_DELETED;
	  assign->obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (assign->obj);
	  assign->recdes.area_size -= sizeof (*assign->obj);
	}

      break;

    default:
      break;
    }

  return scan;
}

/*
 * locator_return_object () - Place an object in communication area
 *
 * return: SCAN_CODE
 *              (Either of S_SUCCESS,
 *                         S_DOESNT_FIT,
 *                         S_DOESNT_EXIST,
 *                         S_ERROR)
 *
 *   assign(in/out): Description for returing the desired object
 *                  (Set as a side effect to next free area)
 *   class_oid(in):
 *   oid(in): Identifier of the desired object
 *
 * Note: The desired object is placed in the assigned return area when
 *              the state of the object(chn) in the client workspace is
 *              different from the one on disk. If the object does not fit in
 *              assigned return area, the length of the object is returned as
 *              a negative value in the area recdes length.
 */
static SCAN_CODE
locator_return_object (THREAD_ENTRY * thread_p,
		       LOCATOR_RETURN_NXOBJ * assign,
		       OID * class_oid, OID * oid)
{
  SCAN_CODE scan;		/* Scan return value for next operation */

  /*
   * The next object is placed in the assigned recdes area iff the cached
   * object is obsolete and the object fits in the recdes
   */

  scan = heap_get (thread_p, oid, &assign->recdes, assign->ptr_scancache,
		   COPY);

  scan =
    locator_return_object_assign (thread_p, assign, class_oid, oid, scan);

  return scan;
}

/*
 * xlocator_fetch () - Lock and fetch an object, and prefetch some other objects
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   thrd(in):
 *   oid(in): Object identifier of requested object
 *   lock(in): Lock to acquire before the object is fetched
 *   class_oid(in): Class identifier of the object
 *   prefetching(in): true when pretching of neighbors is desired
 *   fetch_area(in/out): Pointer to area where the objects are placed
                         (set to point to fetching area)
 *
 * Note: This function locks and fetches the object associated with the
 *              given oid. The object is only placed in the fetch area when
 *              the state of the object (chn) in the workspace (client) is
 *              different from the one on disk. The class along with several
 *              other neighbors of the object may also be included in the
 *              fetch area. It is up to the caller if the additional objects
 *              are cached. Fetch_area is set to NULL when there is an error
 *              or when there is not a need to send any object back since the
 *              cache coherent numbers were the same as those on disk. The
 *              caller must check the return value of the function to find out
 *              if there was any error.
 *
 *       The returned fetch area should be freed by the caller.
 */
int
xlocator_fetch (THREAD_ENTRY * thread_p, OID * oid, LOCK lock,
		OID * class_oid, int prefetching, LC_COPYAREA ** fetch_area)
{
  OID tmp_oid;			/* Uses to hold the class_oid when
				 * it is not know by the caller */
  LC_COPYAREA_DESC prefetch_des;
  LOCATOR_RETURN_NXOBJ nxobj;	/* Description to return next obj   */
  int copyarea_length;
  SCAN_CODE scan = S_ERROR;
  int error_code = NO_ERROR;
  DB_VALUE lock_val;

  if (class_oid == NULL)
    {
      /* The class_oid is not known by the caller. */
      class_oid = &tmp_oid;
      OID_SET_NULL (class_oid);
    }

  if (OID_ISNULL (class_oid))
    {
      /*
       * Caller does not know the class of the object. Get the class
       * identifier from disk
       */
      if (heap_get_class_oid (thread_p, class_oid, oid) == NULL)
	{
	  /* Unable to find the class of the object.. return */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  assert (!OID_ISNULL (class_oid));

  /* Obtain the desired lock */
  if (lock != NULL_LOCK)
    {
      DB_MAKE_OID (&lock_val, oid);
      if (lock_object (thread_p, &lock_val, lock, LK_UNCOND_LOCK) !=
	  LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  /*
   * Fetch the object and its class
   */

  error_code = NO_ERROR;

  /* Assume that the needed object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (true)
    {
      nxobj.comm_area = *fetch_area =
	locator_allocate_copy_area_by_length (copyarea_length);
      if (nxobj.comm_area == NULL)
	{
	  nxobj.mobjs = NULL;
	  error_code = ER_FAILED;
	  goto error;
	}

      nxobj.ptr_scancache = NULL;
      nxobj.mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (nxobj.comm_area);
      nxobj.obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_IN_COPYAREA (nxobj.comm_area, &nxobj.recdes);
      nxobj.area_offset = 0;
      nxobj.mobjs->num_objs = 0;

      /* Get the interested object first */

      scan = locator_return_object (thread_p, &nxobj, class_oid, oid);
      if (scan == S_SUCCESS)
	{
	  break;
	}
      /* Get the real length of current fetch/copy area */

      copyarea_length = nxobj.comm_area->length;
      locator_free_copy_area (nxobj.comm_area);

      /*
       * If the object does not fit even when the copy area seems to be
       * large enough, increase the copy area by at least one page size.
       */

      if (scan != S_DOESNT_FIT)
	{
	  nxobj.comm_area = *fetch_area = NULL;
	  error_code = ER_FAILED;
	  goto error;
	}
      if ((-nxobj.recdes.length) > copyarea_length)
	{
	  copyarea_length =
	    (DB_ALIGN (-nxobj.recdes.length, MAX_ALIGNMENT) +
	     sizeof (*nxobj.mobjs));
	}
      else
	{
	  copyarea_length += DB_PAGESIZE;
	}
    }

  /*
   * Then, get the interested class, if given class coherency number is not
   * current.
   */

  scan =
    locator_return_object (thread_p, &nxobj, oid_Root_class_oid, class_oid);
  if (scan == S_SUCCESS && nxobj.mobjs->num_objs == 2)
    {
      LC_COPYAREA_ONEOBJ *first, *second;
      LC_COPYAREA_ONEOBJ save;
      /*
       * It is better if the class is cached first, so swap the
       * description. The object was stored first because it has
       * priority of retrieval, however, if both the object and its
       * class fits, the class should go first for performance reasons
       */

      /* Save the object information, then move the class information */
      first = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      second = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (first);

      /* Swap the values */
      save = *first;
      *first = *second;
      *second = save;
    }

  prefetch_des.mobjs = nxobj.mobjs;
  prefetch_des.obj = &nxobj.obj;

  prefetch_des.offset = &nxobj.area_offset;
  prefetch_des.recdes = &nxobj.recdes;

  /*
   * Find any decache notifications and prefetch any neighbors of the
   * instance
   */

  if (prefetching && nxobj.mobjs->num_objs > 0)
    {
      error_code = heap_prefetch (thread_p, class_oid, oid, &prefetch_des);
    }

  if (nxobj.mobjs->num_objs == 0)
    {
      /*
       * Don't need to send anything. The cache coherency numbers were
       * identical. Deallocate the area and return without failure
       */
      locator_free_copy_area (nxobj.comm_area);
      *fetch_area = NULL;
    }

error:

  return error_code;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * xlocator_get_class () - Lock and fetch the class of an instance, and prefetch
 *                    given instance and some other instances of class
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   class_oid(in/out): Class identifier of the object. (It is set as a side
 *                 effect when its initial value is a null OID)
 *   oid(in): Object identifier of the instance of the desired class
 *   lock(in): Lock to acquire before the class is acquired/fetched
 *                 Note that the lock is for the class.
 *   prefetching(in): true when pretching of neighbors is desired
 *   fetch_area(in/out): Pointer to area where the objects are placed (set to
 *                 point to fetching area)
 *
 * Note: This function locks and fetches the class of the given
 *              instance. The class is only placed in a communication copy
 *              area when the state of the class (class_chn) in the workspace
 *              (client) is different from the one on disk. Other neighbors of
 *              the class are included in the copy_area. It is up to the
 *              caller if the additional classes are cached.  Fetch_area is
 *              set to NULL when there is an error or when there is not a need
 *              to send any object back since the cache coherent numbers were
 *              the same as those on disk. The caller must check the return
 *              value of the function to find out if there was any error.
 *
 * Note: The returned fetch area should be freed by the caller.
 */
int
xlocator_get_class (THREAD_ENTRY * thread_p, OID * class_oid,
		    const OID * oid, LOCK lock, int prefetching,
		    LC_COPYAREA ** fetch_area)
{
  int error_code;
  DB_VALUE lock_val;

  if (OID_ISNULL (class_oid))
    {
      /*
       * Caller does not know the class of the object. Get the class identifier
       * from disk
       */
      if (heap_get_class_oid (thread_p, class_oid, oid) == NULL)
	{
	  /*
	   * Unable to find out the class identifier.
	   */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  /* Now acquired the desired lock */
  if (lock != NULL_LOCK)
    {
      /* Now acquired the desired lock */
      DB_MAKE_OID (&lock_val, class_oid);
      if (lock_object (thread_p, &lock_val, lock,
		       LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *fetch_area = NULL;
	  return ER_FAILED;
	}
    }

  /*
   * Now fetch the class, the instance and optinally prefetch some
   * neighbors of the instance
   */

  error_code = xlocator_fetch (NULL, class_oid, NULL_LOCK,
			       oid_Root_class_oid, prefetching, fetch_area);

  return error_code;
}
#endif

/*
 * xlocator_fetch_all () - Fetch all instances of a class
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap file where the instances of the class are placed
 *   lock(in): Lock to acquired (Set as a side effect to NULL_LOCKID)
 *   class_oid(in): Class identifier of the instances to fetch
 *   nobjects(out): Total number of objects to fetch.
 *   nfetched(out): Current number of object fetched.
 *   last_oid(out): Object identifier of last fetched object
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_fetch_all (THREAD_ENTRY * thread_p, const HFID * hfid, LOCK * lock,
		    OID * class_oid, INT64 * nobjects, INT64 * nfetched,
		    OID * last_oid, LC_COPYAREA ** fetch_area)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in
				 * area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area  */
  RECDES recdes;		/* Record descriptor for
				 * insertion */
  int offset;			/* Place to store next object in
				 * area */
  int round_length;		/* Length of object rounded to
				 * integer alignment */
  int copyarea_length;
  OID oid;
  HEAP_SCANCACHE scan_cache;
  SCAN_CODE scan;
  int error_code = NO_ERROR;
  DB_VALUE lock_val;

  if (OID_ISNULL (last_oid))
    {
      /* FIRST TIME. */

      /* Obtain the desired lock for the class scan */
      DB_MAKE_OID (&lock_val, class_oid);
      if (*lock != NULL_LOCK
	  && lock_object (thread_p, &lock_val, *lock,
			  LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *fetch_area = NULL;
	  *lock = NULL_LOCK;
	  *nobjects = -1;
	  *nfetched = -1;

	  error_code = ER_FAILED;
	  goto error;
	}

      /* Get statistics */
      last_oid->volid = hfid->vfid.volid;
      last_oid->pageid = NULL_PAGEID;
      last_oid->slotid = NULL_SLOTID;
      /* Estimate the number of objects to be fetched */
      *nfetched = 0;
      error_code = heap_estimate_num_objects (thread_p, hfid, nobjects);
      if (error_code != NO_ERROR)
	{
	  *fetch_area = NULL;
	  goto error;
	}
    }

  /* Set OID to last fetched object */
  COPY_OID (&oid, last_oid);

  /* Start a scan cursor for getting several classes */
  error_code = heap_scancache_start (thread_p, &scan_cache, hfid,
				     class_oid, true);
  if (error_code != NO_ERROR)
    {
      *fetch_area = NULL;

      goto error;
    }

  /* Assume that the next object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (true)
    {
      *fetch_area = locator_allocate_copy_area_by_length (copyarea_length);
      if (*fetch_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &scan_cache);

	  error_code = ER_FAILED;
	  goto error;
	}

      mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (*fetch_area);
      LC_RECDES_IN_COPYAREA (*fetch_area, &recdes);
      obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
      mobjs->num_objs = 0;
      offset = 0;

      while ((scan = heap_next (thread_p, hfid, class_oid, &oid, &recdes,
				&scan_cache, COPY)) == S_SUCCESS)
	{
	  mobjs->num_objs++;
	  COPY_OID (&obj->class_oid, class_oid);
	  COPY_OID (&obj->oid, &oid);
	  obj->hfid = NULL_HFID;
	  obj->length = recdes.length;
	  obj->offset = offset;
	  obj->operation = LC_FETCH;
	  obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
	  round_length = DB_ALIGN (recdes.length, MAX_ALIGNMENT);
#if !defined(NDEBUG)
	  /* suppress valgrind UMW error */
	  memset (recdes.data + recdes.length, 0,
		  MIN (round_length - recdes.length,
		       recdes.area_size - recdes.length));
#endif
	  offset += round_length;
	  recdes.data += round_length;
	  recdes.area_size -= round_length + sizeof (*obj);

	  if (mobjs->num_objs == DB_INT32_MAX)
	    {
	      /* Prevent overflow */
	      break;
	    }
	}

      if (scan != S_DOESNT_FIT || mobjs->num_objs > 0)
	{
	  break;
	}
      /*
       * The first object does not fit into given copy area
       * Get a larger area
       */

      /* Get the real length of current fetch/copy area */
      copyarea_length = (*fetch_area)->length;
      locator_free_copy_area (*fetch_area);

      /*
       * If the object does not fit even when the copy area seems to be
       * large enough, increase the copy area by at least one page size.
       */

      if ((-recdes.length) > copyarea_length)
	{
	  copyarea_length =
	    DB_ALIGN (-recdes.length, MAX_ALIGNMENT) + sizeof (*mobjs);
	}
      else
	{
	  copyarea_length += DB_PAGESIZE;
	}
    }

  if (scan == S_END)
    {
      /*
       * This is the end of the loop. Indicate the caller that no more calls
       * are needed by setting nobjects and nfetched to the same value.
       */
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  locator_free_copy_area (*fetch_area);
	  *fetch_area = NULL;
	  *nobjects = *nfetched = -1;

	  goto error;
	}

      *nfetched += mobjs->num_objs;
      *nobjects = *nfetched;
      OID_SET_NULL (last_oid);
    }
  else if (scan == S_ERROR)
    {
      /* There was an error.. */
      (void) heap_scancache_end (thread_p, &scan_cache);

      locator_free_copy_area (*fetch_area);
      *fetch_area = NULL;
      *nobjects = *nfetched = -1;

      error_code = ER_FAILED;
      goto error;
    }
  else if (mobjs->num_objs != 0)
    {
      heap_scancache_end_when_scan_will_resume (thread_p, &scan_cache);
      /* Set the last_oid.. and the number of fetched objects */
      obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
      COPY_OID (last_oid, &obj->oid);
      *nfetched += mobjs->num_objs;
      /*
       * If the guess on the number of objects to fetch was low, reset the
       * value, so that the caller continue to call us until the end of the
       * scan
       */
      if (*nobjects <= *nfetched)
	{
	  *nobjects = *nfetched + 10;
	}
    }
  else
    {
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

error:
  return error_code;
}

/*
 * xlocator_fetch_lockset () - Lock and fetch many objects
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockset(in/out): Request for finding mising classes and the lock requested
 *                  objects (Set as a side effect)
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_fetch_lockset (THREAD_ENTRY * thread_p, LC_LOCKSET * lockset,
			LC_COPYAREA ** fetch_area)
{
  LOCATOR_RETURN_NXOBJ nxobj;	/* Description to return next obj   */
  struct lc_lockset_reqobj *reqobjs;	/* Description of requested objects */
  struct lc_lockset_classof *reqclasses;	/* Description of classes of requested
						 * objects. */
  int copyarea_length;
  SCAN_CODE scan = S_SUCCESS;
  int i, j;
  LC_COPYAREA *new_area = NULL;
  int error_code = NO_ERROR;


  *fetch_area = NULL;

  reqobjs = lockset->objects;
  reqclasses = lockset->classes;

  if (lockset->num_reqobjs_processed == -1)
    {
      /*
       * FIRST CALL.
       * Initialize num of object processed.
       * Make sure that all classes are known and lock the classes and objects
       */
      lockset->num_reqobjs_processed = 0;
      lockset->num_classes_of_reqobjs_processed = 0;

      error_code =
	locator_find_lockset_missing_class_oids (thread_p, lockset);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      /* Obtain the locks */

      if (lock_objects_lock_set (thread_p, lockset) != LK_GRANTED)
	{
	  if (lockset->quit_on_errors != false)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	}
    }

  /* Start a scan cursor for getting several classes */
  error_code = heap_scancache_start (thread_p, &nxobj.area_scancache, NULL,
				     NULL, true);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  nxobj.ptr_scancache = &nxobj.area_scancache;

  /*
   * Assume that there are not any objects larger than one page. If there are
   * the number of pages is fixed later.
   */

  copyarea_length = DB_PAGESIZE;

  nxobj.mobjs = NULL;
  nxobj.comm_area = NULL;

  while (scan == S_SUCCESS
	 && ((lockset->num_classes_of_reqobjs_processed
	      < lockset->num_classes_of_reqobjs)
	     || (lockset->num_reqobjs_processed < lockset->num_reqobjs)))
    {
      new_area = locator_allocate_copy_area_by_length (copyarea_length);
      if (new_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &nxobj.area_scancache);
	  /* There was an error.. */
	  if (nxobj.mobjs != NULL)
	    {
	      locator_free_copy_area (nxobj.comm_area);
	      nxobj.comm_area = NULL;
	    }
	  error_code = ER_FAILED;
	  goto error;
	}

      nxobj.comm_area = new_area;

      nxobj.mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (nxobj.comm_area);
      nxobj.obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_IN_COPYAREA (nxobj.comm_area, &nxobj.recdes);
      nxobj.area_offset = 0;
      nxobj.mobjs->num_objs = 0;

      /*
       * CLASSES
       * Place the classes on the communication area, don't place those classes
       * with correct chns.
       */

      for (i = lockset->num_classes_of_reqobjs_processed;
	   scan == S_SUCCESS && i < lockset->num_classes_of_reqobjs; i++)
	{
	  if (OID_ISNULL (&reqclasses[i].oid))
	    {
	      lockset->num_classes_of_reqobjs_processed++;
	      continue;
	    }
	  if (OID_ISTEMP (&reqclasses[i].oid))
	    {
	      lockset->num_classes_of_reqobjs_processed++;
	      continue;
	    }
	  scan =
	    locator_return_object (thread_p, &nxobj, oid_Root_class_oid,
				   &reqclasses[i].oid);
	  if (scan == S_SUCCESS)
	    {
	      lockset->num_classes_of_reqobjs_processed++;
	    }
	  else if (scan == S_DOESNT_FIT && nxobj.mobjs->num_objs == 0)
	    {
	      /*
	       * The first object does not fit into given copy area
	       * Get a larger area
	       */

	      /* Get the real length of current fetch/copy area */

	      copyarea_length = nxobj.comm_area->length;

	      /*
	       * If the object does not fit even when the copy area seems
	       * to be large enough, increase the copy area by at least one
	       * page size.
	       */

	      if ((-nxobj.recdes.length) > copyarea_length)
		{
		  copyarea_length =
		    (DB_ALIGN (-nxobj.recdes.length, MAX_ALIGNMENT) +
		     sizeof (*nxobj.mobjs));
		}
	      else
		{
		  copyarea_length += DB_PAGESIZE;
		}

	      locator_free_copy_area (nxobj.comm_area);
	      nxobj.comm_area = NULL;
	      scan = S_SUCCESS;
	      break;		/* finish the for */
	    }
	  else if (scan != S_DOESNT_FIT
		   && (scan == S_DOESNT_EXIST
		       || lockset->quit_on_errors == false))
	    {
	      OID_SET_NULL (&reqclasses[i].oid);
	      lockset->num_classes_of_reqobjs_processed += 1;
	      scan = S_SUCCESS;
	    }
	  else
	    {
	      break;		/* Quit on errors */
	    }
	}

      if (i >= lockset->num_classes_of_reqobjs)
	{
	  /*
	   * DONE WITH CLASSES... NOW START WITH INSTANCES
	   * Place the instances in the fetching area, don't place those
	   * instances with correct chns or when they have been placed through
	   * the class array
	   */

	  for (i = lockset->num_reqobjs_processed;
	       scan == S_SUCCESS && i < lockset->num_reqobjs; i++)
	    {
	      if (OID_ISNULL (&reqobjs[i].oid)
		  || OID_ISTEMP (&reqobjs[i].oid)
		  || reqobjs[i].class_index == -1
		  || OID_ISNULL (&reqclasses[reqobjs[i].class_index].oid))
		{
		  lockset->num_reqobjs_processed += 1;
		  continue;
		}

	      if (OID_IS_ROOTOID (&reqclasses[reqobjs[i].class_index].oid))
		{
		  /*
		   * The requested object is a class. If this object is a class
		   * of other requested objects, the object has already been
		   * processed in the previous class iteration
		   */
		  for (j = 0; j < lockset->num_classes_of_reqobjs; j++)
		    {
		      if (OID_EQ (&reqobjs[i].oid, &reqclasses[j].oid))
			{
			  /* It has already been processed */
			  lockset->num_reqobjs_processed += 1;
			  break;
			}
		    }
		  if (j < lockset->num_classes_of_reqobjs)
		    {
		      continue;
		    }
		}

	      /* Now return the object */
	      scan =
		locator_return_object (thread_p, &nxobj,
				       &reqclasses[reqobjs[i].
						   class_index].oid,
				       &reqobjs[i].oid);
	      if (scan == S_SUCCESS)
		{
		  lockset->num_reqobjs_processed++;
		  continue;
		}

	      if (scan == S_DOESNT_FIT && nxobj.mobjs->num_objs == 0)
		{
		  /*
		   * The first object does not fit into given copy area
		   * Get a larger area
		   */

		  /* Get the real length of current fetch/copy area */

		  copyarea_length = nxobj.comm_area->length;

		  /*
		   * If the object does not fit even when the copy area
		   * seems to be large enough, increase the copy area by at
		   * least one page size.
		   */

		  if ((-nxobj.recdes.length) > copyarea_length)
		    {
		      copyarea_length = ((-nxobj.recdes.length)
					 + sizeof (*nxobj.mobjs));
		    }
		  else
		    {
		      copyarea_length += DB_PAGESIZE;
		    }

		  locator_free_copy_area (nxobj.comm_area);
		  nxobj.comm_area = NULL;
		  scan = S_SUCCESS;
		  break;	/* finish the for */
		}
	      else if (scan != S_DOESNT_FIT
		       && (scan == S_DOESNT_EXIST
			   || lockset->quit_on_errors == false))
		{
		  OID_SET_NULL (&reqobjs[i].oid);
		  lockset->num_reqobjs_processed += 1;
		  scan = S_SUCCESS;
		}
	    }
	}
    }

  /* End the scan cursor */
  error_code = heap_scancache_end (thread_p, &nxobj.area_scancache);
  if (error_code != NO_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	  nxobj.comm_area = NULL;
	}
      goto error;
    }

  if (scan == S_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	  nxobj.comm_area = NULL;
	}
      error_code = ER_FAILED;
      goto error;
    }
  else if (nxobj.mobjs != NULL && nxobj.mobjs->num_objs == 0)
    {
      locator_free_copy_area (nxobj.comm_area);
      nxobj.comm_area = NULL;
    }
  else
    {
      *fetch_area = nxobj.comm_area;
    }

error:
  return error_code;
}

/*
 * locator_start_force_scan_cache () -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   scan_cache(in/out):
 *   hfid(in):
 *   force_page_allocation(in): force new page allocation
 *   class_oid(in):
 */
int
locator_start_force_scan_cache (THREAD_ENTRY * thread_p,
				HEAP_SCANCACHE * scan_cache,
				const HFID * hfid,
				const int force_page_allocation,
				const OID * class_oid)
{
  return heap_scancache_start_modify (thread_p, scan_cache, hfid,
				      force_page_allocation, class_oid);
}

/*
 * locator_end_force_scan_cache () -
 *
 * return:
 *
 *   scan_cache(in):
 */
void
locator_end_force_scan_cache (THREAD_ENTRY * thread_p,
			      HEAP_SCANCACHE * scan_cache)
{
  heap_scancache_end_modify (thread_p, scan_cache);
}

/*
 * locator_insert_force () - Insert the given object on this heap
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object is going to be inserted
 *   class_oid(in):
 *   oid(in/out): The new object identifier
 *   recdes(in): The object in disk format
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *              between heap changes.
 *   force_count(in):
 *
 * Note: The given object is inserted on this heap and all appropriate
 *              index entries are inserted.
 */
static int
locator_insert_force (THREAD_ENTRY * thread_p, HFID * hfid, OID * class_oid,
		      OID * oid, RECDES * recdes,
		      HEAP_SCANCACHE * scan_cache, int *force_count)
{
  char *classname;		/* Classname to update */
  LC_COPYAREA *cache_attr_copyarea = NULL;
  int error_code = NO_ERROR;
  OID real_class_oid;
  HFID real_hfid;
  HEAP_SCANCACHE *local_scan_cache = NULL;

  assert (class_oid != NULL && !OID_ISNULL (class_oid));

  HFID_COPY (&real_hfid, hfid);
  COPY_OID (&real_class_oid, class_oid);

  local_scan_cache = scan_cache;

  *force_count = 0;

  /*
   * This is a new object. The object must be locked in exclusive mode,
   * once its OID is assigned. We just do it for the classes, the new
   * instances are not locked since another client cannot get to them,
   * in any way. How can a client know their OIDs
   */

  /* insert object and lock it */

  if (heap_insert (thread_p, &real_hfid, &real_class_oid, oid, recdes,
		   local_scan_cache) == NULL)
    {
      /*
       * Problems inserting the object...Maybe, the transaction should be
       * aborted by the caller...Quit..
       */
      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  error_code = ER_FAILED;
	}
      goto error2;
    }

  if (OID_IS_ROOTOID (&real_class_oid))
    {
      /*
       * A CLASS: Add the classname to class_OID entry and add the class
       *          to the catalog. Update both the permanent and transient
       *          classname tables
       *          remove XASL cache entries which is relevant with that class
       */

      classname = or_class_name (recdes);

      if (ehash_insert (thread_p, locator_Eht_classnames,
			(void *) classname, oid) == NULL)
	{
	  /*
	   * error inserting the hash entry information.
	   *
	   * Maybe the transaction should be aborted by the caller.
	   */
	  error_code = er_errid ();
	  if (error_code == NO_ERROR)
	    {
	      error_code = ER_FAILED;
	    }
	  goto error1;
	}

      /* Indicate new oid to transient table */
      locator_permoid_class_name (thread_p, classname, oid);

      if (!OID_IS_ROOTOID (oid) && catalog_insert (thread_p, recdes, oid) < 0)
	{
	  /*
	   * There is an error inserting the hash entry or catalog
	   * information. Maybe, the transaction should be aborted by
	   * the caller...Quit
	   */
	  error_code = er_errid ();
	  if (error_code == NO_ERROR)
	    {
	      error_code = ER_FAILED;
	    }
	  goto error1;
	}

      if (catcls_Enable == true
	  && catcls_insert_catalog_classes (thread_p, recdes) != NO_ERROR)
	{
	  error_code = er_errid ();
	  if (error_code == NO_ERROR)
	    {
	      error_code = ER_FAILED;
	    }
	  goto error1;
	}
    }
  else
    {
      /*
       * AN INSTANCE: Apply the necessary index insertions
       */
      error_code = locator_add_or_remove_index (thread_p, recdes, oid,
						&real_class_oid, true, true,
						true, &real_hfid);
      if (error_code != NO_ERROR)
	{
	  goto error1;
	}
    }

  *force_count = 1;

error1:
  /* update the OID of the class with the actual partition in which
   * the object was inserted
   */
  COPY_OID (class_oid, &real_class_oid);
  HFID_COPY (hfid, &real_hfid);

error2:
  if (cache_attr_copyarea != NULL)
    {
      locator_free_copy_area (cache_attr_copyarea);
    }

  return error_code;
}

/*
 * locator_update_force () - Update the given object
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object is going to be inserted
 *   class_oid(in):
 *   oid(in): The object identifier
 *   oldrecdes(in):
 *   recdes(in):  The object in disk format
 *   att_id(in): Updated attr id array
 *   n_att_id(in): Updated attr id array length
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                   between heap changes.
 *   force_count(out):
 *   repl_inf(in): replication info
 *
 * Note: The given object is updated on this heap and all appropriate
 *              index entries are updated.
 */
static int
locator_update_force (THREAD_ENTRY * thread_p, HFID * hfid, OID * class_oid,
		      OID * oid, RECDES * oldrecdes, RECDES * recdes,
		      ATTR_ID * att_id, int n_att_id,
		      HEAP_SCANCACHE * scan_cache, int *force_count)
{
  char *old_classname = NULL;	/* Classname that may have been
				 * renamed */
  char *classname = NULL;	/* Classname to update */
  bool isold_object;		/* Make sure that this is an old
				 * object */
  RECDES copy_recdes = RECDES_INITIALIZER;
  SCAN_CODE scan;

  LC_COPYAREA *cache_attr_copyarea = NULL;
  int error = NO_ERROR;
  HEAP_SCANCACHE *local_scan_cache;

  assert (class_oid != NULL);
  assert (!OID_ISNULL (class_oid));

  /*
   * While scanning objects, the given scancache does not fix the last
   * accessed page. So, the object must be copied to the record descriptor.
   */
  copy_recdes.data = NULL;

  *force_count = 0;

  if (OID_IS_ROOTOID (class_oid))
    {
      /*
       * A CLASS: classes do not have any indices...however, the classname
       * to oid table may need to be updated
       */
      classname = or_class_name (recdes);
      old_classname = heap_get_class_name_alloc_if_diff (thread_p, oid,
							 classname);
      /*
       * Compare the classname pointers. If the same pointers classes are the
       * same since the class was no malloc
       */
      if (old_classname != NULL && old_classname != classname)
	{
	  /* Different names, the class was renamed. */
	  OID existing_oid;

	  /* Make it sure there's no existing class which has new class name */
	  error = ehash_search (thread_p, locator_Eht_classnames,
				classname, &existing_oid);
	  if (error != EH_KEY_NOTFOUND)
	    {
	      if (error == EH_KEY_FOUND)
		{
		  error = ER_LC_CLASSNAME_EXIST;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
			  1, classname);
		}
	      else
		{
		  error = er_errid ();
		  error = ((error == NO_ERROR) ? ER_FAILED : error);
		}
	      free_and_init (old_classname);
	      goto exit_on_error;
	    }

	  error = NO_ERROR;
	  if (ehash_insert (thread_p, locator_Eht_classnames,
			    classname, oid) == NULL
	      || ehash_delete (thread_p, locator_Eht_classnames,
			       old_classname) == NULL)
	    {
	      /*
	       * Problems inserting/deleting the new name to the classname to
	       * OID table.
	       * Maybe, the transaction should be aborted by the caller.
	       * Quit..
	       */
	      free_and_init (old_classname);

	      error = ER_FAILED;
	      goto exit_on_error;
	    }
	}

      error = heap_classrepr_decache_and_lock (thread_p, oid);
      if (error != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if ((catcls_Enable == true) && (old_classname != NULL))
	{
	  error = catcls_update_catalog_classes (thread_p,
						 old_classname, recdes);
	  if (error != NO_ERROR)
	    {
	      heap_classrepr_unlock_class (oid);
	      goto exit_on_error;
	    }
	}

      if (heap_update (thread_p, hfid, class_oid, oid, recdes, &isold_object,
		       scan_cache) == NULL)
	{
	  /*
	   * Problems updating the object...Maybe, the transaction should be
	   * aborted by the caller...Quit..
	   */
	  error = ER_FAILED;
	  heap_classrepr_unlock_class (oid);
	  goto exit_on_error;
	}

      if (isold_object)
	{
	  /* Update the catalog as long as it is not the root class */
	  if (!OID_IS_ROOTOID (oid))
	    {
	      error = catalog_update (thread_p, recdes, oid);
	      if (error < 0)
		{
		  /*
		   * An error occurred during the update of the catalog
		   */
		  heap_classrepr_unlock_class (oid);
		  goto exit_on_error;
		}
	    }
	  if (old_classname != NULL && old_classname != classname)
	    {
	      free_and_init (old_classname);
	    }
	}
      else
	{
	  /*
	   * NEW CLASS
	   * The class was flushed for first time. Add the classname to
	   * class_OID entry and add the class to the catalog. We don't need
	   * to update the transient table since the class has already a
	   * permananet OID...
	   */
	  classname = or_class_name (recdes);
	  if (ehash_insert (thread_p, locator_Eht_classnames,
			    (void *) classname, oid) == NULL
	      || (!OID_IS_ROOTOID (oid)
		  && catalog_insert (thread_p, recdes, oid) < 0))
	    {
	      /*
	       * There is an error inserting the hash entry or catalog
	       * information. The transaction must be aborted by the caller
	       */
	      error = ER_FAILED;
	      heap_classrepr_unlock_class (oid);
	      goto exit_on_error;
	    }
	  if (catcls_Enable == true)
	    {
	      error = catcls_insert_catalog_classes (thread_p, recdes);
	      if (error != NO_ERROR)
		{
		  heap_classrepr_unlock_class (oid);
		  goto exit_on_error;
		}
	    }
	}
#if defined(SERVER_MODE)
      assert (heap_classrepr_has_cache_entry (thread_p, oid) == false);
#endif

      heap_classrepr_unlock_class (oid);

      assert (prm_get_integer_value (PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES) >=
	      1000);

      /* remove XASL cache entries which is relevant with that class */
      if (!OID_IS_ROOTOID (oid)
	  && qexec_remove_xasl_cache_ent_by_class (thread_p, oid,
						   1) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_update_force:"
			" qexec_remove_xasl_cache_ent_by_class"
			" failed for class { %d %d %d }\n",
			oid->pageid, oid->slotid, oid->volid);
	}

    }
  else
    {
      local_scan_cache = scan_cache;

      /* AN INSTANCE: Update indices if any */

      if (oldrecdes == NULL)
	{
	  scan = heap_get (thread_p, oid, &copy_recdes, local_scan_cache,
			   COPY);
	  oldrecdes = &copy_recdes;
	}
      else
	{
	  scan = S_SUCCESS;
	}

      if (heap_update (thread_p, hfid, class_oid, oid, recdes, &isold_object,
		       local_scan_cache) == NULL)
	{
	  /*
	   * Problems updating the object...Maybe, the transaction should be
	   * aborted by the caller...Quit..
	   */
	  error = ER_FAILED;
	  goto exit_on_error;
	}

      if (scan == S_SUCCESS)
	{
	  /* Update the indices */
	  error = locator_update_index (thread_p, recdes, oldrecdes,
					att_id, n_att_id, oid, class_oid,
					true, true);
	  if (error != NO_ERROR)
	    {
	      /*
	       * There is an error updating the index... Quit... The
	       * transaction must be aborted by the caller
	       */
	      goto exit_on_error;
	    }
	}
      else
	{
	  /*
	   * We could not get the object.
	   * The object may be a new instance, that is only the address
	   * (no content) is known by the heap manager.
	   */
	  int err_id = er_errid ();

	  if (err_id == ER_HEAP_NODATA_NEWADDRESS)
	    {
	      er_clear ();	/* clear the error code */

	      /* check iff is not shard tabe
	       */
	      assert (!heap_classrepr_is_shard_table (thread_p, class_oid));
	      assert (oid->groupid == or_grp_id (recdes));
	      assert (oid->groupid == GLOBAL_GROUPID);

#if 1				/* defense code */
	      if (oid->groupid != GLOBAL_GROUPID)
		{
		  oid->groupid = GLOBAL_GROUPID;
		}
#endif
	      error = locator_add_or_remove_index (thread_p, recdes, oid,
						   class_oid, true,
						   true, true, hfid);
	      if (error != NO_ERROR)
		{
		  goto exit_on_error;
		}
	    }
	  else
	    {
	      if (err_id == ER_HEAP_UNKNOWN_OBJECT)
		{
		  er_log_debug (ARG_FILE_LINE, "locator_update_force: "
				"unknown oid ( %d|%d|%d )\n",
				oid->pageid, oid->slotid, oid->volid);
		}

	      error = ER_FAILED;
	      goto exit_on_error;
	    }
	}

      /*
       * for replication,
       * We have to set UPDATE LSA number to the log info.
       * The target log info was already created when the locator_update_index
       */
      if (log_does_allow_replication () == true)
	{
	  error = repl_add_update_lsa (thread_p, oid);
	  if (error != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
    }

  *force_count = 1;

exit_on_error:

  if (cache_attr_copyarea != NULL)
    {
      locator_free_copy_area (cache_attr_copyarea);
    }

  return error;
}

/*
 * locator_delete_force () - Delete the given object
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap where the object is going to be inserted
 *   oid(in): The object identifier
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                   between heap changes.
 *   force_count(in):
 *
 * Note: The given object is deleted on this heap and all appropiate
 *              index entries are deleted.
 */
static int
locator_delete_force (THREAD_ENTRY * thread_p, HFID * hfid, OID * oid,
		      HEAP_SCANCACHE * scan_cache, int *force_count)
{
  bool isold_object;		/* Make sure that this is an old object
				 * during the deletion */
  OID class_oid = NULL_OID_INITIALIZER;	/* Class identifier */
  char *classname;		/* Classname to update */
  RECDES copy_recdes = RECDES_INITIALIZER;
  int error_code = NO_ERROR;

  /* Update note :
   *   While scanning objects, the given scancache does not fix the last
   *   accessed page. So, the object must be copied to the record descriptor.
   * Changes :
   *   (1) variable name : peek_recdes => copy_recdes
   *   (2) function call : heap_get(..., PEEK, ...) => heap_get(..., COPY, ...)
   *   (3) SCAN_CODE scan, char *new_area are added
   */

  copy_recdes.data = NULL;

  *force_count = 0;

  /*
   * Is the object a class ?
   */
  isold_object = true;

  if (heap_get_with_class_oid (thread_p, &class_oid, oid, &copy_recdes,
			       scan_cache, COPY) != S_SUCCESS)
    {
      error_code = er_errid ();

      if (error_code == ER_HEAP_NODATA_NEWADDRESS)
	{
	  isold_object = false;
	  er_clear ();		/* clear ER_HEAP_NODATA_NEWADDRESS */

	  error_code = NO_ERROR;
	}
      else if (error_code == ER_HEAP_UNKNOWN_OBJECT)
	{
	  isold_object = false;
	  er_clear ();

	  error_code = NO_ERROR;
	  goto error;
	}
      else
	{
	  /*
	   * Problems reading the object...Maybe, the transaction should be
	   * aborted by the caller...Quit..
	   */
	  if (error_code == NO_ERROR)
	    {
	      error_code = ER_FAILED;
	    }
	  goto error;
	}
    }

  if (isold_object == false)
    {
      OID_SET_NULL (&class_oid);
    }

  if (heap_delete (thread_p, hfid, oid, scan_cache, &class_oid) == NULL)
    {
      /*
       * Problems deleting the object...Maybe, the transaction should be
       * aborted by the caller...Quit..
       */
      er_log_debug (ARG_FILE_LINE,
		    "locator_delete_force: hf_delete failed for tran %d\n",
		    logtb_get_current_tran_index (thread_p));
      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  error_code = ER_FAILED;
	}
      goto error;
    }

  if (isold_object == true && OID_IS_ROOTOID (&class_oid))
    {
      /*
       * A CLASS: Remove classname to classOID entry
       *          remove class from catalog and
       *          remove any indices on that class
       *          remove XASL cache entries which is relevant with that class
       */

      /* Delete the classname entry */
      classname = or_class_name (&copy_recdes);
      if ((ehash_delete (thread_p, locator_Eht_classnames, (void *) classname)
	   == NULL))
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_delete_force: ehash_delete failed for tran %d\n",
			logtb_get_current_tran_index (thread_p));
	  error_code = er_errid ();
	  if (error_code == NO_ERROR)
	    {
	      error_code = ER_FAILED;
	    }
	  goto error;
	}

      /* Note: by now, the client has probably already requested this class
       * be deleted. We try again here
       * just to be sure it has been marked properly.  Note that we would
       * normally want to check the return code, but we must not check the
       * return code for this one function in its current form, because we
       * cannot distinguish between a class that has already been
       * marked deleted and a real error.
       */
      (void) xlocator_delete_class_name (thread_p, classname);
      /* remove from the catalog... when is not the root */
      if (!OID_IS_ROOTOID (oid))
	{
	  error_code = catalog_delete (thread_p, oid);
	  if (error_code != NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "locator_delete_force: ct_delete_catalog failed "
			    "for tran %d\n",
			    logtb_get_current_tran_index (thread_p));
	      goto error;
	    }
	}
      if (catcls_Enable)
	{
	  error_code = catcls_delete_catalog_classes (thread_p, classname,
						      oid);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      assert (prm_get_integer_value (PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES) >=
	      1000);

      /* remove XASL cache entries which is relevant with that class */
      if (!OID_IS_ROOTOID (oid)
	  && qexec_remove_xasl_cache_ent_by_class (thread_p, oid,
						   1) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"locator_delete_force:"
			" qexec_remove_xasl_cache_ent_by_class"
			" failed for class { %d %d %d }\n",
			oid->pageid, oid->slotid, oid->volid);
	}

    }
  else
    {
      /*
       * Likely an INSTANCE: Apply the necessary index deletions
       */
      if (isold_object == true)
	{
	  error_code = locator_add_or_remove_index (thread_p, &copy_recdes,
						    oid, &class_oid,
						    false, true, true, hfid);
	  if (error_code != NO_ERROR)
	    {
	      /*
	       * There is an error deleting the index... Quit... The
	       * transaction must be aborted by the caller
	       */
	      goto error;
	    }

	  assert (!heap_is_big_length (copy_recdes.length)
		  || copy_recdes.type == REC_BIGONE);

	  if (copy_recdes.type == REC_BIGONE)
	    {
	      log_append_del_ovfl_record (thread_p, &class_oid, &copy_recdes);
	    }
	}
    }

  *force_count = 1;

#if defined(ENABLE_UNUSED_FUNCTION)
  if (isold_object == true && !OID_IS_ROOTOID (&class_oid))
    {
      /* decrease the counter of the catalog */
      locator_decrease_catalog_count (thread_p, &class_oid);
    }
#endif

error:

  return error_code;
}

/*
 * locator_repl_add_error_to_copyarea () - place error info into copy area that
 *                                         will be sent to client
 *
 * return:
 *
 *   copy_area(out): copy area where error info will be placed
 *   recdes(in): struct that describes copy_area
 *   obj(in): object that describes the operation which caused an error
 *   key(in): primary key value
 *   err_code(in):
 *   err_msg(in):
 */
static void
locator_repl_add_error_to_copyarea (LC_COPYAREA ** copy_area, RECDES * recdes,
				    LC_COPYAREA_ONEOBJ * obj,
				    DB_IDXKEY * key,
				    int err_code, const char *err_msg)
{
  LC_COPYAREA *new_copy_area = NULL;
  LC_COPYAREA_MANYOBJS *reply_mobjs = NULL;
  LC_COPYAREA_ONEOBJ *reply_obj = NULL;
  char *ptr;
  int packed_length = 0, round_length;
  int prev_offset, prev_length;

  assert (key != NULL);

  reply_mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (*copy_area);

  reply_obj =
    LC_FIND_ONEOBJ_PTR_IN_COPYAREA (reply_mobjs, reply_mobjs->num_objs);

  packed_length += OR_IDXKEY_ALIGNED_SIZE (key);
  packed_length += OR_INT_SIZE;
  packed_length += or_packed_string_length (err_msg, NULL);

  if (packed_length > recdes->area_size)
    {
      prev_offset = CAST_BUFLEN (recdes->data - (*copy_area)->mem);
      prev_length = (*copy_area)->length;

      new_copy_area =
	locator_reallocate_copy_area_by_length (*copy_area,
						(*copy_area)->length +
						DB_PAGESIZE);
      if (new_copy_area == NULL)
	{
	  /* failed to reallocate copy area. skip the current error */
	  return;
	}
      else
	{
	  recdes->data = new_copy_area->mem + prev_offset;
	  recdes->area_size +=
	    CAST_BUFLEN (new_copy_area->length - prev_length);
	  *copy_area = new_copy_area;
	}
    }

  ptr = recdes->data;
  ptr = or_pack_db_idxkey (ptr, key);
  assert (ptr != NULL);
  ptr = or_pack_int (ptr, err_code);
  ptr = or_pack_string (ptr, err_msg);

  reply_obj->length = CAST_BUFLEN (ptr - recdes->data);
  reply_obj->offset = CAST_BUFLEN (recdes->data - (*copy_area)->mem);
  COPY_OID (&reply_obj->class_oid, &obj->class_oid);
  reply_obj->operation = obj->operation;

  reply_mobjs->num_objs++;

  recdes->length = reply_obj->length;
  round_length = DB_ALIGN (recdes->length, MAX_ALIGNMENT);
#if !defined(NDEBUG)
  /* suppress valgrind UMW error */
  memset (recdes->data + recdes->length, 0,
	  MIN (round_length - recdes->length,
	       recdes->area_size - recdes->length));
#endif
  recdes->data += round_length;
  recdes->area_size -= round_length + sizeof (*reply_obj);

  return;
}

/*
 * locator_repl_prepare_force () - prepare required info for each operation
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   thread_p(in):
 *   shard_group_id(out):
 *   obj(in): object that describes the current operation
 *   old_recdes(out): original record needed for update operation
 *   recdes(in/out): record to be applied
 *   key(in): primary key value
 *   force_scancache(in):
 */
static int
locator_repl_prepare_force (THREAD_ENTRY * thread_p, int *shard_group_id,
			    LC_COPYAREA_ONEOBJ * obj,
			    RECDES * old_recdes, RECDES * recdes,
			    DB_IDXKEY * key, HEAP_SCANCACHE * force_scancache)
{
  int error_code = NO_ERROR;
  int last_repr_id = -1;
  BTID btid;
  SCAN_CODE scan;

  assert (key != NULL);
  assert (shard_group_id != NULL);

  *shard_group_id = NULL_GROUPID;
  if (obj->operation != LC_FLUSH_DELETE)
    {
      last_repr_id = heap_get_class_repr_id (thread_p, &obj->class_oid);
      if (last_repr_id == 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CT_INVALID_REPRID, 1,
		  last_repr_id);
	  return ER_CT_INVALID_REPRID;
	}
      *shard_group_id = or_grp_id (recdes);

      error_code = or_set_rep_id (recdes, last_repr_id);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}
    }

  if (obj->operation != LC_FLUSH_INSERT)
    {
      assert (obj->operation == LC_FLUSH_DELETE
	      || obj->operation == LC_FLUSH_UPDATE);
      error_code = btree_get_pkey_btid (thread_p, &obj->class_oid, &btid);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      if (xbtree_find_unique (thread_p, &obj->class_oid, &btid, key,
			      &obj->oid) != BTREE_KEY_FOUND)
	{
	  /* ignore error */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_OBJECT_NOT_FOUND,
		  0);
	  return ER_OBJ_OBJECT_NOT_FOUND;
	}
      assert (*shard_group_id == NULL_GROUPID
	      || *shard_group_id == obj->oid.groupid);

      *shard_group_id = obj->oid.groupid;
    }


  if (obj->operation == LC_FLUSH_UPDATE)
    {
      assert (!OID_ISNULL (&obj->oid));

      scan = heap_get (thread_p, &obj->oid, old_recdes,
		       force_scancache, PEEK);

      if (scan != S_SUCCESS)
	{
	  assert (er_errid () != NO_ERROR);
	  error_code = er_errid ();
	  if (error_code == ER_HEAP_UNKNOWN_OBJECT)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "locator_repl_prepare_force : "
			    "unknown oid ( %d|%d|%d )\n", obj->oid.pageid,
			    obj->oid.slotid, obj->oid.volid);
	    }

	  return error_code;
	}
    }

  return NO_ERROR;
}

/*
 * locator_repl_get_key_value () - read pkey value from copy_area
 *
 * return: length of unpacked key value
 *
 *   key(out): primary key value
 *   force_area(in):
 *   obj(in): object that describes memory location of key_value
 *
 */
static int
locator_repl_get_key_value (DB_IDXKEY * key, const char **class_name,
			    LC_COPYAREA * force_area,
			    LC_COPYAREA_ONEOBJ * obj)
{
  char *ptr, *start_ptr;

  assert (key != NULL && class_name != NULL);

  start_ptr = ptr = force_area->mem + obj->offset;
  ptr = or_unpack_db_idxkey (ptr, key);

  ptr = or_unpack_string_nocopy (ptr, class_name);

  assert (ptr != NULL);

  return (int) (ptr - start_ptr);
}

/*
 * xlocator_repl_force () - Updates objects sent by log applier
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   force_area(in): Copy area where objects are placed
 *
 * Note: This function applies all the desired operations on each of
 *              object placed in the force_area.
 */
int
xlocator_repl_force (THREAD_ENTRY * thread_p, LC_COPYAREA * force_area,
		     LC_COPYAREA ** reply_area)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area        */
  RECDES recdes = RECDES_INITIALIZER;	/* Record descriptor for object      */
  RECDES old_recdes = RECDES_INITIALIZER;
  RECDES reply_recdes = RECDES_INITIALIZER;	/* Describe copy area for reply */
  int i;
  HEAP_SCANCACHE *force_scancache = NULL;
  HEAP_SCANCACHE scan_cache;
  int force_count;
  LOG_LSA oneobj_lsa;
  int error = NO_ERROR;
  int num_continue_on_error = 0;
  DB_IDXKEY key;
  int packed_key_value_len;
  HFID prev_hfid;
  DB_VALUE *shard_key = NULL;
  DB_VALUE class_lock_val;
  int shard_groupid = NULL_GROUPID;
  bool is_shard_table = false;
  const char *class_name;
  LC_FIND_CLASSNAME status;
  bool need_commit = false;
  CIRP_CT_LOG_ANALYZER analyzer_info;
  CIRP_CT_LOG_APPLIER applier_info;


  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (force_area);

  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
  obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);

  HFID_SET_NULL (&prev_hfid);

  DB_IDXKEY_MAKE_NULL (&key);

  LC_RECDES_IN_COPYAREA (*reply_area, &reply_recdes);

  for (i = 0; i < mobjs->num_objs; i++)
    {
      er_clear ();

      obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);

      packed_key_value_len = locator_repl_get_key_value (&key, &class_name,
							 force_area, obj);

      status = xlocator_find_class_oid (thread_p, class_name,
					&obj->class_oid, NULL_LOCK);
      if (status == LC_CLASSNAME_ERROR || status == LC_CLASSNAME_DELETED)
	{
	  error = ER_LC_UNKNOWN_CLASSNAME;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, class_name);

	  GOTO_EXIT_ON_ERROR;
	}

      DB_MAKE_OID (&class_lock_val, &obj->class_oid);
      if (lock_object (thread_p, &class_lock_val,
		       S_LOCK, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  error = er_errid ();

	  GOTO_EXIT_ON_ERROR;
	}

      LC_REPL_RECDES_FOR_ONEOBJ (force_area, obj, packed_key_value_len,
				 &recdes);

      error = heap_get_hfid_from_class_oid (thread_p, &obj->class_oid,
					    &obj->hfid);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (HFID_EQ (&prev_hfid, &obj->hfid) != true && force_scancache != NULL)
	{
	  locator_end_force_scan_cache (thread_p, force_scancache);
	  force_scancache = NULL;
	}

      if (force_scancache == NULL
	  && (obj->operation != LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE
	      && obj->operation != LC_FLUSH_HA_CATALOG_APPLIER_UPDATE))
	{
	  /* Initialize a modify scancache */
	  error = locator_start_force_scan_cache (thread_p, &scan_cache,
						  &obj->hfid, 0,
						  &obj->class_oid);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  force_scancache = &scan_cache;
	  HFID_COPY (&prev_hfid, &obj->hfid);
	}

      error = xtran_server_start_topop (thread_p, &oneobj_lsa);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (obj->operation != LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE
	  && obj->operation != LC_FLUSH_HA_CATALOG_APPLIER_UPDATE)
	{
	  error = locator_repl_prepare_force (thread_p, &shard_groupid, obj,
					      &old_recdes, &recdes,
					      &key, force_scancache);
	  if (error == ER_OBJ_OBJECT_NOT_FOUND)
	    {
	      char pkey_buf[255];
	      int ha_error;

	      assert (obj->operation == LC_FLUSH_DELETE
		      || obj->operation == LC_FLUSH_UPDATE);
	      if (obj->operation == LC_FLUSH_UPDATE)
		{
		  ha_error = ER_HA_LA_FAILED_TO_APPLY_UPDATE;
		}
	      else
		{
		  ha_error = ER_HA_LA_FAILED_TO_APPLY_DELETE;
		}

	      help_sprint_idxkey (&key, pkey_buf, sizeof (pkey_buf) - 1);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ha_error, 4, class_name, pkey_buf,
		      error, "internal server error.");

	      /* ignore error */
	      error = NO_ERROR;
	      goto end_one_object;
	    }
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (shard_groupid < GLOBAL_GROUPID)
	    {
	      assert (false);
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (shard_groupid == GLOBAL_GROUPID)
	    {
	      is_shard_table = false;
	      shard_key = NULL;
	    }
	  else
	    {
	      is_shard_table = true;
	      shard_key = &key.vals[0];
	    }

	  error = lock_shard_key_lock (thread_p, shard_groupid, shard_key,
				       &obj->class_oid, is_shard_table,
				       false, true);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      switch (obj->operation)
	{
	case LC_FLUSH_INSERT:
	  error = locator_insert_force (thread_p, &obj->hfid,
					&obj->class_oid, &obj->oid,
					&recdes, force_scancache,
					&force_count);

	  if (error == NO_ERROR)
	    {
	      /* monitor */
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_INSERTS, 1);
	    }
	  break;

	case LC_FLUSH_UPDATE:
	  error = locator_update_force (thread_p, &obj->hfid,
					&obj->class_oid, &obj->oid, NULL,
					&recdes, NULL, 0,
					force_scancache, &force_count);

	  if (error == NO_ERROR)
	    {
	      /* monitor */
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_UPDATES, 1);
	    }
	  break;

	case LC_FLUSH_DELETE:
	  error = locator_delete_force (thread_p, &obj->hfid, &obj->oid,
					force_scancache, &force_count);

	  if (error == NO_ERROR)
	    {
	      /* monitor */
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_DELETES, 1);
	    }
	  break;
	case LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE:
	  assert (strncasecmp (class_name, CT_LOG_ANALYZER_NAME,
			       strlen (CT_LOG_ANALYZER_NAME)) == 0);
	  assert (recdes.length == sizeof (CIRP_CT_LOG_ANALYZER));

	  memcpy (&analyzer_info, recdes.data, sizeof (CIRP_CT_LOG_ANALYZER));
	  error = qexec_upsert_analyzer_info (thread_p, &key, &analyzer_info);
	  need_commit = true;
	  break;
	case LC_FLUSH_HA_CATALOG_APPLIER_UPDATE:
	  assert (strncasecmp (class_name, CT_LOG_APPLIER_NAME,
			       strlen (CT_LOG_APPLIER_NAME)) == 0);
	  assert (recdes.length == sizeof (CIRP_CT_LOG_APPLIER));

	  memcpy (&applier_info, recdes.data, sizeof (CIRP_CT_LOG_APPLIER));
	  error = qexec_upsert_applier_info (thread_p, &key, &applier_info);
	  need_commit = true;
	  break;

	default:
	  /*
	   * Problems forcing the object. Don't known what flush/force operation
	   * to execute on the object... This is a system error...
	   * Maybe, the transaction should be aborted by the caller...Quit..
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LC_BADFORCE_OPERATION, 4, obj->operation,
		  obj->oid.volid, obj->oid.pageid, obj->oid.slotid);
	  error = ER_LC_BADFORCE_OPERATION;
	  break;
	}			/* end-switch */

    end_one_object:
      if (error != NO_ERROR)
	{
	  if (er_la_ignore_on_error (error) == false)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  locator_repl_add_error_to_copyarea (reply_area, &reply_recdes, obj,
					      &key, error, "");

	  error = NO_ERROR;
	  num_continue_on_error++;

	  (void) xtran_server_end_topop (thread_p,
					 LOG_RESULT_TOPOP_ABORT, &oneobj_lsa);
	}
      else
	{
	  (void) xtran_server_end_topop (thread_p,
					 LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
					 &oneobj_lsa);
	}

      (void) db_idxkey_clear (&key);
    }

  if (force_scancache != NULL)
    {
      locator_end_force_scan_cache (thread_p, force_scancache);
    }

  if (need_commit == true
      && xtran_server_commit (thread_p) != TRAN_UNACTIVE_COMMITTED)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  if (num_continue_on_error > 0)
    {
      return ER_LC_PARTIALLY_FAILED_TO_FLUSH;
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid error code");
    }

  (void) db_idxkey_clear (&key);

  if (force_scancache != NULL)
    {
      locator_end_force_scan_cache (thread_p, force_scancache);
    }

  (void) xtran_server_abort (thread_p);

  return error;
}

/*
 * xlocator_force () - Updates objects placed on page
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   force_area(in): Copy area where objects are placed
 *
 * Note: This function applies all the desired operations on each of
 *              object placed in the force_area.
 */
int
xlocator_force (THREAD_ENTRY * thread_p, LC_COPYAREA * force_area)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area        */
  RECDES recdes;		/* Record descriptor for object      */
  int i;
  HEAP_SCANCACHE *force_scancache = NULL;
  HEAP_SCANCACHE scan_cache;
  int force_count;
  LOG_LSA lsa, oneobj_lsa;
  int error = NO_ERROR;

  /* need to start a topop to ensure the atomic operation. */
  error = xtran_server_start_topop (thread_p, &lsa);
  if (error != NO_ERROR)
    {
      return error;
    }

  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (force_area);

  obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
  obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
  LC_RECDES_IN_COPYAREA (force_area, &recdes);

#if !defined(NDEBUG)
#if defined(SERVER_MODE)
  if (logtb_find_current_client_type (thread_p) != BOOT_CLIENT_CREATEDB)
    {
      assert (mobjs->num_objs == 1);
    }
#else
  if (db_get_client_type () != BOOT_CLIENT_CREATEDB)
    {
      assert (mobjs->num_objs == 1);
    }
#endif
#endif

  for (i = 0; i < mobjs->num_objs; i++)
    {
      obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
      LC_RECDES_TO_GET_ONEOBJ (force_area, obj, &recdes);

      /*
       * Initialize a modify scancache
       */
      error = locator_start_force_scan_cache (thread_p, &scan_cache,
					      &obj->hfid, 0, NULL);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      force_scancache = &scan_cache;

      if (LOG_CHECK_REPL_BROKER (thread_p))
	{
	  error = xtran_server_start_topop (thread_p, &oneobj_lsa);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      switch (obj->operation)
	{
	case LC_FLUSH_INSERT:
	  error = locator_insert_force (thread_p, &obj->hfid, &obj->class_oid,
					&obj->oid, &recdes, force_scancache,
					&force_count);

	  if (error == NO_ERROR)
	    {
	      /* monitor */
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_INSERTS, 1);
	    }
	  break;

	case LC_FLUSH_UPDATE:
	  error = locator_update_force (thread_p, &obj->hfid, &obj->class_oid,
					&obj->oid, NULL, &recdes,
					NULL, 0,
					force_scancache, &force_count);

	  if (error == NO_ERROR)
	    {
	      /* monitor */
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_UPDATES, 1);
	    }
	  break;

	case LC_FLUSH_DELETE:
	  error = locator_delete_force (thread_p, &obj->hfid, &obj->oid,
					force_scancache, &force_count);

	  if (error == NO_ERROR)
	    {
	      /* monitor */
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_DELETES, 1);
	    }
	  break;

	default:
	  /*
	   * Problems forcing the object. Don't known what flush/force operation
	   * to execute on the object... This is a system error...
	   * Maybe, the transaction should be aborted by the caller...Quit..
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LC_BADFORCE_OPERATION, 4, obj->operation,
		  obj->oid.volid, obj->oid.pageid, obj->oid.slotid);
	  error = ER_LC_BADFORCE_OPERATION;
	  break;
	}			/* end-switch */

      if (LOG_CHECK_REPL_BROKER (thread_p))
	{
	  if (error != NO_ERROR)
	    {
	      (void) xtran_server_end_topop (thread_p,
					     LOG_RESULT_TOPOP_ABORT,
					     &oneobj_lsa);
	    }
	  else
	    {
	      (void) xtran_server_end_topop (thread_p,
					     LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
					     &oneobj_lsa);
	    }
	}

      if (error != NO_ERROR)
	{
	  /*
	   * Problems... Maybe, the transaction should
	   * be aborted by the caller...Quit..
	   */
	  GOTO_EXIT_ON_ERROR;
	}

      assert (force_scancache != NULL);
      locator_end_force_scan_cache (thread_p, force_scancache);
      force_scancache = NULL;
    }				/* end-for */

#if 0
done:
#endif

  assert (force_scancache == NULL);
  if (force_scancache != NULL)
    {
      locator_end_force_scan_cache (thread_p, force_scancache);
    }

  (void) xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
				 &lsa);

  return error;

exit_on_error:

  if (force_scancache != NULL)
    {
      locator_end_force_scan_cache (thread_p, force_scancache);
    }

  (void) xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);

  assert_release (error == ER_FAILED || error == er_errid ());
  return error;
}

/*
 * locator_allocate_copy_area_by_attr_info () - Transforms attribute
 *              information into a disk representation and allocates a
 *              LC_COPYAREA big enough to fit the representation
 *
 * return: the allocated LC_COPYAREA if all OK, NULL otherwise
 *
 *   attr_info(in/out): Attribute information
 *                      (Set as a side effect to fill the rest of values)
 *   old_recdes(in): The old representation of the object or NULL if this is a
 *                   new object (to be inserted).
 *   new_recdes(in): The resulting new representation of the object.
 *   shard_groupid(in):
 *   copyarea_length_hint(in): An estimated size for the LC_COPYAREA or -1 if
 *                             an estimated size is not known.
 *
 * Note: The allocated should be freed by using locator_free_copy_area ()
 */
LC_COPYAREA *
locator_allocate_copy_area_by_attr_info (THREAD_ENTRY * thread_p,
					 HEAP_CACHE_ATTRINFO * attr_info,
					 RECDES * old_recdes,
					 RECDES * new_recdes,
					 int shard_groupid,
					 const int copyarea_length_hint)
{
  LC_COPYAREA *copyarea = NULL;
  int copyarea_length =
    copyarea_length_hint <= 0 ? DB_PAGESIZE : copyarea_length_hint;
  SCAN_CODE scan = S_DOESNT_FIT;

  assert (attr_info != NULL);
  assert (shard_groupid != NULL_GROUPID);

  while (scan == S_DOESNT_FIT)
    {
      copyarea = locator_allocate_copy_area_by_length (copyarea_length);
      if (copyarea == NULL)
	{
	  break;
	}

      new_recdes->data = copyarea->mem;
      new_recdes->area_size = copyarea->length;

      scan = heap_attrinfo_transform_to_disk (thread_p, attr_info,
					      old_recdes, new_recdes,
					      shard_groupid);
      if (scan != S_SUCCESS)
	{
	  /* Get the real length used in the copy area */
	  copyarea_length = copyarea->length;
	  locator_free_copy_area (copyarea);
	  copyarea = NULL;
	  new_recdes->data = NULL;
	  new_recdes->area_size = 0;

	  /* Is more space needed ? */
	  if (scan == S_DOESNT_FIT)
	    {
	      /*
	       * The object does not fit into copy area, increase the area
	       * to estimated size included in length of record descriptor.
	       */
	      if (copyarea_length < (-new_recdes->length))
		{
		  copyarea_length =
		    DB_ALIGN (-new_recdes->length, MAX_ALIGNMENT);
		}
	      else
		{
		  /*
		   * This is done for security purposes only, since the
		   * transformation may not have given us the correct length,
		   * somehow.
		   */
		  copyarea_length += DB_PAGESIZE;
		}
	    }
	}
    }
  return copyarea;
}

/*
 * locator_attribute_info_force () - Force an object represented by attribute
 *                                   information structure
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Where of the object
 *   oid(in/out): The object identifier
 *                   (Set as a side effect when operation is insert)
 *   attr_info(in/out): Attribute information
 *                   (Set as a side effect to fill the rest of values)
 *   att_id(in): Updated attr id array
 *   n_att_id(in/out): Updated attr id array length
 *                   (Set as a side effect to fill the rest of values)
 *   operation(in): Type of operation (either LC_FLUSH_INSERT,
 *                   LC_FLUSH_UPDATE, or LC_FLUSH_DELETE)
 *   scan_cache(in):
 *   force_count(out):
 *
 * Note: Force an object represented by an attribute information structure.
 *       For insert the oid is set as a side effect.
 *       For delete, the attr_info does not need to be given.
 */
int
locator_attribute_info_force (THREAD_ENTRY * thread_p, const HFID * hfid,
			      OID * oid, HEAP_CACHE_ATTRINFO * attr_info,
			      ATTR_ID * att_id, int n_att_id,
			      LC_COPYAREA_OPERATION operation,
			      HEAP_SCANCACHE * scan_cache, int *force_count)
{
  LC_COPYAREA *copyarea = NULL;
  RECDES new_recdes = RECDES_INITIALIZER;
  SCAN_CODE scan;		/* Scan return value for next operation */
  RECDES copy_recdes = RECDES_INITIALIZER;
  RECDES *old_recdes = NULL;
  int error = NO_ERROR;
  HFID class_hfid;
  OID class_oid;

  /*
   * While scanning objects, the given scancache does not fix the last
   * accessed page. So, the object must be copied to the record descriptor.
   */
  copy_recdes.data = NULL;

  /* Backup the provided class_oid and class_hfid because the
   * locator actions bellow will change them if this is a pruning
   * operation. This changes must not be reflected in the calls to this
   * function.
   */
  HFID_COPY (&class_hfid, hfid);
  if (attr_info != NULL)
    {
      COPY_OID (&class_oid, &attr_info->class_oid);
    }
  switch (operation)
    {
    case LC_FLUSH_UPDATE:
      scan = heap_get (thread_p, oid, &copy_recdes, scan_cache, COPY);
      if (scan == S_SUCCESS)
	{
	  old_recdes = &copy_recdes;
	}
      else if (scan == S_ERROR || scan == S_DOESNT_FIT)
	{
	  /* Whenever an error including an interrupt was broken out,
	   * quit the update.
	   */
	  return ER_FAILED;
	}
      else if (scan == S_DOESNT_EXIST)
	{
	  int err_id = er_errid ();

	  if (err_id == ER_HEAP_NODATA_NEWADDRESS)
	    {
	      /* it is an immature record. go ahead to update */
	      er_clear ();
	    }
	  else if (err_id == ER_HEAP_UNKNOWN_OBJECT)
	    {
	      /* This means that the object we're looking for does not exist.
	       * This information is useful for the caller of this function
	       * so return this error code instead of ER_FAILD.
	       */
	      return err_id;
	    }
	  else
	    {
	      return ((err_id == NO_ERROR) ? ER_FAILED : err_id);
	    }
	}
      else
	{
	  assert (false);
	  return ER_FAILED;
	}

      /* Fall through */

    case LC_FLUSH_INSERT:
      copyarea = locator_allocate_copy_area_by_attr_info (thread_p, attr_info,
							  old_recdes,
							  &new_recdes,
							  oid->groupid, -1);
      if (copyarea == NULL)
	{
	  error = ER_FAILED;
	  break;
	}

      /* Assume that it has indices */
      if (operation == LC_FLUSH_INSERT)
	{
	  error = locator_insert_force (thread_p, &class_hfid, &class_oid,
					oid, &new_recdes, scan_cache,
					force_count);
	}
      else
	{
	  assert (operation == LC_FLUSH_UPDATE);
	  error = locator_update_force (thread_p, &class_hfid,
					&class_oid, oid,
					old_recdes, &new_recdes,
					att_id, n_att_id, scan_cache,
					force_count);
	}
      if (copyarea != NULL)
	{
	  locator_free_copy_area (copyarea);
	  copyarea = NULL;
	  new_recdes.data = NULL;
	  new_recdes.area_size = 0;
	}
      break;

    case LC_FLUSH_DELETE:
      error = locator_delete_force (thread_p, &class_hfid, oid,
				    scan_cache, force_count);
      break;

    default:
      /*
       * Problems forcing the object. Don't known what flush/force operation
       * to execute on the object... This is a system error...
       * Maybe, the transaction should be aborted by the caller...Quit..
       */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_BADFORCE_OPERATION, 4,
	      operation, oid->volid, oid->pageid, oid->slotid);
      error = ER_LC_BADFORCE_OPERATION;
      break;
    }

  return error;
}

/*
 * locator_add_or_remove_index () - Add or remove index entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   recdes(in): The object
 *   inst_oid(in): The object identifier
 *   class_oid(in): The class object identifier
 *   is_insert(in): whether to add or remove the object from the indexes
 *   op_type(in):
 *   datayn(in): true if the target object is "data",
 *                false if the target object is "schema"
 *   need_replication(in): true if replication is needed
 *   hfid(in):
 *
 * Note:Either insert indices (in_insert) or delete indices.
 */
int
locator_add_or_remove_index (THREAD_ENTRY * thread_p, RECDES * recdes,
			     OID * inst_oid, OID * class_oid, int is_insert,
			     bool datayn, bool need_replication,
			     UNUSED_ARG HFID * hfid)
{
  int num_found;
  int i, num_btids;
  HEAP_CACHE_ATTRINFO index_attrinfo;
  BTID btid;
  DB_IDXKEY *key_ins_del = NULL;
  DB_IDXKEY key;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  OR_INDEX *indexp = NULL;
  BTID_INT btid_int;
  int error_code = NO_ERROR;

  assert_release (class_oid != NULL);
  assert_release (!OID_ISNULL (class_oid));

  DB_IDXKEY_MAKE_NULL (&key);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid_int);

  COPY_OID (&(btid_int.cls_oid), class_oid);

  /*
   *  Populate the index_attrinfo structure.
   *  Return the number of indexed attributes found.
   */
  num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
					      &index_attrinfo, &idx_info);
  num_btids = idx_info.num_btids;

  if (num_found == 0)
    {
      return NO_ERROR;
    }
  else if (num_found < 0)
    {
      return ER_FAILED;
    }

  btid_int.classrepr = index_attrinfo.last_classrepr;
  btid_int.classrepr_cache_idx = index_attrinfo.last_cacheindex;

  /*
   *  At this point, there are indices and the index attrinfo has
   *  been initialized
   *
   *  Read the values of the indexed attributes
   */
  if (idx_info.has_single_col)
    {
      error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, recdes,
						&index_attrinfo);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  for (i = 0; i < num_btids; i++)
    {
      indexp = &(index_attrinfo.last_classrepr->indexes[i]);

      /*
       *  Generate a B-tree key contained in a DB_VALUE and return a
       *  pointer to it.
       */
      assert (DB_IDXKEY_IS_NULL (&key));
      error_code = heap_attrvalue_get_key (thread_p, i, &index_attrinfo,
					   inst_oid, recdes, &btid, &key);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      btid_int.sys_btid = &btid;
      btid_int.indx_id = i;

      if (is_insert)
	{
	  key_ins_del = btree_insert (thread_p, &btid_int, &key);
	}
      else
	{
	  key_ins_del = btree_delete (thread_p, &btid_int, &key);
	}

      /*
       * for replication,
       * Following step would be executed only when the target index is a
       * primary key.
       * The right place to insert a replication log info is here
       * to avoid another "fetching key values"
       * Generates the replication log info. for data insert/delete
       * for the update cases, refer to locator_update_force()
       */
      if (need_replication
	  && INDEX_IS_PRIMARY_KEY (indexp)
	  && key_ins_del != NULL && log_does_allow_replication () == true)
	{
#if 1				/* TODO - */
	  key.size--;		/* remove rightmost OID */
#endif

	  error_code = repl_log_insert (thread_p, class_oid, inst_oid,
					datayn ? LOG_REPLICATION_DATA :
					LOG_REPLICATION_SCHEMA,
					is_insert ? RVREPL_DATA_INSERT :
					RVREPL_DATA_DELETE, &key);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      db_idxkey_clear (&key);

      if (key_ins_del == NULL)
	{
	  if (error_code == NO_ERROR)
	    {
	      error_code = er_errid ();
	      if (error_code == NO_ERROR)
		{
		  error_code = ER_FAILED;
		}
	    }
	  goto error;
	}
    }

error:

  heap_attrinfo_end (thread_p, &index_attrinfo);

  return error_code;
}


/*
 * locator_update_index () - Update index entries
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   new_recdes(in): The new recdes object
 *   old_recdes(in): The old recdes object
 *   att_id(in): Updated attr id array
 *   n_att_id(in): Updated attr id array length
 *   inst_oid(in): The object identifier
 *   class_oid(in): The class object identifier
 *   data_update(in):
 *   need_replication(in): true if replication is needed.
 *
 * Note: Updatet the index entries of the given object.
 */
int
locator_update_index (THREAD_ENTRY * thread_p, RECDES * new_recdes,
		      RECDES * old_recdes, ATTR_ID * att_id, int n_att_id,
		      OID * inst_oid, OID * class_oid,
		      UNUSED_ARG bool data_update, bool need_replication)
{
  int error_code = NO_ERROR;
  HEAP_CACHE_ATTRINFO space_attrinfo[2];
  HEAP_CACHE_ATTRINFO *new_attrinfo = NULL;
  HEAP_CACHE_ATTRINFO *old_attrinfo = NULL;
  int new_num_found, old_num_found;
  BTID new_btid, old_btid;
  int pk_btid_index = -1;
  DB_IDXKEY new_key, old_key;
  bool new_isnull, old_isnull;
  OR_INDEX *indexp = NULL;
  BTID_INT btid_int;
  int i, j, k, num_btids;
  UNUSED_VAR int old_num_btids;
  bool found_btid = true;
  HEAP_IDX_ELEMENTS_INFO new_idx_info;
  HEAP_IDX_ELEMENTS_INFO old_idx_info;
  bool same_key = true;
  int c = DB_UNK;

  assert_release (class_oid != NULL);
  assert_release (!OID_ISNULL (class_oid));

  DB_IDXKEY_MAKE_NULL (&new_key);
  DB_IDXKEY_MAKE_NULL (&old_key);

  new_num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
						  &space_attrinfo[0],
						  &new_idx_info);
  num_btids = new_idx_info.num_btids;
  if (new_num_found < 0)
    {
      return ER_FAILED;
    }
  new_attrinfo = &space_attrinfo[0];

  old_num_found = heap_attrinfo_start_with_index (thread_p, class_oid, NULL,
						  &space_attrinfo[1],
						  &old_idx_info);
  old_num_btids = old_idx_info.num_btids;
  if (old_num_found < 0)
    {
      error_code = ER_FAILED;
      goto error;
    }
  old_attrinfo = &space_attrinfo[1];

  if (new_num_found != old_num_found)
    {
      if (new_num_found > 0)
	{
	  heap_attrinfo_end (thread_p, &space_attrinfo[0]);
	}
      if (old_num_found > 0)
	{
	  heap_attrinfo_end (thread_p, &space_attrinfo[1]);
	}
      return ER_FAILED;
    }

  if (new_num_found == 0)
    {
      return NO_ERROR;
    }

  /*
   * There are indices and the index attrinfo has been initialized
   * Indices must be updated when the indexed attributes have changed in value
   * Get the new and old values of key and update the index when
   * the keys are different
   */

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid_int);

  COPY_OID (&(btid_int.cls_oid), class_oid);

  new_attrinfo = &space_attrinfo[0];
  old_attrinfo = &space_attrinfo[1];

  error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, new_recdes,
					    new_attrinfo);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = heap_attrinfo_read_dbvalues (thread_p, inst_oid, old_recdes,
					    old_attrinfo);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  btid_int.classrepr = new_attrinfo->last_classrepr;
  btid_int.classrepr_cache_idx = new_attrinfo->last_cacheindex;

  /*
   *  Ensure that we have the same number of indexes and
   *  get the number of B-tree IDs.
   */
  if (old_attrinfo->last_classrepr->n_indexes !=
      new_attrinfo->last_classrepr->n_indexes)
    {
      error_code = ER_FAILED;
      goto error;
    }

  for (i = 0; i < num_btids; i++)
    {
      indexp = &(new_attrinfo->last_classrepr->indexes[i]);

      if (pk_btid_index == -1 && need_replication
	  && INDEX_IS_PRIMARY_KEY (indexp)
	  && log_does_allow_replication () == true)
	{
	  pk_btid_index = i;
	}

      /* check for specified update attributes */
      if (att_id)
	{
	  found_btid = false;	/* geuss as not found */

	  for (j = 0; j < n_att_id && !found_btid; j++)
	    {
	      for (k = 0; k < indexp->n_atts && !found_btid; k++)
		{
		  if (att_id[j] == (ATTR_ID) (indexp->atts[k]->id))
		    {		/* the index key_type has updated attr */
		      found_btid = true;
		    }
		}
	    }

	  if (!found_btid)
	    {
	      continue;		/* skip and go ahead */
	    }
	}

      assert (DB_IDXKEY_IS_NULL (&new_key));
      error_code =
	heap_attrvalue_get_key (thread_p, i, new_attrinfo, inst_oid,
				new_recdes, &new_btid, &new_key);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      assert (DB_IDXKEY_IS_NULL (&old_key));
      error_code =
	heap_attrvalue_get_key (thread_p, i, old_attrinfo, inst_oid,
				old_recdes, &old_btid, &old_key);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      new_isnull = DB_IDXKEY_IS_NULL (&new_key);
      old_isnull = DB_IDXKEY_IS_NULL (&old_key);

      same_key = true;		/* init */
      if ((new_isnull && !old_isnull) || (old_isnull && !new_isnull))
	{
	  same_key = false;
	}
      else
	{
	  if (!(new_isnull && old_isnull))
	    {
	      c = btree_compare_key (thread_p, NULL, &old_key, &new_key,
				     NULL);
	      if (c == DB_UNK)
		{
		  assert (false);
		  error_code = er_errid ();
		  goto error;
		}

	      if (c != DB_EQ)
		{
		  same_key = false;
		}
	    }
	}

      if (!same_key)
	{
	  btid_int.sys_btid = &new_btid;
	  btid_int.indx_id = i;

	  error_code = btree_update (thread_p, &btid_int, &old_key, &new_key);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}

      db_idxkey_clear (&new_key);
      db_idxkey_clear (&old_key);
    }				/* for (i = 0; ...) */

  if (pk_btid_index != -1)
    {
      assert (DB_IDXKEY_IS_NULL (&old_key));
      error_code = heap_attrvalue_get_key (thread_p, pk_btid_index,
					   old_attrinfo, inst_oid,
					   old_recdes, &old_btid, &old_key);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

#if 1				/* TODO - */
      old_key.size--;		/* remove rightmost OID */
#endif

      error_code = repl_log_insert (thread_p, class_oid, inst_oid,
				    LOG_REPLICATION_DATA,
				    RVREPL_DATA_UPDATE, &old_key);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      db_idxkey_clear (&old_key);
    }

  heap_attrinfo_end (thread_p, new_attrinfo);
  heap_attrinfo_end (thread_p, old_attrinfo);

  return error_code;

error:

  db_idxkey_clear (&new_key);
  db_idxkey_clear (&old_key);

  /* Deallocate any index_list .. if any */

  if (new_attrinfo != NULL)
    {
      heap_attrinfo_end (thread_p, new_attrinfo);
    }

  if (old_attrinfo != NULL)
    {
      heap_attrinfo_end (thread_p, old_attrinfo);
    }

  return error_code;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * locator_check_class () - Check consistency of a class
 *
 * return: valid
 *
 *   repair(in):
 */
static DISK_ISVALID
locator_check_class (THREAD_ENTRY * thread_p, OID * class_oid,
		     RECDES * peek, HFID * class_hfid, bool repair)
{
  DISK_ISVALID isvalid = DISK_VALID, rv = DISK_VALID;
  HEAP_CACHE_ATTRINFO attr_info;
  int i;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  BTID *btid;
  ATTR_ID *attrids = NULL;
  int n_attrs;
  char *btname = NULL;

  if (heap_attrinfo_start_with_index (thread_p, class_oid, peek, &attr_info,
				      &idx_info) < 0)
    {
      return DISK_ERROR;
    }

  if (idx_info.num_btids <= 0)
    {
      heap_attrinfo_end (thread_p, &attr_info);
      return DISK_VALID;
    }

  for (i = 0; i < idx_info.num_btids && rv != DISK_ERROR; i++)
    {
      btid = heap_indexinfo_get_btid (i, &attr_info);
      if (btid == NULL)
	{
	  isvalid = DISK_ERROR;
	  break;
	}
      n_attrs = heap_indexinfo_get_num_attrs (i, &attr_info);
      if (n_attrs <= 0)
	{
	  isvalid = DISK_ERROR;
	  break;
	}

      attrids = (ATTR_ID *) malloc (n_attrs * sizeof (ATTR_ID));
      if (attrids == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, n_attrs * sizeof (ATTR_ID));
	  isvalid = DISK_ERROR;
	  break;
	}

      if (heap_indexinfo_get_attrids (i, &attr_info, attrids) != NO_ERROR)
	{
	  free_and_init (attrids);
	  isvalid = DISK_ERROR;
	  break;
	}

      if (heap_get_indexname_of_btid (thread_p, class_oid, btid, &btname) !=
	  NO_ERROR)
	{
	  free_and_init (attrids);
	  isvalid = DISK_ERROR;
	  break;
	}

      if (rv != DISK_VALID)
	{
	  isvalid = DISK_ERROR;
	}

      free_and_init (attrids);
      if (btname)
	{
	  free_and_init (btname);
	}
    }

  heap_attrinfo_end (thread_p, &attr_info);
  return isvalid;
}

/*
 * locator_check_by_class_oid () - Check consistency of a class
 *
 * return: valid
 *
 *   repair(in):
 *
 */
DISK_ISVALID
locator_check_by_class_oid (THREAD_ENTRY * thread_p, OID * cls_oid,
			    HFID * hfid, bool repair)
{
  RECDES peek = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan;
  DISK_ISVALID rv = DISK_ERROR;

  if (heap_scancache_quick_start (&scan) != NO_ERROR)
    {
      return DISK_ERROR;
    }

  if (heap_get (thread_p, cls_oid, &peek, &scan, PEEK) == S_SUCCESS)
    {
      rv = locator_check_class (thread_p, cls_oid, &peek, hfid, repair);
    }
  else
    {
      rv = DISK_ERROR;
    }

  heap_scancache_end (thread_p, &scan);

  return rv;
}
#endif

/*
 * xlocator_find_lockhint_class_oids () - Find the oids associated with the given
 *                                  classes
 *
 * return: LC_FIND_CLASSNAME
 *                        (either of LC_CLASSNAME_EXIST,
 *                                   LC_CLASSNAME_DELETED,
 *                                   LC_CLASSNAME_ERROR)
 *
 *   num_classes(in): Number of needed classes
 *   many_classnames(in): Name of the classes
 *   many_locks(in): The desired lock for each class
 *   guessed_class_oids(in):
 *   guessed_class_chns(in):
 *   quit_on_errors(in): Wheater to continue finding the classes in case of
 *                          an error, such as a class does not exist or a lock
 *                          one a may not be granted.
 *   hlock(in): hlock structure which is set to describe the
 *                          classes
 *   fetch_area(in):
 *
 * Note: This function find the class identifiers of the given class
 *              names and requested subclasses of the above classes, and lock
 *              the classes with given locks. The function does not quit when
 *              an error is found and the value of quit_on_errors is false.
 *              In this case the class (an may be its subclasses) with the
 *              error is not locked/fetched.
 *              The function tries to lock all the classes at once, however if
 *              this fails and the function is allowed to continue when errors
 *              are detected, the classes are locked individually.
 *
 *              The subclasses are only guessed for locking purposed and they
 *              should not be used for any other purposes. For example, the
 *              subclasses should not given to the upper levels of the system.
 *
 *              In general the function is used to find out all needed classes
 *              and lock them togheter.
 */
LC_FIND_CLASSNAME
xlocator_find_lockhint_class_oids (THREAD_ENTRY * thread_p, int num_classes,
				   const char **many_classnames,
				   LOCK * many_locks,
				   UNUSED_ARG OID * guessed_class_oids,
				   int quit_on_errors,
				   LC_LOCKHINT ** hlock,
				   LC_COPYAREA ** fetch_area)
{
  int tran_index;
  EH_SEARCH search;
  LOCATOR_TMP_CLASSNAME_ENTRY *entry;
  const char *classname;
  LOCK tmp_lock;
  LC_FIND_CLASSNAME find = LC_CLASSNAME_EXIST;
  LC_FIND_CLASSNAME allfind = LC_CLASSNAME_EXIST;
  int retry;
  int i, j;
  int n;
  DB_VALUE lock_val;

  *fetch_area = NULL;

  /*
   * Let's assume the number of classes that are going to be described in the
   * lockhint area.
   */

  *hlock = locator_allocate_lockhint (num_classes, quit_on_errors);
  if (*hlock == NULL)
    {
      return LC_CLASSNAME_ERROR;
    }

  /*
   * Find the class oids of the given classnames.
   */

  tran_index = logtb_get_current_tran_index (thread_p);

  for (i = 0;
       i < num_classes && (allfind == LC_CLASSNAME_EXIST
			   || quit_on_errors == false); i++)
    {
      classname = many_classnames[i];
      if (classname == NULL)
	{
	  continue;
	}

      n = (*hlock)->num_classes;
      find = LC_CLASSNAME_EXIST;
      retry = 1;

      while (retry)
	{
	  retry = 0;

	  /*
	   * Describe the hinted class
	   */

	  (*hlock)->classes[n].lock = many_locks[i];

	  if (csect_enter_as_reader (thread_p,
				     CSECT_LOCATOR_SR_CLASSNAME_TABLE,
				     INF_WAIT) != NO_ERROR)
	    {
	      return LC_CLASSNAME_ERROR;
	    }

	  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
		   mht_get (locator_Mht_classnames, classname));

	  if (entry != NULL)
	    {
	      /*
	       * We can only proceed if the entry belongs to the current transaction,
	       * otherwise, we must lock the class associated with the classname and
	       * retry the operation once the lock is granted.
	       */

	      COPY_OID (&(*hlock)->classes[n].oid, &entry->oid);

	      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	      if (entry->tran_index == tran_index)
		{
		  if (entry->action == LC_CLASSNAME_DELETED
		      || entry->action == LC_CLASSNAME_DELETED_RENAME)
		    {
		      find = LC_CLASSNAME_DELETED;
		    }
		}
	      else if (entry->action != LC_CLASSNAME_EXIST)
		{
		  /*
		   * Do not know the fate of this entry until the transaction is
		   * committed or aborted. Get the lock and retry later on.
		   */
		  if ((*hlock)->classes[n].lock != NULL_LOCK)
		    {
		      tmp_lock = (*hlock)->classes[n].lock;
		    }
		  else
		    {
		      tmp_lock = S_LOCK;
		    }

		  DB_MAKE_OID (&lock_val, &(*hlock)->classes[n].oid);
		  if (lock_object (thread_p, &lock_val, tmp_lock,
				   LK_UNCOND_LOCK) != LK_GRANTED)
		    {
		      /*
		       * Unable to acquired the lock
		       */
		      find = LC_CLASSNAME_ERROR;
		    }
		  else
		    {
		      /*
		       * Try again
		       * Remove the lock.. since the above was a dirty read
		       */
		      lock_unlock_object (thread_p, &lock_val,
					  LK_UNLOCK_TYPE_NORMAL);
		      retry = 1;
		      continue;
		    }
		}
	    }
	  else
	    {
	      /*
	       * Is there a class with such a name on the permanent classname
	       * hash table ?
	       */
	      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);

	      search = ehash_search (thread_p, locator_Eht_classnames,
				     classname, &(*hlock)->classes[n].oid);
	      if (search != EH_KEY_FOUND)
		{
		  if (search == EH_KEY_NOTFOUND)
		    {
		      find = LC_CLASSNAME_DELETED;
		    }
		  else
		    {
		      find = LC_CLASSNAME_ERROR;
		    }
		}
	      else
		{
		  if (csect_enter (thread_p, CSECT_LOCATOR_SR_CLASSNAME_TABLE,
				   INF_WAIT) != NO_ERROR)
		    {
		      return LC_CLASSNAME_ERROR;
		    }
		  /* Double check : already cached ? */
		  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
			   mht_get (locator_Mht_classnames, classname));
		  if (entry == NULL)
		    {

		      if (((int) mht_count (locator_Mht_classnames) <
			   MAX_CLASSNAME_CACHE_ENTRIES)
			  || (locator_decache_class_name_entries () ==
			      NO_ERROR))
			{

			  entry = ((LOCATOR_TMP_CLASSNAME_ENTRY *)
				   malloc (sizeof (*entry)));

			  if (entry == NULL)
			    {
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_OUT_OF_VIRTUAL_MEMORY, 1,
				      sizeof (*entry));
			      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
			      return LC_CLASSNAME_ERROR;
			    }

			  entry->name = strdup (classname);
			  if (entry->name == NULL)
			    {
			      free_and_init (entry);
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_OUT_OF_VIRTUAL_MEMORY, 1,
				      strlen (classname));
			      csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
			      return LC_CLASSNAME_ERROR;
			    }

			  entry->tran_index = NULL_TRAN_INDEX;
			  entry->action = LC_CLASSNAME_EXIST;
			  COPY_OID (&entry->oid, &(*hlock)->classes[n].oid);
			  (void) mht_put (locator_Mht_classnames,
					  entry->name, entry);
			}
		    }
		  csect_exit (CSECT_LOCATOR_SR_CLASSNAME_TABLE);
		}
	    }
	}

      if (find == LC_CLASSNAME_EXIST)
	{
	  n++;
	  (*hlock)->num_classes = n;
	}
      else
	{
	  if (allfind != LC_CLASSNAME_ERROR)
	    {
	      allfind = find;
	    }
	  if (find == LC_CLASSNAME_DELETED)
	    {
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LC_UNKNOWN_CLASSNAME, 1, classname);
	    }
	}
    }

  /*
   * Eliminate any duplicates. Note that we did not want to do above since
   * we did not want to modify the original arrays.
   */

  for (i = 0; i < (*hlock)->num_classes; i++)
    {
      if (OID_ISNULL (&(*hlock)->classes[i].oid))
	{
	  continue;
	}
      /*
       * Is this duplicated ?
       */
      for (j = i + 1; j < (*hlock)->num_classes; j++)
	{
	  if (OID_EQ (&(*hlock)->classes[i].oid, &(*hlock)->classes[j].oid))
	    {
	      /* Duplicate class, merge the lock and the subclass entry */
	      assert ((*hlock)->classes[i].lock >= NULL_LOCK
		      && (*hlock)->classes[j].lock >= NULL_LOCK);
	      (*hlock)->classes[i].lock =
		lock_Conv[(*hlock)->classes[i].lock]
		[(*hlock)->classes[j].lock];
	      assert ((*hlock)->classes[i].lock != NA_LOCK);

	      /* Now eliminate the entry */
	      OID_SET_NULL (&(*hlock)->classes[j].oid);
	    }
	}
    }

  if (allfind == LC_CLASSNAME_EXIST || quit_on_errors == false)
    {
      if (xlocator_fetch_lockhint_classes (thread_p, (*hlock), fetch_area) !=
	  NO_ERROR)
	{
	  allfind = LC_CLASSNAME_ERROR;
	  if (quit_on_errors == true)
	    {
	      locator_free_lockhint ((*hlock));
	      *hlock = NULL;
	    }
	}
    }
  else
    {
      locator_free_lockhint ((*hlock));
      *hlock = NULL;
    }

  return allfind;
}

/*
 * xlocator_fetch_lockhint_classes () - Lock and fetch a set of classes
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   lockhint(in): Description of hinted classses
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_fetch_lockhint_classes (THREAD_ENTRY * thread_p,
				 LC_LOCKHINT * lockhint,
				 LC_COPYAREA ** fetch_area)
{
  LOCATOR_RETURN_NXOBJ nxobj;	/* Description to return next object */
  SCAN_CODE scan = S_SUCCESS;
  int copyarea_length;
  int i;
  int error_code = NO_ERROR;
  DB_VALUE lock_val;
  LC_COPYAREA *new_area = NULL;

  *fetch_area = NULL;

  if (lockhint->num_classes <= 0)
    {
      lockhint->num_classes_processed = lockhint->num_classes;
      return NO_ERROR;
    }

  if (lockhint->num_classes_processed == -1)
    {
      /*
       * FIRST CALL.
       * Initialize num of object processed.
       */
      lockhint->num_classes_processed = 0;

      /* Obtain the locks */
      if (lock_classes_lock_hint (thread_p, lockhint) != LK_GRANTED)
	{
	  if (lockhint->quit_on_errors != false)
	    {
	      return ER_FAILED;
	    }
	  else
	    {
	      error_code = ER_FAILED;
	      /* Lock individual classes */
	      for (i = 0; i < lockhint->num_classes; i++)
		{
		  if (OID_ISNULL (&lockhint->classes[i].oid))
		    {
		      continue;
		    }

		  DB_MAKE_OID (&lock_val, &lockhint->classes[i].oid);
		  if (lock_object (thread_p, &lock_val,
				   lockhint->classes[i].lock,
				   LK_UNCOND_LOCK) != LK_GRANTED)
		    {
		      OID_SET_NULL (&lockhint->classes[i].oid);
		    }
		  else
		    {
		      /* We are unable to continue since we lock at least one */
		      error_code = NO_ERROR;
		    }
		}
	      if (error_code != NO_ERROR)
		{
		  return error_code;
		}
	    }
	}
    }

  /*
   * Start a scan cursor for getting the classes
   */

  error_code = heap_scancache_start (thread_p, &nxobj.area_scancache, NULL,
				     NULL, true);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  nxobj.ptr_scancache = &nxobj.area_scancache;

  /*
   * Assume that there are not any classes larger than one page. If there are
   * the number of pages is fixed later.
   */

  /* Assume that the needed object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  nxobj.mobjs = NULL;
  nxobj.comm_area = NULL;

  while (scan == S_SUCCESS
	 && (lockhint->num_classes_processed < lockhint->num_classes))
    {
      new_area = locator_allocate_copy_area_by_length (copyarea_length);
      if (new_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &nxobj.area_scancache);
	  /* There was an error.. */
	  if (nxobj.mobjs != NULL)
	    {
	      locator_free_copy_area (nxobj.comm_area);
	      nxobj.comm_area = NULL;
	    }
	  return ER_FAILED;
	}

      nxobj.comm_area = new_area;

      nxobj.mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (nxobj.comm_area);
      nxobj.obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (nxobj.mobjs);
      LC_RECDES_IN_COPYAREA (nxobj.comm_area, &nxobj.recdes);
      nxobj.area_offset = 0;
      nxobj.mobjs->num_objs = 0;

      /*
       * Place the classes on the communication area, don't place those classes
       * with correct chns.
       */

      for (i = lockhint->num_classes_processed;
	   scan == S_SUCCESS && i < lockhint->num_classes; i++)
	{
	  if (OID_ISNULL (&lockhint->classes[i].oid)
	      || OID_ISTEMP (&lockhint->classes[i].oid))
	    {
	      lockhint->num_classes_processed += 1;
	      continue;
	    }

	  /* Now return the object */
	  scan = locator_return_object (thread_p, &nxobj,
					oid_Root_class_oid,
					&lockhint->classes[i].oid);
	  if (scan == S_SUCCESS)
	    {
	      lockhint->num_classes_processed += 1;
	    }
	  else if (scan == S_DOESNT_FIT && nxobj.mobjs->num_objs == 0)
	    {
	      /*
	       * The first object on the copy area does not fit.
	       * Get a larger area
	       */

	      /* Get the real length of the fetch/copy area */

	      copyarea_length = nxobj.comm_area->length;

	      if ((-nxobj.recdes.length) > copyarea_length)
		{
		  copyarea_length =
		    (DB_ALIGN (-nxobj.recdes.length, MAX_ALIGNMENT) +
		     sizeof (*nxobj.mobjs));
		}
	      else
		{
		  copyarea_length += DB_PAGESIZE;
		}

	      locator_free_copy_area (nxobj.comm_area);
	      nxobj.comm_area = NULL;
	      scan = S_SUCCESS;
	      break;		/* finish the for */
	    }
	  else if (scan != S_DOESNT_FIT
		   && (scan == S_DOESNT_EXIST
		       || lockhint->quit_on_errors == false))
	    {
	      OID_SET_NULL (&lockhint->classes[i].oid);
	      lockhint->num_classes_processed += 1;
	      scan = S_SUCCESS;
	    }
	  else
	    {
	      break;		/* Quit on errors */
	    }
	}
    }

  /* End the scan cursor */
  error_code = heap_scancache_end (thread_p, &nxobj.area_scancache);
  if (error_code != NO_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	  nxobj.comm_area = NULL;
	}
      return error_code;
    }

  if (scan == S_ERROR)
    {
      /* There was an error.. */
      if (nxobj.mobjs != NULL)
	{
	  locator_free_copy_area (nxobj.comm_area);
	  nxobj.comm_area = NULL;
	}
      return ER_FAILED;
    }
  else if (nxobj.mobjs != NULL && nxobj.mobjs->num_objs == 0)
    {
      locator_free_copy_area (nxobj.comm_area);
      nxobj.comm_area = NULL;
    }
  else
    {
      *fetch_area = nxobj.comm_area;
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * xlocator_assign_oid_batch () - Assign a group of permanent oids
 *
 * return:  NO_ERROR if all OK, ER_ status otherwise
 *
 *   oidset(in): LC_OIDSET describing all of the temp oids
 *
 * Note:Permanent oids are assigned to each of the temporary oids
 *              listed in the LC_OIDSET.
 */
int
xlocator_assign_oid_batch (THREAD_ENTRY * thread_p, LC_OIDSET * oidset)
{
  LC_CLASS_OIDSET *class_oidset;
  LC_OIDMAP *oid;
  int error_code = NO_ERROR;

  /* establish a rollback point in case we get an error part way through */
  if (log_start_system_op (thread_p) == NULL)
    {
      return ER_FAILED;
    }

  /* Now assign the OID's stop on the first error */
#if 1				/* TODO - trace */
  assert (oidset->classes != NULL);
  assert (oidset->classes->next == NULL);
#endif
  for (class_oidset = oidset->classes; class_oidset != NULL;
       class_oidset = class_oidset->next)
    {
      for (oid = class_oidset->oids; oid != NULL; oid = oid->next)
	{
	  error_code = xlocator_assign_oid (thread_p, &class_oidset->hfid,
					    &oid->oid, oid->est_size,
					    &class_oidset->class_oid, NULL);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  /* accept the operation */
  log_end_system_op (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER);

  return error_code;

error:
  /* rollback the operation */
  log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
  return error_code;
}

/*
 * locator_increase_catalog_count () -
 *
 * return:
 *
 *   cls_oid(in): Class OID
 *
 * Note:Increase the 'tot_objects' counter of the CLS_INFO
 *        and do update the catalog record in-place.
 */
static void
locator_increase_catalog_count (THREAD_ENTRY * thread_p, OID * cls_oid)
{
  CLS_INFO *cls_infop = NULL;

  /* retrieve the class information */
  cls_infop = catalog_get_class_info (thread_p, cls_oid);

  if (cls_infop == NULL)
    {
      return;
    }

  if (cls_infop->hfid.vfid.fileid < 0 || cls_infop->hfid.vfid.volid < 0)
    {
      /* The class does not have a heap file (i.e. it has no instances);
         so no statistics can be obtained for this class; just set
         'tot_objects' field to 0. */
      /* Is it safe not to initialize the other fields of CLS_INFO? */
      cls_infop->tot_objects = 0;
    }
  else
    {
      /* increase the 'tot_objects' counter */
      cls_infop->tot_objects++;
    }

  /* update the class information to the catalog */
  /* NOTE that tot_objects may not be correct because changes are NOT logged. */
  (void) catalog_update_class_info (thread_p, cls_oid, cls_infop, true);

  catalog_free_class_info (cls_infop);
}

/*
 * locator_decrease_catalog_count  () -
 *
 * return:
 *
 *   cls_oid(in): Class OID
 *
 * Note: Descrease the 'tot_objects' counter of the CLS_INFO
 *        and do update the catalog record in-place.
 */
static void
locator_decrease_catalog_count (THREAD_ENTRY * thread_p, OID * cls_oid)
{
  CLS_INFO *cls_infop = NULL;

  /* retrieve the class information */
  cls_infop = catalog_get_class_info (thread_p, cls_oid);

  if (cls_infop == NULL)
    {
      return;
    }

  if (cls_infop->hfid.vfid.fileid < 0 || cls_infop->hfid.vfid.volid < 0)
    {
      /* The class does not have a heap file (i.e. it has no instances);
         so no statistics can be obtained for this class; just set
         'tot_objects' field to 0. */
      /* Is it an error to delete an instance with no heap file? */
      cls_infop->tot_objects = 0;
    }

  /* decrease the 'tot_objects' counter */
  if (cls_infop->tot_objects > 0)
    {
      cls_infop->tot_objects--;
    }

  /* update the class information to the catalog */
  /* NOTE that tot_objects may not be correct because changes are NOT logged. */
  (void) catalog_update_class_info (thread_p, cls_oid, cls_infop, true);

  catalog_free_class_info (cls_infop);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * xrepl_set_info () -
 *
 * return:
 *
 *   repl_info(in):
 */
int
xrepl_set_info (THREAD_ENTRY * thread_p, REPL_INFO * repl_info)
{
  int error_code = NO_ERROR;

  if (log_does_allow_replication () == true)
    {
      switch (repl_info->repl_info_type)
	{
	case REPL_INFO_TYPE_SCHEMA:
	  error_code =
	    repl_log_insert_schema (thread_p,
				    (REPL_INFO_SCHEMA *) repl_info->info);
	  break;
	default:
	  error_code = ER_REPL_ERROR;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
		  "can't make repl schema info");
	  break;
	}
    }

  return error_code;
}

/*
 * xrepl_log_get_eof_lsa () -
 *
 * return:
 */
void
xrepl_log_get_eof_lsa (THREAD_ENTRY * thread_p, LOG_LSA * lsa)
{
  return log_get_eof_lsa (thread_p, lsa);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * xlocator_lock_and_fetch_all () - Fetch all class instances that can be locked
 *				    in specified locked time
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   hfid(in): Heap file where the instances of the class are placed
 *   instance_lock(in): Instance lock to aquire
 *   instance_lock_timeout(in): Timeout for instance lock
 *   class_oid(in): Class identifier of the instances to fetch
 *   class_lock(in): Lock to acquire (Set as a side effect to NULL_LOCKID)
 *   nobjects(out): Total number of objects to fetch.
 *   nfetched(out): Current number of object fetched.
 *   nfailed_instance_locks(out): count failed instance locks
 *   last_oid(out): Object identifier of last fetched object
 *   fetch_area(in/out): Pointer to area where the objects are placed
 *
 */
int
xlocator_lock_and_fetch_all (THREAD_ENTRY * thread_p, const HFID * hfid,
			     LOCK * instance_lock,
			     UNUSED_ARG int *instance_lock_timeout,
			     OID * class_oid, LOCK * class_lock,
			     int *nobjects, int *nfetched,
			     int *nfailed_instance_locks,
			     OID * last_oid, LC_COPYAREA ** fetch_area)
{
  LC_COPYAREA_MANYOBJS *mobjs;	/* Describe multiple objects in
				 * area */
  LC_COPYAREA_ONEOBJ *obj;	/* Describe on object in area  */
  RECDES recdes;		/* Record descriptor for
				 * insertion */
  int offset;			/* Place to store next object in
				 * area */
  int round_length;		/* Length of object rounded to
				 * integer alignment */
  int copyarea_length;
  OID oid;
  HEAP_SCANCACHE scan_cache;
  SCAN_CODE scan;
  int error_code = NO_ERROR;
  DB_VALUE lock_val;

  if (fetch_area == NULL)
    {
      return ER_FAILED;
    }
  *fetch_area = NULL;

  if (nfailed_instance_locks == NULL)
    {
      return ER_FAILED;
    }
  *nfailed_instance_locks = 0;

  if (OID_ISNULL (last_oid))
    {
      /* FIRST TIME. */

      DB_MAKE_OID (&lock_val, class_oid);
      /* Obtain the desired lock for the class scan */
      if (*class_lock != NULL_LOCK
	  && lock_object (thread_p, &lock_val,
			  *class_lock, LK_UNCOND_LOCK) != LK_GRANTED)
	{
	  /*
	   * Unable to acquired lock
	   */
	  *class_lock = NULL_LOCK;
	  *nobjects = -1;
	  *nfetched = -1;

	  error_code = ER_FAILED;
	  goto error;
	}

      /* Get statistics */
      last_oid->volid = hfid->vfid.volid;
      last_oid->pageid = NULL_PAGEID;
      last_oid->slotid = NULL_SLOTID;
      /* Estimate the number of objects to be fetched */
      *nfetched = 0;
      error_code = heap_estimate_num_objects (thread_p, hfid, nobjects);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  /* Set OID to last fetched object */
  COPY_OID (&oid, last_oid);

  /* Start a scan cursor for getting several classes */
  error_code = heap_scancache_start (thread_p, &scan_cache, hfid,
				     class_oid, true);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /* Assume that the next object can fit in one page */
  copyarea_length = DB_PAGESIZE;

  while (true)
    {
      *fetch_area = locator_allocate_copy_area_by_length (copyarea_length);
      if (*fetch_area == NULL)
	{
	  (void) heap_scancache_end (thread_p, &scan_cache);
	  error_code = ER_FAILED;
	  goto error;
	}

      mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (*fetch_area);
      LC_RECDES_IN_COPYAREA (*fetch_area, &recdes);
      obj = LC_START_ONEOBJ_PTR_IN_COPYAREA (mobjs);
      mobjs->num_objs = 0;
      offset = 0;

      while (true)
	{
	  if (instance_lock && (*instance_lock != NULL_LOCK))
	    {
	      scan = heap_next (thread_p, hfid, class_oid, &oid, &recdes,
				&scan_cache, COPY);
	      if (scan != S_SUCCESS)
		{
		  break;
		}

	      scan = heap_get (thread_p, &oid, &recdes, &scan_cache, COPY);
	      if (scan != S_SUCCESS)
		{
		  (*nfailed_instance_locks)++;
		  continue;
		}

	    }
	  else
	    {
	      scan = heap_next (thread_p, hfid, class_oid, &oid, &recdes,
				&scan_cache, COPY);
	      if (scan != S_SUCCESS)
		{
		  break;
		}
	    }

	  mobjs->num_objs++;
	  COPY_OID (&obj->class_oid, class_oid);
	  COPY_OID (&obj->oid, &oid);
	  obj->hfid = NULL_HFID;
	  obj->length = recdes.length;
	  obj->offset = offset;
	  obj->operation = LC_FETCH;
	  obj = LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (obj);
	  round_length = DB_ALIGN (recdes.length, MAX_ALIGNMENT);
#if !defined(NDEBUG)
	  /* suppress valgrind UMW error */
	  memset (recdes.data + recdes.length, 0,
		  MIN (round_length - recdes.length,
		       recdes.area_size - recdes.length));
#endif
	  offset += round_length;
	  recdes.data += round_length;
	  recdes.area_size -= round_length + sizeof (*obj);
	}

      if (scan != S_DOESNT_FIT || mobjs->num_objs > 0)
	{
	  break;
	}
      /*
       * The first object does not fit into given copy area
       * Get a larger area
       */

      /* Get the real length of current fetch/copy area */
      copyarea_length = (*fetch_area)->length;
      locator_free_copy_area (*fetch_area);

      /*
       * If the object does not fit even when the copy area seems to be
       * large enough, increase the copy area by at least one page size.
       */

      if ((-recdes.length) > copyarea_length)
	{
	  copyarea_length =
	    DB_ALIGN (-recdes.length, MAX_ALIGNMENT) + sizeof (*mobjs);
	}
      else
	{
	  copyarea_length += DB_PAGESIZE;
	}
    }

  if (scan == S_END)
    {
      /*
       * This is the end of the loop. Indicate the caller that no more calls
       * are needed by setting nobjects and nfetched to the same value.
       */
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  *nobjects = *nfetched = -1;
	  goto error;
	}

      *nfetched += mobjs->num_objs;
      *nobjects = *nfetched;
      OID_SET_NULL (last_oid);
    }
  else if (scan == S_ERROR)
    {
      /* There was an error.. */
      (void) heap_scancache_end (thread_p, &scan_cache);
      *nobjects = *nfetched = -1;
      error_code = ER_FAILED;
      goto error;
    }
  else if (mobjs->num_objs != 0)
    {
      heap_scancache_end_when_scan_will_resume (thread_p, &scan_cache);
      /* Set the last_oid.. and the number of fetched objects */
      obj = LC_PRIOR_ONEOBJ_PTR_IN_COPYAREA (obj);
      COPY_OID (last_oid, &obj->oid);
      *nfetched += mobjs->num_objs;
      /*
       * If the guess on the number of objects to fetch was low, reset the
       * value, so that the caller continue to call us until the end of the
       * scan
       */
      if (*nobjects <= *nfetched)
	{
	  *nobjects = *nfetched + 10;
	}
    }
  else
    {
      error_code = heap_scancache_end (thread_p, &scan_cache);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

end:

  return error_code;

error:
  if (*fetch_area != NULL)
    {
      locator_free_copy_area (*fetch_area);
      *fetch_area = NULL;
    }
  return error_code;
}
#endif
