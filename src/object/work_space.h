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
 * work_space.h: External definitions for the workspace manager.
 *
 */

#ifndef _WORK_SPACE_H_
#define _WORK_SPACE_H_

#ident "$Id$"

#include <stdio.h>
#include "oid.h"
#include "storage_common.h"
#include "dbtype.h"
#include "dbdef.h"
#include "quick_fit.h"
#include "locator.h"

typedef struct ws_repl_flush_err WS_REPL_FLUSH_ERR;
struct ws_repl_flush_err
{
  struct ws_repl_flush_err *error_link;
  OID class_oid;
  int operation;
  int error_code;
  char *error_msg;
  DB_IDXKEY key;		/* PK */
};

typedef struct ws_repl_obj WS_REPL_OBJ;
struct ws_repl_obj
{
  struct ws_repl_obj *next;
  char class_name[1024];
  DB_IDXKEY key;		/* PK */
  int operation;
  RECDES *recdes;
};

typedef struct ws_repl_list WS_REPL_LIST;
struct ws_repl_list
{
  WS_REPL_OBJ *head;
  WS_REPL_OBJ *tail;
  int num_items;
};

/*
 * DB_OBJECT
 *    This is the primary workspace structure used as a reference to a
 *    persistent database object.  Commonly known as a "memory object
 *    pointer" or MOP.
 *    This is typedefd as DB_OBJECT rather than WS_OBJECT because this
 *    structure is visible at all levels and it makes it easier to pass
 *    these around.
 */

/* typede for DB_OBJECT & MOP is in dbtype.h */

struct db_object
{

  OID ws_oid;			/* local copy of the OID pointer */
  struct db_object *class_mop;	/* pointer to class */
  void *object;			/* pointer to attribute values */

  struct db_object *class_link;	/* link for class instances list */
  struct db_object *dirty_link;	/* link for dirty list */
  struct db_object *hash_link;	/* link for workspace hash table */
  struct db_object *commit_link;	/* link for obj to be reset at commit/abort */
  LOCK ws_lock;			/* object lock */

  unsigned dirty:1;		/* dirty flag */
  unsigned deleted:1;		/* deleted flag */
  unsigned no_objects:1;	/* optimization for classes */
  unsigned pinned:1;		/* to prevent swapping */
};


typedef struct ws_memoid WS_MEMOID;

struct ws_memoid
{
  OID oid;
  MOP pointer;
};

/*
 * MOBJ
 *    Defines a pointer to the memory allocated for an object in the
 *    workspace.  This is not a MOP but rather the space used for the
 *    storage of the object's attributes.
 *    Might need to make this void* if the pointer sizes are different.
 *
 */

typedef char *MOBJ;

/*
 * MAPFUNC
 *    Shorthand typedef for the function that is passed to the workspace
 *    mapping functions.
 */

typedef int (*MAPFUNC) (MOP mop, void *args);

/*
 * WS_MAP
 *    Performance hack used by the locator.
 *    Used to map over the MOPs in the workspace without calling a mapping
 *    function.  Probably minor performence increase by avoiding some
 *    function calls.
 */

#define WS_MAP(mopvar, stuff) \
  { int __slot__; MOP __mop__; \
    for (__slot__ = 0 ; __slot__ < ws_Mop_table_size ; __slot__++) { \
      for (__mop__ = ws_Mop_table[__slot__].head ; __mop__ != NULL ; \
           __mop__ = __mop__->hash_link) { \
        mopvar = __mop__; \
        stuff \
	} \
    } \
  }

/*
 * IS_CLASS_MOP, IS_ROOT_MOP
 *    Macros for testing types of MOPs.
 *    Could make these functions so we don't need to introduce the
 *    rootclass globals here.
 *    IS_ROOT_MOP is non-zero if the MOP is the rootclass.
 *    IS_CLASS_MOP is non-zero if the class MOP of the object is the
 *    rootclass.
 */

#define IS_CLASS_MOP(mop) (((mop)->class_mop == sm_Root_class_mop) ? 1 : 0)
#define IS_ROOT_MOP(mop) (((mop) == sm_Root_class_mop) ? 1 : 0)

/*
 * WS_STATISTICS
 *    This maintains misc information about the workspace.
 *    It is public primarily for testing purposes.
 */


typedef struct ws_statistics WS_STATISTICS;
struct ws_statistics
{
  int mops_allocated;		/* total number of mops allocated */
  int mops_freed;		/* total reclaimed mops */

  int dirty_list_emergencies;
  int corruptions;
  int uncached_classes;

  int pinned_cleanings;
  int ignored_class_assignments;

  int set_mops_allocated;
  int set_mops_freed;

  int instance_list_emergencies;
};


/*
 * MOP_ITERATOR
 *   Iterator used to walk the MOP table.
 *   Used when it is not possible to access global ws_Mop_table_size
 *   and ws_Mop_table directly.
 */

typedef struct mop_iterator
{
  MOP next;
  unsigned int index;
} MOP_ITERATOR;


/*
 * MOP access macros
 *    Miscellaneous macros that access fields in the MOP structure.
 *    Should use these rather than direct references into the MOP.
 *
 */

#define WS_PUT_COMMIT_MOP(mop)                                     \
  do {                                                              \
    if (!(mop)->commit_link) {                                      \
      (mop)->commit_link = ws_Commit_mops ? ws_Commit_mops : (mop); \
      ws_Commit_mops = (mop);                                       \
     }                                                              \
  } while (0)

#define WS_ISDIRTY(mop) ((mop)->dirty)

#define WS_SET_DIRTY(mop)            \
  do {                               \
    if (!WS_ISDIRTY(mop)) {          \
      (mop)->dirty = 1;              \
      WS_PUT_COMMIT_MOP(mop);        \
      ws_Num_dirty_mop++;            \
    }                                \
  } while (0)

#define WS_RESET_DIRTY(mop)          \
  do {                               \
    if (WS_ISDIRTY(mop)) {           \
      (mop)->dirty = 0;              \
      ws_Num_dirty_mop--;            \
    }                                \
  } while (0)

#define WS_ISMARK_DELETED(mop) ((mop)->deleted)
#define WS_MARKED_DELETED(mop) ((mop)->deleted)

#define WS_SET_DELETED(mop)          \
  do {                               \
    (mop)->deleted = 1;              \
    WS_PUT_COMMIT_MOP(mop);          \
  } while (0)

/*
 * There are also functions for these, should use the macro since they
 * aren't very complicated
 */
#define WS_OID(mop)		(&((mop)->ws_oid))
#define WS_CLASS_MOP(mop) 	((mop)->class_mop)
#define WS_SET_LOCK(mop, lock)      \
  do {                              \
    (mop)->ws_lock = lock;             \
    if (lock != NULL_LOCK)          \
      WS_PUT_COMMIT_MOP(mop);       \
  } while (0)

#define WS_GET_LOCK(mop)	((mop)->ws_lock)
#define WS_ISPINNED(mop) 	((mop)->pinned)
#define WS_SET_NO_OBJECTS(mop)      \
  do {                              \
    (mop)->no_objects = 1;          \
    WS_PUT_COMMIT_MOP(mop);         \
  } while (0)

/*
 * WS_MOP_IS_NULL
 *    Tests for logical "NULLness" of the MOP.
 *    This is true if the MOP pointer is NULL, the MOP has been marked as
 *    deleted, or if the MOP has the "NULL OID".
 *    Note that we have to test for non-virtual MOPs before comparing
 *    against the NULL OID.
 */

#define WS_MOP_IS_NULL(mop)                                          \
  (((mop == NULL) || WS_ISMARK_DELETED(mop) ||                       \
    (OID_ISNULL(&((mop)->ws_oid)))) ? 1 : 0)

/*
 * WS_MAP constants
 *    These are returned as status codes by the workspace mapping functions.
 */


typedef enum ws_map_status_ WS_MAP_STATUS;
enum ws_map_status_
{
  WS_MAP_CONTINUE = 0,
  WS_MAP_FAIL = 1,
  WS_MAP_STOP = 2,
  WS_MAP_SUCCESS = 3
};

/*
 * WS_FIND_MOP constants
 *    These are returned as status codes by the ws_find function.
 */
typedef enum ws_find_mop_status_ WS_FIND_MOP_STATUS;
enum ws_find_mop_status_
{
  WS_FIND_MOP_DELETED = 0,
  WS_FIND_MOP_NOTDELETED = 1
};

/*
 * WS_MOP_TABLE_ENTRY
 */
typedef struct ws_mop_table_entry WS_MOP_TABLE_ENTRY;
struct ws_mop_table_entry
{
  MOP head;
  MOP tail;
};

/*
 * WORKSPACE GLOBALS
 */
extern WS_MOP_TABLE_ENTRY *ws_Mop_table;
extern unsigned int ws_Mop_table_size;
extern MOP ws_Reference_mops;
extern DB_OBJLIST *ws_Resident_classes;
extern MOP ws_Commit_mops;
extern WS_STATISTICS ws_Stats;
extern int ws_Num_dirty_mop;

/*
 *  WORKSPACE FUNCTIONS
 */
/* memory crisis */
extern void ws_abort_transaction (void);

/* startup, shutdown, reset functions */
extern int ws_init (void);
extern void ws_final (void);
extern void ws_clear (void);

/* MOP allocation functions */
extern MOP ws_mop (OID * oid, MOP class_mop);
#if defined (ENABLE_UNUSED_FUNCTION)
extern MOP ws_new_mop (OID * oid, MOP class_mop);
#endif
extern void ws_perm_oid (MOP mop, OID * newoid);
extern int ws_perm_oid_and_class (MOP mop, OID * new_oid,
				  OID * new_class_oid);

/* Set mops */
#if 0
extern MOP ws_make_set_mop (void *setptr);
#endif

/* Dirty list maintenance */
extern void ws_dirty (MOP op);
extern void ws_clean (MOP op);
extern int ws_map_dirty (MAPFUNC function, void *args);
extern void ws_filter_dirty (void);
extern int ws_map_class_dirty (MOP class_op, MAPFUNC function, void *args);

/* Resident instance list maintenance */
#if defined (ENABLE_UNUSED_FUNCTION)	/* TODO - */
extern void ws_set_class (MOP inst, MOP class_mop);
#endif
extern int ws_map_class (MOP class_op, MAPFUNC function, void *args);
extern void ws_mark_instances_deleted (MOP class_op);
extern void ws_remove_resident_class (MOP class_op);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void ws_intern_instances (MOP class_mop);
#endif
extern void ws_disconnect_deleted_instances (MOP class_mop);

/* object cache */
extern void ws_cache (MOBJ obj, MOP mop, MOP class_mop);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void ws_cache_dirty (MOBJ obj, MOP mop, MOP class_mop);
#endif
extern MOP ws_cache_with_oid (MOBJ obj, OID * oid, MOP class_mop);

/* Class name cache */
extern MOP ws_find_class (const char *name);
extern void ws_add_classname (MOBJ classobj, MOP classmop,
			      const char *cl_name);
extern void ws_drop_classname (MOBJ classobj);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void ws_reset_classname_cache (void);
#endif

/* MOP accessor functions */
extern OID *ws_identifier (MOP mop);
extern OID *ws_oid (MOP mop);
extern MOP ws_class_mop (MOP mop);
extern LOCK ws_get_lock (MOP mop);
extern void ws_set_lock (MOP mop, LOCK lock);
extern void ws_mark_deleted (MOP mop);

/* pin functions */
extern int ws_pin (MOP mop, int pin);
extern void ws_pin_instance_and_class (MOP obj, int *opin, int *cpin);
extern void ws_restore_pin (MOP obj, int opin, int cpin);

/* Misc info */
extern int ws_find (MOP mop, MOBJ * obj);
extern int ws_mop_compare (MOP mop1, MOP mop2);
extern void ws_class_has_object_dependencies (MOP mop);
extern int ws_class_has_cached_objects (MOP class_mop);
extern bool ws_has_updated (void);

/* MOP mapping functions */
#if defined (RYE_DEBUG)
extern int ws_map (MAPFUNC function, void *args);
#endif

/* Transaction management support */
#if 0
extern void ws_reset_authorization_cache (void);
#endif
extern void ws_clear_hints (MOP obj, bool leave_pinned);
extern void ws_clear_all_hints (void);
extern void ws_abort_mops (bool only_unpinned);

/* Debugging functions */
extern void ws_dump (FILE * fpp);
#if defined (RYE_DEBUG)
extern void ws_dump_mops (void);
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
extern MOP ws_makemop (int volid, int pageid, int slotid);
extern int ws_count_mops (void);
#endif

/* String utilities */
extern char *ws_copy_string (const char *str);
extern void ws_free_string (const char *str);

/*
 * DB_LIST functions
 *    General purpose list functions, don't have to be part of the workspace
 *    except that the coyp functions assume that they can allocate
 *    within the workspace.
 *    Assumes that the first slot in the structure is a "next" pointer.
 *    Technically, this should use imbedded structures but this hasn't
 *    been a problem for any system yet.
 */

typedef void *(*LCOPIER) (void *);
typedef void (*LFREEER) (void *);
typedef int (*LTOTALER) (void *);

extern void ws_list_free (DB_LIST * list, LFREEER function);
extern int ws_list_total (DB_LIST * list, LTOTALER function);
extern int ws_list_remove (DB_LIST ** list, DB_LIST * element);
extern int ws_list_length (DB_LIST * list);
extern void ws_list_append (DB_LIST ** list, DB_LIST * element);
extern DB_LIST *ws_list_copy (DB_LIST * list, LCOPIER copier, LFREEER freeer);
extern DB_LIST *ws_list_nconc (DB_LIST * list1, DB_LIST * list2);

#define WS_LIST_LENGTH(lst) \
  ws_list_length((DB_LIST *)(lst))
#define WS_LIST_FREE(lst, func) \
  ws_list_free((DB_LIST *)(lst), (LFREEER) func)
#define WS_LIST_APPEND(lst, element) \
  ws_list_append((DB_LIST **)(lst), (DB_LIST *)element)
#define WS_LIST_COPY(lst, copier, freeer) \
  ws_list_copy((DB_LIST *)(lst), (LCOPIER)(copier), (LFREEER)(freeer))
#define WS_LIST_REMOVE(lst, element) \
  ws_list_remove((DB_LIST **)(lst), (DB_LIST *)element)
#define WS_LIST_NCONC(lst1, lst2) \
  ws_list_nconc((DB_LIST *)(lst1), (DB_LIST *)lst2)

/*
 * DB_NAMELIST functions
 *    This is an extension of the basic LIST functions that provide
 *    an additional slot for a name string.  Manipulating lists of this
 *    form is extremely common in the schema manager.
 *    Modified 4/20/93 to supply an optional function to perform
 *    the comparison.  This is primarily for case insensitivity.
 */

/*
 * can't use int return code as this must remain compatible with
 * strcmp() and mbs_strcmp.
 */
typedef DB_C_INT (*NLSEARCHER) (const void *, const void *);

#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_NAMELIST *nlist_remove (DB_NAMELIST ** list, const char *name,
				  NLSEARCHER fcn);
extern int nlist_append (DB_NAMELIST ** list, const char *name,
			 NLSEARCHER fcn, int *added);
extern int nlist_find_or_append (DB_NAMELIST ** list, const char *name,
				 NLSEARCHER fcn, int *position);
#endif
extern DB_NAMELIST *nlist_filter (DB_NAMELIST ** root, const char *name,
				  NLSEARCHER fcn);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_NAMELIST *nlist_copy (DB_NAMELIST * list);
#endif
extern void nlist_free (DB_NAMELIST * list);

/*
 * DB_OBJLIST functions
 *    This is an extension of the basic LIST functions that provide an
 *    additional slot for a MOP pointer.  Manipulating lists of MOPs is
 *    an extremely common operation in the schema manager.
 *    This has a DB_ prefix because it is visible at the application level.
 */

typedef int (*MOPFILTER) (MOP op, void *args);

extern int ml_find (DB_OBJLIST * list, MOP mop);
extern int ml_add (DB_OBJLIST ** list, MOP mop, int *added);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int ml_append (DB_OBJLIST ** list, MOP mop, int *added);
#endif
extern int ml_remove (DB_OBJLIST ** list, MOP mop);
extern void ml_free (DB_OBJLIST * list);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int ml_size (DB_OBJLIST * list);
extern DB_OBJLIST *ml_copy (DB_OBJLIST * list);
extern void ml_filter (DB_OBJLIST ** list, MOPFILTER filter, void *args);
#endif

/*
 * These are external MOP lists that are passed beyond the db_ layer into
 * user application space.  They must be allocated in a special region so they
 * are visible as roots for the garbage collector
 */

extern DB_OBJLIST *ml_ext_alloc_link (void);
extern void ml_ext_free_link (DB_OBJLIST * list);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_OBJLIST *ml_ext_copy (DB_OBJLIST * list);
#endif
extern void ml_ext_free (DB_OBJLIST * list);
#if 0
extern int ml_ext_add (DB_OBJLIST ** list, MOP mop, int *added);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
extern int ws_has_dirty_objects (MOP op, int *isvirt);
#endif

extern bool ws_need_flush (void);

extern int ws_add_to_repl_obj_list (const char *class_name,
				    DB_IDXKEY * key,
				    RECDES * recdes, int operation);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void ws_init_repl_objs (void);
#endif
extern void ws_clear_all_repl_objs (void);
extern void ws_free_repl_obj (WS_REPL_OBJ * obj);
extern WS_REPL_OBJ *ws_get_repl_obj_from_list (void);

extern void ws_set_repl_error_into_error_link (LC_COPYAREA_ONEOBJ * obj,
					       char *content_ptr);

extern WS_REPL_FLUSH_ERR *ws_get_repl_error_from_error_link (void);
extern int ws_clear_all_repl_errors_of_error_link (void);
extern void ws_free_repl_flush_error (WS_REPL_FLUSH_ERR * flush_err);
#endif /* _WORK_SPACE_H_ */
