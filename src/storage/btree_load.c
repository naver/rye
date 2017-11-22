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
 * btree_load.c - B+-Tree Loader
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "btree_load.h"
#include "xserver_interface.h"
#include "file_io.h"
#include "page_buffer.h"
#include "heap_file.h"
#include "file_manager.h"
#include "xserver_interface.h"
#include "perf_monitor.h"

/*
 * xbtree_load_data () - load b+tree index
 *   return: error code or NO_ERROR
 *
 *   btid: B+tree index identifier
 *   class_oid(in): OID of the class for which the index will be created
 *
 */
int
xbtree_load_data (THREAD_ENTRY * thread_p, BTID * btid,
		  OID * class_oid, HFID * hfid)
{
  int status = NO_ERROR;
  SCAN_CODE scan_result;
  DB_IDXKEY key;
  HEAP_SCANCACHE hfscan_cache;
  OID cur_oid;
  RECDES peek_rec = RECDES_INITIALIZER;
  HEAP_CACHE_ATTRINFO attr_info;
  int index_id;
  OR_INDEX *indexp = NULL;
  BTID_INT btid_int;
  int error = NO_ERROR;
  bool attrinfo_inited = false, scancache_inited = false;
  bool top_op_active = false;

  thread_mnt_track_push (thread_p,
			 MNT_STATS_DATA_PAGE_FETCHES_TRACK_BTREE_LOAD_DATA,
			 &status);

  DB_IDXKEY_MAKE_NULL (&key);

#if 1				/* TODO - */
  if (HFID_IS_NULL (hfid))
    {
      /* is from createdb; au_install () */
      if (status == NO_ERROR)
	{
	  thread_mnt_track_pop (thread_p, &status);
	  assert (status == NO_ERROR);
	}

      return NO_ERROR;
    }
#endif

  mnt_stats_event_on (thread_p, MNT_STATS_BTREE_LOAD_DATA);

#if defined(SERVER_MODE)
  {
    DB_VALUE oid_val;
    int tran_index;
    LOG_TDES *tdes;

    DB_MAKE_OID (&oid_val, class_oid);
    if (lock_get_current_lock (thread_p, &oid_val) != U_LOCK)
      {
	assert (false);
	error = ER_GENERIC_ERROR;
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		"xbtree_load_data(): Not acquired U_LOCK on table");
	GOTO_EXIT_ON_ERROR;
      }

    tran_index = logtb_get_current_tran_index (thread_p);
    tdes = LOG_FIND_TDES (tran_index);
    assert (tdes != NULL);
    if (tdes == NULL || tdes->type != TRAN_TYPE_DDL)
      {
	assert (false);
	error = ER_GENERIC_ERROR;
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		"xbtree_load_data(): Not found tran_type TRAN_TYPE_DDL");
	GOTO_EXIT_ON_ERROR;
      }
  }
#endif /* SERVER_MODE */

  error = heap_scancache_start (thread_p, &hfscan_cache, hfid,
				class_oid, true);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  VPID_SET_NULL (&hfscan_cache.last_vpid);
  if (file_find_last_page (thread_p, &hfid->vfid,
			   &hfscan_cache.last_vpid) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  hfscan_cache.read_committed_page = true;
  scancache_inited = true;

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid_int);

  btid_int.sys_btid = btid;
  COPY_OID (&(btid_int.cls_oid), class_oid);

  index_id = heap_attrinfo_start_with_btid (thread_p, class_oid,
					    btid_int.sys_btid, &attr_info);
  if (index_id < 0)
    {
      error = index_id;
      GOTO_EXIT_ON_ERROR;
    }

  attrinfo_inited = true;

  indexp = &(attr_info.last_classrepr->indexes[index_id]);
  assert (INDEX_IS_IN_PROGRESS (indexp));
  if (INDEX_IS_PRIMARY_KEY (indexp))
    {
      assert (false);		/* is impossible */

      /* something wrong */
      error = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      GOTO_EXIT_ON_ERROR;
    }

  btid_int.classrepr = attr_info.last_classrepr;
  btid_int.classrepr_cache_idx = attr_info.last_cacheindex;
  btid_int.indx_id = index_id;

  OID_SET_NULL (&cur_oid);
  while ((scan_result = heap_next (thread_p, hfid, class_oid,
				   &cur_oid, &peek_rec,
				   &hfscan_cache, PEEK)) == S_SUCCESS)
    {
      error = heap_attrinfo_read_dbvalues (thread_p, &cur_oid, &peek_rec,
					   &attr_info);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (DB_IDXKEY_IS_NULL (&key));
      error = heap_attrvalue_get_key (thread_p, index_id,
				      &attr_info, &cur_oid, &peek_rec,
				      btid, &key);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (log_start_system_op (thread_p) == NULL)
	{
	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}
      top_op_active = true;

      if (btree_insert (thread_p, &btid_int, &key) == NULL)
	{
	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}

      log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
      top_op_active = false;

      db_idxkey_clear (&key);
    }				/* while (...) */

  if (scan_result != S_END)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  heap_attrinfo_end (thread_p, &attr_info);
  (void) heap_scancache_end (thread_p, &hfscan_cache);

  mnt_stats_event_off (thread_p, MNT_STATS_BTREE_LOAD_DATA);

  if (status == NO_ERROR)
    {
      thread_mnt_track_pop (thread_p, &status);
      assert (status == NO_ERROR);
    }

  return error;

exit_on_error:

  mnt_stats_event_off (thread_p, MNT_STATS_BTREE_LOAD_DATA);

  if (top_op_active == true)
    {
      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
    }

  db_idxkey_clear (&key);
  if (attrinfo_inited == true)
    {
      heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scancache_inited == true)
    {
      (void) heap_scancache_end (thread_p, &hfscan_cache);
    }

  if (error == NO_ERROR)
    {
      assert (false);
      error = ER_FAILED;
    }

  if (status == NO_ERROR)
    {
      thread_mnt_track_pop (thread_p, &status);
      assert (status == NO_ERROR);
    }

  return error;
}

/*
 * btree_rv_undo_create_index () - Undo the creation of an index file
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: The index file is destroyed completely.
 */
int
btree_rv_undo_create_index (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  const VFID *vfid;
  int ret;

  vfid = (const VFID *) rcv->data;
  ret = file_destroy (thread_p, vfid);

  assert (ret == NO_ERROR);

  return ((ret == NO_ERROR) ? NO_ERROR : er_errid ());
}

/*
 * btree_rv_dump_create_index () -
 *   return: int
 *   length_ignore(in): Length of Recovery Data
 *   data(in): The data being logged
 *
 * Note: Dump the information to undo the creation of an index file
 */
void
btree_rv_dump_create_index (FILE * fp, UNUSED_ARG int length_ignore,
			    void *data)
{
  VFID *vfid;

  vfid = (VFID *) data;
  (void) fprintf (fp, "Undo creation of Index vfid: %d|%d\n",
		  vfid->volid, vfid->fileid);
}
