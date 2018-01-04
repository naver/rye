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
 * btree.c - B+-Tree mananger
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "btree_load.h"
#include "storage_common.h"
#include "error_manager.h"
#include "page_buffer.h"
#include "file_io.h"
#include "file_manager.h"
#include "slotted_page.h"
#include "oid.h"
#include "log_manager.h"
#include "memory_alloc.h"
#include "overflow_file.h"
#include "xserver_interface.h"
#include "btree.h"
#include "scan_manager.h"
#include "thread.h"
#include "heap_file.h"
#include "object_primitive.h"
#include "object_print.h"
#include "list_file.h"
#include "fetch.h"
#include "connection_defs.h"
#include "locator_sr.h"
#include "network_interface_sr.h"
#include "utility.h"
#include "perf_monitor.h"

#include "fault_injection.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define DISK_PAGE_BITS  (DB_PAGESIZE * CHAR_BIT)	/* Num of bits per page   */
#define RESERVED_SIZE_IN_PAGE   sizeof(FILEIO_PAGE_RESERVED)

/* Structure used by btree_range_search to initialize and handle variables
 * needed throughout the process.
 */
typedef struct btree_range_search_helper BTREE_RANGE_SEARCH_HELPER;
struct btree_range_search_helper
{
  OID *mem_oid_ptr;		/* Pointer to OID memory storage */
  int mem_oid_cnt;		/* The capacity of OID memory storage */
  int num_copied_oids;		/* Current count of stored OID's */
};

static PAGE_PTR btree_initialize_new_page_helper (THREAD_ENTRY * thread_p,
						  const VFID * vfid,
						  const FILE_TYPE file_type,
						  const VPID * vpid,
						  INT32 ignore_npages,
						  const unsigned short
						  alignment,
						  const PAGE_TYPE ptype);
static bool btree_initialize_new_root (THREAD_ENTRY * thread_p,
				       const VFID * vfid,
				       const FILE_TYPE file_type,
				       const VPID * vpid, INT32 ignore_npages,
				       void *args);
static bool btree_initialize_new_page (THREAD_ENTRY * thread_p,
				       const VFID * vfid,
				       const FILE_TYPE file_type,
				       const VPID * vpid, INT32 ignore_npages,
				       void *args);

#if defined (ENABLE_UNUSED_FUNCTION)
static DISK_ISVALID btree_check_tree (THREAD_ENTRY * thread_p, BTID * btid);
static DISK_ISVALID btree_check_by_btid (THREAD_ENTRY * thread_p,
					 BTID * btid);
#endif

static void btree_write_fixed_portion_of_non_leaf_record_to_orbuf (OR_BUF *
								   buf,
								   NON_LEAF_REC
								   * nlf_rec);
static int btree_read_fixed_portion_of_non_leaf_record_from_orbuf (OR_BUF *
								   buf,
								   NON_LEAF_REC
								   * nlf_rec);
static int btree_dealloc_page (THREAD_ENTRY * thread_p, BTID_INT * btid,
			       VPID * vpid);
#if defined (ENABLE_UNUSED_FUNCTION)
static DISK_ISVALID btree_check_pages (THREAD_ENTRY * thread_p,
				       BTID_INT * btid, PAGE_PTR pg_ptr,
				       VPID * pg_vpid);
#endif
static int btree_delete_from_leaf (THREAD_ENTRY * thread_p, BTID_INT * btid,
				   PAGE_PTR page_ptr, const DB_IDXKEY * key);
static int btree_merge_level (THREAD_ENTRY * thread_p, BTID_INT * btid,
			      DB_IDXKEY * key, int P_req_mode,
			      short merge_level);
#if defined (ENABLE_UNUSED_FUNCTION)
static int btree_merge_root (THREAD_ENTRY * thread_p, BTID_INT * btid,
			     PAGE_PTR P, PAGE_PTR Q, PAGE_PTR R,
			     VPID * P_vpid, VPID * Q_vpid, VPID * R_vpid,
			     short node_type);
#endif
static int btree_merge_node (THREAD_ENTRY * thread_p, BTID_INT * btid,
			     PAGE_PTR P, PAGE_PTR L, PAGE_PTR Q,
			     UNUSED_ARG VPID * P_vpid, VPID * L_vpid,
			     VPID * Q_vpid, INT16 p_slot_id, short node_type,
			     VPID * child_vpid);
static PAGE_PTR btree_locate_key (THREAD_ENTRY * thread_p,
				  BTID_INT * btid_int, const DB_IDXKEY * key,
				  VPID * pg_vpid, INT16 * slot_id,
				  bool * found);
static PAGE_PTR btree_find_first_leaf (THREAD_ENTRY * thread_p,
				       const BTID_INT * btid,
				       const VPID * top_vpid, VPID * pg_vpid,
				       BTREE_STATS * stat_info);
static PAGE_PTR btree_find_last_leaf (THREAD_ENTRY * thread_p,
				      const BTID_INT * btid,
				      const VPID * top_vpid, VPID * pg_vpid);
static int btree_initialize_bts (THREAD_ENTRY * thread_p, BTREE_SCAN * bts,
				 KEY_VAL_RANGE * key_val_range,
				 FILTER_INFO * filter);
static int btree_prepare_first_search (THREAD_ENTRY * thread_p,
				       BTREE_SCAN * bts);
static int btree_prepare_next_search (THREAD_ENTRY * thread_p,
				      BTREE_SCAN * bts);
static int btree_apply_key_range_and_filter (THREAD_ENTRY * thread_p,
					     BTREE_SCAN * bts,
					     bool * key_range_satisfied,
					     bool * key_filter_satisfied);
static int btree_dump_curr_key (THREAD_ENTRY * thread_p,
				INDX_SCAN_ID * iscan_id);
static void btree_rv_read_keyval_info_nocopy (THREAD_ENTRY * thread_p,
					      char *datap, int data_size,
					      BTID_INT * btid,
					      DB_IDXKEY * key);

static int btree_range_opt_check_add_index_key (THREAD_ENTRY * thread_p,
						BTREE_SCAN * bts,
						MULTI_RANGE_OPT *
						multi_range_opt,
						bool * key_added);
static int btree_top_n_items_binary_search (RANGE_OPT_ITEM ** top_n_items,
					    int *att_idxs,
					    TP_DOMAIN ** domains,
					    bool * desc_order,
					    DB_VALUE * new_key_values,
					    int no_keys, int first, int last,
					    int *new_pos);
static int btree_delete_key_from_leaf (THREAD_ENTRY * thread_p,
				       BTID_INT * btid, PAGE_PTR leaf_pg,
				       INT16 slot_id, const DB_IDXKEY * key);

static void btree_range_search_init_helper (THREAD_ENTRY * thread_p,
					    BTREE_RANGE_SEARCH_HELPER *
					    btrs_helper,
					    BTREE_SCAN * bts,
					    INDX_SCAN_ID * index_scan_id_p);
static int btree_next_range_search (THREAD_ENTRY * thread_p,
				    SCAN_CODE * scan_code, BTREE_SCAN * bts,
				    BTREE_RANGE_SEARCH_HELPER * btrs_helper,
				    INDX_SCAN_ID * index_scan_id_p);
static int btree_prepare_range_search (THREAD_ENTRY * thread_p,
				       BTREE_SCAN * bts);
static int btree_get_satisfied_key (THREAD_ENTRY * thread_p,
				    BTREE_CHECK_KEY * key_check,
				    BTREE_SCAN * bts,
				    BTREE_RANGE_SEARCH_HELPER * btrs_helper,
				    INDX_SCAN_ID * index_scan_id_p);
static int btree_save_range_search_result (THREAD_ENTRY * thread_p,
					   BTREE_RANGE_SEARCH_HELPER *
					   btrs_helper,
					   INDX_SCAN_ID * index_scan_id_p);

static int btree_check_key_cnt (BTID_INT * btid, PAGE_PTR page_p,
				short node_level, short key_cnt);

/*
 * btree_clear_key_value () -
 *   return: cleared flag
 *   clear_flag (in/out):
 *   key (in/out):
 */
bool
btree_clear_key_value (bool * clear_flag, DB_IDXKEY * key)
{
  assert (key != NULL);

  if (*clear_flag == true)
    {
      db_idxkey_clear (key);
      *clear_flag = false;
    }
  else
    {
      DB_IDXKEY_MAKE_NULL (key);
    }

  return *clear_flag;
}

/*
 * Common utility routines
 */

/*
 * btree_init_node_header () -
 *   return:
 *   node_header(in):
 *
 * Note:
 */
int
btree_init_node_header (UNUSED_ARG THREAD_ENTRY * thread_p,
			BTREE_NODE_HEADER * node_header)
{
  node_header->split_info.pivot = 0.0f;
  node_header->split_info.index = 0;
  VPID_SET_NULL (&node_header->next_vpid);
  VPID_SET_NULL (&node_header->prev_vpid);

  node_header->key_cnt = 0;
  node_header->node_level = 1;

  return NO_ERROR;
}

/*
 * btree_write_node_header () -
 *   return:
 *   pg_ptr(out):
 *   header(in):
 *
 * Note: Writes the first record (header record) for non root page.
 * rec must be long enough to hold the header record.
 *
 */
int
btree_write_node_header (BTID_INT * btid, PAGE_PTR pg_ptr,
			 BTREE_NODE_HEADER * header)
{
  RECDES peek_rec = RECDES_INITIALIZER;
  int error_code = NO_ERROR;

  error_code =
    btree_check_key_cnt (btid, pg_ptr, header->node_level, header->key_cnt);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  if (spage_get_record (pg_ptr, HEADER, &peek_rec, PEEK) != S_SUCCESS)
    {
      error_code = er_errid ();
      goto error;
    }
  assert (peek_rec.length == sizeof (BTREE_NODE_HEADER));

  *((BTREE_NODE_HEADER *) (peek_rec.data)) = *header;

  return NO_ERROR;

error:

  if (error_code == NO_ERROR)
    {
      assert (error_code != NO_ERROR);
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * btree_insert_node_header () -
 *   return:
 */
int
btree_insert_node_header (THREAD_ENTRY * thread_p, PAGE_PTR pg_ptr,
			  BTREE_NODE_HEADER * header)
{
  PGNSLOTS num_slots;		/* Number of allocated slots for the page */
  PGNSLOTS num_records;		/* Number of records on page */
  RECDES rec = RECDES_INITIALIZER;
  int error_code = NO_ERROR;

  num_slots = spage_number_of_slots (pg_ptr);
  num_records = spage_number_of_records (pg_ptr);
  if (header != NULL && num_slots == 0 && num_records == 0)
    {
      ;				/* OK - go ahead */
    }
  else
    {				/* TODO - index crash */
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  rec.data = (char *) header;
  rec.area_size = rec.length = sizeof (BTREE_NODE_HEADER);
  rec.type = REC_HOME;

  if (spage_insert_at (thread_p, pg_ptr, HEADER, &rec) != SP_SUCCESS)
    {
      /* Cannot happen; header is smaller than new page... */
      error_code = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  return NO_ERROR;

exit_on_error:

  if (error_code == NO_ERROR)
    {
      assert (error_code != NO_ERROR);
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * btree_read_node_header () -
 *   return:
 *   pg_ptr(in):
 *   header(out):
 *
 * Note: Reads the first record (header record) for a non root page.
 */
int
btree_read_node_header (BTID_INT * btid, PAGE_PTR pg_ptr,
			BTREE_NODE_HEADER * header)
{
  RECDES peek_rec = RECDES_INITIALIZER;
  int error_code = NO_ERROR;

  if (spage_get_record (pg_ptr, HEADER, &peek_rec, PEEK) != S_SUCCESS)
    {
      error_code = er_errid ();
      goto error;
    }
  assert (peek_rec.length == sizeof (BTREE_NODE_HEADER));

  *header = *((BTREE_NODE_HEADER *) (peek_rec.data));

  error_code =
    btree_check_key_cnt (btid, pg_ptr, header->node_level, header->key_cnt);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  return NO_ERROR;

error:

  if (error_code == NO_ERROR)
    {
      assert (error_code != NO_ERROR);
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * btree_write_fixed_portion_of_non_leaf_record () -
 *   return:
 *   rec(in):
 *   non_leaf_rec(in):
 *
 * Note: Writes the fixed portion (preamble) of a non leaf record.
 * rec must be long enough to hold the header info.
 */
void
btree_write_fixed_portion_of_non_leaf_record (RECDES * rec,
					      NON_LEAF_REC * non_leaf_rec)
{
  char *ptr = rec->data;

  OR_PUT_INT (ptr, non_leaf_rec->pnt.pageid);
  ptr += OR_INT_SIZE;

  OR_PUT_SHORT (ptr, non_leaf_rec->pnt.volid);
}

/*
 * btree_read_fixed_portion_of_non_leaf_record () -
 *   return:
 *   rec(in):
 *   non_leaf_rec(in):
 *
 * Note: Reads the fixed portion (preamble) of a non leaf record.
 */
void
btree_read_fixed_portion_of_non_leaf_record (RECDES * rec,
					     NON_LEAF_REC * non_leaf_rec)
{
  char *ptr = rec->data;

  non_leaf_rec->pnt.pageid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  non_leaf_rec->pnt.volid = OR_GET_SHORT (ptr);
}

/*
 * btree_write_fixed_portion_of_non_leaf_record_to_orbuf () -
 *   return:
 *   buf(in):
 *   nlf_rec(in):
 *
 * Note: Writes the fixed portion (preamble) of a non leaf record using
 * the OR_BUF stuff.
 */
static void
btree_write_fixed_portion_of_non_leaf_record_to_orbuf (OR_BUF * buf,
						       NON_LEAF_REC *
						       non_leaf_rec)
{
  or_put_int (buf, non_leaf_rec->pnt.pageid);
  or_put_short (buf, non_leaf_rec->pnt.volid);
}

/*
 * btree_read_fixed_portion_of_non_leaf_record_from_orbuf () -
 *   return: NO_ERROR
 *   buf(in):
 *   non_leaf_rec(in):
 *
 * Note: Reads the fixed portion (preamble) of a non leaf record using
 * the OR_BUF stuff.
 */
static int
btree_read_fixed_portion_of_non_leaf_record_from_orbuf (OR_BUF * buf,
							NON_LEAF_REC *
							non_leaf_rec)
{
  int rc = NO_ERROR;

  non_leaf_rec->pnt.pageid = or_get_int (buf, &rc);
  if (rc == NO_ERROR)
    {
      non_leaf_rec->pnt.volid = or_get_short (buf, &rc);
    }

  return rc;
}

/*
 * btree_get_key_length () -
 *   return:
 *   key(in):
 */
int
btree_get_key_length (const DB_IDXKEY * key)
{
  int i;
  int len = 0;

  assert (key != NULL);
  if (key == NULL)
    {
      return 0;
    }

  assert (key->size > 1);

  len = OR_MULTI_BOUND_BIT_BYTES (key->size);

  for (i = 0; i < key->size; i++)
    {
      if (DB_IS_NULL (&(key->vals[i])))
	{
	  continue;
	}

      len += pr_index_writeval_disk_size (&(key->vals[i]));
    }

  return len;
}

/*
 * btree_write_record () -
 *   return: NO_ERROR
 *   btid(in):
 *   node_rec(in):
 *   key(in):
 *   node_type(in):
 *   rec(out):
 *
 * Note: This routine forms a btree record for both leaf and non leaf pages.
 *
 * node_rec is a NON_LEAF_REC * if we are writing a non leaf page,
 * otherwise it is a LEAF_REC *. ovfl_key indicates whether the key will
 * be written to the page or stored by the overflow manager. If we are
 * writing a non leaf record, oid should be NULL and will be ignored in
 * any case.
 */
int
btree_write_record (UNUSED_ARG THREAD_ENTRY * thread_p,
		    UNUSED_ARG BTID_INT * btid, void *node_rec,
		    const DB_IDXKEY * key, int node_type, RECDES * rec)
{
  OR_BUF buf;
  char *bound_bits;
  int i;
  DB_TYPE type;
  int rc = NO_ERROR;

  assert (key != NULL);

  assert (node_type == BTREE_LEAF_NODE || node_type == BTREE_NON_LEAF_NODE);
  assert (BTREE_IS_VALID_KEY_LEN (btree_get_key_length (key)));

  assert (rec != NULL);
  assert (rec->area_size == DB_PAGESIZE);
  assert (rec->area_size > BTREE_MAX_KEYLEN);

  or_init (&buf, rec->data, BTREE_MAX_KEYLEN);	/* TODO - consider non-leaf */

  if (node_type == BTREE_NON_LEAF_NODE)
    {
      NON_LEAF_REC *non_leaf_rec = (NON_LEAF_REC *) node_rec;

      btree_write_fixed_portion_of_non_leaf_record_to_orbuf (&buf,
							     non_leaf_rec);
    }
#if !defined(NDEBUG)
  else
    {
      OID oid;

      rc = btree_get_oid_from_key (thread_p, btid, key, &oid);
      if (rc != NO_ERROR)
	{
	  goto exit_on_error;
	}
      assert (!OID_ISNULL (&oid));

      assert (oid.groupid >= GLOBAL_GROUPID);
      if (heap_classrepr_is_shard_table (thread_p, &(btid->cls_oid)) == true)
	{
	  assert (oid.groupid > GLOBAL_GROUPID);	/* is shard table */
	}
      else
	{
	  assert (oid.groupid == GLOBAL_GROUPID);	/* is global table */
	}
    }
#endif

  assert (key->size > 1);

  bound_bits = buf.ptr;

  rc = or_advance (&buf, pr_idxkey_init_boundbits (bound_bits, key->size));
  if (rc != NO_ERROR)
    {
      goto exit_on_error;
    }

  for (i = 0; rc == NO_ERROR && i < key->size; i++)
    {
      if (DB_IS_NULL (&(key->vals[i])))
	{
	  assert (OR_MULTI_ATT_IS_UNBOUND (bound_bits, i));
	  continue;
	}

      type = DB_VALUE_DOMAIN_TYPE (&(key->vals[i]));
      assert (tp_valid_indextype (type));

      rc = (*(tp_Type_id_map[type]->index_writeval)) (&buf, &key->vals[i]);

      OR_ENABLE_BOUND_BIT (bound_bits, i);
    }

  if (rc != NO_ERROR)
    {
      goto exit_on_error;
    }

  rec->length = CAST_BUFLEN (buf.ptr - buf.buffer);

#if 0
end:
#endif

  assert (rc == NO_ERROR);

  return rc;

exit_on_error:

  assert (rc != NO_ERROR);

  if (rc == ER_TF_BUFFER_OVERFLOW)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_MAX_KEYLEN, 2,
	      BTREE_MAX_KEYLEN, BTREE_MAX_KEYLEN);
    }

  return rc;
}

/*
 * btree_read_record () -
 *   return:
 *   btid(in):
 *   rec(in):
 *   key(in):
 *   rec_header(in):
 *   node_type(in):
 *   clear_key(in):
 *   copy_key(in):
 *
 * Note: This routine reads a btree record for both leaf and non leaf pages.
 *
 * copy_key indicates whether strings should be malloced and copied
 * or just returned via pointer.
 * clear_key will indicate whether the key needs
 * to be cleared via pr_clear_value by the caller.  If this record is
 * a leaf record, rec_header will be filled in with the LEAF_REC,
 * otherwise, rec_header will be filled in with the NON_LEAF_REC for this
 * record.
 *
 * If you don't want to actually read the key (possibly incurring a
 * malloc for the string), you can send a NULL pointer for the key.
 * index_readval() will do the right thing and simply skip the key in this case.
 */
int
btree_read_record (UNUSED_ARG THREAD_ENTRY * thread_p, BTID_INT * btid,
		   RECDES * rec, DB_IDXKEY * key,
		   void *rec_header, int node_type, bool * clear_key,
		   int copy_key)
{
  int error = NO_ERROR;
  OR_BUF buf;
  NON_LEAF_REC *non_leaf_rec = NULL;
  OR_INDEX *indexp = NULL;
  char *bound_bits;
  int i;
  DB_TYPE type;
  TP_DOMAIN *dom;

  /* Assertions */
  assert (btid != NULL);
  assert (btid->classrepr != NULL);
  assert (btid->classrepr_cache_idx != -1);
  assert (btid->indx_id != -1);

  assert (rec != NULL);
  assert (rec->data != NULL);

  assert (key != NULL);
  assert (DB_IDXKEY_IS_NULL (key));

  assert ((node_type == BTREE_LEAF_NODE && rec_header == NULL)
	  || (node_type == BTREE_NON_LEAF_NODE && rec_header != NULL));

  assert (clear_key != NULL);

  /*
   * Init
   */

  indexp = &(btid->classrepr->indexes[btid->indx_id]);
  assert (indexp->btname != NULL);

#if 1				/* safe code */
  DB_IDXKEY_MAKE_NULL (key);
#endif

  *clear_key = false;

  or_init (&buf, rec->data, rec->length);

  /*
   * Find the beginning position of the key within the record and read
   * the key length.
   */
  if (node_type == BTREE_NON_LEAF_NODE)
    {
      non_leaf_rec = (NON_LEAF_REC *) rec_header;

      error = btree_read_fixed_portion_of_non_leaf_record_from_orbuf (&buf,
								      non_leaf_rec);
      if (error != NO_ERROR)
	{
	  assert (false);
	  goto exit_on_error;
	}
    }

  /* key is within page */

  /*
   * When we read the key, must copy in one case:
   *   we are told to via the copy_key flag.
   */
  *clear_key = (copy_key == COPY_KEY_VALUE) ? true : false;

  /* bitmap is always fully sized */
  key->size = indexp->n_atts + 1;

  bound_bits = buf.ptr;

  error = or_advance (&buf, OR_MULTI_BOUND_BIT_BYTES (key->size));
  if (error != NO_ERROR)
    {
      assert (false);
      goto exit_on_error;
    }

  for (i = 0; error == NO_ERROR && i < indexp->n_atts; i++)
    {
      if (OR_MULTI_ATT_IS_UNBOUND (bound_bits, i))
	{
	  DB_MAKE_NULL (&(key->vals[i]));
	}
      else
	{
	  type = indexp->atts[i]->type;
	  assert (tp_valid_indextype (type));

	  dom = indexp->atts[i]->domain;

	  assert (CAST_STRLEN (buf.endptr - buf.ptr) > 0);

	  error =
	    (*(tp_Type_id_map[type]->index_readval)) (&buf, &(key->vals[i]),
						      dom->precision,
						      dom->scale,
						      dom->collation_id,
						      -1 /* TODO - */ ,
						      *clear_key);
	}
    }

  /* read rightmost OID */
  if (error == NO_ERROR)
    {
      assert (i == indexp->n_atts);

      if (OR_MULTI_ATT_IS_UNBOUND (bound_bits, i))
	{
	  assert (node_type == BTREE_NON_LEAF_NODE);

	  DB_MAKE_NULL (&(key->vals[i]));
	}
      else
	{
	  assert (CAST_STRLEN (buf.endptr - buf.ptr) > 0);

	  error =
	    (*(tp_Type_id_map[DB_TYPE_OID]->index_readval)) (&buf,
							     &(key->vals[i]),
							     DB_DEFAULT_PRECISION,
							     DB_DEFAULT_SCALE,
							     LANG_COERCIBLE_COLL,
							     -1 /* TODO */ ,
							     *clear_key);
	}
    }

  if (error != NO_ERROR)
    {
      assert (false);
      goto exit_on_error;
    }

  assert (BTREE_IS_VALID_KEY_LEN (btree_get_key_length (key)));

#if !defined(NDEBUG)
  if (node_type == BTREE_LEAF_NODE)
    {
      OID oid;

      error = btree_get_oid_from_key (thread_p, btid, key, &oid);
      if (error != NO_ERROR)
	{
	  assert (false);
	  goto exit_on_error;
	}
      assert (!OID_ISNULL (&oid));

      assert (oid.groupid >= GLOBAL_GROUPID);
      if (heap_classrepr_is_shard_table (thread_p, &(btid->cls_oid)) == true)
	{
	  assert (oid.groupid > GLOBAL_GROUPID);	/* is shard table */
	}
      else
	{
	  assert (oid.groupid == GLOBAL_GROUPID);	/* is global table */
	}
    }
#endif

  if (error != NO_ERROR || DB_IDXKEY_IS_NULL (key))
    {
      goto exit_on_error;
    }

  assert (error == NO_ERROR);
  assert (!DB_IDXKEY_IS_NULL (key));

end:

  return error;

exit_on_error:

/* TODO - index crash */
  {
    char index_name_on_table[LINE_MAX];

    /* init */
    strcpy (index_name_on_table, "*UNKNOWN-INDEX*");

    (void) btree_get_indexname_on_table (thread_p, btid, index_name_on_table,
					 LINE_MAX);

    error = ER_BTREE_PAGE_CORRUPTED;
    er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	    index_name_on_table);
    assert (false);
  }

  goto end;
}

/*
 * btree_get_new_page () - GET a NEW PAGE for the B+tree index
 *   return: The pointer to a newly allocated page for the given
 *           B+tree or NULL.
 *           The parameter vpid is set to the page identifier.
 *   btid(in): B+tree index identifier
 *   vpid(out): Set to the page identifier for the newly allocated page
 *   near_vpid(in): A page identifier that may be used in a nearby page
 *                  allocation. (It may be ignored.)
 *
 * Note: Allocates a new page for the B+tree
 */
PAGE_PTR
btree_get_new_page (THREAD_ENTRY * thread_p, BTID_INT * btid, VPID * vpid,
		    VPID * near_vpid)
{
  PAGE_PTR page_ptr = NULL;
  unsigned short alignment;
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

  alignment = BTREE_MAX_ALIGN;

  if (file_alloc_pages (thread_p, &(btid->sys_btid->vfid), vpid, 1, near_vpid,
			btree_initialize_new_page,
			(void *) (&alignment)) == NULL)
    {
      return NULL;
    }

  FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1, 2);
  FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2, 2);
  FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3, 2);
  /*
   * Note: we fetch the page as old since it was initialized during the
   * allocation by btree_initialize_new_page, therefore, we care about
   * the current content of the page.
   */
  page_ptr =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), vpid,
		     PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE);
  if (page_ptr == NULL)
    {
      (void) btree_dealloc_page (thread_p, btid, vpid);
      return NULL;
    }

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_PAGE_ALLOCS, 1,
			       perf_start);

  return page_ptr;
}

/*
 * btree_pgbuf_fix () -
 *
 */
PAGE_PTR
btree_pgbuf_fix (THREAD_ENTRY * thread_p, const VFID * vfid,
		 const VPID * vpid,
		 int requestmode, PGBUF_LATCH_CONDITION condition,
		 const PAGE_TYPE ptype)
{
  PAGE_PTR page_ptr = NULL;

  assert (ptype == PAGE_BTREE_ROOT || ptype == PAGE_BTREE);

  page_ptr =
    pgbuf_fix (thread_p, vpid, OLD_PAGE, requestmode, condition, ptype);

  if (vfid != NULL && page_ptr != NULL)
    {
#if !defined(NDEBUG)
      assert (file_find_page (thread_p, vfid, vpid) == true);

      if (spage_number_of_records (page_ptr) > 0)
	{
	  BTREE_NODE_HEADER node_header;

	  if (btree_read_node_header (NULL, page_ptr, &node_header) !=
	      NO_ERROR)
	    {
	      assert (false);
	      pgbuf_unfix_and_init (thread_p, page_ptr);
	      return NULL;
	    }

	  if (node_header.node_level > 1)
	    {
	      /* non-leaf page */
	      assert (VPID_ISNULL (&node_header.next_vpid));
	    }
	  else
	    {
	      assert (VPID_ISNULL (&node_header.next_vpid)
		      || file_find_page (thread_p, vfid,
					 &node_header.next_vpid) == true);
	    }
	}
#endif
    }

  return page_ptr;
}

/*
 * btree_dealloc_page () -
 *   return: NO_ERROR or error code
 *
 *   btid(in):
 *   vpid(in):
 */
static int
btree_dealloc_page (THREAD_ENTRY * thread_p, BTID_INT * btid, VPID * vpid)
{
  int error = NO_ERROR;
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

  if (log_start_system_op (thread_p) == NULL)
    {
      return ER_FAILED;
    }

  error =
    file_dealloc_page (thread_p, &btid->sys_btid->vfid, vpid, PAGE_BTREE);

  log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_PAGE_DEALLOCS, 1,
			       perf_start);

  return error;
}

/*
 * btree_initialize_new_page_helper () -
 *   return:
 *   vfid(in): File where the new page belongs
 *   file_type(in):
 *   vpid(in): The new page
 *   ignore_npages(in): Number of contiguous allocated pages
 *                      (Ignored in this function. We allocate only one page)
 *   ignore_args(in): More arguments to function.
 *                    Ignored at this moment.
 *
 * Note: Initialize a newly allocated btree page.
 */
static PAGE_PTR
btree_initialize_new_page_helper (THREAD_ENTRY * thread_p, const VFID * vfid,
				  UNUSED_ARG const FILE_TYPE file_type,
				  const VPID * vpid,
				  UNUSED_ARG INT32 ignore_npages,
				  const unsigned short alignment,
				  const PAGE_TYPE ptype)
{
  PAGE_PTR pgptr;

  assert (ptype == PAGE_BTREE_ROOT || ptype == PAGE_BTREE);

  /*
   * fetch and initialize the new page.
   * The parameter UNANCHORED_KEEP_SEQUENCE indicates
   * that the order of records will be preserved
   * during insertions and deletions.
   */

  pgptr = pgbuf_fix_newpg (thread_p, vpid, ptype);
  if (pgptr == NULL)
    {
      return NULL;
    }

  if (vfid != NULL && pgptr != NULL)
    {
      assert (file_find_page (thread_p, vfid, vpid) == true);
    }

  spage_initialize (thread_p, pgptr, UNANCHORED_KEEP_SEQUENCE_BTREE,
		    alignment, DONT_SAFEGUARD_RVSPACE);

  return pgptr;
}

static bool
btree_initialize_new_root (THREAD_ENTRY * thread_p, const VFID * vfid,
			   UNUSED_ARG const FILE_TYPE file_type,
			   const VPID * vpid, UNUSED_ARG INT32 ignore_npages,
			   void *args)
{
  PAGE_PTR pgptr;
  unsigned short alignment;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  alignment = *((unsigned short *) args);

  pgptr =
    btree_initialize_new_page_helper (thread_p, vfid, file_type, vpid,
				      ignore_npages, alignment,
				      PAGE_BTREE_ROOT);
  if (pgptr == NULL)
    {
      return false;
    }

  LOG_ADDR_SET (&addr, vfid, pgptr, -1);
  log_append_redo_data (thread_p, RVBT_GET_NEWROOT, &addr,
			sizeof (alignment), &alignment);
  pgbuf_set_dirty (thread_p, pgptr, FREE);

  return true;
}

static bool
btree_initialize_new_page (THREAD_ENTRY * thread_p, const VFID * vfid,
			   UNUSED_ARG const FILE_TYPE file_type,
			   const VPID * vpid, UNUSED_ARG INT32 ignore_npages,
			   void *args)
{
  PAGE_PTR pgptr;
  unsigned short alignment;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  alignment = *((unsigned short *) args);

  pgptr =
    btree_initialize_new_page_helper (thread_p, vfid, file_type, vpid,
				      ignore_npages, alignment, PAGE_BTREE);
  if (pgptr == NULL)
    {
      return false;
    }

  LOG_ADDR_SET (&addr, vfid, pgptr, -1);
  log_append_redo_data (thread_p, RVBT_GET_NEWPAGE, &addr,
			sizeof (alignment), &alignment);
  pgbuf_set_dirty (thread_p, pgptr, FREE);

  return true;
}

/*
 * btree_search_nonleaf_page () -
 *   return: NO_ERROR
 *   btid(in):
 *   page_ptr(in): Pointer to the non_leaf page to be searched
 *   key(in): Key to find
 *   slot_id(out): Set to the record number that contains the key
 *   child_vpid(out): Set to the child page identifier to be followed,
 *                    or NULL_PAGEID
 *
 * Note: Binary search the page to locate the record that contains the
 * child page pointer to be followed to locate the key, and
 * return the page identifier for this child page.
 */
int
btree_search_nonleaf_page (THREAD_ENTRY * thread_p, BTID_INT * btid,
			   PAGE_PTR page_ptr, const DB_IDXKEY * key,
			   INT16 * slot_id, VPID * child_vpid)
{
  int key_cnt;
  int c;
  bool clear_key;
  /* the start position of non-equal-value column */
  int start_col, left_start_col, right_start_col;
  INT16 left, right;
  INT16 mid = 0;
  DB_IDXKEY mid_key;
  RECDES rec = RECDES_INITIALIZER;
  NON_LEAF_REC non_leaf_rec;
  BTREE_NODE_HEADER node_header;
#if !defined(NDEBUG)
  OR_INDEX *indexp = NULL;
#endif
  int ret = NO_ERROR;

  clear_key = false;
  DB_IDXKEY_MAKE_NULL (&mid_key);

  /* initialize child page identifier */
  VPID_SET_NULL (child_vpid);

  assert (page_ptr != NULL);
  assert (key != NULL);

#if !defined(NDEBUG)
  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  assert (key->size == indexp->n_atts + 1);
#endif

  /* read the node header */
  ret = btree_read_node_header (btid, page_ptr, &node_header);
  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }
  assert (node_header.node_level > 1);

  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);

  if (spage_number_of_records (page_ptr) <= 1)
    {				/* node record underflow */
      er_log_debug (ARG_FILE_LINE,
		    "btree_search_nonleaf_page: node key count underflow: %d",
		    key_cnt);
      goto exit_on_error;
    }

  if (key_cnt == 0)
    {
      /*
       * node has no keys, but a child page pointer
       * So, follow this pointer
       */
      if (spage_get_record (page_ptr, 1, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_read_fixed_portion_of_non_leaf_record (&rec, &non_leaf_rec);
      *slot_id = 1;
      *child_vpid = non_leaf_rec.pnt;

      btree_clear_key_value (&clear_key, &mid_key);

      return NO_ERROR;
    }

#if !defined(NDEBUG)
  /* check left fence */
  if (!DB_IDXKEY_IS_NULL (&(btid->left_fence)))
    {
      mid = 1;			/* get the first key */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      ret = btree_read_record (thread_p, btid, &rec, &mid_key,
			       &non_leaf_rec, BTREE_NON_LEAF_NODE, &clear_key,
			       PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      ret = btree_fence_check_key (thread_p, btid,
				   &(btid->left_fence), &mid_key, false);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  /* check right fence */
  if (!DB_IDXKEY_IS_NULL (&(btid->right_fence)))
    {
      mid = key_cnt;		/* get the last key */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      ret = btree_read_record (thread_p, btid, &rec, &mid_key,
			       &non_leaf_rec, BTREE_NON_LEAF_NODE, &clear_key,
			       PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      ret = btree_fence_check_key (thread_p, btid,
				   &mid_key, &(btid->right_fence), true);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }
#endif

  /* binary search the node to find the child page pointer to be followed */
  c = 0;
  mid = 0;
  left_start_col = right_start_col = 0;

  left = 1;
  right = key_cnt;

  while (left <= right)
    {
      mid = CEIL_PTVDIV ((left + right), 2);	/* get the middle record */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      ret = btree_read_record (thread_p, btid, &rec, &mid_key,
			       &non_leaf_rec, BTREE_NON_LEAF_NODE, &clear_key,
			       PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

#if !defined(NDEBUG)
      assert (mid_key.size == indexp->n_atts + 1);
#endif

      start_col = MIN (left_start_col, right_start_col);

      c = btree_compare_key (thread_p, btid, key, &mid_key, &start_col);

      if (c == DB_UNK)
	{
#if 1
	  assert (er_errid () == ER_TP_CANT_COERCE);
#endif

	  goto exit_on_error;
	}

      if (c == 0)
	{
	  /* child page to be followed has been found */
	  break;
	}

      if (c < 0)
	{
	  right = mid - 1;
	  right_start_col = start_col;
	}
      else
	{
	  left = mid + 1;
	  left_start_col = start_col;
	}
    }

  if (c <= 0)
    {
      /* found child page or
         child page is the one pointed by the middle record */
      *slot_id = mid;
      *child_vpid = non_leaf_rec.pnt;
    }
  else
    {
      /* child page is the one pointed by the record right to the middle  */
      if (spage_get_record (page_ptr, mid + 1, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_read_fixed_portion_of_non_leaf_record (&rec, &non_leaf_rec);
      *child_vpid = non_leaf_rec.pnt;
      *slot_id = mid + 1;
    }

  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  assert (*slot_id >= 1);

#if !defined(NDEBUG)
  /* save left fence */
  if (*slot_id > 1)
    {
      mid = *slot_id - 1;	/* get the left fence */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      ret = btree_read_record (thread_p, btid, &rec, &mid_key,
			       &non_leaf_rec, BTREE_NON_LEAF_NODE, &clear_key,
			       PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      /* check iff narrow left fence */
      ret =
	btree_fence_check_key (thread_p, btid, &(btid->left_fence), &mid_key,
			       false);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      (void) db_idxkey_clear (&(btid->left_fence));

      db_idxkey_clone (&mid_key, &(btid->left_fence));
      assert (!DB_IDXKEY_IS_NULL (&(btid->left_fence)));
    }

  /* save right fence */
  if (*slot_id <= key_cnt)
    {
      mid = *slot_id;		/* get the right fence */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      ret = btree_read_record (thread_p, btid, &rec, &mid_key,
			       &non_leaf_rec, BTREE_NON_LEAF_NODE, &clear_key,
			       PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      /* check iff narrow right fence */
      ret =
	btree_fence_check_key (thread_p, btid, &mid_key, &(btid->right_fence),
			       false);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      (void) db_idxkey_clear (&(btid->right_fence));

      db_idxkey_clone (&mid_key, &(btid->right_fence));
      assert (!DB_IDXKEY_IS_NULL (&(btid->right_fence)));
    }
#endif

  btree_clear_key_value (&clear_key, &mid_key);

  assert (ret == NO_ERROR);

  return ret;

exit_on_error:

  if (ret == NO_ERROR)
    {
      ret = ER_FAILED;
    }

  btree_clear_key_value (&clear_key, &mid_key);

  return ret;
}

/*
 * btree_search_leaf_page () -
 *   return: bool false: key does not exists, true: key exists
 *           (if error, false and slot_id = NULL_SLOTID)
 *   btid(in):
 *   page_ptr(in): Pointer to the leaf page to be searched
 *   key(in): Key to search
 *   slot_id(out): Set to the record number that contains the key if key is
 *                 found, or the record number in which the key should have
 *                 been located if it doesn't exist
 *   max_diff_column_index(out):
 *
 * Note: Binary search the page to find the location of the key.
 * If the key does not exist, it returns the location where it
 * should have been located.
 */
bool
btree_search_leaf_page (THREAD_ENTRY * thread_p, BTID_INT * btid,
			PAGE_PTR page_ptr, const DB_IDXKEY * key,
			INT16 * slot_id, int *max_diff_column_index)
{
  int key_cnt;
  int c;
  bool clear_key;
  /* the start position of non-equal-value column */
  int start_col, left_start_col, right_start_col;
  INT16 left, right, mid;
  DB_IDXKEY mid_key;
  RECDES rec = RECDES_INITIALIZER;
  BTREE_NODE_HEADER node_header;
#if !defined(NDEBUG)
  OR_INDEX *indexp = NULL;
#endif

  assert (btid != NULL);
  assert (key != NULL);
  assert (!DB_IDXKEY_IS_NULL (key));

  clear_key = false;
  DB_IDXKEY_MAKE_NULL (&mid_key);

  *slot_id = NULL_SLOTID;

  if (max_diff_column_index != NULL)
    {
      *max_diff_column_index = -1;
    }

#if !defined(NDEBUG)
  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  assert (key->size == indexp->n_atts + 1);
#endif

  /* read the header record */
  if (btree_read_node_header (btid, page_ptr, &node_header) != NO_ERROR)
    {
      goto exit_on_error;
    }
  assert (node_header.node_level == 1);

  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);

  if (key_cnt < 0)
    {
      char err_buf[LINE_MAX];

      snprintf (err_buf, LINE_MAX, "node key count underflow: %d.", key_cnt);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_EMERGENCY_ERROR, 1,
	      err_buf);

      goto exit_on_error;
    }

  if (key_cnt == 0)
    {
      /* node has no keys
       */
      *slot_id = 1;

      btree_clear_key_value (&clear_key, &mid_key);

      return false;
    }

#if !defined(NDEBUG)
  /* check left fence */
  if (!DB_IDXKEY_IS_NULL (&(btid->left_fence)))
    {
      mid = 1;			/* get the first key */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      if (btree_read_record (thread_p, btid, &rec, &mid_key,
			     NULL, BTREE_LEAF_NODE, &clear_key,
			     PEEK_KEY_VALUE) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if (btree_fence_check_key
	  (thread_p, btid, &(btid->left_fence), &mid_key, false) != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  /* check right fence */
  if (!DB_IDXKEY_IS_NULL (&(btid->right_fence)))
    {
      mid = key_cnt;		/* get the last key */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      if (btree_read_record (thread_p, btid, &rec, &mid_key,
			     NULL, BTREE_LEAF_NODE, &clear_key,
			     PEEK_KEY_VALUE) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if (btree_fence_check_key
	  (thread_p, btid, &mid_key, &(btid->right_fence), true) != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }
#endif

  /*
   * binary search the node to find if the key exists and in which record it
   * exists, or if it doesn't exist , the in which record it should have been
   * located to preserve the order of keys
   */

  c = 0;
  mid = 0;
  left_start_col = right_start_col = 0;

  left = 1;
  right = key_cnt;

  while (left <= right)
    {
      mid = CEIL_PTVDIV ((left + right), 2);	/* get the middle record */
      if (spage_get_record (page_ptr, mid, &rec, PEEK) != S_SUCCESS)
	{
	  char err_buf[LINE_MAX];

	  snprintf (err_buf, LINE_MAX, "invalid middle record!!");
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_EMERGENCY_ERROR, 1,
		  err_buf);

	  goto exit_on_error;
	}

      btree_clear_key_value (&clear_key, &mid_key);

      if (btree_read_record (thread_p, btid, &rec, &mid_key, NULL,
			     BTREE_LEAF_NODE, &clear_key,
			     PEEK_KEY_VALUE) != NO_ERROR)
	{
	  goto exit_on_error;
	}

#if !defined(NDEBUG)
      assert (mid_key.size == indexp->n_atts + 1);
#endif

      start_col = MIN (left_start_col, right_start_col);
      if (start_col >= key->size)
	{
	  char err_buf[LINE_MAX];

	  snprintf (err_buf, LINE_MAX, "invalid leaf page!!");
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_EMERGENCY_ERROR, 1,
		  err_buf);

	  goto exit_on_error;
	}

      c = btree_compare_key (thread_p, btid, key, &mid_key, &start_col);

      if (c == DB_UNK)
	{
#if 1
	  assert (er_errid () == ER_TP_CANT_COERCE);
#endif
	  goto exit_on_error;
	}

      if (max_diff_column_index != NULL)
	{
	  *max_diff_column_index = MAX (*max_diff_column_index, start_col);
	}

      if (c == 0)
	{
	  /* key exists in the middle record */
	  break;
	}

      if (c < 0)
	{
	  right = mid - 1;
	  right_start_col = start_col;
	}
      else
	{
	  left = mid + 1;
	  left_start_col = start_col;
	}
    }

  if (c <= 0)
    {
      /* found key or
       * key not exists, should be inserted in the current middle record */
      *slot_id = mid;
    }
  else
    {
      /* key not exists, should be inserted in the record right to the middle */
      *slot_id = mid + 1;
    }

  btree_clear_key_value (&clear_key, &mid_key);

  return (c == 0) ? true : false;

exit_on_error:

  *slot_id = NULL_SLOTID;

  btree_clear_key_value (&clear_key, &mid_key);

  return false;
}

/*
 * xbtree_add_index () - ADD (create) a new B+tree INDEX
 *   return: BTID * (btid on success and NULL on failure)
 *   btid(out): Set to the created B+tree index identifier
 *              (Note: btid->vfid.volid should be set by the caller)
 *   num_atts(in):
 *   att_type(in): Key type of the index to be created.
 *   class_oid(in): OID of the class for which the index is created
 *   attr_id(in): Identifier of the attribute of the class for which the
 *                index is created.
 *
 * Note: Creates the B+tree index. A file identifier (index identifier)
 * is defined on the given volume. This identifier is used by
 * insertion, deletion and search routines, for the created
 * index. The routine allocates the root page of the tree and
 * initializes the root header information.
 */
BTID *
xbtree_add_index (THREAD_ENTRY * thread_p, BTID * btid, int num_atts,
		  DB_TYPE * att_type, OID * class_oid, int attr_id)
{
  char data_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  BTREE_NODE_HEADER root_header;
  BTREE_NODE_HEADER node_header;
  VPID vpid, root_vpid;
  PAGE_PTR root_page = NULL, leaf_page = NULL;
  NON_LEAF_REC nleaf_rec;
  RECDES rec = RECDES_INITIALIZER;
  FILE_BTREE_DES btree_descriptor;
  bool is_file_created = false;
  unsigned short alignment;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  DB_IDXKEY key;
  BTID_INT btid_int;
  bool top_op_active = false;

  int i;
  int error = NO_ERROR;

  if (num_atts <= 0 || att_type == NULL || class_oid == NULL
      || OID_ISNULL (class_oid))
    {
      return NULL;
    }

  if (log_start_system_op (thread_p) == NULL)
    {
      return NULL;
    }
  top_op_active = true;

  DB_IDXKEY_MAKE_NULL (&key);

  /* create a file descriptor */
  COPY_OID (&btree_descriptor.class_oid, class_oid);
  btree_descriptor.attr_id = attr_id;

  /* create a file descriptor, allocate and initialize the root page */
  if (file_create (thread_p, &btid->vfid, 2, FILE_BTREE, &btree_descriptor,
		   &root_vpid, 1) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_file_created = true;

  alignment = BTREE_MAX_ALIGN;
  if (btree_initialize_new_root (thread_p, &btid->vfid, FILE_BTREE,
				 &root_vpid, 1, (void *) &alignment) == false)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /*
   * Note: we fetch the page as old since it was initialized by
   * btree_initialize_new_root; we want the current contents of
   * the page.
   */
  root_page =
    btree_pgbuf_fix (thread_p, &(btid->vfid), &root_vpid,
		     PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (root_page == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* insert the root header information into the root page */
  btree_init_node_header (thread_p, &root_header);
  root_header.node_level = 2;	/* root is always non-leaf */

  /* insert the root header information into the root page */
  error = btree_insert_node_header (thread_p, root_page, &root_header);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->vfid, root_page, HEADER);
  log_append_undoredo_data (thread_p, RVBT_NDHEADER_INS, &addr, 0,
			    sizeof (root_header), NULL, &root_header);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid_int);

  btid_int.sys_btid = btid;

  leaf_page = btree_get_new_page (thread_p, &btid_int, &vpid, &root_vpid);
  if (leaf_page == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* insert the leaf header information into the leaf page */
  btree_init_node_header (thread_p, &node_header);

  error = btree_insert_node_header (thread_p, leaf_page, &node_header);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->vfid, leaf_page, HEADER);
  log_append_undoredo_data (thread_p, RVBT_NDHEADER_INS, &addr,
			    0, sizeof (node_header), NULL, &node_header);

  pgbuf_set_dirty (thread_p, leaf_page, FREE);
  leaf_page = NULL;

  rec.area_size = DB_PAGESIZE;
  rec.data = PTR_ALIGN (data_buf, BTREE_MAX_ALIGN);
  rec.type = REC_HOME;

  nleaf_rec.pnt = vpid;

  /* construct idxkey; is meaningless Inf seprator
   */

  /* bitmap is always fully sized */
  key.size = num_atts + 1;

  for (i = 0; error == NO_ERROR && i < num_atts; i++)
    {
      assert (tp_valid_indextype (att_type[i]));

      error =
	db_value_domain_max (&(key.vals[i]), att_type[i],
			     DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE,
			     LANG_COERCIBLE_COLL);
    }

  /* write rightmost OID */
  if (error == NO_ERROR)
    {
      assert (i == num_atts);

      error =
	db_value_domain_max (&(key.vals[i]), DB_TYPE_OID,
			     DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE,
			     LANG_COERCIBLE_COLL);
    }

  if (error != NO_ERROR)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  error = btree_write_record (thread_p, &btid_int, &nleaf_rec, &key,
			      BTREE_NON_LEAF_NODE, &rec);
  if (error != NO_ERROR)
    {
      db_idxkey_clear (&key);
      GOTO_EXIT_ON_ERROR;
    }

  if (spage_insert_at (thread_p, root_page, 1, &rec) != SP_SUCCESS)
    {
      db_idxkey_clear (&key);
      GOTO_EXIT_ON_ERROR;
    }

  db_idxkey_clear (&key);

  LOG_ADDR_SET (&addr, &btid->vfid, root_page, 1);
  log_append_undoredo_data (thread_p, RVBT_NDRECORD_INS, &addr,
			    0, rec.length, NULL, rec.data);

  error = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_ADD_INDEX_ERROR1,
			   0, 0);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  pgbuf_set_dirty (thread_p, root_page, FREE);
  root_page = NULL;

  log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
  top_op_active = false;

  file_new_declare_as_old (thread_p, &btid->vfid);

  LOG_ADDR_SET (&addr, NULL, NULL, 0);
  log_append_undo_data (thread_p, RVBT_CREATE_INDEX, &addr, sizeof (VFID),
			&(btid->vfid));

  error = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_ADD_INDEX_ERROR2,
			   0, 0);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* set the B+tree index identifier */
  btid->root_pageid = root_vpid.pageid;

  return btid;

exit_on_error:
  if (root_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, root_page);
    }
  if (leaf_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, leaf_page);
    }

  if (is_file_created)
    {
      (void) file_destroy (thread_p, &btid->vfid);
    }

  VFID_SET_NULL (&btid->vfid);
  btid->root_pageid = NULL_PAGEID;

  if (top_op_active == true)
    {
      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
    }

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_LOAD_FAILED, 0);
    }

  return NULL;
}

/*
 * xbtree_delete_index () -
 *   return: NO_ERROR
 *   btid(in): B+tree index identifier
 *
 * Note: Removes the B+tree index. All pages associated with the index
 * are removed. After the routine is called, the index identifier
 * is not valid any more.
 */
int
xbtree_delete_index (THREAD_ENTRY * thread_p, BTID * btid)
{
  int ret;

  btid->root_pageid = NULL_PAGEID;

  /*
   * even if the first destroy fails for some reason, still try and
   * destroy the overflow file if there is one.
   */
  ret = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_DELETE_INDEX_ERROR1,
			 0, 0);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ret = file_destroy (thread_p, &btid->vfid);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ret = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_DELETE_INDEX_ERROR2,
			 0, 0);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return ret;

exit_on_error:
  assert (ret != NO_ERROR);

  return ret;
}

/*
 * xbtree_find_unique () -
 *   return:
 *   class_oid(in):
 *   btid(in):
 *   key(in):
 *   oid(out):
 *
 * Note: This returns the oid for the given key.  It assumes that the
 * btree is unique.
 */
BTREE_SEARCH
xbtree_find_unique (THREAD_ENTRY * thread_p, OID * class_oid, BTID * btid,
		    DB_IDXKEY * key, OID * oid)
{
  int oid_cnt = 0;
  BTREE_SEARCH status = BTREE_KEY_NOTFOUND;	/* init */
  INDX_SCAN_ID index_scan_id;
  BTREE_SCAN *BTS = NULL;
  OR_INDEX *indexp = NULL;
  /* Unique btree can have at most 1 OID for a key */
  OID temp_oid[2];
  KEY_VAL_RANGE key_val_range;

  assert (!OID_ISNULL (class_oid));

  assert (key != NULL);
  assert (!DB_IDXKEY_IS_NULL (key));

  /* initialize DB_VALUE first for error case */
  key_val_range.range = GE_LE;

  DB_IDXKEY_MAKE_NULL (&key_val_range.lower_key);
  DB_IDXKEY_MAKE_NULL (&key_val_range.upper_key);

  key_val_range.num_index_term = 0;

  BTS = &(index_scan_id.bt_scan);

  /* index scan info */
  BTREE_INIT_SCAN (BTS);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&(BTS->btid_int));

  scan_init_index_scan (&index_scan_id, temp_oid, sizeof (temp_oid));

  /* get class representation of the index */
  COPY_OID (&(BTS->btid_int.cls_oid), class_oid);
  BTS->btid_int.classrepr =
    heap_classrepr_get (thread_p, &(BTS->btid_int.cls_oid), NULL, 0,
			&(BTS->btid_int.classrepr_cache_idx), true);
  if (BTS->btid_int.classrepr == NULL)
    {
      assert (false);
      status = BTREE_ERROR_OCCURRED;
      goto exit_on_error;
    }

  /* get the index ID which corresponds to the BTID */
  BTS->btid_int.indx_id =
    heap_classrepr_find_index_id (BTS->btid_int.classrepr, btid);
  if (BTS->btid_int.indx_id < 0)
    {
      assert (false);
      status = BTREE_ERROR_OCCURRED;
      goto exit_on_error;
    }

  indexp = &(BTS->btid_int.classrepr->indexes[BTS->btid_int.indx_id]);

  /* generate lower_key
   */

  db_idxkey_clone (key, &(key_val_range.lower_key));

#if 1				/* TODO - fix me */
  assert (key_val_range.lower_key.size == indexp->n_atts
	  || key_val_range.lower_key.size == indexp->n_atts + 1);

  key_val_range.lower_key.size = indexp->n_atts;
#endif

  if (btree_coerce_idxkey
      (&(key_val_range.lower_key), indexp,
       indexp->n_atts, BTREE_COERCE_KEY_WITH_MIN_VALUE) != NO_ERROR)
    {
      assert (false);
      status = BTREE_ERROR_OCCURRED;
      goto exit_on_error;
    }

#if 1				/* TODO - */
  assert (!DB_IDXKEY_IS_NULL (&(key_val_range.lower_key)));
  if (btree_key_is_null (&(key_val_range.lower_key)))
    {
      status = BTREE_KEY_NOTFOUND;
      goto exit_on_end;		/* OK */
    }
#endif

  /* generate upper_key
   */

  db_idxkey_clone (key, &(key_val_range.upper_key));

#if 1				/* TODO - fix me */
  assert (key_val_range.upper_key.size == indexp->n_atts
	  || key_val_range.upper_key.size == indexp->n_atts + 1);

  key_val_range.upper_key.size = indexp->n_atts;
#endif

  if (btree_coerce_idxkey
      (&(key_val_range.upper_key), indexp,
       indexp->n_atts, BTREE_COERCE_KEY_WITH_MAX_VALUE) != NO_ERROR)
    {
      assert (false);
      status = BTREE_ERROR_OCCURRED;
      goto exit_on_error;
    }

#if 1				/* TODO - */
  assert (!DB_IDXKEY_IS_NULL (&(key_val_range.upper_key)));
#endif

  /* do not use copy_buf for key-val scan, only use for key-range scan
   */
  key_val_range.range = GE_LE;
  key_val_range.num_index_term = indexp->n_atts;

  oid_cnt =
    btree_keyval_search (thread_p, btid, &key_val_range, &index_scan_id);

  btree_scan_clear_key (BTS);

  if (oid_cnt == 1)
    {
      COPY_OID (oid, index_scan_id.oid_list.oidp);

      assert (oid->groupid >= GLOBAL_GROUPID);

#if !defined(NDEBUG)
      if (heap_classrepr_is_shard_table (thread_p, class_oid) == true)
	{
	  assert (oid->groupid > GLOBAL_GROUPID);	/* is shard table */
	}
      else
	{
	  assert (oid->groupid == GLOBAL_GROUPID);	/* is global table */
	}
#endif

      status = BTREE_KEY_FOUND;
    }
  else if (oid_cnt == 0)
    {
      status = BTREE_KEY_NOTFOUND;
    }
  else if (oid_cnt < 0)
    {
      status = BTREE_ERROR_OCCURRED;
    }
  else
    {
      /* (oid_cnt > 1) */
      assert (false);

      COPY_OID (oid, index_scan_id.oid_list.oidp);
      status = BTREE_ERROR_OCCURRED;
    }

  if (status == BTREE_ERROR_OCCURRED)
    {
      goto exit_on_error;
    }

  assert (status != BTREE_ERROR_OCCURRED);

exit_on_end:

  (void) db_idxkey_clear (&key_val_range.lower_key);
  (void) db_idxkey_clear (&key_val_range.upper_key);

#if 1				/* TODO - */
  if (BTS->btid_int.classrepr)
    {
      assert (BTS->btid_int.classrepr_cache_idx != -1);
      assert (BTS->btid_int.indx_id != -1);

      (void) heap_classrepr_free (BTS->btid_int.classrepr,
				  &(BTS->btid_int.classrepr_cache_idx));
      assert (BTS->btid_int.classrepr_cache_idx == -1);

      BTS->btid_int.classrepr = NULL;
//          BTS->btid_int.classrepr_cache_idx = -1;
//          BTS->btid_int.indx_id = -1;
    }
#endif

  return status;

exit_on_error:

  assert (status == BTREE_ERROR_OCCURRED);

  goto exit_on_end;
}

/*
 * btree_scan_clear_key () -
 *   return:
 *   btree_scan(in):
 */
void
btree_scan_clear_key (BTREE_SCAN * btree_scan)
{
  btree_clear_key_value (&btree_scan->clear_cur_key, &btree_scan->cur_key);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 *       		 db_check consistency routines
 */

/*
 * btree_check_pages () -
 *   return: DISK_VALID, DISK_VALID or DISK_ERROR
 *   btid(in): B+tree index identifier
 *   pg_ptr(in): Page pointer
 *   pg_vpid(in): Page identifier
 *
 * Note: Verify that given page and all its subpages are valid.
 */
static DISK_ISVALID
btree_check_pages (THREAD_ENTRY * thread_p, BTID_INT * btid,
		   PAGE_PTR pg_ptr, VPID * pg_vpid)
{
  VPID page_vpid;		/* Child page identifier */
  PAGE_PTR page = NULL;		/* Child page pointer */
  RECDES rec = RECDES_INITIALIZER;	/* Record descriptor for page node records */
  DISK_ISVALID vld = DISK_ERROR;	/* Validity return code from subtree */
  int key_cnt;			/* Number of keys in the page */
  int i;			/* Loop counter */
  BTREE_NODE_HEADER node_header;
  NON_LEAF_REC nleaf;

  /* Verify the given page */
  vld = file_isvalid_page_partof (thread_p, pg_vpid, &btid->sys_btid->vfid);
  if (vld != DISK_VALID)
    {
      goto error;
    }

  if (btree_read_node_header (NULL, pg_ptr, &node_header) != NO_ERROR)
    {
      vld = DISK_ERROR;
      goto error;
    }

  /* Verify subtree child pages */
  if (node_header.node_level > 1)
    {				/* non-leaf page */
      key_cnt = node_header.key_cnt;
      assert_release (key_cnt >= 0);
      for (i = 1; i <= key_cnt + 1; i++)
	{
	  if (spage_get_record (pg_ptr, i, &rec, PEEK) != S_SUCCESS)
	    {
	      vld = DISK_ERROR;
	      goto error;
	    }
	  btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf);
	  page_vpid = nleaf.pnt;

	  page =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &page_vpid,
			     OLD_PAGE, PGBUF_LATCH_READ,
			     PGBUF_UNCONDITIONAL_LATCH, PAGE_BTREE);
	  if (page == NULL)
	    {
	      vld = DISK_ERROR;
	      goto error;
	    }

	  vld = btree_check_pages (thread_p, btid, page, &page_vpid);
	  if (vld != DISK_VALID)
	    {
	      goto error;
	    }
	  pgbuf_unfix_and_init (thread_p, page);
	}
    }

  return DISK_VALID;

error:

  if (page)
    {
      pgbuf_unfix_and_init (thread_p, page);
    }
  return vld;

}

/*
 * btree_check_tree () -
 *   return: DISK_VALID, DISK_INVALID or DISK_ERROR
 *   btid(in): B+tree index identifier
 *
 * Note: Verify that all the pages of the specified index are valid.
 */
static DISK_ISVALID
btree_check_tree (THREAD_ENTRY * thread_p, BTID * btid)
{
  DISK_ISVALID valid = DISK_ERROR;
  VPID r_vpid;			/* root page identifier */
  PAGE_PTR r_pgptr = NULL;	/* root page pointer */
  BTID_INT btid_int;
  BTREE_NODE_HEADER root_header;

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid_int);

  /* Fetch the root page */
  r_vpid.pageid = btid->root_pageid;
  r_vpid.volid = btid->vfid.volid;
  r_pgptr =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &r_vpid, OLD_PAGE,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (r_pgptr == NULL)
    {
      valid = DISK_ERROR;
      goto error;
    }

  btid_int.sys_btid = btid;

  valid = btree_check_pages (thread_p, &btid_int, r_pgptr, &r_vpid);
  if (valid != DISK_VALID)
    {
      goto error;
    }

  pgbuf_unfix_and_init (thread_p, r_pgptr);

exit_on_end:

  return valid;

error:

  if (r_pgptr)
    {
      pgbuf_unfix_and_init (thread_p, r_pgptr);
    }

  assert (valid != DISK_VALID);

  goto exit_on_end;
}

/*
 * btree_check_by_btid () -
 *   btid(in): B+tree index identifier
 *   return: either: DISK_INVALID, DISK_VALID, DISK_ERROR
 *
 * Note: Verify that all pages of a btree indices are valid.
 */
static DISK_ISVALID
btree_check_by_btid (THREAD_ENTRY * thread_p, BTID * btid)
{
  DISK_ISVALID valid = DISK_ERROR;
  char area[FILE_DUMP_DES_AREA_SIZE];
  char *fd = area;
  int fd_size = FILE_DUMP_DES_AREA_SIZE, size;
  FILE_BTREE_DES *btree_des;
  char *btname;
  VPID vpid;

  size = file_get_descriptor (thread_p, &btid->vfid, fd, fd_size);
  if (size < 0)
    {
      fd_size = -size;
      fd = (char *) malloc (fd_size);
      if (fd == NULL)
	{
	  fd = area;
	  fd_size = FILE_DUMP_DES_AREA_SIZE;
	}
      else
	{
	  size = file_get_descriptor (thread_p, &btid->vfid, fd, fd_size);
	}
    }
  btree_des = (FILE_BTREE_DES *) fd;

  if (file_find_nthpages (thread_p, &btid->vfid, &vpid, 0, 1) != 1)
    {
      goto exit_on_end;
    }

  btid->root_pageid = vpid.pageid;

  /* get the index name of the index key */
  if (heap_get_indexname_of_btid (thread_p, &(btree_des->class_oid),
				  btid, &btname) != NO_ERROR)
    {
      goto exit_on_end;
    }

  valid = btree_check_tree (thread_p, btid);
exit_on_end:
  if (fd != area)
    {
      free_and_init (fd);
    }
  if (btname)
    {
      free_and_init (btname);
    }

  return valid;
}
#endif

int
btree_get_pkey_btid (THREAD_ENTRY * thread_p, OID * cls_oid, BTID * pkey_btid)
{
  OR_CLASSREP *cls_repr;
  OR_INDEX *curr_idx;
  int cache_idx = -1;
  int i;
  int error = NO_ERROR;

  assert (pkey_btid != NULL);

  BTID_SET_NULL (pkey_btid);

  cls_repr =
    heap_classrepr_get (thread_p, cls_oid, NULL, NULL_REPRID, &cache_idx,
			true);
  if (cls_repr == NULL)
    {
      assert (er_errid () != NO_ERROR);
      error = er_errid ();

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  for (i = 0; i < cls_repr->n_indexes; i++)
    {
      curr_idx = &(cls_repr->indexes[i]);
      if (curr_idx == NULL)
	{
	  error = ER_UNEXPECTED;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  break;
	}

      if (curr_idx->type == BTREE_PRIMARY_KEY)
	{
	  BTID_COPY (pkey_btid, &curr_idx->btid);
	  break;
	}
    }

  if (cls_repr != NULL)
    {
      heap_classrepr_free (cls_repr, &cache_idx);
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * btree_check_by_class_oid () -
 *   cls_oid(in):
 *   return: either: DISK_INVALID, DISK_VALID, DISK_ERROR
 *
 */
DISK_ISVALID
btree_check_by_class_oid (THREAD_ENTRY * thread_p, OID * cls_oid)
{
  OR_CLASSREP *cls_repr;
  OR_INDEX *curr;
  BTID btid;
  int i;
  int cache_idx = -1;
  DISK_ISVALID rv = DISK_VALID;

  cls_repr =
    heap_classrepr_get (thread_p, cls_oid, NULL, 0, &cache_idx, true);
  if (cls_repr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_errid (), 0);
      return DISK_ERROR;
    }

  for (i = 0; i < cls_repr->n_indexes; i++)
    {
      curr = &(cls_repr->indexes[i]);
      if (curr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_UNEXPECTED, 0);
	  rv = DISK_ERROR;
	  break;
	}

      BTID_COPY (&btid, &curr->btid);
      if (btree_check_by_btid (thread_p, &btid) != DISK_VALID)
	{
	  rv = DISK_ERROR;
	  break;
	}
    }

  if (cls_repr)
    {
      heap_classrepr_free (cls_repr, &cache_idx);
    }

  return rv;
}

/*
 * btree_check_all () -
 *   return: either: DISK_INVALID, DISK_VALID, DISK_ERROR
 *
 * Note: Verify that all pages of all btree indices are valid.
 */
DISK_ISVALID
btree_check_all (THREAD_ENTRY * thread_p)
{
  int num_files;		/* Number of files in the system */
  BTID btid;			/* Btree index identifier        */
  DISK_ISVALID valid, allvalid;	/* Validation return code        */
  FILE_TYPE file_type;		/* TYpe of file                  */
  int i;			/* Loop counter                  */

  /* Find number of files */
  num_files = file_get_numfiles (thread_p);
  if (num_files < 0)
    {
      return DISK_ERROR;
    }

  allvalid = DISK_VALID;

  /* Go to each file, check only the btree files */
  for (i = 0; i < num_files && allvalid != DISK_ERROR; i++)
    {
      if (file_find_nthfile (thread_p, &btid.vfid, i) != 1)
	{
	  break;
	}

      file_type = file_get_type (thread_p, &btid.vfid);
      if (file_type == FILE_UNKNOWN_TYPE)
	{
	  allvalid = DISK_ERROR;
	  break;
	}

      if (file_type != FILE_BTREE)
	{
	  continue;
	}

      valid = btree_check_by_btid (thread_p, &btid);
      if (valid != DISK_VALID)
	{
	  allvalid = valid;
	}
    }

  return allvalid;
}
#endif

/*
 * btree_delete_key_from_leaf () -
 *   return:
 *   btid(in):
 *   leaf_pg(in):
 *   slot_id(in):
 *   key(in):
 */
static int
btree_delete_key_from_leaf (THREAD_ENTRY * thread_p, BTID_INT * btid,
			    PAGE_PTR leaf_pg, INT16 slot_id,
			    const DB_IDXKEY * key)
{
  int ret = NO_ERROR;
//  char *rv_data;
  char rv_key[OR_OID_SIZE + OR_BTID_ALIGNED_SIZE + BTREE_MAX_KEYLEN +
	      BTREE_MAX_ALIGN];
  int rv_key_len;
//  char rv_data_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  BTREE_NODE_HEADER node_header;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  if (btree_read_node_header (btid, leaf_pg, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert_release (node_header.key_cnt >= 1);

//  rv_data = PTR_ALIGN (rv_data_buf, BTREE_MAX_ALIGN);

  ret = btree_rv_save_keyval (btid, key, rv_key, &rv_key_len);
  if (ret != NO_ERROR)
    {
      return ret;
    }

  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, leaf_pg, slot_id);
  log_append_undoredo_data (thread_p, RVBT_KEYVAL_DELETE, &addr,
			    rv_key_len, 0, rv_key, NULL);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* now delete the btree slot */
  if (spage_delete (thread_p, leaf_pg, slot_id) != slot_id)
    {
      GOTO_EXIT_ON_ERROR;
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* key deleted, update node header */
  node_header.key_cnt--;
  assert_release (node_header.key_cnt >= 0);
  assert (node_header.node_level == 1);

  if (btree_write_node_header (btid, leaf_pg, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  pgbuf_set_dirty (thread_p, leaf_pg, DONT_FREE);

end:

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_delete_from_leaf () -
 *   return: NO_ERROR
 *   btid(in):
 *   page_ptr(in):
 *   key(in):
 *
 * LOGGING Note: When the btree is new, splits and merges will
 * not be committed, but will be attached.  If the transaction
 * is rolled back, the merge and split actions will be rolled
 * back as well.  The undo (and redo) logging for splits and
 * merges are page based (physical) logs, thus the rest of the
 * logs for the undo session must be page based as well.  When
 * the btree is old, splits and merges are committed and all
 * the rest of the logging must be logical (non page based)
 * since pages may change as splits and merges are performed.
 *
 * LOGGING Note2: We adopts a new concept of log, that is a combined log of
 * logical undo and physical redo log, for performance reasons.
 * For key delete, this will be written only when the btree is old.
 * However each undo log and redo log will be written as it is in the rest of
 * the cases(need future work).
 */
static int
btree_delete_from_leaf (THREAD_ENTRY * thread_p,
			BTID_INT * btid, PAGE_PTR page_ptr,
			const DB_IDXKEY * key)
{
  int ret = NO_ERROR;
  INT16 leaf_slot_id = NULL_SLOTID;
  bool found;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  /* find the slot for the key */
  found = btree_search_leaf_page (thread_p, btid, page_ptr,
				  key, &leaf_slot_id, NULL);
  if (leaf_slot_id == NULL_SLOTID)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (!found)
    {
#if !defined(NDEBUG)
      OR_INDEX *indexp;

      assert (btid->classrepr != NULL);
      assert (btid->classrepr_cache_idx != -1);
      assert (btid->indx_id != -1);

      indexp = &(btid->classrepr->indexes[btid->indx_id]);
      assert (INDEX_IS_IN_PROGRESS (indexp));
#endif

      /* key does not exist */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, page_ptr, -1);
      log_append_redo_data (thread_p, RVBT_NOOP, &addr, 0, NULL);
      pgbuf_set_dirty (thread_p, page_ptr, DONT_FREE);

      return NO_ERROR;
    }

  ret = btree_delete_key_from_leaf (thread_p, btid, page_ptr,
				    leaf_slot_id, key);

  return NO_ERROR;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  return ret;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * btree_merge_root () -
 *   return: NO_ERROR
 *   btid(in): B+tree index identifier
 *   P(in): Page pointer for the root to be merged
 *   Q(in): Page pointer for the root child page to be merged
 *   R(in): Page pointer for the root child page to be merged
 *   P_vpid(in): Page identifier for page P
 *   Q_vpid(in): Page identifier for page Q
 *   R_vpid(in): Page identifier for page R
 *   node_type(in): shows whether root child pages are leaf, or not
 *
 * Note: When the root page has only two children (leaf or non_leaf)
 * that can be merged together, then they are merged through
 * this specific root merge operation. The main distinction of
 * this routine from the regular merge operation is that in this
 * the content of the two child pages are moved to the root, in
 * order not to change the original root page. The root can also
 * be a specific non-leaf page, that is, it may have only one key
 * and one child page pointer. In this case, R_id, the page
 * identifier for the page R is NULL_PAGEID. In both cases, the
 * height of the tree is reduced by one, after the merge
 * operation. The two (one) child pages are not deallocated by
 * this routine. Deallocation of these pages are left to the
 * calling routine.
 *
 * Note:  Page Q and Page R contents are not changed by this routine,
 * since these pages will be deallocated by the calling routine.
 */
static int
btree_merge_root (THREAD_ENTRY * thread_p, BTID_INT * btid, PAGE_PTR P,
		  PAGE_PTR Q, PAGE_PTR R, VPID * P_vpid, VPID * Q_vpid,
		  VPID * R_vpid, short node_type)
{
  int left_cnt, right_cnt;
//  RECDES peek_rec = RECDES_INITIALIZER;
  RECDES peek_rec1 = RECDES_INITIALIZER, peek_rec2 =
    RECDES_INITIALIZER, copy_rec = RECDES_INITIALIZER;
  NON_LEAF_REC nleaf_pnt = { {NULL_PAGEID, NULL_VOLID} };
  NON_LEAF_REC nonleaf_rec;
  int i;
//  int offset;
  DB_IDXKEY mid_key;
  bool clear_key = false;
  char *recset_data;		/* for recovery purposes */
  int recset_length;		/* for recovery purposes */
  RECSET_HEADER recset_header;	/* for recovery purposes */
  int sp_success;
//  int recset_data_length;
  PGLENGTH log_addr_offset;
  int ret = NO_ERROR;
//  VPID null_vpid;
  char recset_data_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  char copy_rec_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  BTREE_NODE_HEADER root_header;

  assert (node_type == BTREE_NON_LEAF_NODE);

  DB_IDXKEY_MAKE_NULL (&mid_key);

  /* initializations */
  copy_rec.data = NULL;
  recset_data = NULL;

  /* log the P record contents for undo purposes,
   * if a crash happens the records of root page P will be inserted
   * back. There is no need for undo logs for pages Q and R,
   * since they are not changed by this routine, because they will be
   * deallocated after a succesful merge operation. There is also no need
   * for redo logs for pages Q and R, since these pages will be deallocated
   * by the caller routine.
   */

  /* for recovery purposes */
  recset_data = PTR_ALIGN (recset_data_buf, BTREE_MAX_ALIGN);
  assert (recset_data != NULL);

  left_cnt = spage_number_of_records (Q) - 1;
  right_cnt = spage_number_of_records (R) - 1;

  /* read the first record and the mid key */
  if (spage_get_record (P, 1, &peek_rec1, PEEK) != S_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* we have to copy the key here because we will soon be overwriting this
   * page and our pointer will point to a record from one of the other pages.
   */
  if (btree_read_record
      (thread_p, btid, &peek_rec1, &mid_key, &nleaf_pnt,
       BTREE_NON_LEAF_NODE, &clear_key, COPY_KEY_VALUE) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* get the record to see if it has overflow pages */
  if (spage_get_record (P, 2, &peek_rec2, PEEK) != S_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  btree_read_fixed_portion_of_non_leaf_record (&peek_rec2, &nleaf_pnt);

  /* delete second record */
  /* prepare undo log record */

  log_addr_offset = 2;
  /* log the deleted slotid for undo/redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, log_addr_offset);
  log_append_undoredo_data (thread_p, RVBT_NDRECORD_DEL, &addr,
			    peek_rec2.length,
			    sizeof (log_addr_offset), peek_rec2.data,
			    &log_addr_offset);

  if (spage_delete (thread_p, P, 2) != 2)
    {
      GOTO_EXIT_ON_ERROR;
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* delete first record */
  /* prepare undo log record */

  log_addr_offset = 1;
  /* log the deleted slotid for undo/redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, log_addr_offset);
  log_append_undoredo_data (thread_p, RVBT_NDRECORD_DEL, &addr,
			    peek_rec1.length,
			    sizeof (log_addr_offset), peek_rec1.data,
			    &log_addr_offset);

  if (spage_delete (thread_p, P, 1) != 1)
    {
      GOTO_EXIT_ON_ERROR;
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* Log the page Q records for undo/redo purposes on page P. */
  recset_header.rec_cnt = left_cnt;
  recset_header.first_slotid = 1;
  ret =
    btree_rv_util_save_page_records (thread_p, btid, Q, 1, left_cnt, 1,
				     recset_data, IO_MAX_PAGE_SIZE,
				     &recset_length);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* move content of the left page to the root page */
  for (i = 1; i <= left_cnt; i++)
    {
      if (spage_get_record (Q, i, &peek_rec2, PEEK) != S_SUCCESS
	  || ((sp_success = spage_insert_at (thread_p, P, i, &peek_rec2))
	      != SP_SUCCESS))
	{
	  if (i > 1)
	    {
	      recset_header.rec_cnt = i - 1;
	      recset_header.first_slotid = 1;
	      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, -1);
	      log_append_undo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
				    sizeof (RECSET_HEADER), &recset_header);
	    }
	  GOTO_EXIT_ON_ERROR;
	}
    }				/* for */

  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, -1);
  log_append_undoredo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
			    sizeof (RECSET_HEADER), recset_length,
			    &recset_header, recset_data);
  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* increment lsa of the page to be deallocated */
  LOG_ADDR_SET (&addr, NULL, Q, 0);
  log_skip_logging_set_lsa (thread_p, &addr);
  pgbuf_set_dirty (thread_p, Q, DONT_FREE);

  if (node_type == BTREE_NON_LEAF_NODE)
    {				/* form the middle record in the root */

      copy_rec.area_size = DB_PAGESIZE;
      copy_rec.data = PTR_ALIGN (copy_rec_buf, BTREE_MAX_ALIGN);

      if (spage_get_record (P, left_cnt, &copy_rec, COPY) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      btree_read_fixed_portion_of_non_leaf_record (&copy_rec, &nleaf_pnt);
      nonleaf_rec.pnt = nleaf_pnt.pnt;

      ret =
	btree_write_record (thread_p, btid, &nonleaf_rec, &mid_key,
			    node_type, &copy_rec);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (spage_update (thread_p, P, left_cnt, &copy_rec) != SP_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the new node record for redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, left_cnt);
      log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			    &addr, copy_rec.length, copy_rec.data);
      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

      copy_rec.data = NULL;
    }

  /* Log the page R records for undo purposes on page P. */
  recset_header.rec_cnt = right_cnt;
  recset_header.first_slotid = left_cnt + 1;

  ret =
    btree_rv_util_save_page_records (thread_p, btid, R, 1, right_cnt,
				     left_cnt + 1, recset_data,
				     IO_MAX_PAGE_SIZE, &recset_length);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* move content of the right page to the root page */
  for (i = 1; i <= right_cnt; i++)
    {
      if (spage_get_record (R, i, &peek_rec2, PEEK) != S_SUCCESS
	  || spage_insert_at (thread_p, P, left_cnt + i, &peek_rec2)
	  != SP_SUCCESS)
	{
	  if (i > 1)
	    {
	      recset_header.rec_cnt = i - 1;
	      recset_header.first_slotid = left_cnt + 1;
	      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, -1);
	      log_append_undo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
				    sizeof (RECSET_HEADER), &recset_header);
	    }
	  GOTO_EXIT_ON_ERROR;
	}
    }				/* for */

  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, -1);
  log_append_undoredo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
			    sizeof (RECSET_HEADER), recset_length,
			    &recset_header, recset_data);
  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* update root page */
  if (btree_read_node_header (btid, P, &root_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the before image of the root header */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
  log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (root_header), &root_header);

  /* Care must be taken here in figuring the key count. Remember that
   * leaf nodes have the real key cnt, while, for some stupid reason,
   * non leaf nodes have a key count one less than the actual count.
   */
  if (node_type == BTREE_LEAF_NODE)
    {
      assert (false);		/* root can not be leaf */
      root_header.key_cnt = left_cnt + right_cnt;
      assert_release (root_header.key_cnt >= 0);
    }
  else
    {
      root_header.key_cnt = left_cnt + right_cnt - 1;
      assert_release (root_header.key_cnt >= 0);
    }

  VPID_SET_NULL (&root_header.next_vpid);
  VPID_SET_NULL (&root_header.prev_vpid);
  root_header.split_info.pivot = BTREE_SPLIT_DEFAULT_PIVOT;
  root_header.split_info.index = 1;
  root_header.node_level--;
  assert (root_header.node_level > 1);

  if (btree_write_node_header (btid, P, &root_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
  log_append_redo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (root_header), &root_header);
  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  pgbuf_set_dirty (thread_p, P, DONT_FREE);

  /* increment lsa of the page to be deallocated */
  LOG_ADDR_SET (&addr, NULL, R, 0);
  log_skip_logging_set_lsa (thread_p, &addr);
  pgbuf_set_dirty (thread_p, R, DONT_FREE);

exit_on_end:

  btree_clear_key_value (&clear_key, &mid_key);

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  return ret;

exit_on_error:

  if (ret == NO_ERROR)
    {
      ret = er_errid ();
      if (ret == NO_ERROR)
	{
	  ret = ER_FAILED;
	}
    }
  assert (ret != NO_ERROR);

  goto exit_on_end;
}
#endif

/*
 * btree_merge_node () -
 *   return: NO_ERROR
 *   btid(in): The B+tree index identifier
 *   P(in): Page pointer for the parent page of page Q
 *   L(in): Page pointer for the left sibling of page Q
 *   Q(in): Page pointer for the child page of P that will be merged
 *   next_page(in):
 *   P_vpid(in): Page identifier for page P
 *   L_vpid(in): Page identifier for page L
 *   Q_vpid(in): Page identifier for page Q
 *   p_slot_id(in): The slot of parent page P which points to page Q
 *   node_type(in): shows whether page Q is a leaf page, or not
 *   child_vpid(in): Child page identifier to be followed, L.
 *
 * Note: Page Q is merged with page L which must be its left sibling.
 * After the merge operation, page Q becomes ready for deallocation.
 * Deallocation is left to the calling routine.
 *
 * Note:  The page which will be deallocated by the caller after a
 * succesful merge operation is not changed by this routine.
 */
static int
btree_merge_node (THREAD_ENTRY * thread_p, BTID_INT * btid, PAGE_PTR P,
		  PAGE_PTR L, PAGE_PTR Q, UNUSED_ARG VPID * P_vpid,
		  VPID * L_vpid, VPID * Q_vpid, INT16 p_slot_id,
		  short node_type, VPID * child_vpid)
{
  INT16 left_slotid, right_slotid;
  PAGE_PTR left_pg = NULL;
  PAGE_PTR right_pg = NULL;
  VPID left_vpid;
//VPID next_vpid;
//  VPID right_vpid;
  int left_cnt, right_cnt;
  BTREE_NODE_HEADER pheader;
  BTREE_NODE_HEADER left_header;
  BTREE_NODE_HEADER right_header;
  RECDES peek_rec1 = RECDES_INITIALIZER,
    peek_rec2 = RECDES_INITIALIZER, copy_rec = RECDES_INITIALIZER;
  NON_LEAF_REC nleaf_pnt, nonleaf_rec, junk_rec;
  int i;
  DB_IDXKEY left_key;
  bool clear_lkey = false;
  char *recset_data;		/* for recovery purposes */
  int recset_length;		/* for recovery purposes */
  RECSET_HEADER recset_header;	/* for recovery purposes */
  int ret = NO_ERROR;
  char copy_rec_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  char recset_data_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  assert (P == NULL || (P_vpid != NULL && !VPID_ISNULL (P_vpid)));

  assert (L != NULL);
  assert (Q != NULL);
  assert (L_vpid != NULL && !VPID_ISNULL (L_vpid));
  assert (Q_vpid != NULL && !VPID_ISNULL (Q_vpid));

  /* initializations */
  recset_data = NULL;
  copy_rec.data = NULL;
  VPID_SET_NULL (child_vpid);
  copy_rec.area_size = DB_PAGESIZE;
  copy_rec.data = PTR_ALIGN (copy_rec_buf, BTREE_MAX_ALIGN);
  recset_data = PTR_ALIGN (recset_data_buf, BTREE_MAX_ALIGN);

  DB_IDXKEY_MAKE_NULL (&left_key);

  left_pg = L;
  left_slotid = p_slot_id - 1;
  left_vpid = *L_vpid;

  right_pg = Q;
  right_slotid = p_slot_id;

  /* get left page header information */
  if (btree_read_node_header (btid, left_pg, &left_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the old header record for undo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, HEADER);
  log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (left_header), &left_header);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  if (P != NULL)
    {
      /* is not merge root */

      if (btree_read_node_header (btid, P, &pheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the old header record for undo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
      log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			    sizeof (pheader), &pheader);
    }
  else
    {
      RECDES peek_rec = RECDES_INITIALIZER;
      PGLENGTH log_addr_offset;

      /* is merge root */

      /* left_pg is root page pointer
       */
      assert (node_type == BTREE_NON_LEAF_NODE);
      assert (left_header.node_level > 2);

      /* read the first record */
      if (spage_get_record (left_pg, 1, &peek_rec, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* delete first record */
      /* prepare undo log record */

      log_addr_offset = 1;
      /* log the deleted slotid for undo/redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, log_addr_offset);
      log_append_undoredo_data (thread_p, RVBT_NDRECORD_DEL, &addr,
				peek_rec.length,
				sizeof (log_addr_offset), peek_rec.data,
				&log_addr_offset);

      if (spage_delete (thread_p, left_pg, 1) != 1)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (spage_number_of_records (left_pg) == 1);
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* get right page header information */
  if (btree_read_node_header (btid, right_pg, &right_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (right_header.node_level > 1 || right_header.key_cnt == 0);
  assert (right_header.key_cnt >= 0);
//  VPID_COPY (&next_vpid, &right_header.next_vpid);

  left_cnt = spage_number_of_records (left_pg) - 1;
  right_cnt = spage_number_of_records (right_pg) - 1;
  assert (node_type == BTREE_NON_LEAF_NODE || right_cnt == 0);

  if (P != NULL)
    {
      /* is not merge root */

      assert (left_header.key_cnt >= 0);
    }

  /* move the keys from the right page to the left page */
  if (P != NULL && node_type == BTREE_NON_LEAF_NODE)
    {
      /* is not merge root */
      /* move the key from the parent P to left child */
      if (spage_get_record (P, left_slotid, &peek_rec1, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (btree_read_record (thread_p, btid, &peek_rec1, &left_key,
			     &junk_rec, BTREE_NON_LEAF_NODE, &clear_lkey,
			     PEEK_KEY_VALUE) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* Warning!!! Don't use peek_rec1 again since left_key may point
       * into it.  Use peek_rec2.
       */

      /* we need to use COPY here instead of PEEK because we are updating
       * the child page pointer in place.
       */
      if (spage_get_record (left_pg, left_cnt, &copy_rec, COPY) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the old node record for undo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, left_cnt);
      log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			    copy_rec.length, copy_rec.data);
      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

      btree_read_fixed_portion_of_non_leaf_record (&copy_rec, &nleaf_pnt);
      nonleaf_rec.pnt = nleaf_pnt.pnt;

      ret =
	btree_write_record (thread_p, btid, &nonleaf_rec, &left_key,
			    node_type, &copy_rec);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (spage_update (thread_p, left_pg, left_cnt, &copy_rec) != SP_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the new node record for redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, left_cnt);
      log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			    &addr, copy_rec.length, copy_rec.data);

      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);
    }

  /* Log the right page records for undo purposes on the left page. */
  recset_header.rec_cnt = right_cnt;
  recset_header.first_slotid = left_cnt + 1;
  ret =
    btree_rv_util_save_page_records (thread_p, btid, right_pg, 1, right_cnt,
				     left_cnt + 1, recset_data,
				     IO_MAX_PAGE_SIZE, &recset_length);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* move content of the right page to the left page */
  for (i = 1; i <= right_cnt; i++)
    {
      if ((spage_get_record (right_pg, i, &peek_rec2, PEEK)
	   != S_SUCCESS)
	  || (spage_insert_at (thread_p, left_pg, left_cnt + i,
			       &peek_rec2) != SP_SUCCESS))
	{
	  if (i > 1)
	    {
	      recset_header.rec_cnt = i - 1;
	      recset_header.first_slotid = left_cnt + 1;
	      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, -1);
	      log_append_undo_data (thread_p, RVBT_INS_PGRECORDS,
				    &addr, sizeof (RECSET_HEADER),
				    &recset_header);
	    }
	  GOTO_EXIT_ON_ERROR;
	}
    }				/* for */

  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, -1);
  log_append_undoredo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
			    sizeof (RECSET_HEADER), recset_length,
			    &recset_header, recset_data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  if (P != NULL)
    {
      /* is not merge root */

      /* update parent page P, use COPY here instead of PEEK because we
       * are updating the child pointer in place.
       */
      if (spage_get_record (P, right_slotid, &copy_rec, COPY) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the old node record for undo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, right_slotid);
      log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			    copy_rec.length, copy_rec.data);

      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

      btree_read_fixed_portion_of_non_leaf_record (&copy_rec, &nleaf_pnt);
      nleaf_pnt.pnt = left_vpid;

      btree_write_fixed_portion_of_non_leaf_record (&copy_rec, &nleaf_pnt);
      if (spage_update (thread_p, P, right_slotid, &copy_rec) != SP_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the new node record for redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, right_slotid);
      log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			    &addr, copy_rec.length, copy_rec.data);

      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

      /* get and log the old node record to be deleted for undo purposes */

      if (spage_get_record (P, left_slotid, &peek_rec2, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      btree_read_fixed_portion_of_non_leaf_record (&peek_rec2, &nleaf_pnt);

      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, left_slotid);
      log_append_undoredo_data (thread_p, RVBT_NDRECORD_DEL, &addr,
				peek_rec2.length,
				sizeof (left_slotid), peek_rec2.data,
				&left_slotid);

      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

      if (spage_delete (thread_p, P, left_slotid) != left_slotid)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* update left page header
   */

  /* The key count already incorporates the semantics of the
   * non leaf/leaf key count difference (non leaf key count is one less
   * than the actual key count).  We simply increment the key count by the
   * actual number of keys we've added to the left page.
   */
  if (P != NULL)
    {
      /* is not merge root */
      assert_release (left_header.key_cnt >= 0);
      left_header.key_cnt += right_cnt;
      assert_release (left_header.key_cnt >= right_cnt);
    }
  else
    {
      /* is merge root */
      assert (left_header.key_cnt == 0);
      left_header.key_cnt += (right_cnt - 1);
      assert_release (left_header.key_cnt == (right_cnt - 1));

      left_header.node_level--;
      assert (left_header.node_level > 1);
    }
  assert_release (left_header.key_cnt >= 0);

  VPID_COPY (&left_header.next_vpid, &right_header.next_vpid);
  left_header.split_info.pivot = BTREE_SPLIT_DEFAULT_PIVOT;
  left_header.split_info.index = 1;

  if (btree_write_node_header (btid, left_pg, &left_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, left_pg, HEADER);
  log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			&addr, sizeof (left_header), &left_header);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  *child_vpid = left_vpid;

  pgbuf_set_dirty (thread_p, left_pg, DONT_FREE);

  /* increment lsa of the page to be deallocated */
  LOG_ADDR_SET (&addr, NULL, right_pg, 0);
  log_skip_logging_set_lsa (thread_p, &addr);
  pgbuf_set_dirty (thread_p, right_pg, DONT_FREE);

  if (P != NULL)
    {
      /* is not merge root */

      /* update parent page header */

      assert_release (pheader.key_cnt >= 1);
      pheader.key_cnt--;
      assert_release (pheader.key_cnt >= 0);
      assert (pheader.node_level > 1);

      if (btree_write_node_header (btid, P, &pheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* log the new header record for redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
      log_append_redo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			    sizeof (pheader), &pheader);

      FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

      pgbuf_set_dirty (thread_p, P, DONT_FREE);
    }

  ret = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_MERGE_ERROR1, 0, 0);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  btree_clear_key_value (&clear_lkey, &left_key);

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  return ret;

exit_on_error:

  btree_clear_key_value (&clear_lkey, &left_key);

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_delete () -
 *
 *   return: (the specified key on succes, or NULL on failure)
 *
 *   btid(in): B+tree index identifier
 *   key(in): Key from which the specified value will be deleted
 */
DB_IDXKEY *
btree_delete (THREAD_ENTRY * thread_p, BTID_INT * btid, DB_IDXKEY * key)
{
  BTREE_NODE_HEADER root_header, *node_header = NULL;
#if 0				/* !defined(NDEBUG) */
  bool is_active;
#endif
  VPID P_vpid, Q_vpid;
  PAGE_PTR P = NULL, Q = NULL;
  INT16 p_slot_id;
  int ret_val;
#if defined(SERVER_MODE)
  bool old_check_interrupt;
#endif /* SERVER_MODE */
  int non_leaf_request_mode = PGBUF_LATCH_READ;
  int Q_used, Left_used, *node_used = NULL;
  INT16 key_cnt;
  bool is_root_empty, is_leaf_empty, is_q_empty;
  short root_level, merge_level, i;
  int P_req_mode;
  UINT64 perf_start;

  assert (btid->sys_btid != NULL);
  assert (!OID_ISNULL (&(btid->cls_oid)));
  assert (btid->classrepr != NULL);
#if !defined(NDEBUG)
  assert (DB_IDXKEY_IS_NULL (&(btid->left_fence)));
  assert (DB_IDXKEY_IS_NULL (&(btid->right_fence)));
#endif

  PERF_MON_GET_CURRENT_TIME (perf_start);

  assert (key != NULL);
  if (key == NULL)
    {
      return key;
    }

  assert (file_is_new_file (thread_p, &(btid->sys_btid->vfid)) ==
	  FILE_OLD_FILE);

  assert (!BTREE_INVALID_INDEX_ID (btid->sys_btid));

#if defined(SERVER_MODE)
  old_check_interrupt = thread_set_check_interrupt (thread_p, false);
#endif /* SERVER_MODE */

#if 0				/* !defined(NDEBUG) */
  is_active = logtb_is_current_active (thread_p);
#endif

  P_vpid.volid = btid->sys_btid->vfid.volid;	/* read the root page */
  P_vpid.pageid = btid->sys_btid->root_pageid;

  P =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &P_vpid,
		     non_leaf_request_mode, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (P == NULL)
    {
      goto error;
    }

  if (btree_read_node_header (btid, P, &root_header) != NO_ERROR)
    {
      goto error;
    }

  key_cnt = root_header.key_cnt;
  assert (key_cnt >= 0);
  is_root_empty = (key_cnt == 0) ? true : false;
  assert (is_root_empty || (spage_number_of_records (P) > 2));

  root_level = root_header.node_level;
  assert (root_level > 1);

  node_header =
    (BTREE_NODE_HEADER *) malloc (sizeof (BTREE_NODE_HEADER) *
				  (root_level + 2));
  if (node_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (BTREE_NODE_HEADER) * (root_level + 2));
      goto error;
    }

  node_used = (int *) malloc (sizeof (int) * (root_level + 2));
  if (node_used == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (int) * (root_level + 2));
      goto error;
    }

  i = root_level;
  assert (i > 1);
  node_header[i] = root_header;
  assert (node_header[i].node_level == i);

  node_used[i] = DB_PAGESIZE - spage_get_free_space (thread_p, P);

  while (node_header[i].node_level > 1)
    {
      /* find and get the child page to be followed */
      if (btree_search_nonleaf_page (thread_p, btid, P, key,
				     &p_slot_id, &Q_vpid) != NO_ERROR)
	{
	  goto error;
	}

      if (node_header[i].node_level > 2)
	{
	  /* Q is non leaf node */
	  Q =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			     non_leaf_request_mode,
			     PGBUF_UNCONDITIONAL_LATCH, PAGE_BTREE);
	}
      else
	{
	  /* Q is leaf node */
	  Q =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			     PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
			     PAGE_BTREE);
	}
      if (Q == NULL)
	{
	  goto error;
	}

      i--;

      /* read the header record */
      if (btree_read_node_header (btid, Q, &(node_header[i])) != NO_ERROR)
	{
	  goto error;
	}

      assert (node_header[i + 1].node_level - 1 == node_header[i].node_level);
      assert (node_header[i].node_level == i);

      node_used[i] = DB_PAGESIZE - spage_get_free_space (thread_p, Q);

      pgbuf_unfix_and_init (thread_p, P);
      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;
    }

  assert (i == 1);
  assert (node_header[i + 1].node_level - 1 == node_header[i].node_level);
  assert (node_header[i].node_level == 1);

  /* a leaf page is reached, perform the deletion */
  assert (P != NULL);
  if (btree_delete_from_leaf (thread_p, btid, P, key) != NO_ERROR)
    {
      goto error;
    }

  /* read the header record */
  if (btree_read_node_header (btid, P, &(node_header[i])) != NO_ERROR)
    {
      goto error;
    }

  node_used[i] = DB_PAGESIZE - spage_get_free_space (thread_p, P);

  key_cnt = node_header[i].key_cnt;
  assert (key_cnt >= 0);
  is_leaf_empty = (key_cnt == 0) ? true : false;
  assert (is_leaf_empty || (spage_number_of_records (P) > 1));

  assert (node_header[i].node_level == 1);

#if !defined(NDEBUG)
  (void) spage_check_num_slots (thread_p, P);
#endif

  pgbuf_unfix_and_init (thread_p, P);

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  if (is_leaf_empty && prm_get_bool_value (PRM_ID_BTREE_MERGE_ENABLED))
    {
      assert (node_header[1].key_cnt == 0);

      for (merge_level = 2; merge_level < root_level; merge_level++)
	{
	  Q_used = node_used[merge_level];
	  Left_used = Q_used;	/* guess */

	  if (node_header[merge_level - 1].key_cnt == 0)
	    {
	      assert (node_header[merge_level].key_cnt >= 0);
	      node_header[merge_level].key_cnt--;	/* recalc fan-out */
	    }
	  key_cnt = node_header[merge_level].key_cnt;
	  is_q_empty = (key_cnt <= 0) ? true : false;

	  if (((Left_used + Q_used + FIXED_EMPTY) < DB_PAGESIZE)
	      || is_q_empty)
	    {
	      ;			/* go ahead */
	    }
	  else
	    {
	      break;
	    }
	}

      assert (merge_level <= root_level);
      if (merge_level >= root_level)
	{
	  P_req_mode = PGBUF_LATCH_WRITE;
	}
      else
	{
	  P_req_mode = PGBUF_LATCH_READ;
	}

      if (merge_level == root_level && root_level == 2 && is_root_empty)
	{
	  ;			/* nop - nothing to merge */
	}
      else
	{
	  ret_val =
	    btree_merge_level (thread_p, btid, key, P_req_mode, merge_level);
	  if (ret_val != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  if (node_header != NULL)
    {
      free_and_init (node_header);
    }
  if (node_used != NULL)
    {
      free_and_init (node_used);
    }

#if defined(SERVER_MODE)
  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);
#endif /* SERVER_MODE */

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_DELETES, 1,
			       perf_start);

  return key;

error:

  if (P)
    {
      pgbuf_unfix_and_init (thread_p, P);
    }
  if (Q)
    {
      pgbuf_unfix_and_init (thread_p, Q);
    }

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  if (node_header != NULL)
    {
      free_and_init (node_header);
    }
  if (node_used != NULL)
    {
      free_and_init (node_used);
    }

#if defined(SERVER_MODE)
  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);
#endif /* SERVER_MODE */

  return NULL;
}

/*
 * btree_merge_level () -
 *
 *   return:
 *
 *   btid(in): B+tree index identifier
 *   key(in): Key from which the specified value will be deleted
 *   P_req_mode(in):
 *   merge_level(in):
 */
static int
btree_merge_level (THREAD_ENTRY * thread_p, BTID_INT * btid, DB_IDXKEY * key,
		   int P_req_mode, short merge_level)
{
  int status = NO_ERROR;
  BTREE_NODE_HEADER pheader, qheader, /* rheader, */ left_header;
  VPID P_vpid, Q_vpid, /* R_vpid, */ Left_vpid, child_vpid;
  VPID *del_vpid = NULL, *new_del_vpid = NULL;
  int i, d = 0, del_vpid_size, exp_size;
  PAGE_PTR P = NULL, Q = NULL, /* R = NULL, */ Left = NULL;
  PAGE_PTR next_page = NULL;
  RECDES peek_rec = RECDES_INITIALIZER;
  NON_LEAF_REC nleaf_ptr;
//  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  int Q_used, /* R_used, */ Left_used;
  INT16 p_slot_id, m_slot_id;
  int top_op_active = 0;
//  bool is_active;
//  int ret_val = NO_ERROR;
  short node_type;
  INT16 key_cnt;
  bool is_root_empty, /* is_p_empty, */ is_q_empty /* , is_r_empty, */ ;
//  bool is_left_empty;
  short root_level;
  int Q_req_mode;
  bool do_merge = false;
  UINT64 perf_start;

  assert (merge_level > 1);

  thread_mnt_track_push (thread_p,
			 MNT_STATS_DATA_PAGE_FETCHES_TRACK_BTREE_MERGE_LEVEL,
			 &status);

  PERF_MON_GET_CURRENT_TIME (perf_start);

  /* init */
  d = 0;
  exp_size = 10;

  del_vpid_size = exp_size;	/* guess */
  del_vpid = (VPID *) malloc (sizeof (VPID) * del_vpid_size);
  if (del_vpid == NULL)
    {
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (VPID) * del_vpid_size);
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < del_vpid_size; i++)
    {
      VPID_SET_NULL (&(del_vpid[i]));
    }

#if 0				/* TODO - trace */
  db_idxkey_print (key);
  fflush (stdout);
#endif

#if 0				/* TODO - */
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_DEALLOC_PAGE_ERROR1);
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_DEALLOC_PAGE_ERROR2);
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_DEALLOC_PAGE_ERROR3);
#endif

#if 0
  copy_rec.data = NULL;
  copy_rec1.data = NULL;

  is_active = logtb_is_current_active (thread_p);
#endif

#if !defined(NDEBUG)
  assert (DB_IDXKEY_IS_NULL (&(btid->left_fence)));
  assert (DB_IDXKEY_IS_NULL (&(btid->right_fence)));
#endif

  P_vpid.volid = btid->sys_btid->vfid.volid;	/* read the root page */
  P_vpid.pageid = btid->sys_btid->root_pageid;

  P = btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &P_vpid,
		       P_req_mode, PGBUF_UNCONDITIONAL_LATCH,
		       PAGE_BTREE_ROOT);
  if (P == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (btree_read_node_header (btid, P, &pheader) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  root_level = pheader.node_level;
  assert (root_level > 1);
  node_type = BTREE_NON_LEAF_NODE;
  key_cnt = pheader.key_cnt;
  assert (key_cnt >= 0);

  is_root_empty = (key_cnt == 0) ? true : false;
  assert (is_root_empty || (spage_number_of_records (P) > 2));

  assert (P_req_mode == PGBUF_LATCH_READ || merge_level >= root_level);

  while (P_req_mode == PGBUF_LATCH_WRITE && is_root_empty && root_level > 2)
    {				/* root merge possible */
      assert (node_type == BTREE_NON_LEAF_NODE);

#if 0				/* dbg - print */
      fprintf (stdout, "merge root: root_level: %d\n", root_level);
#endif

      /* read the first record */
      if (spage_get_record (P, 1, &peek_rec, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      btree_read_fixed_portion_of_non_leaf_record (&peek_rec, &nleaf_ptr);
      Q_vpid = nleaf_ptr.pnt;
      Q = btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			   PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
			   PAGE_BTREE);
      if (Q == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (top_op_active == 0);

      /* Start system permanent operation */
      log_start_system_op (thread_p);
      top_op_active = 1;

      if (btree_merge_node (thread_p, btid, NULL, P, Q,
			    NULL, &P_vpid, &Q_vpid,
			    1, node_type, &child_vpid) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (next_page == NULL);

      if (!VPID_EQ (&child_vpid, &P_vpid))
	{
	  assert (false);

	  /* do not unfix P, Q before topop rollback */
	  log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
	  top_op_active = 0;

	  GOTO_EXIT_ON_ERROR;
	}

      assert (file_is_new_file
	      (thread_p, &(btid->sys_btid->vfid)) == FILE_OLD_FILE);

      log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);

      top_op_active = 0;

      pgbuf_unfix_and_init (thread_p, Q);

      if (d >= del_vpid_size)
	{
	  new_del_vpid =
	    realloc (del_vpid, sizeof (VPID) * (del_vpid_size + exp_size));
	  if (new_del_vpid == NULL)
	    {
	      assert (false);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      sizeof (VPID) * (del_vpid_size + exp_size));
	      GOTO_EXIT_ON_ERROR;
	    }

	  for (i = del_vpid_size; i < del_vpid_size + exp_size; i++)
	    {
	      VPID_SET_NULL (&(new_del_vpid[i]));
	    }

	  del_vpid = new_del_vpid;
	  del_vpid_size += exp_size;
	}

      del_vpid[d++] = Q_vpid;	/* VPID_COPY */
      assert (d <= del_vpid_size);

      /* get the header record */
      if (btree_read_node_header (btid, P, &pheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (pheader.node_level == root_level - 1);
      root_level = pheader.node_level;
      assert (root_level > 1);
      node_type = BTREE_NON_LEAF_NODE;
      key_cnt = pheader.key_cnt;
      assert (key_cnt >= 0);

      is_root_empty = (key_cnt == 0) ? true : false;
      assert (is_root_empty || (spage_number_of_records (P) > 2));
    }				/* while */

#if !defined(NDEBUG)
  assert (DB_IDXKEY_IS_NULL (&(btid->left_fence)));
  assert (DB_IDXKEY_IS_NULL (&(btid->right_fence)));
#endif

  while (node_type == BTREE_NON_LEAF_NODE)
    {
      assert (VPID_ISNULL (&pheader.next_vpid));

      /* find and get the child page to be followed */
      if (btree_search_nonleaf_page (thread_p, btid, P, key,
				     &p_slot_id, &Q_vpid) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (p_slot_id >= 1);

      if (merge_level == pheader.node_level - 1)
	{
	  Q_req_mode = PGBUF_LATCH_WRITE;
	}
      else
	{
	  Q_req_mode = P_req_mode;
	}

      assert (Q_req_mode == PGBUF_LATCH_READ
	      || merge_level >= pheader.node_level - 1);
      assert (P_req_mode == PGBUF_LATCH_READ
	      || Q_req_mode == PGBUF_LATCH_WRITE);

      Q = btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			   Q_req_mode, PGBUF_UNCONDITIONAL_LATCH, PAGE_BTREE);
      if (Q == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (pheader.node_level > 2)
	{
	  /* Q is non leaf node */
	}
      else
	{
	  /* Q is leaf node */
	  assert (Q_req_mode == PGBUF_LATCH_WRITE);
	}

#if !defined(NDEBUG)
      assert ((merge_level < pheader.node_level - 1
	       && P_req_mode == PGBUF_LATCH_READ
	       && Q_req_mode == PGBUF_LATCH_READ)
	      || (merge_level == pheader.node_level - 1
		  && P_req_mode == PGBUF_LATCH_READ
		  && Q_req_mode == PGBUF_LATCH_WRITE)
	      || (merge_level > pheader.node_level - 1
		  && P_req_mode == PGBUF_LATCH_WRITE
		  && Q_req_mode == PGBUF_LATCH_WRITE));
#endif

      if (P_req_mode == PGBUF_LATCH_WRITE && Q_req_mode == PGBUF_LATCH_WRITE
	  && p_slot_id > 1)
	{			/* left sibling accessible */
	  assert (merge_level >= pheader.node_level);
	  assert (pheader.node_level >= 2);

	  for (m_slot_id = p_slot_id; m_slot_id > 1; m_slot_id--)
	    {
	      if (spage_get_record (P, m_slot_id - 1, &peek_rec, PEEK) !=
		  S_SUCCESS)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      btree_read_fixed_portion_of_non_leaf_record (&peek_rec,
							   &nleaf_ptr);
	      Left_vpid = nleaf_ptr.pnt;

	      if (pheader.node_level > 2)
		{
		  /* Left is non leaf node */
		  Left =
		    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid),
				     &Left_vpid, PGBUF_LATCH_WRITE,
				     PGBUF_UNCONDITIONAL_LATCH, PAGE_BTREE);
		  if (Left == NULL)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	      else
		{
		  /* try to fix left page conditionally */
		  Left =
		    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid),
				     &Left_vpid, PGBUF_LATCH_WRITE,
				     PGBUF_CONDITIONAL_LATCH, PAGE_BTREE);
		  if (Left == NULL)
		    {
		      /* unfix Q page */
		      pgbuf_unfix_and_init (thread_p, Q);

		      Left =
			btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid),
					 &Left_vpid,
					 PGBUF_LATCH_WRITE,
					 PGBUF_UNCONDITIONAL_LATCH,
					 PAGE_BTREE);
		      if (Left == NULL)
			{
			  GOTO_EXIT_ON_ERROR;
			}

		      Q =
			btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid),
					 &Q_vpid, PGBUF_LATCH_WRITE,
					 PGBUF_UNCONDITIONAL_LATCH,
					 PAGE_BTREE);
		      if (Q == NULL)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		    }
		}

	      /* read the header record */
	      if (btree_read_node_header (btid, Q, &qheader) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      assert (pheader.node_level - 1 == qheader.node_level);
	      node_type = BTREE_GET_NODE_TYPE (qheader.node_level);
	      key_cnt = qheader.key_cnt;
	      assert (key_cnt >= 0);

	      is_q_empty = (key_cnt == 0) ? true : false;
	      Q_used = DB_PAGESIZE - spage_get_free_space (thread_p, Q);

	      /* read the header record */
	      if (btree_read_node_header (btid, Left, &left_header) !=
		  NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      assert (pheader.node_level - 1 == left_header.node_level);
	      node_type = BTREE_GET_NODE_TYPE (left_header.node_level);
#if 0
	      key_cnt = left_header.key_cnt;
	      assert (key_cnt >= 0);
#else
	      assert (left_header.key_cnt >= 0);
#endif

#if 0
	      is_left_empty = (key_cnt == 0) ? true : false;
#endif
	      Left_used = DB_PAGESIZE - spage_get_free_space (thread_p, Left);

	      do_merge = false;	/* init */

	      if (node_type == BTREE_NON_LEAF_NODE)
		{
		  if ((Left_used + Q_used + FIXED_EMPTY) < DB_PAGESIZE)
		    {
		      do_merge = true;
		    }
		}
	      else
		{
		  if (is_q_empty)
		    {
		      do_merge = true;
		    }
		}

	      if (!do_merge)
		{		/* not merged */
		  pgbuf_unfix_and_init (thread_p, Left);
		  assert (Q != NULL);
		  assert (!VPID_ISNULL (&Q_vpid));
		  break;	/* exit for-loop */
		}

	      assert (top_op_active == 0);

	      /* left merge possible */

	      /* start system permanent operation */
	      log_start_system_op (thread_p);
	      top_op_active = 1;

	      if (btree_merge_node (thread_p, btid, P, Left, Q,
				    &P_vpid, &Left_vpid, &Q_vpid,
				    m_slot_id, node_type,
				    &child_vpid) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      assert (next_page == NULL);

	      if (!VPID_EQ (&child_vpid, &Left_vpid))
		{
		  assert (false);

		  /* do not unfix P, Left, Q before topop rollback */
		  log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
		  top_op_active = 0;

		  GOTO_EXIT_ON_ERROR;
		}

	      if (node_type == BTREE_LEAF_NODE)
		{
		  next_page = btree_get_next_page (thread_p, btid, Left);
		  if (next_page != NULL)
		    {
		      if (btree_set_vpid_previous_vpid (thread_p,
							btid,
							next_page,
							&Left_vpid)
			  != NO_ERROR)
			{
			  GOTO_EXIT_ON_ERROR;
			}
		    }
		}

	      assert (file_is_new_file
		      (thread_p, &(btid->sys_btid->vfid)) == FILE_OLD_FILE);

	      log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);

	      if (next_page)
		{
		  pgbuf_unfix_and_init (thread_p, next_page);
		}

	      top_op_active = 0;

	      /* child page to be followed is Left */
	      pgbuf_unfix_and_init (thread_p, Q);

	      if (d >= del_vpid_size)
		{
		  new_del_vpid =
		    realloc (del_vpid,
			     sizeof (VPID) * (del_vpid_size + exp_size));
		  if (new_del_vpid == NULL)
		    {
		      assert (false);
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      sizeof (VPID) * (del_vpid_size + exp_size));
		      GOTO_EXIT_ON_ERROR;
		    }

		  for (i = del_vpid_size; i < del_vpid_size + exp_size; i++)
		    {
		      VPID_SET_NULL (&(new_del_vpid[i]));
		    }

		  del_vpid = new_del_vpid;
		  del_vpid_size += exp_size;
		}

	      del_vpid[d++] = Q_vpid;	/* VPID_COPY */
	      assert (d <= del_vpid_size);

	      Q = Left;
	      Left = NULL;
	      Q_vpid = Left_vpid;
	    }			/* for (m_slot_id = p_slot_id; ...) */
	}

      /* release parent page P, and repeat the same operations from child
       * page Q on
       */
      pgbuf_unfix_and_init (thread_p, P);
      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;

      /* node_type must be recalculated */
      if (btree_read_node_header (btid, P, &pheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      node_type = BTREE_GET_NODE_TYPE (pheader.node_level);
#if 0
      key_cnt = pheader.key_cnt;
      assert (key_cnt >= 0);
#else
      assert (pheader.key_cnt >= 0);
#endif

      if (node_type == BTREE_NON_LEAF_NODE)
	{
	  P_req_mode = Q_req_mode;
	}
    }				/* while */

  /* a leaf page is reached, perform the deletion */
  assert (pheader.node_level == 1);

  assert (top_op_active == 0);
  assert (P != NULL);
  assert (Q == NULL);
  assert (Left == NULL);
  assert (next_page == NULL);

#if !defined(NDEBUG)
  (void) spage_check_num_slots (thread_p, P);
#endif

  pgbuf_unfix_and_init (thread_p, P);

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  if (del_vpid != NULL)
    {
#if 0				/* dbg - print */
      if (d > 1)
	{
	  fprintf (stdout, "_%d", d);
	}
#endif
      assert (d <= del_vpid_size);
      for (i = 0; i < d; i++)
	{
	  if (VPID_ISNULL (&(del_vpid[i])))
	    {
	      assert (false);
	      continue;
	    }

	  if (btree_dealloc_page (thread_p, btid, &(del_vpid[i])) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      free_and_init (del_vpid);
    }

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_MERGES, 1,
			       perf_start);

  if (status == NO_ERROR)
    {
      thread_mnt_track_pop (thread_p, &status);
      assert (status == NO_ERROR);
    }

  return NO_ERROR;

exit_on_error:

  /* do not unfix P, Q before topop rollback */
  if (top_op_active)
    {
      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
    }

  if (P)
    {
      pgbuf_unfix_and_init (thread_p, P);
    }
  if (Q)
    {
      pgbuf_unfix_and_init (thread_p, Q);
    }
  if (Left)
    {
      pgbuf_unfix_and_init (thread_p, Left);
    }
  if (next_page)
    {
      pgbuf_unfix_and_init (thread_p, next_page);
    }

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  if (del_vpid != NULL)
    {
      assert (d <= del_vpid_size);
      for (i = 0; i < d; i++)
	{
	  if (VPID_ISNULL (&(del_vpid[i])))
	    {
	      assert (false);
	      continue;
	    }

	  /* give up error checking */
	  (void) btree_dealloc_page (thread_p, btid, &(del_vpid[i]));
	}
      free_and_init (del_vpid);
    }

  if (status == NO_ERROR)
    {
      thread_mnt_track_pop (thread_p, &status);
      assert (status == NO_ERROR);
    }

  return ER_FAILED;
}

/*
 * btree_split_next_pivot () -
 *   return:
 *   split_info(in):
 *   new_value(in):
 *   max_index(in):
 */
int
btree_split_next_pivot (BTREE_NODE_SPLIT_INFO * split_info,
			float new_value, int max_index)
{
  float new_pivot;

  assert (split_info->pivot >= 0.0f);
  assert (split_info->pivot <= 1.0f);

  split_info->index = MIN (split_info->index + 1, max_index);

  if (split_info->pivot == 0)
    {
      new_pivot = new_value;
    }
  else
    {
      /* cumulative moving average(running average) */
      new_pivot = split_info->pivot;
      new_pivot = (new_pivot + ((new_value - new_pivot) / split_info->index));
    }

  split_info->pivot = MAX (0.0f, MIN (1.0f, new_pivot));

  return NO_ERROR;
}

/*
 * btree_update () -
 *   return: NO_ERROR
 *   btid(in): B+tree index identifier
 *   old_key(in): Old key value
 *   new_key(in): New key value
 *
 * Note: Deletes the <old_key, oid> key-value pair from the B+tree
 * index and inserts the <new_key, oid> key-value pair to the
 * B+tree index which results in the update of the specified
 * index entry for the given object identifier.
 */
int
btree_update (THREAD_ENTRY * thread_p, BTID_INT * btid,
	      DB_IDXKEY * old_key, DB_IDXKEY * new_key)
{
  int ret = NO_ERROR;

  assert (btid->sys_btid != NULL);
  assert (!OID_ISNULL (&(btid->cls_oid)));
  assert (btid->classrepr != NULL);

  if (btree_delete (thread_p, btid, old_key) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (btree_insert (thread_p, btid, new_key) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

end:

  mnt_stats_counter (thread_p, MNT_STATS_BTREE_UPDATES, 1);

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_locate_key () - Locate a key node and position
 *   return: int true: key_found, false: key_not found
 *               (if error, false and slot_id = NULL_SLOTID)
 *   btid_int(in): B+tree index identifier
 *   key(in): Key to locate
 *   pg_vpid(out): Set to the page identifier that contains the key or should
 *                 contain the key if the key was to be inserted.
 *   slot_id(out): Set to the number (position) of the record that contains the
 *                 key or would contain the key if the key was to be inserted.
 *   found(out):
 *
 * Note: Searchs the B+tree index to locate the page and record that
 * contains the key, or would contain the key if the key was to be located.
 */
static PAGE_PTR
btree_locate_key (THREAD_ENTRY * thread_p, BTID_INT * btid_int,
		  const DB_IDXKEY * key, VPID * pg_vpid,
		  INT16 * slot_id, bool * found)
{
  PAGE_PTR P = NULL, Q = NULL;
  VPID P_vpid, Q_vpid;
  INT16 p_slot_id;
  BTREE_NODE_HEADER node_header;
#if !defined(NDEBUG)
  short P_node_level;
#endif

  assert (key != NULL);

  assert (!BTREE_INVALID_INDEX_ID (btid_int->sys_btid));
#if !defined(NDEBUG)
  assert (DB_IDXKEY_IS_NULL (&(btid_int->left_fence)));
  assert (DB_IDXKEY_IS_NULL (&(btid_int->right_fence)));
#endif

  *found = false;
  *slot_id = NULL_SLOTID;

  P_vpid.volid = btid_int->sys_btid->vfid.volid;	/* read the root page */
  P_vpid.pageid = btid_int->sys_btid->root_pageid;
  P =
    btree_pgbuf_fix (thread_p, &(btid_int->sys_btid->vfid), &P_vpid,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (P == NULL)
    {
      goto error;
    }

  if (btree_read_node_header (btid_int, P, &node_header) != NO_ERROR)
    {
      goto error;
    }
  assert (node_header.node_level > 1);

#if !defined(NDEBUG)
  P_node_level = node_header.node_level;
#endif

  while (node_header.node_level > 1)
    {
      /* get the child page to follow */
      if (btree_search_nonleaf_page (thread_p, btid_int, P, key,
				     &p_slot_id, &Q_vpid) != NO_ERROR)
	{
	  goto error;
	}

      Q =
	btree_pgbuf_fix (thread_p, &(btid_int->sys_btid->vfid), &Q_vpid,
			 PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			 PAGE_BTREE);
      if (Q == NULL)
	{
	  goto error;
	}

      pgbuf_unfix_and_init (thread_p, P);

      /* read the header record */
      if (btree_read_node_header (btid_int, Q, &node_header) != NO_ERROR)
	{
	  goto error;
	}

#if !defined(NDEBUG)
      assert (P_node_level - 1 == node_header.node_level);
#endif

      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;
#if !defined(NDEBUG)
      P_node_level = node_header.node_level;
#endif
    }

#if !defined(NDEBUG)
  assert (P_node_level == 1);
#endif

  /* leaf page is reached */
  *found = btree_search_leaf_page (thread_p, btid_int, P, key, slot_id, NULL);
  if (*slot_id == NULL_SLOTID)
    {
      goto error;
    }

  *pg_vpid = P_vpid;

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid_int->left_fence));
  (void) db_idxkey_clear (&(btid_int->right_fence));
#endif

  /* NOTE that we do NOT release the page latch on P here */
  assert (*slot_id != NULL_SLOTID);

  return P;

error:

  if (P)
    {
      pgbuf_unfix_and_init (thread_p, P);
    }
  if (Q)
    {
      pgbuf_unfix_and_init (thread_p, Q);
    }

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid_int->left_fence));
  (void) db_idxkey_clear (&(btid_int->right_fence));
#endif

  assert (*slot_id == NULL_SLOTID);

  return NULL;
}

/*
 * btree_find_lower_bound_leaf () -
 *   return: NO_ERROR
 *   top_vpid(in):
 *   BTS(in):
 *   stat_info(in):
 *
 * Note: Find the first/last leaf page of the B+tree index.
 */
int
btree_find_lower_bound_leaf (THREAD_ENTRY * thread_p,
			     const VPID * top_vpid, BTREE_SCAN * BTS,
			     BTREE_STATS * stat_info)
{
  int key_cnt;
  BTREE_NODE_HEADER node_header;
  int ret = NO_ERROR;

  assert (stat_info == NULL || BTS->use_desc_index == 0);

  if (BTS->use_desc_index)
    {
      BTS->C_page =
	btree_find_last_leaf (thread_p, &BTS->btid_int, top_vpid,
			      &BTS->C_vpid);
    }
  else
    {
      BTS->C_page =
	btree_find_first_leaf (thread_p, &BTS->btid_int, top_vpid,
			       &BTS->C_vpid, stat_info);
    }

  if (BTS->C_page == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* get header information (key_cnt) */
  if (btree_read_node_header (NULL, BTS->C_page, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  key_cnt = node_header.key_cnt;

  assert (key_cnt >= 0);
  assert (node_header.node_level == 1);

  if (!(key_cnt >= 0)
      || (node_header.node_level > 1)
      || (key_cnt + 1 != spage_number_of_records (BTS->C_page)))
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* set slot id and OID position */
  if (BTS->use_desc_index)
    {
      BTS->slot_id = key_cnt;
    }
  else
    {
      BTS->slot_id = 1;
    }


  if (key_cnt == 0)
    {
      /* tree is empty; need to unfix current leaf page */
      ret = btree_find_next_record (thread_p, BTS, BTS->use_desc_index, NULL);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }
  else
    {
      assert_release (BTS->slot_id <= key_cnt);
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_find_first_leaf () -
 *   return: page pointer
 *   btid(in):
 *   top_vpid(in):
 *   pg_vpid(in):
 *   stat_info(in):
 *
 * Note: Find the page identifier for the first leaf page of the B+tree index.
 */
static PAGE_PTR
btree_find_first_leaf (THREAD_ENTRY * thread_p, const BTID_INT * btid,
		       const VPID * top_vpid, VPID * pg_vpid,
		       BTREE_STATS * stat_info)
{
  PAGE_PTR P_page = NULL, C_page = NULL;
  VPID P_vpid, C_vpid;
  INT16 node_type;
  BTREE_NODE_HEADER node_header;
  NON_LEAF_REC nleaf;
  RECDES rec = RECDES_INITIALIZER;
  int key_cnt = 0, free_space;

  VPID_SET_NULL (pg_vpid);

  /* read the top page */
  P_vpid = *top_vpid;
  P_page =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &P_vpid,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     (P_vpid.volid == btid->sys_btid->vfid.volid
		      && P_vpid.pageid ==
		      btid->sys_btid->
		      root_pageid) ? PAGE_BTREE_ROOT : PAGE_BTREE);
  if (P_page == NULL)
    {
      goto error;
    }

  if (btree_read_node_header (NULL, P_page, &node_header) != NO_ERROR)
    {
      goto error;
    }

  node_type = BTREE_GET_NODE_TYPE (node_header.node_level);
  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);

  while (node_type == BTREE_NON_LEAF_NODE)
    {
      /* get the first child page to follow */
      assert (VPID_ISNULL (&node_header.next_vpid));

      if (spage_number_of_records (P_page) <= 1)
	{			/* node record underflow */
	  er_log_debug (ARG_FILE_LINE, "btree_find_first_leaf: node key count"
			" underflow: %d. Operation Ignored.",
			spage_number_of_records (P_page) - 1);
	  goto error;
	}

      /* get the first record */
      if (spage_get_record (P_page, 1, &rec, PEEK) != S_SUCCESS)
	{
	  goto error;
	}

      btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf);
      C_vpid = nleaf.pnt;
      C_page =
	btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &C_vpid,
			 PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			 PAGE_BTREE);
      if (C_page == NULL)
	{
	  goto error;
	}

      pgbuf_unfix_and_init (thread_p, P_page);

      if (btree_read_node_header (NULL, C_page, &node_header) != NO_ERROR)
	{
	  goto error;
	}

      node_type = BTREE_GET_NODE_TYPE (node_header.node_level);
      key_cnt = node_header.key_cnt;
      assert_release (key_cnt >= 0);

      P_page = C_page;
      C_page = NULL;
      P_vpid = C_vpid;
    }

  assert (P_page != NULL);
  assert (node_header.node_level == 1);

  if (key_cnt != 0)
    {
      goto end;			/* OK */
    }

again:

  assert (P_page != NULL);

  /* fix the next leaf page and set slot_id and oid_pos if it exists. */
  VPID_COPY (&C_vpid, &node_header.next_vpid);
  if (C_vpid.pageid != NULL_PAGEID)
    {
      C_page =
	btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &C_vpid,
			 PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			 PAGE_BTREE);
      if (C_page == NULL)
	{
	  goto error;
	}

      /* unfix the previous leaf page if it is fixed. */
      if (P_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, P_page);
	  /* do not clear bts->P_vpid for UNCONDITIONAL lock request handling */
	}

      /* check if the current leaf page has valid slots */
      if (C_page != NULL)
	{
	  if (btree_read_node_header (NULL, C_page, &node_header) != NO_ERROR)
	    {
	      goto error;
	    }
	  key_cnt = node_header.key_cnt;
	  assert_release (key_cnt >= 0);
	  assert (node_header.node_level == 1);

	  if (key_cnt <= 0)
	    {			/* empty page */
	      /* keep empty leaf page's stat */
	      if (stat_info != NULL)
		{
		  stat_info->leafs++;

		  free_space =
		    spage_max_space_for_new_record (thread_p, C_page);
		  stat_info->tot_free_space += free_space;
		}

	      P_page = C_page;
	      C_page = NULL;
	      goto again;
	    }
	  P_vpid = C_vpid;
	  P_page = C_page;
	}
    }

  /* NOTE that we do NOT release the page latch on P here */
end:

  assert (P_page != NULL);

  /* keep leaf page's stat */
  if (stat_info != NULL)
    {
      stat_info->leafs++;

      free_space = spage_max_space_for_new_record (thread_p, P_page);
      stat_info->tot_free_space += free_space;
    }

  *pg_vpid = P_vpid;

  return P_page;

error:

  if (P_page)
    {
      pgbuf_unfix_and_init (thread_p, P_page);
    }
  if (C_page)
    {
      pgbuf_unfix_and_init (thread_p, C_page);
    }

  return NULL;
}

/*
 * btree_find_last_leaf () -
 *   return: page pointer
 *   btid(in):
 *   top_vpid(in):
 *   pg_vpid(in):
 *
 * Note: Find the page identifier for the last leaf page of the B+tree index.
 */
static PAGE_PTR
btree_find_last_leaf (THREAD_ENTRY * thread_p, const BTID_INT * btid,
		      const VPID * top_vpid, VPID * pg_vpid)
{
  PAGE_PTR P = NULL, Q = NULL;
  VPID P_vpid, Q_vpid;
  BTREE_NODE_HEADER node_header;
  INT16 node_type;
  NON_LEAF_REC nleaf;
  RECDES rec = RECDES_INITIALIZER;
  INT16 num_records;

  VPID_SET_NULL (pg_vpid);

  /* read the top page */
  P_vpid = *top_vpid;
  P =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &P_vpid,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     (P_vpid.volid == btid->sys_btid->vfid.volid
		      && P_vpid.pageid ==
		      btid->sys_btid->
		      root_pageid) ? PAGE_BTREE_ROOT : PAGE_BTREE);
  if (P == NULL)
    {
      goto error;
    }

  if (btree_read_node_header (NULL, P, &node_header) != NO_ERROR)
    {
      goto error;
    }

  node_type = BTREE_GET_NODE_TYPE (node_header.node_level);

  while (node_type == BTREE_NON_LEAF_NODE)
    {
      /* get the first child page to follow */

      num_records = spage_number_of_records (P);
      if (num_records <= 1)
	{			/* node record underflow */
	  er_log_debug (ARG_FILE_LINE, "btree_find_last_leaf: node key count"
			" underflow: %d. Operation Ignored.",
			num_records - 1);
	  goto error;
	}

      /* get the last record */
      if (spage_get_record (P, num_records - 1, &rec, PEEK) != S_SUCCESS)
	{
	  goto error;
	}

      btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf);
      Q_vpid = nleaf.pnt;
      Q =
	btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			 PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			 PAGE_BTREE);
      if (Q == NULL)
	{
	  goto error;
	}

      if (btree_read_node_header (NULL, Q, &node_header) != NO_ERROR)
	{
	  goto error;
	}

      node_type = BTREE_GET_NODE_TYPE (node_header.node_level);

      pgbuf_unfix_and_init (thread_p, P);

      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;
    }

  /* leaf page is reached */
  *pg_vpid = P_vpid;

  /* NOTE that we do NOT release the page latch on P here */
  return P;

error:

  if (P)
    {
      pgbuf_unfix_and_init (thread_p, P);
    }
  if (Q)
    {
      pgbuf_unfix_and_init (thread_p, Q);
    }

  return NULL;
}

/*
 * btree_keyval_search () -
 *   return: the number of object identifiers in the set pointed
 *           at by oids_ptr, or -1 if an error occurs. Since there can be
 *           many object identifiers for the given key, to avoid main
 *           memory limitations, the set of object identifiers are returned
 *           iteratively. At each call, the btree_scan is modified, to
 *           remember the old search position.
 *   btid(in):
 *      btid: B+tree index identifier
 *   key(in): Key to be searched for its object identifier set
 *   isidp(in):
 *
 * Note: Finds the set of object identifiers for the given key.
 * if the key is not found, a 0 count is returned. Otherwise,
 * the area pointed at by oids_ptr is filled with one group of
 * object identifiers.
 *
 * Note: the btree_scan structure must first be initialized by using the macro
 * BTREE_INIT_SCAN() defined in bt.h
 *
 * Note: After the first iteration, caller can use BTREE_END_OF_SCAN() macro
 * defined in bt.h to understand the end of range.
 */
int
btree_keyval_search (THREAD_ENTRY * thread_p, BTID * btid,
		     KEY_VAL_RANGE * key_val_range, INDX_SCAN_ID * isidp)
{
  /* this is just a GE_LE range search with the same key */
  BTREE_SCAN *BTS = NULL;	/* Btree range search scan structure */
  int rc;

  assert (btid != NULL);
  assert (key_val_range != NULL);
  assert (isidp != NULL);

  /* pointer to index scan info. structure */
  BTS = &(isidp->bt_scan);
  assert (!OID_ISNULL (&(BTS->btid_int.cls_oid)));

  assert (BTS->btid_int.sys_btid == NULL);
  BTS->btid_int.sys_btid = btid;

  assert (isidp->need_count_only == false);
  assert (isidp->oid_list.oid_buf_size == OR_OID_SIZE * 2);
  assert (isidp->key_limit_lower == -1);
  assert (isidp->key_limit_upper == -1);

  assert (BTS->btid_int.classrepr != NULL);
  assert (BTS->btid_int.classrepr_cache_idx != -1);
  assert (BTS->btid_int.indx_id != -1);

  rc = btree_range_search (thread_p, btid, key_val_range, NULL, isidp);

  return rc;
}

/*
 * btree_coerce_idxkey () -
 *   return: NO_ERROR or error code
 *   key(in/out):
 *   indexp(in):
 *   num_term(in): #terms associated with index key range
 *   key_minmax(in): MIN_VALUE or MAX_VALUE
 *
 * Note:
 */

int
btree_coerce_idxkey (DB_IDXKEY * key,
		     OR_INDEX * indexp, int num_term, int key_minmax)
{
  int ssize, dsize;
  DB_TYPE dp_type;
  TP_DOMAIN *dp;
  int minmax;
  bool part_key_desc = false;

  int i, num_dbvals;
  int err = NO_ERROR;

  assert (indexp != NULL);
  assert (num_term >= 0);
  assert (key_minmax == BTREE_COERCE_KEY_WITH_MIN_VALUE
	  || key_minmax == BTREE_COERCE_KEY_WITH_MAX_VALUE);

  /* assuming all parameters are not NULL pointer, and 'src_key' is not NULL
     value */

  /* Query optimizer makes the search key('src_key') as sequence type
     even if partial key was specified.
     One more assumption is that query optimizer make the
     search key(either complete or partial) in the same order (of
     sequence) of B+tree key domain. */

  /* get number of elements of the 'src_key' */
  ssize = key->size;
  assert (ssize >= 0);
  assert (ssize <= num_term);

  /* count number of elements of sequence type of the B+tree key domain */
  dsize = indexp->n_atts + 1;
  assert (dsize >= 2);
  assert (dsize >= ssize);

  if (ssize < 0 || dsize < 2 || dsize < ssize || ssize > num_term)
    {
      /* something wrong with making search key in query optimizer */
      err = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, err, 1, "");

      return err;
    }

  if (ssize == dsize)
    {
      return NO_ERROR;		/* OK */
    }

  /* do coercing, append min or max value of the coressponding domain
     type to the partial search key value */

  num_dbvals = dsize - ssize;
  assert (num_dbvals > 0);

  if (num_dbvals <= 0)
    {
      assert (false);		/* is impossible */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_GENERIC_ERROR, 1, "");
      fprintf (stderr, "Error: btree_coerce_idxkey (num_dbval %d)\n",
	       num_dbvals);
      return ER_GENERIC_ERROR;
    }

#if 1				/* safe code */
  assert (num_term <= indexp->n_atts);
  if (num_term > indexp->n_atts)
    {
      assert (false);		/* is impossible */
      return ER_FAILED;
    }
#endif

  /* get the last domain element of partial-key */
  i = MAX (0, num_term - 1);
  part_key_desc = indexp->asc_desc[i];

  for (i = ssize; i < indexp->n_atts && err == NO_ERROR; i++)
    {
      /* server doesn't treat DB_TYPE_OBJECT, so that convert it to
         DB_TYPE_OID */
      dp_type = indexp->atts[i]->type;
      if (dp_type == DB_TYPE_OBJECT)
	{
	  dp_type = DB_TYPE_OID;
	}

      assert (tp_valid_indextype (dp_type));

      dp = indexp->atts[i]->domain;
      assert (dp != NULL);
      assert (dp_type == dp->type->id);

      minmax = key_minmax;	/* init */
      if (minmax == BTREE_COERCE_KEY_WITH_MIN_VALUE)
	{
	  if (!part_key_desc)
	    {			/* CASE 1, 2 */
	      if (indexp->asc_desc[i] == 0)
		{		/* CASE 1 */
		  ;		/* nop */
		}
	      else
		{		/* CASE 2 */
		  minmax = BTREE_COERCE_KEY_WITH_MAX_VALUE;
		}
	    }
	  else
	    {			/* CASE 3, 4 */
	      if (indexp->asc_desc[i] == 0)
		{		/* CASE 3 */
		  minmax = BTREE_COERCE_KEY_WITH_MAX_VALUE;
		}
	      else
		{		/* CASE 4 */
		  ;		/* nop */
		}
	    }
	}
      else if (minmax == BTREE_COERCE_KEY_WITH_MAX_VALUE)
	{
	  if (!part_key_desc)
	    {			/* CASE 1, 2 */
	      if (indexp->asc_desc[i] == 0)
		{		/* CASE 1 */
		  ;		/* nop */
		}
	      else
		{		/* CASE 2 */
		  minmax = BTREE_COERCE_KEY_WITH_MIN_VALUE;
		}
	    }
	  else
	    {			/* CASE 3, 4 */
	      if (indexp->asc_desc[i] == 0)
		{		/* CASE 3 */
		  minmax = BTREE_COERCE_KEY_WITH_MIN_VALUE;
		}
	      else
		{		/* CASE 4 */
		  ;		/* nop */
		}
	    }
	}

      if (minmax == BTREE_COERCE_KEY_WITH_MIN_VALUE)
	{
	  if (i < num_term)
	    {
	      err = db_value_domain_min (&(key->vals[i]), dp_type,
					 dp->precision, dp->scale,
					 dp->collation_id);
	      assert (err == NO_ERROR);
	    }
	  else
	    {
	      err = db_value_domain_init (&(key->vals[i]), dp_type,
					  dp->precision, dp->scale);
	      assert (err == NO_ERROR);
	    }
	}
      else if (minmax == BTREE_COERCE_KEY_WITH_MAX_VALUE)
	{
	  err = db_value_domain_max (&(key->vals[i]), dp_type,
				     dp->precision, dp->scale,
				     dp->collation_id);
	  assert (err == NO_ERROR);
	}
      else
	{
	  assert (false);
	  err = ER_FAILED;
	}

      key->size++;
    }

  if (err == NO_ERROR)
    {
      assert (key->size == dsize - 1);
      assert (i == dsize - 1);

#if 1				/* safe code */
      if (i != dsize - 1)
	{
	  assert (false);	/* is impossible */
	  return ER_FAILED;
	}
#endif

      /* is rightmost OID type */

      minmax = key_minmax;	/* init */
      if (minmax == BTREE_COERCE_KEY_WITH_MIN_VALUE)
	{
	  if (part_key_desc)
	    {
	      minmax = BTREE_COERCE_KEY_WITH_MAX_VALUE;
	    }
	}
      else if (minmax == BTREE_COERCE_KEY_WITH_MAX_VALUE)
	{
	  if (part_key_desc)
	    {
	      minmax = BTREE_COERCE_KEY_WITH_MIN_VALUE;
	    }
	}

      if (minmax == BTREE_COERCE_KEY_WITH_MIN_VALUE)
	{
	  err = db_value_domain_init (&(key->vals[i]), DB_TYPE_OID,
				      DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	  assert (err == NO_ERROR);
	}
      else if (minmax == BTREE_COERCE_KEY_WITH_MAX_VALUE)
	{
	  err = db_value_domain_max (&(key->vals[i]), DB_TYPE_OID,
				     DB_DEFAULT_PRECISION,
				     DB_DEFAULT_SCALE, LANG_COERCIBLE_COLL);
	  assert (err == NO_ERROR);
	}
      else
	{
	  assert (false);
	  err = ER_FAILED;
	}

      key->size++;
    }

  if (err != NO_ERROR)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
    }

  return err;
}

/*
 * btree_initialize_bts () -
 *   return: NO_ERROR
 *   bts(in): pointer to B+-tree scan structure
 *   key_val_range(in): the range of key range
 *   filter(in): key filter
 *
 * Note: Initialize a new B+-tree scan structure for an index scan.
 */
static int
btree_initialize_bts (UNUSED_ARG THREAD_ENTRY * thread_p, BTREE_SCAN * bts,
		      KEY_VAL_RANGE * key_val_range, FILTER_INFO * filter)
{
#if !defined(NDEBUG)
  BTID_INT *btid = NULL;
//  OR_INDEX *indexp = NULL;
#endif
  int ret = NO_ERROR;

#if !defined(NDEBUG)
  assert (bts != NULL);
  btid = &(bts->btid_int);

  assert (!OID_ISNULL (&(btid->cls_oid)));
  assert (btid->classrepr != NULL);
  assert (btid->classrepr_cache_idx != -1);
  assert (btid->indx_id != -1);

  assert (!BTREE_INVALID_INDEX_ID (btid->sys_btid));

//  indexp = &(btid->classrepr->indexes[btid->indx_id]);
#endif

  /* initialize page related fields */
  /* previous leaf page, current leaf page, overflow page */
  BTREE_INIT_SCAN (bts);

  /* initialize current key related fields */
  DB_IDXKEY_MAKE_NULL (&(bts->cur_key));
  bts->clear_cur_key = false;

  /* initialize the key range with given information */
  /*
   * Set up the keys and make sure that they have the proper domain
   * (by coercing, if necessary). Open-ended searches will have one or
   * both of lower_key or upper_key set to NULL so that we no longer have to do
   * DB_IS_NULL () tests on them.
   */
  /* to fix multi-column index NULL problem */
  bts->key_val = key_val_range;

  /* initialize key fileter */
  bts->key_filter = filter;	/* valid pointer or NULL */

  bts->read_keys = 0;
  bts->qualified_keys = 0;

  assert (ret == NO_ERROR);

  return ret;

#if 0
exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
#endif
}

/*
 * btree_find_next_record () -
 *   return: NO_ERROR
 *   bts(in):
 *   stat_info(in):
 *
 * Note: This functions finds the next index record(or slot).
 * Then, it adjusts the slot_id and oid_pos information
 * about the oid-set contained in the found index slot.
 */
int
btree_find_next_record (THREAD_ENTRY * thread_p, BTREE_SCAN * bts,
			int direction, BTREE_STATS * stat_info)
{
  BTREE_NODE_HEADER node_header;
  int key_cnt, free_space;
  PAGE_PTR temp_page = NULL;
  int ret = NO_ERROR;

  assert (stat_info == NULL || direction == 0);

  /* if direction, we are in the descending index mode. */
  if (direction)
    {
      /*
       * Assumptions : last accessed leaf page is fixed.
       *    - bts->C_page : NOT NULL
       *    - bts->P_page : NULL or NOT NULL
       */

      /* "previous" in this case means the actual next leaf */

      /* unfix the previous leaf page if it is fixed. */
      if (bts->P_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, bts->P_page);
	  bts->P_vpid.pageid = NULL_PAGEID;
	}

      /*
       * If the previous index record exists in the current leaf page,
       * the previous index record(slot) and OID position can be identified
       * easily.
       */
      if (bts->slot_id > 1)
	{
	  bts->slot_id -= 1;

	  goto end;		/* OK */
	}
      bts->P_vpid = bts->C_vpid;
      bts->P_page = bts->C_page;

      if (btree_read_node_header (NULL, bts->C_page, &node_header) !=
	  NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      bts->C_page = NULL;

    again_desc:

      /* move in the previous page */
      /* fix the previous leaf page and set slot_id and oid_pos if exists. */
      VPID_COPY (&bts->C_vpid, &node_header.prev_vpid);

      if (bts->C_vpid.pageid != NULL_PAGEID)
	{
	  bts->C_page =
	    btree_pgbuf_fix (thread_p, &(bts->btid_int.sys_btid->vfid),
			     &bts->C_vpid, PGBUF_LATCH_READ,
			     PGBUF_CONDITIONAL_LATCH, PAGE_BTREE);
	  if (bts->C_page == NULL)
	    {
	      if (er_errid () == NO_ERROR)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_DESC_ISCAN_ABORTED, 3,
			  bts->btid_int.sys_btid->vfid.volid,
			  bts->btid_int.sys_btid->vfid.fileid,
			  bts->btid_int.sys_btid->root_pageid);
		}
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      if (temp_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, temp_page);
	}

      /* check if the current leaf page has valid slots */
      if (bts->C_page != NULL)
	{
#if 1
	  assert (stat_info == NULL);
#else
	  /* keep leaf page's stat */
	  if (stat_info != NULL)
	    {
	      stat_info->leafs++;

	      free_space =
		spage_max_space_for_new_record (thread_p, bts->C_page);
	      stat_info->tot_free_space += free_space;
	    }
#endif

	  if (btree_read_node_header (NULL, bts->C_page, &node_header) !=
	      NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  key_cnt = node_header.key_cnt;
	  assert_release (key_cnt >= 0);
	  assert (node_header.node_level == 1);

	  if (key_cnt <= 0)
	    {			/* empty page */
	      temp_page = bts->C_page;
	      bts->C_page = NULL;
	      goto again_desc;
	    }

	  bts->slot_id = key_cnt;
	}
      else
	{
	  /* if the page doesn't have a previous one, the slot id remains
	   * on the first key (in concordance with the normal asc case when
	   * when no next page found, the slot id remains the last key_cnt)
	   */
	  bts->slot_id = 1;
	}

      /* unfix the previous leaf page if it is fixed and a valid C_vpid */
      if (bts->P_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, bts->P_page);
	  /* do not clear bts->P_vpid for UNCONDITIONAL lock request handling
	   */
	}

      goto end;
    }

  /*
   * Assumptions : last accessed leaf page is fixed.
   *    - bts->C_page != NULL
   *    - bts->P_page == NULL
   */

  /* unfix the previous leaf page if it is fixed. */
  if (bts->P_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, bts->P_page);
      bts->P_vpid.pageid = NULL_PAGEID;
    }

  if (bts->C_page == NULL)
    {
      return ER_FAILED;
    }

  /* get header information (key_cnt) from the current leaf page */
  if (btree_read_node_header (NULL, bts->C_page, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);
  assert (node_header.node_level == 1);

  /*
   * If the next index record exists in the current leaf page,
   * the next index record(slot) and OID position can be identified easily.
   */
  if (bts->slot_id < key_cnt)
    {
      bts->slot_id += 1;
      goto end;			/* OK */
    }

  /*
   * bts->slot_id >= key_cnt
   * The next index record exists in the next leaf page.
   */
  bts->P_vpid = bts->C_vpid;
  bts->P_page = bts->C_page;
  bts->C_page = NULL;

again:

  /* fix the next leaf page and set slot_id and oid_pos if it exists. */
  VPID_COPY (&bts->C_vpid, &node_header.next_vpid);
  if (bts->C_vpid.pageid != NULL_PAGEID)
    {
      bts->C_page =
	btree_pgbuf_fix (thread_p, &(bts->btid_int.sys_btid->vfid),
			 &bts->C_vpid, PGBUF_LATCH_READ,
			 PGBUF_UNCONDITIONAL_LATCH, PAGE_BTREE);
      if (bts->C_page == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      bts->slot_id = 1;

      /* unfix the previous leaf page if it is fixed. */
      if (bts->P_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, bts->P_page);
	  /* do not clear bts->P_vpid for UNCONDITIONAL lock request handling */
	}
    }

  /* If bts->C_vpid.pageid == NULL_PAGEID, then bts->C_page == NULL. */

  if (temp_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, temp_page);
    }

  /* check if the current leaf page has valid slots */
  if (bts->C_page != NULL)
    {
      /* keep leaf page's stat */
      if (stat_info != NULL)
	{
	  stat_info->leafs++;

	  free_space = spage_max_space_for_new_record (thread_p, bts->C_page);
	  stat_info->tot_free_space += free_space;
	}

      if (btree_read_node_header (NULL, bts->C_page, &node_header) !=
	  NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      key_cnt = node_header.key_cnt;
      assert_release (key_cnt >= 0);
      assert (node_header.node_level == 1);

      if (key_cnt <= 0)
	{			/* empty page */
	  temp_page = bts->C_page;
	  bts->C_page = NULL;
	  goto again;
	}
    }

end:

  return ret;

exit_on_error:

  if (temp_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, temp_page);
    }

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_prepare_first_search () - Prepare for the first index scan
 *   return: NO_ERROR
 *   bts(in): pointer to B+-tree scan structure
 *
 * Note: This function finds the first oid-set to be scanned.
 * This function is invoked in the first index scan.
 * Then, it searches down the B+-tree, fixes the needed index pages,
 * and sets the slot_id and oid_pos information of the first index scan.
 */
static int
btree_prepare_first_search (THREAD_ENTRY * thread_p, BTREE_SCAN * bts)
{
  bool found;
  VPID root_vpid;
  int key_cnt;
  BTREE_NODE_HEADER node_header;
  int ret = NO_ERROR;

  assert (bts != NULL);
  assert (bts->btid_int.sys_btid != NULL);

  /* search down the tree to find the first oidset */
  /*
   * Following information must be gotten.
   * bts->C_vpid, bts->C_page, bts->slot_id, bts->oid_pos
   */

  /*
   * If the key range does not have a lower bound key value,
   * the first key of the index is used as the lower bound key value.
   */
  if (DB_IDXKEY_IS_NULL (&(bts->key_val->lower_key)))
    {				/* The key range has no bottom */
      assert (bts->key_val->range == INF_INF);

      root_vpid.volid = bts->btid_int.sys_btid->vfid.volid;
      root_vpid.pageid = bts->btid_int.sys_btid->root_pageid;

      ret = btree_find_lower_bound_leaf (thread_p, &root_vpid, bts, NULL);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      return NO_ERROR;		/* OK */
    }

  /*
   * bts->key_range.lower_key != NULL
   * Find out and fix the first leaf page
   * that contains the given lower bound key value.
   */
  bts->C_page = btree_locate_key (thread_p, &bts->btid_int,
				  &(bts->key_val->lower_key),
				  &bts->C_vpid, &bts->slot_id, &found);
  if (bts->C_page == NULL)
    {
      assert (bts->slot_id == NULL_SLOTID);

      GOTO_EXIT_ON_ERROR;
    }
  assert (found == false);

  if (bts->use_desc_index)
    {
      bts->slot_id--;

      if (bts->slot_id == 0)
	{
	  /*
	   * The lower bound key does not exist in the current leaf page.
	   * Therefore, get the last slot of the previous leaf page.
	   */
	  ret =
	    btree_find_next_record (thread_p, bts, bts->use_desc_index, NULL);
	  if (ret != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  /* get header information (key_cnt) */
  if (bts->C_page == NULL)
    {
      return ret;
    }

  if (btree_read_node_header (&bts->btid_int, bts->C_page, &node_header) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);
  assert (node_header.node_level == 1);

  if (bts->slot_id > key_cnt)
    {
      /*
       * The lower bound key does not exist in the current leaf page.
       * Therefore, get the first slot of the next leaf page.
       */
      ret = btree_find_next_record (thread_p, bts, bts->use_desc_index, NULL);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_prepare_next_search () - Prepare for the next index scan
 *   return: NO_ERROR
 *   bts(in): pointer to B+-tree scan structure
 *
 * Note: This function finds the next oid-set to be scaned.
 * This function is invoked by the next index scan.
 * Then it fixes the needed index pages, and sets
 * the slot_id and oid_pos information of the next index scan.
 */
static int
btree_prepare_next_search (THREAD_ENTRY * thread_p, BTREE_SCAN * bts)
{
  BTREE_NODE_HEADER node_header;
  int key_cnt;
  bool found;
  int ret = NO_ERROR;

  /*
   * Assumptions :
   * 1. bts->C_vpid.pageid != NULL_PAGEID
   * 2. bts->O_vpid.pageid is NULL_PAGEID or not NULL_PAGEID.
   * 3. bts->P_vpid.pageid == NULL_PAGEID
   * 4. bts->slot_id indicates the last accessed slot
   * 5. 1 < bts->oid_pos <= (last oid position + 1)
   */

  /* fix the current leaf page */
  bts->C_page = pgbuf_fix_without_validation (thread_p, &bts->C_vpid,
					      OLD_PAGE, PGBUF_LATCH_READ,
					      PGBUF_UNCONDITIONAL_LATCH,
					      PAGE_BTREE);
  if (bts->C_page == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* check if the current leaf page has been changed */
  if (!LSA_EQ (&bts->cur_leaf_lsa, pgbuf_get_lsa (bts->C_page)))
    {
      /*
       * The current leaf page has been changed.
       * unfix the current leaf page
       */
      pgbuf_unfix_and_init (thread_p, bts->C_page);

      /* find out the last accessed index record */
      bts->C_page =
	btree_locate_key (thread_p, &bts->btid_int, &bts->cur_key,
			  &bts->C_vpid, &bts->slot_id, &found);
      if (bts->C_page == NULL)
	{
	  assert (bts->slot_id == NULL_SLOTID);

	  GOTO_EXIT_ON_ERROR;
	}

      if (!found)
	{
	  /*
	   * bts->cur_key might be deleted.
	   * get header information (key_cnt)
	   */
	  if (btree_read_node_header
	      (&bts->btid_int, bts->C_page, &node_header) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  key_cnt = node_header.key_cnt;
	  assert_release (key_cnt >= 0);
	  assert (node_header.node_level == 1);

	  if (bts->slot_id > key_cnt)
	    {
	      ret = btree_find_next_record (thread_p, bts,
					    bts->use_desc_index, NULL);
	      if (ret != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	    }
	  /* In case of bts->slot_id <= key_cnt, everything is OK. */
	}
      /* if found, everything is OK. */
    }

  /*
   * If the current leaf page has not been changed,
   * bts->slot_id and bts->oid_pos are still valid.
   */

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_apply_key_range_and_filter () - Apply key range and key filter condition
 *   return: NO_ERROR
 *   bts(in)	: pointer to B+-tree scan structure
 *   is_key_range_satisfied(out): true, or false
 *   is_key_filter_satisfied(out): true, or false
 *
 * Note: This function applies key range condition and key filter condition
 * to the current key value saved in B+-tree scan structure.
 * The results of the evaluation of the given conditions are
 * returned throught key_range_satisfied and key_filter_satisfied.
 */
static int
btree_apply_key_range_and_filter (THREAD_ENTRY * thread_p, BTREE_SCAN * bts,
				  bool * is_key_range_satisfied,
				  bool * is_key_filter_satisfied)
{
  int c;			/* comparison result */
  DB_LOGICAL ev_res;		/* evaluation result */
  int ret = NO_ERROR;

  *is_key_range_satisfied = *is_key_filter_satisfied = false;

  /* Key Range Checking */
  if (DB_IDXKEY_IS_NULL (&(bts->key_val->upper_key)))
    {
      assert (bts->key_val->range == INF_INF);

      c = 1;			/* DB_GT */
    }
  else
    {
      c = btree_compare_key (thread_p, &(bts->btid_int),
			     &(bts->key_val->upper_key), &(bts->cur_key),
			     NULL);
      if (c == DB_UNK)
	{
	  /* error should have been set */
	  assert (er_errid () == ER_TP_CANT_COERCE);

	  GOTO_EXIT_ON_ERROR;
	}

      /* when using descending index the comparison should be changed again */
      if (bts->use_desc_index)
	{
	  c = -c;
	}
    }

  if (c == 0)
    {				/* is impossible case; last is OID, should be different */
      char index_name_on_table[LINE_MAX];

      /* init */
      strcpy (index_name_on_table, "*UNKNOWN-INDEX*");

      (void) btree_get_indexname_on_table (thread_p, &(bts->btid_int),
					   index_name_on_table, LINE_MAX);

      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_PAGE_CORRUPTED,
	      1, index_name_on_table);
      assert (false);

      GOTO_EXIT_ON_ERROR;
    }

  if (c < 0)			/* DB_LT */
    {
      *is_key_range_satisfied = false;
    }
#if 0
  else if (c == 0)		/* DB_EQ */
    {
      if (bts->key_val->range == GT_LE
	  || bts->key_val->range == GE_LE || bts->key_val->range == INF_LE)
	{
	  *is_key_range_satisfied = true;
	}
      else
	{
	  *is_key_range_satisfied = false;
	}
    }
#endif
  else				/* DB_GT */
    {
      *is_key_range_satisfied = true;
    }

  if (*is_key_range_satisfied)
    {
      assert (!DB_IDXKEY_IS_NULL (&(bts->cur_key)));

#if !defined(NDEBUG)
      if (bts->key_val->num_index_term >= 1)
	{
	  DB_VALUE *ep;		/* element ptr */

	  /* peek the last element from key range elements */
	  assert (bts->cur_key.size > bts->key_val->num_index_term);
	  ep = &(bts->cur_key.vals[bts->key_val->num_index_term - 1]);

	  assert (!DB_IS_NULL (ep));
	}
#endif

      /*
       * Only in case that key_range_satisfied is true,
       * the key filter can be applied to the current key value.
       */
      *is_key_filter_satisfied = true;
      if (bts->key_filter && bts->key_filter->scan_pred)
	{
	  ev_res =
	    eval_key_filter (thread_p, &(bts->cur_key), bts->key_filter);
	  if (ev_res != V_TRUE)
	    {
	      *is_key_filter_satisfied = false;
	    }

	  if (ev_res == V_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      /* check shard groupid
       */
      if (*is_key_filter_satisfied)
	{
	  OID oid;

	  if (btree_get_oid_from_key
	      (thread_p, &(bts->btid_int), &(bts->cur_key), &oid) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  assert (!OID_ISNULL (&oid));
	  assert (oid.groupid >= GLOBAL_GROUPID);

	  /* filter-out shard table's OID
	   */
	  if (oid.groupid > GLOBAL_GROUPID	/* is shard table's OID */
	      && !SHARD_GROUP_OWN (thread_p, oid.groupid))
	    {
	      *is_key_filter_satisfied = false;
	    }
	}
    }

#if 0
end:
#endif
  assert ((*is_key_range_satisfied == false
	   && *is_key_filter_satisfied == false)
	  || (*is_key_range_satisfied == true
	      && *is_key_filter_satisfied == false)
	  || (*is_key_range_satisfied == true
	      && *is_key_filter_satisfied == true));

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_attrinfo_read_dbvalues () -
 *      Find db_values of desired attributes of given key
 *
 *   curr_key(in): the current key
 *   classrepr(in):
 *   indx_id(in):
 *   attr_info(in/out): The attribute information structure which describe the
 *                      desired attributes
 *
 * Note: Find DB_VALUES of desired attributes of given key.
 * The attr_info structure must have already been initialized
 * with the desired attributes.
 */
int
btree_attrinfo_read_dbvalues (UNUSED_ARG THREAD_ENTRY * thread_p,
			      const DB_IDXKEY * curr_key,
			      OR_CLASSREP * classrepr, int indx_id,
			      HEAP_CACHE_ATTRINFO * attr_info)
{
  int i, j, error = NO_ERROR;
  HEAP_ATTRVALUE *attr_value;
  bool found;

  if (curr_key == NULL || classrepr == NULL || indx_id < 0
      || attr_info == NULL)
    {
      return ER_FAILED;
    }

  attr_value = attr_info->values;
  for (i = 0; i < attr_info->num_values; i++)
    {
      found = false;
      for (j = 0; j < classrepr->indexes[indx_id].n_atts; j++)
	{
	  if (attr_value->attrid == classrepr->indexes[indx_id].atts[j]->id)
	    {
	      found = true;
	      break;
	    }
	}

      if (found == false)
	{
	  error = ER_FAILED;
	  goto error;
	}

      if (pr_clear_value (&(attr_value->dbvalue)) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto error;
	}

      /* peek value */
      assert (curr_key->size > j);
      attr_value->dbvalue = curr_key->vals[j];
      attr_value->dbvalue.need_clear = false;

      if (pr_is_string_type (DB_VALUE_DOMAIN_TYPE (&(attr_value->dbvalue))))
	{
	  int precision = DB_VALUE_PRECISION (&(attr_value->dbvalue));
	  int string_length = DB_GET_STRING_LENGTH (&(attr_value->dbvalue));

	  if (precision == TP_FLOATING_PRECISION_VALUE)
	    {
	      assert (false);	/* TODO - trace */
	      precision = DB_MAX_STRING_LENGTH;
	    }

	  assert (string_length <= precision);
	}

      attr_value->state = HEAP_WRITTEN_ATTRVALUE;
      attr_value++;
    }

  return NO_ERROR;

error:

  attr_value = attr_info->values;
  for (i = 0; i < attr_info->num_values; i++)
    {
      attr_value->state = HEAP_UNINIT_ATTRVALUE;
    }

  return error;
}

/*
 * btree_dump_curr_key () -
 *      Dump the current key
 *
 *   iscan_id(in): index scan id
 */
static int
btree_dump_curr_key (THREAD_ENTRY * thread_p, INDX_SCAN_ID * iscan_id)
{
  BTREE_SCAN *bts = NULL;	/* pointer to B+-tree scan structure */
  HEAP_CACHE_ATTRINFO *attr_info;
  REGU_VARIABLE_LIST regu_list;
  OID oid;
  int error;

  if (iscan_id == NULL
      || iscan_id->indx_cov.list_id == NULL
      || iscan_id->indx_cov.val_descr == NULL
      || iscan_id->indx_cov.output_val_list == NULL
      || iscan_id->indx_cov.tplrec == NULL)
    {
      return ER_FAILED;
    }

  /* pointer to index scan info. structure */
  bts = &(iscan_id->bt_scan);

  assert (iscan_id->rest_attrs.num_attrs > 0);
  assert (iscan_id->pred_attrs.num_attrs == 0);	/* TODO - trace */

  if (iscan_id->rest_attrs.num_attrs > 0)
    {
      /* normal index scan or join index scan */
      attr_info = iscan_id->rest_attrs.attr_cache;
      regu_list = iscan_id->rest_regu_list;
      assert_release (attr_info != NULL);
      assert_release (regu_list != NULL);
    }
  else if (iscan_id->pred_attrs.num_attrs > 0)
    {
      /* rest_attrs.num_attrs == 0 if index scan term is
       * join index scan with always-true condition.
       * example: SELECT ... FROM X inner join Y on 1 = 1;
       */
      attr_info = iscan_id->pred_attrs.attr_cache;
      regu_list = iscan_id->scan_pred.regu_list;
      assert_release (attr_info != NULL);
      assert_release (regu_list != NULL);
    }
  else
    {
      assert_release (false);
      attr_info = NULL;
      regu_list = NULL;
    }

  error =
    btree_attrinfo_read_dbvalues (thread_p, &(bts->cur_key),
				  bts->btid_int.classrepr,
				  bts->btid_int.indx_id, attr_info);
  if (error != NO_ERROR)
    {
      return error;
    }

  error =
    btree_get_oid_from_key (thread_p, &(bts->btid_int), &bts->cur_key, &oid);
  if (error != NO_ERROR)
    {
      return error;
    }

  error =
    fetch_val_list (thread_p, regu_list, iscan_id->indx_cov.val_descr, &oid,
		    NULL, NULL, PEEK);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = qexec_insert_tuple_into_list (thread_p, iscan_id->indx_cov.list_id,
					iscan_id->indx_cov.output_val_list,
					iscan_id->indx_cov.val_descr,
					iscan_id->indx_cov.tplrec);
  if (error != NO_ERROR)
    {
      return error;
    }

  return NO_ERROR;
}

/*
 * btree_find_min_or_max_key () -
 *   return: NO_ERROR
 *   class_oid(in):
 *   btid(in):
 *   top_vpid(in):
 *   idxkey(out):
 *   find_min_key(in):
 */
int
btree_find_min_or_max_key (THREAD_ENTRY * thread_p, OID * class_oid,
			   BTID * btid, const VPID * top_vpid,
			   DB_IDXKEY * idxkey, int find_min_key)
{
  bool clear_key = false;
  INDX_SCAN_ID index_scan_id;
  BTREE_SCAN *BTS;
  OR_INDEX *indexp = NULL;
  /* Unique btree can have at most 1 OID for a key */
  OID temp_oid[2];
  int ret = NO_ERROR;
  RECDES rec = RECDES_INITIALIZER;

  assert (class_oid != NULL);
  assert (btid != NULL);
  assert (top_vpid != NULL);

  if (idxkey == NULL)
    {
      return NO_ERROR;
    }

  assert (DB_IDXKEY_IS_NULL (idxkey));
  DB_IDXKEY_MAKE_NULL (idxkey);

  BTS = &(index_scan_id.bt_scan);

  /* index scan info */
  BTREE_INIT_SCAN (BTS);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&(BTS->btid_int));

  scan_init_index_scan (&index_scan_id, temp_oid, sizeof (temp_oid));

  BTS->btid_int.sys_btid = btid;

  /* get class representation of the index */
  COPY_OID (&(BTS->btid_int.cls_oid), class_oid);
  BTS->btid_int.classrepr =
    heap_classrepr_get (thread_p, &(BTS->btid_int.cls_oid), NULL, 0,
			&(BTS->btid_int.classrepr_cache_idx), true);
  if (BTS->btid_int.classrepr == NULL)
    {
      assert (false);
      ret = ER_FAILED;
      GOTO_EXIT_ON_ERROR;
    }

  /* get the index ID which corresponds to the BTID */
  BTS->btid_int.indx_id =
    heap_classrepr_find_index_id (BTS->btid_int.classrepr, btid);
  if (BTS->btid_int.indx_id < 0)
    {
      assert (false);
      ret = ER_FAILED;
      GOTO_EXIT_ON_ERROR;
    }

  assert (BTS->btid_int.classrepr != NULL);
  assert (BTS->btid_int.classrepr_cache_idx != -1);
  assert (BTS->btid_int.indx_id != -1);

  indexp = &(BTS->btid_int.classrepr->indexes[BTS->btid_int.indx_id]);

#if 0
  /* single-column index */
  assert (indexp->n_atts == 1);
#endif

  /*
   * in case of desc domain index,
   * we have to find the min/max key in opposite order.
   */
  if (indexp->asc_desc[0])
    {
      find_min_key = !find_min_key;
    }

  if (find_min_key)
    {
      BTS->use_desc_index = 0;
    }
  else
    {
      BTS->use_desc_index = 1;
    }

  ret = btree_find_lower_bound_leaf (thread_p, top_vpid, BTS, NULL);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (!BTREE_END_OF_SCAN (BTS))
    {
      if (spage_get_record (BTS->C_page, BTS->slot_id, &rec, PEEK) !=
	  S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      ret =
	btree_read_record (thread_p, &BTS->btid_int, &rec, idxkey,
			   NULL, BTREE_LEAF_NODE, &clear_key, PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (DB_IDXKEY_IS_NULL (idxkey))
	{
	  assert (false);	/* is impossible */
	  GOTO_EXIT_ON_ERROR;
	}
    }

  assert (BTS->btid_int.classrepr != NULL);

end:

  if (BTS->P_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, BTS->P_page);
    }

  if (BTS->C_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, BTS->C_page);
    }

#if 1				/* TODO - */
  if (BTS->btid_int.classrepr)
    {
      assert (BTS->btid_int.classrepr_cache_idx != -1);
      assert (BTS->btid_int.indx_id != -1);

      (void) heap_classrepr_free (BTS->btid_int.classrepr,
				  &(BTS->btid_int.classrepr_cache_idx));
      assert (BTS->btid_int.classrepr_cache_idx == -1);

      BTS->btid_int.classrepr = NULL;
//          BTS->btid_int.classrepr_cache_idx = -1;
//          BTS->btid_int.indx_id = -1;
    }
#endif

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * Recovery functions
 */

/*
 * btree_rv_util_save_page_records () - Save a set of page records
 *   return: int
 *   page_ptr(in): Page Pointer
 *   first_slotid(in): First Slot identifier to be saved
 *   rec_cnt(in): Number of slots to be saved
 *   ins_slotid(in): First Slot identifier to reinsert set of records
 *   data(out): Data area where the records will be stored
 *             (Enough space(DB_PAGESIZE) must have been allocated by caller
 *   data_len(in): Data area buffer length
 *   length(out): Effective length of the data area after save is completed
 *
 * Note: Copy the set of records to designated data area.
 *
 * Note: This is a UTILITY routine, but not an actual recovery routine
 */
int
btree_rv_util_save_page_records (THREAD_ENTRY * thread_p, BTID_INT * btid,
				 PAGE_PTR page_ptr,
				 INT16 first_slotid,
				 int rec_cnt, INT16 ins_slotid,
				 char *data, const int data_len, int *length)
{
  RECDES rec = RECDES_INITIALIZER;
  int i, offset, wasted;
  char *datap;

  /* Assertions */
  assert (btid != NULL);
  assert (btid->classrepr != NULL);
  assert (btid->classrepr_cache_idx != -1);
  assert (btid->indx_id != -1);

  assert (data_len == IO_MAX_PAGE_SIZE);

  *length = 0;
  datap = (char *) data + sizeof (RECSET_HEADER);
  offset = sizeof (RECSET_HEADER);
  wasted = DB_WASTED_ALIGN (offset, BTREE_MAX_ALIGN);
  datap += wasted;
  offset += wasted;

  for (i = 0; i < rec_cnt; i++)
    {
      if (spage_get_record (page_ptr, first_slotid + i, &rec, PEEK)
	  != S_SUCCESS)
	{
	  break;
	}

      if (offset + 2 > data_len)
	{
	  assert (false);
	  break;
	}
      *(INT16 *) datap = rec.length;
      datap += 2;
      offset += 2;

      if (offset + 2 > data_len)
	{
	  assert (false);
	  break;
	}
      *(INT16 *) datap = rec.type;
      datap += 2;
      offset += 2;

      if (offset + rec.length > data_len)
	{
	  assert (false);
	  break;
	}
      memcpy (datap, rec.data, rec.length);
      datap += rec.length;
      offset += rec.length;

      wasted = DB_WASTED_ALIGN (offset, BTREE_MAX_ALIGN);
      if (offset + wasted > data_len)
	{
	  assert (false);
	  break;
	}
      datap += wasted;
      offset += wasted;
    }

  if (i < rec_cnt)
    {				/* TODO - index crash */
      char index_name_on_table[LINE_MAX];

      /* init */
      strcpy (index_name_on_table, "*UNKNOWN-INDEX*");

      (void) btree_get_indexname_on_table (thread_p, btid,
					   index_name_on_table, LINE_MAX);

      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_PAGE_CORRUPTED,
	      1, index_name_on_table);
      assert (false);

      return ER_FAILED;
    }

  datap = data;
  ((RECSET_HEADER *) datap)->rec_cnt = rec_cnt;
  ((RECSET_HEADER *) datap)->first_slotid = ins_slotid;
  *length = offset;

  return NO_ERROR;
}

/*
 * btree_rv_save_keyval () - Save a < key, value > pair for logical log purposes
 *   return: NO_ERROR
 *   btid(in): B+tree index identifier
 *   disk(in): Key to be saved
 *   data(in): Data area where the above fields will be stored
 *             (Note: The caller should FREE the allocated area.)
 *   length(in): Length of the data area after save is completed
 *
 * Note: Copy the adequate key-value information to the data area and
 * return this data area.
 *
 * Note: This is a UTILITY routine, but not an actual recovery routine
 *
 * Warning: This routine assumes that the keyval is from a leaf page and
 * not a nonleaf page.  Because of this assumption, we use the
 * index domain and not the nonleaf domain to write out the key
 * value.  Currently all calls to this routine are from leaf
 * pages.  Be careful if you add a call to this routine.
 */
int
btree_rv_save_keyval (BTID_INT * btid, const DB_IDXKEY * key,
		      char *data, int *length)
{
  char *ptr;
  OR_BUF buf;
  char *bound_bits;
  int i;
  DB_TYPE type;
  int rc = NO_ERROR;

  assert (key != NULL);

#if !defined(NDEBUG)
  {
    OR_INDEX *indexp = NULL;

    assert (btid->classrepr != NULL);
    assert (btid->classrepr_cache_idx != -1);
    assert (btid->indx_id != -1);

    indexp = &(btid->classrepr->indexes[btid->indx_id]);

    assert (key->size == indexp->n_atts + 1);
  }
#endif

  *length = 0;

  ptr = or_pack_oid (data, &(btid->cls_oid));

  ptr = or_pack_btid (ptr, btid->sys_btid);

  or_init (&buf, ptr, BTREE_MAX_KEYLEN);

  assert (key->size > 1);

  bound_bits = buf.ptr;

  rc = or_advance (&buf, pr_idxkey_init_boundbits (bound_bits, key->size));
  if (rc != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; rc == NO_ERROR && i < key->size; i++)
    {
      if (DB_IS_NULL (&(key->vals[i])))
	{
	  assert (OR_MULTI_ATT_IS_UNBOUND (bound_bits, i));

	  continue;
	}

      type = DB_VALUE_DOMAIN_TYPE (&(key->vals[i]));
      assert (tp_valid_indextype (type));

      rc = (*(tp_Type_id_map[type]->index_writeval)) (&buf, &(key->vals[i]));

      OR_ENABLE_BOUND_BIT (bound_bits, i);
    }

  *length = CAST_STRLEN (buf.ptr - data);

  return rc;

exit_on_error:

  return (rc == NO_ERROR && (rc = er_errid ()) == NO_ERROR) ? ER_FAILED : rc;
}

/*
 * btree_rv_nodehdr_redo_insert () - Recover a node header insertion. used for redo
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover a node header insertion by reinserting the node header
 * for redo purposes.
 */
int
btree_rv_nodehdr_redo_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  RECDES rec = RECDES_INITIALIZER;
  int sp_success;

  assert (recv != NULL);
  assert (recv->length == sizeof (BTREE_NODE_HEADER));

  rec.type = REC_HOME;
  rec.area_size = rec.length = recv->length;
  rec.data = (char *) recv->data;
  sp_success = spage_insert_at (thread_p, recv->pgptr, HEADER, &rec);
  if (sp_success != SP_SUCCESS)
    {
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_GENERIC_ERROR, 1, "");
	}
      assert (false);
      return er_errid ();
    }
  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * btree_rv_nodehdr_undo_insert () - Recover a node header insertion. used for undo
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover a node header insertion by deletion  the node header
 * for undo purposes.
 */
int
btree_rv_nodehdr_undo_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  PGSLOTID pg_slotid;

  pg_slotid = spage_delete (thread_p, recv->pgptr, HEADER);
  if (pg_slotid == NULL_SLOTID)
    {
      assert (false);
      ;				/* TODO - avoid compile error */
    }

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);
  return NO_ERROR;
}

/*
 * btree_rv_noderec_undoredo_update () - Recover an update to a node record. used either
 *                         for undo or redo
 *   return:
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover the update to a node record
 */
int
btree_rv_noderec_undoredo_update (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  RECDES rec = RECDES_INITIALIZER;
  INT16 slotid;
  int sp_success;

  slotid = recv->offset;

  rec.type = REC_HOME;
  rec.area_size = rec.length = recv->length;
  rec.data = (char *) recv->data;

  sp_success = spage_update (thread_p, recv->pgptr, slotid, &rec);
  if (sp_success != SP_SUCCESS)
    {
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_GENERIC_ERROR, 1, "");
	}
      assert (false);
      return er_errid ();
    }

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * btree_rv_noderec_redo_insert () - Recover a node record insertion. used for redo
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover a node record insertion by reinserting the record for
 * redo purposes
 */
int
btree_rv_noderec_redo_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  RECDES rec = RECDES_INITIALIZER;
  INT16 slotid;
  int sp_success;

  slotid = recv->offset;

  rec.type = REC_HOME;
  rec.area_size = rec.length = recv->length;
  rec.data = (char *) recv->data;

  sp_success = spage_insert_at (thread_p, recv->pgptr, slotid, &rec);
  if (sp_success != SP_SUCCESS)
    {
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_GENERIC_ERROR, 1, "");
	}
      assert (false);
      return er_errid ();
    }
  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * btree_rv_noderec_undo_insert () - Recover a node record insertion. used for undo
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover a node record insertion by deleting the record for
 * undo purposes
 */
int
btree_rv_noderec_undo_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  INT16 slotid;
  PGSLOTID pg_slotid;

  slotid = recv->offset;
  pg_slotid = spage_delete_for_recovery (thread_p, recv->pgptr, slotid);
  if (pg_slotid == NULL_SLOTID)
    {
      assert (false);
      ;				/* TODO - avoid compile error */
    }

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * btree_rv_noderec_dump_slot_id () -
 *   return: int
 *   length(in): Length of Recovery Data
 *   data(in): The data being logged
 *
 * Note: Dump the slot id for the slot to be deleted for undo purposes
 */

void
btree_rv_noderec_dump_slot_id (FILE * fp, UNUSED_ARG int length, void *data)
{
  fprintf (fp, " Slot_id: %d \n", *(INT16 *) data);
}

/*
 * btree_rv_pagerec_insert () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Put a set of records to the page
 */
int
btree_rv_pagerec_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  RECDES rec = RECDES_INITIALIZER;
  const RECSET_HEADER *recset_header;
  char *datap;
  int i, offset, wasted;
  int sp_success;

  /* initialization */
  recset_header = (const RECSET_HEADER *) recv->data;

  /* insert back saved records */
  datap = (char *) recv->data + sizeof (RECSET_HEADER);
  offset = sizeof (RECSET_HEADER);
  wasted = DB_WASTED_ALIGN (offset, BTREE_MAX_ALIGN);
  datap += wasted;
  offset += wasted;
  for (i = 0; i < recset_header->rec_cnt; i++)
    {
      rec.area_size = rec.length = *(INT16 *) datap;
      datap += 2;
      offset += 2;
      rec.type = *(INT16 *) datap;
      assert (rec.type == REC_HOME);
      datap += 2;
      offset += 2;
      rec.data = datap;
      datap += rec.length;
      offset += rec.length;
      wasted = DB_WASTED_ALIGN (offset, BTREE_MAX_ALIGN);
      datap += wasted;
      offset += wasted;
      sp_success = spage_insert_at (thread_p, recv->pgptr,
				    recset_header->first_slotid + i, &rec);
      if (sp_success != SP_SUCCESS)
	{
	  if (sp_success != SP_ERROR)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_GENERIC_ERROR, 1, "");
	    }
	  assert (false);
	  goto error;
	}			/* if */
    }				/* for */

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;

error:

  return er_errid ();

}

/*
 * btree_rv_pagerec_delete () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Delete a set of records from the page for undo or redo purpose
 */
int
btree_rv_pagerec_delete (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  const RECSET_HEADER *recset_header;
  int i;

  recset_header = (const RECSET_HEADER *) recv->data;

  /* delete all specified records from the page */
  for (i = 0; i < recset_header->rec_cnt; i++)
    {
      if (spage_delete (thread_p, recv->pgptr,
			recset_header->first_slotid)
	  != recset_header->first_slotid)
	{
	  assert (false);
	  return er_errid ();
	}
    }

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;
}

#if 1
void
btree_rv_pagerec_dump (FILE * fp, UNUSED_ARG int length, void *data)
{
  const RECSET_HEADER *recset_header;

  recset_header = (const RECSET_HEADER *) data;

  fprintf (fp, " rec_cnt: %d , first_slotid: %d \n", 
           recset_header->rec_cnt, recset_header->first_slotid);
}
#endif

/*
 * btree_rv_newpage_redo_init_helper () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Initialize a B+tree page.
 */
static int
btree_rv_newpage_redo_init_helper (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  unsigned short alignment;

  alignment = *(const unsigned short *) recv->data;
  assert (alignment == BTREE_MAX_ALIGN);

  spage_initialize (thread_p, recv->pgptr,
		    UNANCHORED_KEEP_SEQUENCE_BTREE, alignment,
		    DONT_SAFEGUARD_RVSPACE);

  return NO_ERROR;
}

int
btree_rv_newroot_redo_init (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  (void) pgbuf_set_page_ptype (thread_p, recv->pgptr, PAGE_BTREE_ROOT);

  return btree_rv_newpage_redo_init_helper (thread_p, recv);
}

int
btree_rv_newpage_redo_init (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  (void) pgbuf_set_page_ptype (thread_p, recv->pgptr, PAGE_BTREE);

  return btree_rv_newpage_redo_init_helper (thread_p, recv);
}

/*
 * btree_rv_newpage_undo_alloc () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Undo a new page allocation
 */
int
btree_rv_newpage_undo_alloc (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  const PAGEID_STRUCT *pageid_struct;
  int ret = NO_ERROR;

#if 0				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */
  assert (false);
#endif

  pageid_struct = (const PAGEID_STRUCT *) recv->data;

  ret =
    file_dealloc_page (thread_p, &pageid_struct->vfid, &pageid_struct->vpid,
		       PAGE_BTREE);
  if (ret != NO_ERROR)
    {
      assert (false);
      ;				/* TODO - avoid compile error */
    }

  return NO_ERROR;
}

/*
 * btree_rv_newpage_dump_undo_alloc () -
 *   return: int
 *   length(in): Length of Recovery Data
 *   data(in): The data being logged
 *
 * Note: Dump undo information of new page creation
 */
void
btree_rv_newpage_dump_undo_alloc (FILE * fp, UNUSED_ARG int length,
				  void *data)
{
  PAGEID_STRUCT *pageid_struct = (PAGEID_STRUCT *) data;

  fprintf (fp,
	   "Deallocating page from Volid = %d, Fileid = %d\n",
	   pageid_struct->vfid.volid, pageid_struct->vfid.fileid);
}

/*
 * btree_rv_read_keyval_info_nocopy () -
 *   return:
 *   datap(in):
 *   data_size(in):
 *   btid(in):
 *   oid(in):
 *   key(in):
 *
 * Note: read the keyval info from a recovery record.
 *
 * Warning: This assumes that the keyvalue has the index's domain and
 * not the nonleaf domain.  This should be the case since this
 * is a logical operation and not a physical one.
 */
static void
btree_rv_read_keyval_info_nocopy (THREAD_ENTRY * thread_p,
				  char *datap, int data_size,
				  BTID_INT * btid, DB_IDXKEY * key)
{
  OR_BUF buf;
  char *start = datap;
  OR_INDEX *indexp = NULL;
  char *bound_bits;
  int i;
  DB_TYPE type;
  TP_DOMAIN *dom;
  int rc = NO_ERROR;

  assert (btid->classrepr == NULL);

  /* extract class oid  */
  datap = or_unpack_oid (datap, &(btid->cls_oid));

  /* extract the stored btid, key, oid data */
  datap = or_unpack_btid (datap, btid->sys_btid);

  /* get class representation of the index */
  assert (!OID_ISNULL (&(btid->cls_oid)));
  btid->classrepr =
    heap_classrepr_get (thread_p, &(btid->cls_oid), NULL, 0,
			&(btid->classrepr_cache_idx), true);
  if (btid->classrepr == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  /* get the index ID which corresponds to the BTID */
  btid->indx_id =
    heap_classrepr_find_index_id (btid->classrepr, btid->sys_btid);
  if (btid->indx_id < 0)
    {
      assert (false);
      goto exit_on_error;
    }

  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  or_init (&buf, datap, CAST_STRLEN (data_size - (datap - start)));

  /* bitmap is always fully sized */
  key->size = indexp->n_atts + 1;

  bound_bits = buf.ptr;

  rc = or_advance (&buf, OR_MULTI_BOUND_BIT_BYTES (key->size));
  if (rc != NO_ERROR)
    {
      assert (false);
      goto exit_on_error;
    }

  /* Do not copy the string--just use the pointer.  The pr_ routines
   * for strings and sets have different semantics for length.
   */

  for (i = 0; rc == NO_ERROR && i < indexp->n_atts; i++)
    {
      if (OR_MULTI_ATT_IS_UNBOUND (bound_bits, i))
	{
	  DB_MAKE_NULL (&(key->vals[i]));
	}
      else
	{
	  type = indexp->atts[i]->type;
	  assert (tp_valid_indextype (type));

	  dom = indexp->atts[i]->domain;

	  assert (CAST_STRLEN (buf.endptr - buf.ptr) > 0);

	  rc =
	    (*(tp_Type_id_map[type]->index_readval)) (&buf, &(key->vals[i]),
						      dom->precision,
						      dom->scale,
						      dom->collation_id,
						      -1 /* TODO - */ ,
						      false /* not copy */ );
	}
    }

  /* read rightmost OID */
  if (rc == NO_ERROR)
    {
      assert (i == indexp->n_atts);

      if (OR_MULTI_ATT_IS_UNBOUND (bound_bits, i))
	{
	  assert (false);	/* is impossible */
	  DB_MAKE_NULL (&(key->vals[i]));
	}
      else
	{
	  assert (CAST_STRLEN (buf.endptr - buf.ptr) > 0);

	  rc =
	    (*(tp_Type_id_map[DB_TYPE_OID]->index_readval)) (&buf,
							     &(key->vals[i]),
							     DB_DEFAULT_PRECISION,
							     DB_DEFAULT_SCALE,
							     LANG_COERCIBLE_COLL,
							     -1 /* TODO */ ,
							     false
							     /* not copy */ );
	}
    }

  if (rc != NO_ERROR)
    {
      assert (false);
      goto exit_on_error;
    }

  assert (BTREE_IS_VALID_KEY_LEN (btree_get_key_length (key)));	/* TODO - */

  return;

exit_on_error:

  return;
}

/*
 * btree_rv_keyval_undo_insert () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Undo the insertion of a <key, val> pair to the B+tree,
 * by deleting the <key, val> pair from the tree.
 */
int
btree_rv_keyval_undo_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  BTID_INT btid;
  BTID sys_btid;
  DB_IDXKEY key;
  char *datap;
  int datasize;
  int err = NO_ERROR;

  DB_IDXKEY_MAKE_NULL (&key);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid);

  /* btid needs a place to unpack the sys_btid into.  We'll use stack space. */
  btid.sys_btid = &sys_btid;

  /* extract the stored btid, key, oid data */
  datap = (char *) recv->data;
  datasize = recv->length;
  btree_rv_read_keyval_info_nocopy (thread_p, datap, datasize, &btid, &key);

  if (btree_delete (thread_p, &btid, &key) == NULL)
    {
      err = er_errid ();
      assert (err == ER_BTREE_UNKNOWN_KEY || err == NO_ERROR);
    }

exit_on_end:

#if 1				/* TODO - */
  if (btid.classrepr)
    {
      assert (btid.classrepr_cache_idx != -1);
      assert (btid.indx_id != -1);

      (void) heap_classrepr_free (btid.classrepr,
				  &(btid.classrepr_cache_idx));
      assert (btid.classrepr_cache_idx == -1);

      btid.classrepr = NULL;
//          btid.classrepr_cache_idx = -1;
//          btid.indx_id = -1;
    }
#endif

  return err;

#if 0
exit_on_error:
#endif

  goto exit_on_end;
}

/*
 * btree_rv_keyval_undo_delete () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: undo the deletion of a <key, val> pair to the B+tree,
 * by inserting the <key, val> pair to the tree.
 */
int
btree_rv_keyval_undo_delete (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  BTID_INT btid;
  BTID sys_btid;
  DB_IDXKEY key;
  char *datap;
  int datasize;
  int err = NO_ERROR;

  DB_IDXKEY_MAKE_NULL (&key);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid);

  /* btid needs a place to unpack the sys_btid into.  We'll use stack space. */
  btid.sys_btid = &sys_btid;

  /* extract the stored btid, key, oid data */
  datap = (char *) recv->data;
  datasize = recv->length;
  btree_rv_read_keyval_info_nocopy (thread_p, datap, datasize, &btid, &key);

  if (btree_insert (thread_p, &btid, &key) == NULL)
    {
      err = er_errid ();
      assert (err == ER_BTREE_DUPLICATE_OID || err == NO_ERROR);
    }

exit_on_end:

#if 1				/* TODO - */
  if (btid.classrepr)
    {
      assert (btid.classrepr_cache_idx != -1);
      assert (btid.indx_id != -1);

      (void) heap_classrepr_free (btid.classrepr,
				  &(btid.classrepr_cache_idx));
      assert (btid.classrepr_cache_idx == -1);

      btid.classrepr = NULL;
//          btid.classrepr_cache_idx = -1;
//          btid.indx_id = -1;
    }
#endif

  return err;

#if 0
exit_on_error:
#endif

  goto exit_on_end;
}

/*
 * btree_rv_keyval_dump () -
 *   return: int
 *   length(in): Length of Recovery Data
 *   data(in): The data being logged
 *
 * Note: Dump undo information <key-value> insertion
 */
void
btree_rv_keyval_dump (FILE * fp, UNUSED_ARG int length, void *data)
{
  BTID btid;

  (void) or_unpack_btid (data, &btid);
  fprintf (fp, " BTID = { { %d , %d }, %d} \n ",
	   btid.vfid.volid, btid.vfid.fileid, btid.root_pageid);
}

/*
 * btree_rv_undoredo_copy_page () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Copy a whole page back for undo or redo purposes
 */
int
btree_rv_undoredo_copy_page (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  (void) memcpy (recv->pgptr, recv->data, DB_PAGESIZE);

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);
  return NO_ERROR;
}

/*
 * btree_rv_leafrec_redo_delete () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover a leaf record deletion for redo purposes
 */
int
btree_rv_leafrec_redo_delete (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  BTREE_NODE_HEADER node_header;
  INT16 slotid;

  slotid = recv->offset;

  /*
   * update the page header
   */

  if (btree_read_node_header (NULL, recv->pgptr, &node_header) != NO_ERROR)
    {
      assert (false);
      goto error;
    }

  /* redo the deletion of the btree slot */
  if (spage_delete (thread_p, recv->pgptr, slotid) != slotid)
    {
      assert (false);
      goto error;
    }

  /* update key_cnt */
  assert_release (node_header.key_cnt >= 1);
  node_header.key_cnt--;
  assert_release (node_header.key_cnt >= 0);

  assert (node_header.node_level == 1);

  if (btree_write_node_header (NULL, recv->pgptr, &node_header) != NO_ERROR)
    {
      assert (false);
      goto error;
    }

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;

error:

  return er_errid ();
}

/*
 * btree_rv_nop () -
 *   return: int
 *   recv(in): Recovery structure
 *
 *
 * Note: Does nothing. This routine is used for to accompany some
 * compensating redo logs which are supposed to do nothing.
 */
int
btree_rv_nop (UNUSED_ARG THREAD_ENTRY * thread_p, UNUSED_ARG LOG_RCV * recv)
{
  return NO_ERROR;
}

/*
 * btree_key_is_null () -
 *   return: Return true if DB_VALUE is a NULL multi-column
 *           key and false otherwise.
 *   key(in): Pointer to multi-column key
 *
 * Note: Check the multi-column key for a NULL value. In terms of the B-tree,
 * a NULL multi-column key is a sequence in which each element is NULL.
 */
bool
btree_key_is_null (const DB_IDXKEY * key)
{
  int i;

  assert (key != NULL);

#if 1				/* safe code */
  if (key == NULL || DB_IDXKEY_IS_NULL (key))
    {
      assert (false);
      return true;
    }
#endif

  assert (key->size > 1);

  /* skip out rightmost OID col */
  for (i = 0; i < key->size - 1; i++)
    {
      if (!DB_IS_NULL (&(key->vals[i])))
	{
	  return false;		/* found not null */
	}
    }

  return true;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * btree_key_has_null () -
 *   return: Return true if DB_VALUE is a multi-column key
 *           and has a NULL element in it and false otherwise.
 *   key(in): Pointer to multi-column key
 *
 * Note: Check the multi-column  key has a NULL element.
 */
bool
btree_key_has_null (const DB_IDXKEY * key)
{
  int i;

  assert (key != NULL);
  assert (!DB_IDXKEY_IS_NULL (key));

#if 1				/* safe code */
  if (key == NULL || DB_IDXKEY_IS_NULL (key))
    {
      assert (false);
      return true;
    }
#endif

  assert (key->size > 1);

  /* skip out rightmost OID col */
  for (i = 0; i < key->size - 1; i++)
    {
      if (DB_IS_NULL (&(key->vals[i])))
	{
	  return true;		/* found null */
	}
    }

  return false;
}
#endif

void
btree_get_indexname_on_table (THREAD_ENTRY * thread_p,
			      const BTID_INT * btid, char *buffer,
			      const int buffer_len)
{
  char *class_name = NULL;
  OR_INDEX *indexp = NULL;

  if (thread_p == NULL)
    {
      assert (false);		/* invalid input args */
      return;
    }

  if (btid == NULL || buffer == NULL || buffer_len < LINE_MAX)
    {
      assert (false);		/* invalid input args */
      return;
    }

  if (btid->classrepr == NULL || btid->classrepr_cache_idx == -1
      || btid->indx_id == -1 || btid->indx_id >= btid->classrepr->n_indexes)
    {
      assert (false);		/* invalid input args */
      return;
    }

  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  assert (!OID_ISNULL (&(btid->cls_oid)));
  assert (indexp->btname != NULL);

  if (!OID_ISNULL (&(btid->cls_oid)))
    {
      class_name = heap_get_class_name (thread_p, &(btid->cls_oid));
    }

  snprintf (buffer, buffer_len, "INDEX %s ON TABLE %s",
	    (indexp->btname) ? indexp->btname : "*UNKNOWN-INDEX*",
	    (class_name) ? class_name : "*UNKNOWN-TABLE*");

  if (class_name)
    {
      free_and_init (class_name);
    }

  return;
}

/*
 * btree_set_vpid_previous_vpid () - Sets the prev VPID of a page
 *   return: error code
 *   btid(in): BTID
 *   page_p(in):
 *   prev(in): a vpid to be set as previous for the input page
 */
int
btree_set_vpid_previous_vpid (THREAD_ENTRY * thread_p, BTID_INT * btid,
			      PAGE_PTR page_p, VPID * prev)
{
  BTREE_NODE_HEADER node_header;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  if (page_p == NULL)
    {
      return NO_ERROR;
    }

  if (btree_read_node_header (btid, page_p, &node_header) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* log the old header record for undo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, page_p, HEADER);
  log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (node_header), &node_header);

  VPID_COPY (&node_header.prev_vpid, prev);
  if (btree_write_node_header (btid, page_p, &node_header) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, page_p, HEADER);
  log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			&addr, sizeof (node_header), &node_header);

  pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

  return NO_ERROR;
}

static int
btree_check_key_cnt (BTID_INT * btid, PAGE_PTR page_p, short node_level,
		     short key_cnt)
{
  PGNSLOTS num_slots;		/* Number of allocated slots for the page */
  PGNSLOTS num_records;		/* Number of records on page */
  THREAD_ENTRY *thread_p;

  if (page_p == NULL || node_level <= 0 || key_cnt < 0)
    {
      goto exit_on_error;
    }

  num_slots = spage_number_of_slots (page_p);
  num_records = spage_number_of_records (page_p);
  if (num_slots <= 0 || num_records <= 0 || num_slots != num_records)
    {
      goto exit_on_error;
    }

  if (node_level > 1)
    {				/* non-leaf node */
      if (key_cnt + 2 == num_records)
	{
	  return NO_ERROR;
	}
    }
  else
    {				/* leaf node */
      if (key_cnt + 1 == num_records)
	{
	  return NO_ERROR;
	}
    }

exit_on_error:

/* TODO - index crash */
  thread_p = thread_get_thread_entry_info ();
  if (thread_p != NULL)
    {
      char index_name_on_table[LINE_MAX];

      /* init */
      strcpy (index_name_on_table, "*UNKNOWN-INDEX*");

      (void) btree_get_indexname_on_table (thread_p, btid,
					   index_name_on_table, LINE_MAX);

      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_PAGE_CORRUPTED,
	      1, index_name_on_table);
    }
  assert (false);

  return ER_FAILED;
}

int
btree_compare_key (UNUSED_ARG THREAD_ENTRY * thread_p, BTID_INT * btid,
		   const DB_IDXKEY * key1, const DB_IDXKEY * key2,
		   int *start_colp)
{
  DB_VALUE_COMPARE_RESULT c = DB_UNK;
  int d;			/* diff position */
  OR_INDEX *indexp = NULL;

  assert (key1 != NULL);
  assert (key2 != NULL);
  assert (key1->size == key2->size);

  d = 0;			/* init */

  c = pr_idxkey_compare (key1, key2, -1, &d);
  assert_release (c == DB_UNK || (DB_LT <= c && c <= DB_GT));

#if !defined(NDEBUG)
  if (c == DB_UNK)
    {
      assert (er_errid () == ER_TP_CANT_COERCE);
    }
#endif

  /* check iff is desc */
  if (c == DB_LT || c == DB_GT)
    {
      if (btid != NULL)
	{
	  assert (btid->classrepr != NULL);
	  assert (btid->classrepr_cache_idx != -1);
	  assert (btid->indx_id != -1);

	  indexp = &(btid->classrepr->indexes[btid->indx_id]);

	  assert (d < indexp->n_atts + 1);

	  if (d < indexp->n_atts && indexp->asc_desc[d])
	    {
	      c = ((c == DB_GT) ? DB_LT : (c == DB_LT) ? DB_GT : c);
	    }
	}
    }

  if (start_colp != NULL)
    {
      *start_colp = d;
    }

  assert_release (c == DB_UNK || (DB_LT <= c && c <= DB_GT));

  return c;
}

/*
 * btree_get_oid_from_key () -
 *   return: NO_ERROR
 *
 *   btid(in):
 *   key(in):
 *   oid(out):
 */
int
btree_get_oid_from_key (UNUSED_ARG THREAD_ENTRY * thread_p,
			UNUSED_ARG BTID_INT * btid, const DB_IDXKEY * key,
			OID * oid)
{
  const OID *op;
#if !defined(NDEBUG)
  OR_INDEX *indexp = NULL;
#endif

  assert (btid != NULL);
  assert (btid->classrepr != NULL);
  assert (btid->classrepr_cache_idx != -1);
  assert (btid->indx_id != -1);

  assert (key != NULL);
  assert (oid != NULL);

#if !defined(NDEBUG)
  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  assert (key->size == indexp->n_atts + 1);
#endif

  if (key != NULL && key->size > 1)
    {
      op = DB_GET_OID (&(key->vals[key->size - 1]));
      if (op != NULL && !OID_ISNULL (op))
	{
	  memcpy (oid, op, OR_OID_SIZE);
	  assert (!OID_ISNULL (oid));

	  return NO_ERROR;
	}
    }

  assert (false);		/* is impossible */

  return ER_FAILED;
}

/*
 * btree_range_opt_check_add_index_key () - adds a key in the array of top N
 *	      keys of multiple range search optimization structure
 *   return: error code
 *   thread_p(in):
 *   bts(in):
 *   multi_range_opt(in): multiple range search optimization structure
 *   key_added(out): if key is added or not
 */
static int
btree_range_opt_check_add_index_key (THREAD_ENTRY * thread_p,
				     BTREE_SCAN * bts,
				     MULTI_RANGE_OPT * multi_range_opt,
				     bool * key_added)
{
  DB_VALUE *new_key_value = NULL;
  int error = NO_ERROR, i = 0;

  assert (multi_range_opt->use == true);

  if (DB_IDXKEY_IS_NULL (&(bts->cur_key))
      || multi_range_opt->sort_att_idx == NULL)
    {
      return ER_FAILED;
    }

  *key_added = true;

  assert (multi_range_opt->no_attrs != 0);
  if (multi_range_opt->no_attrs == 0)
    {
      return ER_FAILED;
    }

  new_key_value =
    (DB_VALUE *) malloc (multi_range_opt->no_attrs * sizeof (DB_VALUE));
  if (new_key_value == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (DB_VALUE *) * multi_range_opt->no_attrs);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  for (i = 0; i < multi_range_opt->no_attrs; i++)
    {
      DB_MAKE_NULL (&new_key_value[i]);

      /* peek value */
      assert (bts->cur_key.size > multi_range_opt->sort_att_idx[i]);
      new_key_value[i] = bts->cur_key.vals[multi_range_opt->sort_att_idx[i]];
      new_key_value[i].need_clear = false;
    }

  if (multi_range_opt->cnt == multi_range_opt->size)
    {
      int c = 0;
      DB_VALUE comp_key_value;
      bool reject_new_elem = false;
      RANGE_OPT_ITEM *last_item = NULL;

      last_item = multi_range_opt->top_n_items[multi_range_opt->size - 1];
      assert (last_item != NULL);

      /* if all keys are equal, the new element is rejected */
      reject_new_elem = true;
      for (i = 0; i < multi_range_opt->no_attrs; i++)
	{
	  DB_MAKE_NULL (&comp_key_value);

	  /* peek value */
	  assert (last_item->index_value.size >
		  multi_range_opt->sort_att_idx[i]);
	  comp_key_value =
	    last_item->index_value.vals[multi_range_opt->sort_att_idx[i]];
	  comp_key_value.need_clear = false;

	  c = (*(multi_range_opt->sort_col_dom[i]->type->cmpval))
	    (&comp_key_value, &new_key_value[i], 1, 1,
	     multi_range_opt->sort_col_dom[i]->collation_id);
	  if (c != 0)
	    {
	      /* see if new element should be rejected or accepted and stop
	       * checking keys
	       */
	      reject_new_elem =
		(multi_range_opt->is_desc_order[i]) ? (c > 0) : (c < 0);
	      break;
	    }
	}
      if (reject_new_elem)
	{
	  /* do not add */
	  *key_added = false;

	  if (new_key_value != NULL)
	    {
	      free_and_init (new_key_value);
	    }
	  return NO_ERROR;
	}

      /* overwrite the last item with the new key and OIDs */
      db_idxkey_clear (&(last_item->index_value));
      db_idxkey_clone (&(bts->cur_key), &(last_item->index_value));
      error =
	btree_get_oid_from_key (thread_p, &(bts->btid_int), &bts->cur_key,
				&(last_item->inst_oid));
      if (error != NO_ERROR)
	{
	  goto exit;
	}
    }
  else
    {
      RANGE_OPT_ITEM *curr_item = NULL;

      /* just insert on last position available */
      assert (multi_range_opt->cnt < multi_range_opt->size);

      curr_item = (RANGE_OPT_ITEM *) malloc (sizeof (RANGE_OPT_ITEM));
      if (curr_item == NULL)
	{
	  if (new_key_value != NULL)
	    {
	      free_and_init (new_key_value);
	    }
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      DB_IDXKEY_MAKE_NULL (&(curr_item->index_value));

      multi_range_opt->top_n_items[multi_range_opt->cnt] = curr_item;

      db_idxkey_clone (&(bts->cur_key), &(curr_item->index_value));
      error =
	btree_get_oid_from_key (thread_p, &(bts->btid_int), &bts->cur_key,
				&(curr_item->inst_oid));
      if (error != NO_ERROR)
	{
	  goto exit;
	}

      multi_range_opt->cnt++;

      if (multi_range_opt->sort_col_dom == NULL)
	{
	  multi_range_opt->sort_col_dom =
	    (TP_DOMAIN **) malloc (multi_range_opt->no_attrs *
				   sizeof (TP_DOMAIN *));
	  if (multi_range_opt->sort_col_dom == NULL)
	    {
	      goto exit;
	    }

	  for (i = 0; i < multi_range_opt->no_attrs; i++)
	    {
	      multi_range_opt->sort_col_dom[i] =
		tp_domain_resolve_value (&new_key_value[i]);
	    }
	}
    }

  /* find the position for this element */
  /* if there is only one element => nothing to do */
  if (multi_range_opt->cnt > 1)
    {
      int pos = 0;

      error =
	btree_top_n_items_binary_search (multi_range_opt->top_n_items,
					 multi_range_opt->sort_att_idx,
					 multi_range_opt->sort_col_dom,
					 multi_range_opt->is_desc_order,
					 new_key_value,
					 multi_range_opt->no_attrs, 0,
					 multi_range_opt->cnt - 1, &pos);
      if (error != NO_ERROR)
	{
	  goto exit;
	}

      if (pos != multi_range_opt->cnt - 1)
	{
	  RANGE_OPT_ITEM *temp_item;
	  int mem_size =
	    (multi_range_opt->cnt - 1 - pos) * sizeof (RANGE_OPT_ITEM *);

	  /* copy last item to temp */
	  temp_item = multi_range_opt->top_n_items[multi_range_opt->cnt - 1];

	  /* move all items one position to the right in order to free the
	   * position for the new item
	   */
	  memcpy (multi_range_opt->buffer, &multi_range_opt->top_n_items[pos],
		  mem_size);
	  memcpy (&multi_range_opt->top_n_items[pos + 1],
		  multi_range_opt->buffer, mem_size);

	  /* put new item at its designated position */
	  multi_range_opt->top_n_items[pos] = temp_item;
	}
      else
	{
	  /* the new item is already in the correct position */
	}
    }

exit:
  if (new_key_value != NULL)
    {
      free_and_init (new_key_value);
    }
  return error;
}

/*
 * btree_top_n_items_binary_search () - searches for the right position for
 *				        the keys in new_key_values in top
 *					N item list
 *
 * return	       : error code
 * top_n_items (in)    : current top N item list
 * att_idxs (in)       : indexes for idxkey attributes
 * domains (in)	       : domains for idxkey attributes
 * desc_order (in)     : is descending order for idxkey attributes
 *			 if NULL, ascending order will be considered
 * new_key_values (in) : key values for the new item
 * no_keys (in)	       : number of keys that are compared
 * first (in)	       : position of the first item in current range
 * last (in)	       : position of the last item in current range
 * new_pos (out)       : the position where the new item fits
 *
 * NOTE	: At each step, split current range in half and compare with the
 *	  middle item. If all keys are equal save the position of middle item.
 *	  If middle item is better, look between middle and last, otherwise
 *	  look between first and middle.
 *	  The recursion stops when the range cannot be split anymore
 *	  (first + 1 <= last), when normally first is better and last is worse
 *	  and the new item should replace last. There is a special case when
 *	  the new item is better than all items in top N. In this case,
 *	  first must be 0 and an extra compare is made (to see if new item
 *	  should in fact replace first).
 */
static int
btree_top_n_items_binary_search (RANGE_OPT_ITEM ** top_n_items,
				 int *att_idxs, TP_DOMAIN ** domains,
				 bool * desc_order, DB_VALUE * new_key_values,
				 int no_keys, int first, int last,
				 int *new_pos)
{
  DB_VALUE comp_key_value;
  RANGE_OPT_ITEM *comp_item;
  int i, c;

  int middle;
  assert (last >= first && new_pos != NULL);
  if (last <= first + 1)
    {
      if (first == 0)
	{
	  /* need to check if the new key is smaller than the first */
	  comp_item = top_n_items[0];

	  for (i = 0; i < no_keys; i++)
	    {
	      DB_MAKE_NULL (&comp_key_value);

	      /* peek value */
	      assert (comp_item->index_value.size > att_idxs[i]);
	      comp_key_value = comp_item->index_value.vals[att_idxs[i]];
	      comp_key_value.need_clear = false;

	      c = (*(domains[i]->type->cmpval)) (&comp_key_value,
						 &new_key_values[i], 1, 1,
						 domains[i]->collation_id);
	      if (c != 0)
		{
		  if ((desc_order != NULL && desc_order[i] ? c > 0 : c < 0))
		    {
		      /* new value is not better than the first */
		      break;
		    }
		  else
		    {
		      /* new value is better than the first */
		      new_pos = 0;
		      return NO_ERROR;
		    }
		}
	    }
	  /* new value is equal to first, fall through */
	}
      /* here: the new values should be between first and last */
      *new_pos = last;
      return NO_ERROR;
    }

  /* compare new value with the value in the middle of the current range */
  middle = (last + first) / 2;
  comp_item = top_n_items[middle];

  for (i = 0; i < no_keys; i++)
    {
      DB_MAKE_NULL (&comp_key_value);

      /* peek value */
      assert (comp_item->index_value.size > att_idxs[i]);
      comp_key_value = comp_item->index_value.vals[att_idxs[i]];
      comp_key_value.need_clear = false;

      c = (*(domains[i]->type->cmpval)) (&comp_key_value, &new_key_values[i],
					 1, 1, domains[i]->collation_id);
      if (c != 0)
	{
	  if ((desc_order != NULL && desc_order[i] ? c > 0 : c < 0))
	    {
	      /* the new value is worse than the one in the middle */
	      first = middle;
	    }
	  else
	    {
	      /* the new value is better than the one in the middle */
	      last = middle;
	    }
	  return btree_top_n_items_binary_search (top_n_items, att_idxs,
						  domains, desc_order,
						  new_key_values, no_keys,
						  first, last, new_pos);
	}
    }
  /* all keys were equal, the new item can be put in current position */
  *new_pos = middle;
  return NO_ERROR;
}


/*
 * btree_range_search () -
 *   return: OIDs count or error code
 *   btid(in): B+-tree identifier
 *   range(in): the range of key range
 *   filter(in): key filter
 *   isidp(in):
 *
 * Note: This functions performs key range search function.
 */
int
btree_range_search (THREAD_ENTRY * thread_p, UNUSED_ARG BTID * btid,
		    KEY_VAL_RANGE * key_val_range,
		    FILTER_INFO * filter, INDX_SCAN_ID * index_scan_id_p)
{
  BTREE_SCAN *bts = NULL;	/* B+-tree scan structure */
  BTREE_RANGE_SEARCH_HELPER btrs_helper;
  int error = NO_ERROR;
  SCAN_CODE br_scan = S_END;
  bool resume_search = false;

  assert (file_is_new_file (thread_p, &btid->vfid) == FILE_OLD_FILE);
  assert (index_scan_id_p != NULL);

  /* pointer to index scan info. structure */
  bts = &(index_scan_id_p->bt_scan);
  if (OID_ISNULL (&(bts->btid_int.cls_oid)))
    {
      assert_release (false);	/* should impossible */
      GOTO_EXIT_ON_ERROR;
    }

  /* initialize key filter */
  bts->key_filter = filter;	/* valid pointer or NULL */

  /* copy use desc index information in the BTS to have it available in
   * the btree functions.
   */
  if (index_scan_id_p->indx_info)
    {
      bts->use_desc_index = index_scan_id_p->indx_info->use_desc_index;
    }
  else
    {
      bts->use_desc_index = 0;
    }

  /* The first request of btree_range_search() */
  if (VPID_ISNULL (&(bts->C_vpid)))
    {
      /* check range */
      if (!BTREE_VALID_RANGE (key_val_range->range))
	{
	  error = ER_BTREE_INVALID_RANGE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	  GOTO_EXIT_ON_ERROR;
	}

      /* initialize the bts */
      error = btree_initialize_bts (thread_p, bts, key_val_range, filter);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }
  else
    {
      mnt_stats_counter (thread_p, MNT_STATS_BTREE_RESUMES, 1);
    }

  btree_range_search_init_helper (thread_p, &btrs_helper, bts,
				  index_scan_id_p);

  error = btree_prepare_range_search (thread_p, bts);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  do
    {
      error = btree_next_range_search (thread_p, &br_scan, bts, &btrs_helper,
				       index_scan_id_p);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (br_scan == S_SUCCESS)
	{
	  error = btree_save_range_search_result (thread_p, &btrs_helper,
						  index_scan_id_p);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (index_scan_id_p->need_count_only == false
	      && btrs_helper.num_copied_oids == btrs_helper.mem_oid_cnt)
	    {
	      /* We have no more room. */
	      LSA_COPY (&bts->cur_leaf_lsa, pgbuf_get_lsa (bts->C_page));

	      /* do not clear bts->cur_key for btree_prepare_next_search */
	      resume_search = true;
	      break;
	    }
	}
    }
  while (br_scan == S_SUCCESS);

  assert (error == NO_ERROR);

  if (resume_search == false)
    {
      /* clear all the used keys */
      btree_scan_clear_key (bts);

      /* set the end of scan */
      VPID_SET_NULL (&(bts->C_vpid));
    }

  /* unfix all the index pages */
  if (bts->P_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, bts->P_page);
    }

  if (bts->C_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, bts->C_page);
    }

  if (index_scan_id_p->key_limit_upper != -1
      && btrs_helper.num_copied_oids != -1)
    {
      if ((DB_BIGINT) btrs_helper.num_copied_oids >=
	  index_scan_id_p->key_limit_upper)
	{
	  index_scan_id_p->key_limit_upper = 0;
	}
      else
	{
	  index_scan_id_p->key_limit_upper -= btrs_helper.num_copied_oids;
	}
    }

  return btrs_helper.num_copied_oids;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  /* clear all the used keys */
  btree_scan_clear_key (bts);

  /* set the end of scan */
  /*
   * we need to make sure that
   * BTREE_END_OF_SCAN() return true in the error cases.
   */
  VPID_SET_NULL (&(bts->C_vpid));

  /* unfix all the index pages */
  if (bts->P_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, bts->P_page);
    }

  if (bts->C_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, bts->C_page);
    }

  return error;
}

/*
 * btree_next_range_search()-
 *   return                  : error code.
 *
 *   scan_code(out)          : scan code
 *   bts (in)                : B-tree scan data.
 *   btrs_helper (in/out)    : B-tree range search helper.
 *   index_scan_id_p (in)    : Index scan data.
 */
static int
btree_next_range_search (THREAD_ENTRY * thread_p, SCAN_CODE * scan_code,
			 BTREE_SCAN * bts,
			 BTREE_RANGE_SEARCH_HELPER * btrs_helper,
			 INDX_SCAN_ID * index_scan_id_p)
{
  int error = NO_ERROR;
  BTREE_CHECK_KEY key_check = BTREE_KEY_ERROR;

  *scan_code = S_SUCCESS;

  do
    {
      if (bts->is_first_search == false)
	{
	  /* find the next index record */
	  error =
	    btree_find_next_record (thread_p, bts, bts->use_desc_index, NULL);
	  if (error != NO_ERROR)
	    {
	      *scan_code = S_ERROR;
	      break;
	    }
	}
      bts->is_first_search = false;

      error = btree_get_satisfied_key (thread_p, &key_check, bts,
				       btrs_helper, index_scan_id_p);
      if (error != NO_ERROR)
	{
	  assert (key_check == BTREE_KEY_ERROR);

	  *scan_code = S_ERROR;
	  break;
	}
      assert (key_check != BTREE_KEY_ERROR);
    }
  while (key_check == BTREE_KEY_NOT_SATISFIED);

  if (key_check == BTREE_KEY_OUT_OF_RANGE)
    {
      *scan_code = S_END;
    }

  return error;
}

/*
 * btree_range_search_init_helper () - Initialize btree_range_search_helper
 *				       at the start of a search.
 *
 * return		: Void.
 * thread_p (in)	: Thread entry.
 * btrs_helper (out)	: B-tree range search helper.
 * bts (in)		: B-tree scan data.
 * index_scan_id_p (in) : Index scan data.
 */
static void
btree_range_search_init_helper (UNUSED_ARG THREAD_ENTRY * thread_p,
				BTREE_RANGE_SEARCH_HELPER * btrs_helper,
				UNUSED_ARG BTREE_SCAN * bts,
				INDX_SCAN_ID * index_scan_id_p)
{
  btrs_helper->mem_oid_ptr = NULL;

  btrs_helper->num_copied_oids = 0;

  if (SCAN_IS_INDEX_COVERED (index_scan_id_p))
    {
      assert (index_scan_id_p->oid_list.oidp == NULL);

      btrs_helper->mem_oid_cnt = index_scan_id_p->indx_cov.max_tuples;
      btrs_helper->mem_oid_ptr = NULL;
    }
  else
    {
      btrs_helper->mem_oid_cnt =
	index_scan_id_p->oid_list.oid_buf_size / OR_OID_SIZE;
      btrs_helper->mem_oid_ptr = index_scan_id_p->oid_list.oidp;
    }

  btrs_helper->num_copied_oids = 0;	/* # of copied OIDs */
}

/*
 * btree_get_satisfied_key () - Used in the context of
 *					btree_range_search function, this
 *					obtains a set of OIDs once the
 *					search is positioned to on a key in a
 *					a leaf node,
 *
 * return		   : Error code.
 *
 * key_check(out)          :
 * bts (in)		   : B-tree scan data.
 * btrs_helper (in/out)	   : B-tree range search helper.
 * index_scan_id_p (in)	   : Index scan data.
 * key_check(out)	   :
 *
 */
static int
btree_get_satisfied_key (THREAD_ENTRY * thread_p, BTREE_CHECK_KEY * key_check,
			 BTREE_SCAN * bts,
			 BTREE_RANGE_SEARCH_HELPER * btrs_helper,
			 INDX_SCAN_ID * index_scan_id_p)
{
  RECDES rec = RECDES_INITIALIZER;
  bool is_key_range_satisfied;	/* Does current key satisfy range */
  bool is_key_filter_satisfied;	/* Does current key satisfy filter */
#if !defined(NDEBUG)
  int c;
  bool clear_save_key;
  DB_IDXKEY save_key;
#endif
  int error = NO_ERROR;

  assert (bts != NULL);
  assert (btrs_helper != NULL);
  assert (key_check != NULL);

#if !defined(NDEBUG)
  clear_save_key = false;
  DB_IDXKEY_MAKE_NULL (&save_key);
#endif

  *key_check = BTREE_KEY_ERROR;

  if (VPID_ISNULL (&(bts->C_vpid)))
    {
      /* It reached at the end of leaf level */
      *key_check = BTREE_KEY_OUT_OF_RANGE;
      return NO_ERROR;
    }

  /* Find the position of OID list to be searched in the index entry */
  rec.area_size = 0;
  if (spage_get_record (bts->C_page, bts->slot_id, &rec, PEEK) != S_SUCCESS)
    {
      error = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
  db_idxkey_clone (&bts->cur_key, &save_key);
  clear_save_key = true;
#endif

  btree_clear_key_value (&bts->clear_cur_key, &bts->cur_key);

  error =
    btree_read_record (thread_p, &bts->btid_int, &rec, &bts->cur_key,
		       NULL, BTREE_LEAF_NODE,
		       &bts->clear_cur_key, COPY_KEY_VALUE);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
  if (!DB_IDXKEY_IS_NULL (&save_key))
    {
      /* save_key < cur_key */
      c =
	btree_compare_key (thread_p, &bts->btid_int, &save_key, &bts->cur_key,
			   NULL);

      if (bts->use_desc_index)
	{
	  c = ((c == DB_GT) ? DB_LT : (c == DB_LT) ? DB_GT : c);
	}

      if (c != DB_LT)
	{
#if 1				/* TODO - trace */
	  db_idxkey_print (&save_key);
	  fprintf (stdout, "\t");
	  db_idxkey_print (&bts->cur_key);
	  fprintf (stdout, "\n");
	  fflush (stdout);
#endif

	  assert (false);

	  error = ER_FAILED;

	  GOTO_EXIT_ON_ERROR;
	}
    }

  btree_clear_key_value (&clear_save_key, &save_key);
#endif

  /* apply key range and key filter to the new key value */
  is_key_range_satisfied = is_key_filter_satisfied = false;
  error = btree_apply_key_range_and_filter (thread_p, bts,
					    &is_key_range_satisfied,
					    &is_key_filter_satisfied);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (is_key_range_satisfied == false)
    {
      *key_check = BTREE_KEY_OUT_OF_RANGE;
      return NO_ERROR;
    }

  bts->read_keys++;

  if (is_key_filter_satisfied == false)
    {
      *key_check = BTREE_KEY_NOT_SATISFIED;
      return NO_ERROR;
    }

  if (index_scan_id_p->key_limit_lower != -1
      && (DB_BIGINT) btrs_helper->num_copied_oids <
      index_scan_id_p->key_limit_lower)
    {
      /* do not copy OID, just update key_limit_lower */
      index_scan_id_p->key_limit_lower -= 1;

      *key_check = BTREE_KEY_NOT_SATISFIED;
      return NO_ERROR;
    }

  if (index_scan_id_p->key_limit_upper != -1
      && (DB_BIGINT) btrs_helper->num_copied_oids >=
      index_scan_id_p->key_limit_upper)
    {
      /* Upper key limit is reached, stop searching */
      *key_check = BTREE_KEY_OUT_OF_RANGE;
      return NO_ERROR;
    }

  bts->qualified_keys++;

  *key_check = BTREE_KEY_SATISFIED;

  assert (error == NO_ERROR);

  return NO_ERROR;

exit_on_error:
  *key_check = BTREE_KEY_ERROR;

  if (error == NO_ERROR)
    {
      error = ER_FAILED;
    }

#if !defined(NDEBUG)
  btree_clear_key_value (&clear_save_key, &save_key);
#endif

  return error;
}

/*
 * btree_save_range_search_result () - Handles one OID
 *				 Used in the context of btree_range_search
 *				 function.
 *
 * return		: Error code.
 * thread_p (in)	: Thread entry.
 * btrs_helper (in)	: B-tree range search helper.
 * index_scan_id_p (in) : Index scan data.
 */
static int
btree_save_range_search_result (THREAD_ENTRY * thread_p,
				BTREE_RANGE_SEARCH_HELPER * btrs_helper,
				INDX_SCAN_ID * index_scan_id_p)
{
  BTREE_SCAN *bts = NULL;
  bool key_added;

  assert (btrs_helper != NULL);
  assert (index_scan_id_p != NULL);

  /* pointer to index scan info. structure */
  bts = &(index_scan_id_p->bt_scan);

  if (index_scan_id_p->need_count_only == false)
    {
      if (index_scan_id_p->multi_range_opt.use)
	{
	  /* Multiple range optimization */
	  /* Add current key to TOP N sorted keys */
	  if (btree_range_opt_check_add_index_key (thread_p, bts,
						   &index_scan_id_p->
						   multi_range_opt,
						   &key_added) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	  if (key_added == false)
	    {
	      /* Current item didn't fit in the TOP N keys, and the
	       * following items in current btree_range_search iteration
	       * will not be better. Go to end of scan.
	       */

	      /* set the end of scan */
	      VPID_SET_NULL (&bts->C_vpid);
	      return NO_ERROR;
	    }
	}
      else if (SCAN_IS_INDEX_COVERED (index_scan_id_p))
	{
	  /* Covering Index */
	  if (btree_dump_curr_key (thread_p, index_scan_id_p) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}
      else
	{
	  /* normal scan - copy OID */
	  /* No special case: store OID's in OID buffer */
	  if (btree_get_oid_from_key (thread_p, &(bts->btid_int),
				      &bts->cur_key,
				      btrs_helper->mem_oid_ptr) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	  btrs_helper->mem_oid_ptr++;
	}
    }

  /* Increment OID's count */
  btrs_helper->num_copied_oids++;

  return NO_ERROR;
}

/*
 * btree_prepare_range_search () - Prepares range search on first call or
 *				   when resumed.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * bts (in/out)	 : B-tree scan data.
 */
static int
btree_prepare_range_search (THREAD_ENTRY * thread_p, BTREE_SCAN * bts)
{
  int error = NO_ERROR;

  if (VPID_ISNULL (&(bts->C_vpid)))
    {
      /* the first request */
      error = btree_prepare_first_search (thread_p, bts);
    }
  else
    {
      /* not the first request */
      error = btree_prepare_next_search (thread_p, bts);
    }

  return error;
}

#if !defined(NDEBUG)
/*
 * btree_fence_check_key () -
 *   return: NO_ERROR or ER_GENERIC_ERROR
 *
 *   thread_p(in):
 *   btid(in):
 *   left_key(in):
 *   right_key(in):
 *   with_eq(in);
 */
int
btree_fence_check_key (THREAD_ENTRY * thread_p,
		       BTID_INT * btid,
		       const DB_IDXKEY * left_key,
		       const DB_IDXKEY * right_key, const bool with_eq)
{
  int c;
  char left_str[LINE_MAX];
  char right_str[LINE_MAX];

  assert (left_key != NULL);
  assert (right_key != NULL);

  if (DB_IDXKEY_IS_NULL (left_key) || DB_IDXKEY_IS_NULL (right_key))
    {
      return NO_ERROR;		/* give up */
    }

  c = btree_compare_key (thread_p, btid, left_key, right_key, NULL);

  if (with_eq)
    {
      if (!(c == DB_LT || c == DB_EQ))
	{
	  goto exit_on_error;
	}
    }
  else
    {
      if (!(c == DB_LT))
	{
	  goto exit_on_error;
	}
    }

  return NO_ERROR;

exit_on_error:

  assert (false);

  help_sprint_idxkey (left_key, left_str, sizeof (left_str) - 1);
  left_str[LINE_MAX - 1] = '\0';

  help_sprint_idxkey (right_key, right_str, LINE_MAX - 1);
  right_str[LINE_MAX - 1] = '\0';

  if (with_eq)
    {
      er_log_debug (ARG_FILE_LINE,
		    "B+tree fence violation: required %s <= %s", left_str,
		    right_str);
    }
  else
    {
      er_log_debug (ARG_FILE_LINE, "B+tree fence violation: required %s < %s",
		    left_str, right_str);
    }
  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");

  return ER_GENERIC_ERROR;
}
#endif
