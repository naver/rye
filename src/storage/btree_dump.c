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

#include "fault_injection.h"

/* this must be the last header file included!!! */
#include "dbval.h"

static void btree_dump_key (FILE * fp, const DB_IDXKEY * key);
static int btree_index_capacity (THREAD_ENTRY * thread_p, OID * cls_oid,
				 BTID * btid, BTREE_CAPACITY * cpc);
static int btree_dump_capacity (THREAD_ENTRY * thread_p, FILE * fp,
				BTID * btid);
static int btree_get_subtree_capacity (THREAD_ENTRY * thread_p,
				       BTID_INT * btid, PAGE_PTR pg_ptr,
				       BTREE_CAPACITY * cpc);
static void btree_print_space (FILE * fp, int n);

/* Dump routines */
static void btree_dump_leaf_record (THREAD_ENTRY * thread_p, FILE * fp,
				    BTID_INT * btid, RECDES * rec, int n);
static void btree_dump_non_leaf_record (THREAD_ENTRY * thread_p, FILE * fp,
					BTID_INT * btid, RECDES * rec, int n,
					int print_key);

static const char *node_type_to_string (short node_type);

#if 1				/* defined(ENABLE_UNUSED_FUNCTION) */
static int btree_dump_subtree (THREAD_ENTRY * thread_p, FILE * fp,
			       BTID_INT * btid, PAGE_PTR pg_ptr,
			       VPID * pg_vpid, int dump_level, bool dump_key,
			       int n);
#endif

/*
 * btree_dump_key () -
 *   return:
 *   key(in):
 */
static void
btree_dump_key (FILE * fp, const DB_IDXKEY * key)
{
  assert (key != NULL);

  fprintf (fp, " ");

  db_idxkey_fprint (fp, key);

  fprintf (fp, " ");
}

/*
 * btree_dump_leaf_record () -
 *   return: nothing
 *   btid(in): B+tree index identifier
 *   rec(in): Pointer to a record in a leaf page of the tree
 *   n(in): Indentation left margin (number of preceding blanks)
 *
 * Note: Dumps the content of a leaf record, namely key and the set of
 * values for the key.
 */
static void
btree_dump_leaf_record (THREAD_ENTRY * thread_p, FILE * fp, BTID_INT * btid,
			RECDES * rec, int n)
{
  OID oid;
  DB_IDXKEY key;
  bool clear_key;

  DB_IDXKEY_MAKE_NULL (&key);

  /* output the leaf record structure content */
  btree_print_space (fp, n);

  (void) btree_read_record (thread_p, btid, rec, &key, NULL,
			    BTREE_LEAF_NODE, &clear_key, PEEK_KEY_VALUE);

  fprintf (fp, "Key: ");
  btree_dump_key (fp, &key);

  /* output the values */
  fprintf (fp, "  Values: ");

  /* output the oid */
  btree_get_oid_from_key (thread_p, btid, &key, &oid);
  fprintf (fp, " (%d, %d, %d) ", oid.volid, oid.pageid, oid.slotid);

  btree_clear_key_value (&clear_key, &key);

  fflush (fp);
}

/*
 * btree_dump_non_leaf_record () -
 *   return: void
 *   btid(in):
 *   rec(in): Pointer to a record in a non_leaf page
 *   n(in): Indentation left margin (number of preceding blanks)
 *   print_key(in):
 *
 * Note: Dumps the content of a nonleaf record, namely key and child
 * page identifier.
 */
static void
btree_dump_non_leaf_record (THREAD_ENTRY * thread_p, FILE * fp,
			    BTID_INT * btid, RECDES * rec, int n,
			    int print_key)
{
  NON_LEAF_REC non_leaf_record;
  int key_len;
  DB_IDXKEY key;
  bool clear_key;

  DB_IDXKEY_MAKE_NULL (&key);

  non_leaf_record.pnt.pageid = NULL_PAGEID;
  non_leaf_record.pnt.volid = NULL_VOLID;

  /* output the non_leaf record structure content */
  (void) btree_read_record (thread_p, btid, rec, &key,
			    &non_leaf_record, BTREE_NON_LEAF_NODE, &clear_key,
			    PEEK_KEY_VALUE);

  btree_print_space (fp, n);
  fprintf (fp, "Child_Page: {%d , %d}\t",
	   non_leaf_record.pnt.volid, non_leaf_record.pnt.pageid);

  if (print_key)
    {
      key_len = btree_get_key_length (&key);
      fprintf (fp, "Key_Len: %5d  Key: ", key_len);
      btree_dump_key (fp, &key);
    }
  else
    {
      fprintf (fp, "No Key ");
    }

  btree_clear_key_value (&clear_key, &key);

  fflush (fp);
}

/*
 *       		     b+tree space routines
 */

/*
 * btree_get_subtree_capacity () -
 *   return: NO_ERROR
 *   btid(in):
 *   pg_ptr(in):
 *   cpc(in):
 */
static int
btree_get_subtree_capacity (THREAD_ENTRY * thread_p, BTID_INT * btid,
			    PAGE_PTR pg_ptr, BTREE_CAPACITY * cpc)
{
  RECDES rec = RECDES_INITIALIZER;	/* Page record descriptor */
  int free_space;		/* Total free space of the Page */
  BTREE_NODE_HEADER node_header;
  int key_cnt;			/* Page key count */
  NON_LEAF_REC nleaf_ptr;	/* NonLeaf Record pointer */
  VPID page_vpid;		/* Child page identifier */
  PAGE_PTR page = NULL;		/* Child page pointer */
  int i;			/* Loop counter */

  bool clear_key = false;
  PAGE_PTR ovfp = NULL;
  DB_IDXKEY key1;
  int ret = NO_ERROR;

  DB_IDXKEY_MAKE_NULL (&key1);

  /* initialize capacity structure */
  cpc->dis_key_cnt = 0;
  cpc->tot_val_cnt = 0;
  cpc->avg_val_per_key = 0;
  cpc->leaf_pg_cnt = 0;
  cpc->nleaf_pg_cnt = 0;
  cpc->tot_pg_cnt = 0;
  cpc->height = 0;
  cpc->sum_rec_len = 0;
  cpc->sum_key_len = 0;
  cpc->avg_key_len = 0;
  cpc->avg_rec_len = 0;
  cpc->tot_free_space = 0;
  cpc->tot_space = 0;
  cpc->tot_used_space = 0;
  cpc->avg_pg_key_cnt = 0;
  cpc->avg_pg_free_sp = 0;

  free_space = spage_get_free_space (thread_p, pg_ptr);

  if (btree_read_node_header (NULL, pg_ptr, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);

  if (node_header.node_level > 1)
    {				/* a non-leaf page */
      BTREE_CAPACITY cpc2;

      /* traverse all the subtrees of this non_leaf page and accumulate
       * the statistical data in the cpc structure
       */
      for (i = 1; i <= (key_cnt + 1); i++)
	{
	  if (spage_get_record (pg_ptr, i, &rec, PEEK) != S_SUCCESS)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf_ptr);
	  page_vpid = nleaf_ptr.pnt;
	  page =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &page_vpid,
			     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			     PAGE_BTREE);
	  if (page == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  ret = btree_get_subtree_capacity (thread_p, btid, page, &cpc2);
	  if (ret != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* form the cpc structure for a non-leaf node page */
	  cpc->dis_key_cnt += cpc2.dis_key_cnt;
	  cpc->tot_val_cnt += cpc2.tot_val_cnt;
	  cpc->leaf_pg_cnt += cpc2.leaf_pg_cnt;
	  cpc->nleaf_pg_cnt += cpc2.nleaf_pg_cnt;
	  cpc->tot_pg_cnt += cpc2.tot_pg_cnt;
	  cpc->height = cpc2.height + 1;
	  cpc->sum_rec_len += cpc2.sum_rec_len;
	  cpc->sum_key_len += cpc2.sum_key_len;
	  cpc->tot_free_space += cpc2.tot_free_space;
	  cpc->tot_space += cpc2.tot_space;
	  cpc->tot_used_space += cpc2.tot_used_space;
	  pgbuf_unfix_and_init (thread_p, page);
	}			/* for */
      cpc->avg_val_per_key = ((cpc->dis_key_cnt > 0) ?
			      (cpc->tot_val_cnt / cpc->dis_key_cnt) : 0);
      cpc->nleaf_pg_cnt += 1;
      cpc->tot_pg_cnt += 1;
      cpc->tot_free_space += free_space;
      cpc->tot_space += DB_PAGESIZE;
      cpc->tot_used_space += (DB_PAGESIZE - free_space);
      cpc->avg_key_len = ((cpc->dis_key_cnt > 0) ?
			  ((int) (cpc->sum_key_len / cpc->dis_key_cnt)) : 0);
      cpc->avg_rec_len = ((cpc->dis_key_cnt > 0) ?
			  ((int) (cpc->sum_rec_len / cpc->dis_key_cnt)) : 0);
      cpc->avg_pg_key_cnt = ((cpc->leaf_pg_cnt > 0) ?
			     ((int) (cpc->dis_key_cnt / cpc->leaf_pg_cnt)) :
			     0);
      cpc->avg_pg_free_sp = ((cpc->tot_pg_cnt > 0) ?
			     (cpc->tot_free_space / cpc->tot_pg_cnt) : 0);
    }
  else
    {				/* a leaf page */

      /* form the cpc structure for a leaf node page */
      cpc->dis_key_cnt = key_cnt;
      cpc->leaf_pg_cnt = 1;
      cpc->nleaf_pg_cnt = 0;
      cpc->tot_pg_cnt = 1;
      cpc->height = 1;
      cpc->tot_val_cnt += key_cnt;
      for (i = 1; i <= cpc->dis_key_cnt; i++)
	{
	  if (spage_get_record (pg_ptr, i, &rec, PEEK) != S_SUCCESS)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  cpc->sum_rec_len += rec.length;
	  cpc->sum_key_len += rec.length;

#if !defined (NDEBUG)
	  /* read the current record key */
	  ret = btree_read_record (thread_p, btid, &rec, &key1, NULL,
				   BTREE_LEAF_NODE, &clear_key,
				   PEEK_KEY_VALUE);
	  if (ret != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  assert (rec.length == btree_get_key_length (&key1));

	  btree_clear_key_value (&clear_key, &key1);
#endif
	}			/* for */
      cpc->avg_val_per_key = ((cpc->dis_key_cnt > 0) ?
			      (cpc->tot_val_cnt / cpc->dis_key_cnt) : 0);
      cpc->avg_key_len = ((cpc->dis_key_cnt > 0) ?
			  ((int) (cpc->sum_key_len / cpc->dis_key_cnt)) : 0);
      cpc->avg_rec_len = ((cpc->dis_key_cnt > 0) ?
			  ((int) (cpc->sum_rec_len / cpc->dis_key_cnt)) : 0);
      cpc->tot_free_space = (float) free_space;
      cpc->tot_space = DB_PAGESIZE;
      cpc->tot_used_space = (cpc->tot_space - cpc->tot_free_space);
      cpc->avg_pg_key_cnt = ((cpc->leaf_pg_cnt > 0) ?
			     (cpc->dis_key_cnt / cpc->leaf_pg_cnt) : 0);
      cpc->avg_pg_free_sp = ((cpc->tot_pg_cnt > 0) ?
			     (cpc->tot_free_space / cpc->tot_pg_cnt) : 0);

    }				/* if-else */

  return ret;

exit_on_error:

  if (page)
    {
      pgbuf_unfix_and_init (thread_p, page);
    }
  if (ovfp)
    {
      pgbuf_unfix_and_init (thread_p, ovfp);
    }

  btree_clear_key_value (&clear_key, &key1);

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * btree_index_capacity () -
 *   return: NO_ERROR
 *   cls_oid(in):
 *   btid(in): B+tree index identifier
 *   cpc(out): Set to contain index capacity information
 *
 * Note: Form and return index capacity/space related information
 */
static int
btree_index_capacity (THREAD_ENTRY * thread_p, OID * cls_oid,
		      BTID * btid, BTREE_CAPACITY * cpc)
{
  VPID root_vpid;		/* root page identifier */
  PAGE_PTR root = NULL;		/* root page pointer */
  BTID_INT btid_int;
  BTREE_NODE_HEADER root_header;
  int ret = NO_ERROR;

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&btid_int);

  /* read root page */
  root_vpid.pageid = btid->root_pageid;
  root_vpid.volid = btid->vfid.volid;
  root =
    btree_pgbuf_fix (thread_p, &(btid->vfid), &root_vpid, PGBUF_LATCH_READ,
		     PGBUF_UNCONDITIONAL_LATCH, PAGE_BTREE_ROOT);
  if (root == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ret = btree_read_node_header (NULL, root, &root_header);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  btid_int.sys_btid = btid;

  /* get class representation of the index */
  COPY_OID (&(btid_int.cls_oid), cls_oid);
  btid_int.classrepr =
    heap_classrepr_get (thread_p, &(btid_int.cls_oid), NULL, 0,
			&(btid_int.classrepr_cache_idx), true);
  if (btid_int.classrepr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* get the index ID which corresponds to the BTID */
  btid_int.indx_id =
    heap_classrepr_find_index_id (btid_int.classrepr, btid_int.sys_btid);
  if (btid_int.indx_id < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* traverse the tree and store the capacity info */
  ret = btree_get_subtree_capacity (thread_p, &btid_int, root, cpc);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (root_header.node_level == cpc->height);

  pgbuf_unfix_and_init (thread_p, root);

exit_on_end:

#if 1				/* TODO - */
  if (btid_int.classrepr)
    {
      assert (btid_int.classrepr_cache_idx != -1);
      assert (btid_int.indx_id != -1);

      (void) heap_classrepr_free (btid_int.classrepr,
				  &(btid_int.classrepr_cache_idx));
      assert (btid_int.classrepr_cache_idx == -1);

      btid_int.classrepr = NULL;
//    btid_int.classrepr_cache_idx = -1;
//    btid_int.indx_id = -1;
    }
#endif

  return ret;

exit_on_error:

  if (root)
    {
      pgbuf_unfix_and_init (thread_p, root);
    }

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

/*
 * btree_dump_capacity () -
 *   return: NO_ERROR
 *   btid(in): B+tree index identifier
 *
 * Note: Dump index capacity/space information.
 */
static int
btree_dump_capacity (THREAD_ENTRY * thread_p, FILE * fp, BTID * btid)
{
  BTREE_CAPACITY cpc;
  int ret = NO_ERROR;
  char area[FILE_DUMP_DES_AREA_SIZE];
  char *file_des = NULL;
  char *index_name = NULL;
  char *class_name = NULL;
  int file_des_size = 0;
  int size = 0;
  OID class_oid;

  assert (fp != NULL);
  assert (btid != NULL);

  /* get class_name and index_name */
  file_des = area;
  file_des_size = FILE_DUMP_DES_AREA_SIZE;

  size = file_get_descriptor (thread_p, &btid->vfid, file_des, file_des_size);
  if (size <= 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  class_oid = ((FILE_HEAP_DES *) file_des)->class_oid;

  class_name = heap_get_class_name (thread_p, &class_oid);

  /* get index capacity information */
  ret = btree_index_capacity (thread_p, &class_oid, btid, &cpc);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* get index name */
  if (heap_get_indexname_of_btid (thread_p, &class_oid, btid,
				  &index_name) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  fprintf (fp,
	   "\n--------------------------------------------------"
	   "-----------\n");
  fprintf (fp, "BTID: {{%d, %d}, %d}, %s on %s, CAPACITY INFORMATION:\n",
	   btid->vfid.volid, btid->vfid.fileid, btid->root_pageid,
	   (index_name == NULL) ? "*UNKNOWN_INDEX*" : index_name,
	   (class_name == NULL) ? "*UNKNOWN_CLASS*" : class_name);

  /* dump the capacity information */
  fprintf (fp, "\nDistinct Key Count: %d\n", cpc.dis_key_cnt);
  fprintf (fp, "Total Value Count: %d\n", cpc.tot_val_cnt);
  fprintf (fp, "Average Value Count Per Key: %d\n", cpc.avg_val_per_key);
  fprintf (fp, "Total Page Count: %d\n", cpc.tot_pg_cnt);
  fprintf (fp, "Leaf Page Count: %d\n", cpc.leaf_pg_cnt);
  fprintf (fp, "NonLeaf Page Count: %d\n", cpc.nleaf_pg_cnt);
  fprintf (fp, "Height: %d\n", cpc.height);
  fprintf (fp, "Average Key Length: %d\n", cpc.avg_key_len);
  fprintf (fp, "Average Record Length: %d\n", cpc.avg_rec_len);
  fprintf (fp, "Total Index Space: %.0f bytes\n", cpc.tot_space);
  fprintf (fp, "Used Index Space: %.0f bytes\n", cpc.tot_used_space);
  fprintf (fp, "Free Index Space: %.0f bytes\n", cpc.tot_free_space);
  fprintf (fp, "Average Page Free Space: %.0f bytes\n", cpc.avg_pg_free_sp);
  fprintf (fp, "Average Page Key Count: %d\n", cpc.avg_pg_key_cnt);
  fprintf (fp, "--------------------------------------------------"
	   "-----------\n");

end:

  if (class_name != NULL)
    {
      free_and_init (class_name);
    }

  if (index_name != NULL)
    {
      free_and_init (index_name);
    }

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

  goto end;
}

/*
 * btree_dump_capacity_all () -
 *   return: NO_ERROR
 *
 * Note: Dump the capacity/space information of all indices.
 */
int
btree_dump_capacity_all (THREAD_ENTRY * thread_p, FILE * fp)
{
  int num_files;		/* Number of files in the system */
  BTID btid;			/* Btree index identifier */
  VPID vpid;			/* Index root page identifier */
  int i;			/* Loop counter */
  int ret = NO_ERROR;

  /* Find number of files */
  num_files = file_get_numfiles (thread_p);
  if (num_files < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* Go to each file, check only the btree files */
  for (i = 0; i < num_files; i++)
    {
      if (file_find_nthfile (thread_p, &btid.vfid, i) != 1)
	{
	  break;
	}

      if (file_get_type (thread_p, &btid.vfid) != FILE_BTREE)
	{
	  continue;
	}

      if (file_find_nthpages (thread_p, &btid.vfid, &vpid, 0, 1) != 1)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      btid.root_pageid = vpid.pageid;

      ret = btree_dump_capacity (thread_p, fp, &btid);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }				/* for */

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

#if 1				/* defined(ENABLE_UNUSED_FUNCTION) */
/*
 * b+tree dump routines
 */

/*
 * btree_dump_tree () - for debugging
 *   return: error code
 *
 *   btid(in): Internal btree block
 *   fp(in):
 *   demp_level(in):
 *   dump_key(in):
 */
int
btree_dump_tree (THREAD_ENTRY * thread_p, FILE * fp, BTID_INT * btid,
		 int dump_level, bool dump_key)
{
  VPID p_vpid;
  PAGE_PTR p_pgptr = NULL;
  BTREE_NODE_HEADER node_header;
  int error = NO_ERROR;

  /* Fetch the root page */
  p_vpid.pageid = btid->sys_btid->root_pageid;
  p_vpid.volid = btid->sys_btid->vfid.volid;
  p_pgptr =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &p_vpid,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (p_pgptr == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  error = btree_read_node_header (NULL, p_pgptr, &node_header);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = btree_dump_subtree (thread_p, fp, btid,
			      p_pgptr, &p_vpid, dump_level, dump_key, 2);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  pgbuf_unfix_and_init (thread_p, p_pgptr);

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (p_pgptr)
    {
      pgbuf_unfix_and_init (thread_p, p_pgptr);
    }

  assert (error != NO_ERROR);

  return error;
}

/*
 * btree_dump_subtree () -
 *    return: error code
 *
 *    btid(in): Internal btree block
 *    fp(in):
 *    demp_level(in):
 *    dump_key(in):
 */
static int
btree_dump_subtree (THREAD_ENTRY * thread_p, FILE * fp, BTID_INT * btid,
		    PAGE_PTR pg_ptr, VPID * pg_vpid, int dump_level,
		    bool dump_key, int n)
{
  VPID page_vpid;		/* Child page identifier */
  PAGE_PTR page = NULL;		/* Child page pointer */
  RECDES rec = RECDES_INITIALIZER;	/* Record descriptor for page node records */
  int key_cnt;			/* Number of keys in the page */
  int i;			/* Loop counter */
  BTREE_NODE_HEADER node_header;
  NON_LEAF_REC nleaf;
  int error = NO_ERROR;

  error = btree_read_node_header (NULL, pg_ptr, &node_header);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (dump_level == 0 || dump_level == node_header.node_level)
    {
      btree_dump_page (thread_p, fp, btid, pg_ptr, pg_vpid, n, dump_key);
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
	      error = er_errid ();
	      GOTO_EXIT_ON_ERROR;
	    }
	  btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf);
	  page_vpid = nleaf.pnt;

	  page =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &page_vpid,
			     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			     PAGE_BTREE);
	  if (page == NULL)
	    {
	      error = er_errid ();
	      GOTO_EXIT_ON_ERROR;
	    }

	  error = btree_dump_subtree (thread_p, fp, btid, page, &page_vpid,
				      dump_level, dump_key, n + 4);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  pgbuf_unfix_and_init (thread_p, page);
	}
    }

  return error;

exit_on_error:

  if (page)
    {
      pgbuf_unfix_and_init (thread_p, page);
    }

  return error;

}
#endif

/*
 * btree_print_space () -
 *   return:
 *   n(in):
 */
static void
btree_print_space (FILE * fp, int n)
{

  while (n--)			/* print n space character */
    {
      fprintf (fp, " ");
    }

}

/*
 * btree_dump_page () -
 *   return: nothing
 *   btid(in): B+tree index identifier
 *   btname(in):
 *   page_ptr(in): Page pointer
 *   pg_vpid(in): Page identifier
 *   n(in): Identation left margin (number of preceding blanks)
 *   level(in):
 *
 * Note: Dumps the content of the given page of the tree.
 */
void
btree_dump_page (THREAD_ENTRY * thread_p, FILE * fp,
		 BTID_INT * btid, PAGE_PTR page_ptr, VPID * pg_vpid,
		 int n, bool dump_key)
{
  int key_cnt;
  int i;
  RECDES rec = RECDES_INITIALIZER;
  short node_type;
  BTREE_NODE_HEADER node_header;
  VPID next_vpid;
  VPID prev_vpid;
  OID class_oid, inst_oid;
  char *class_name = NULL;
  char *btname = NULL;
  DB_IDXKEY key;
  bool clear_key;

  DB_IDXKEY_MAKE_NULL (&key);

  /* get the header record */
  if (btree_read_node_header (NULL, page_ptr, &node_header) != NO_ERROR)
    {
      fprintf (fp,
	       "btree_dump_page: invalid node header: VPID(%d,%d)\n",
	       pg_vpid->volid, pg_vpid->pageid);
      return;
    }

  key_cnt = node_header.key_cnt;
  assert_release (key_cnt >= 0);
  node_type = BTREE_GET_NODE_TYPE (node_header.node_level);
  VPID_COPY (&next_vpid, &node_header.next_vpid);
  VPID_COPY (&prev_vpid, &node_header.prev_vpid);
  btree_print_space (fp, n);
  fprintf (fp,
	   "\n<<<<<<<<<<<<<<<<  N O D E   P A G E  >>>>>>>>>>>>>>>>> \n\n");
  btree_print_space (fp, n);

  if (node_type == BTREE_LEAF_NODE && key_cnt > 0)
    {
      if (spage_get_record (page_ptr, 1, &rec, PEEK) != S_SUCCESS)
	{
	  fprintf (fp, "btree_dump_page: %s\n", er_msg ());
	  return;
	}

      clear_key = false;
      if (btree_read_record (thread_p, btid, &rec, &key, NULL,
			     BTREE_LEAF_NODE, &clear_key,
			     PEEK_KEY_VALUE) != NO_ERROR)
	{
	  fprintf (fp, "btree_dump_page: %s\n", er_msg ());
	  return;
	}

      btree_get_oid_from_key (thread_p, btid, &key, &inst_oid);
      btree_clear_key_value (&clear_key, &key);

      heap_get_class_oid (thread_p, &class_oid, &inst_oid);
      if (OID_ISNULL (&class_oid))
	{
	  fprintf (fp, "btree_dump_page: not found class_oid\n");
	  return;
	}

      class_name = heap_get_class_name (thread_p, &class_oid);
      if (heap_get_indexname_of_btid (thread_p, &class_oid,
				      btid->sys_btid, &btname) != NO_ERROR)
	{
	  fprintf (fp, "btree_dump_page: %s\n", er_msg ());
	  if (class_name)
	    {
	      free_and_init (class_name);
	    }
	  return;
	}

      fprintf (fp, "INDEX %s ON CLASS %s (CLASS_OID:%2d|%4d|%2d) \n\n",
	       (btname) ? btname : "*UNKNOWN-INDEX*",
	       (class_name) ? class_name : "*UNKNOWN-TABLE*",
	       class_oid.volid, class_oid.pageid, class_oid.slotid);

      if (class_name)
	{
	  free_and_init (class_name);
	}
      if (btname)
	{
	  free_and_init (btname);
	}
    }

  /* output header information */
  fprintf (fp,
	   "--- Page_Id: {%d , %d} Node_Type: %s Level: %d Key_Cnt: %d Next_Page_Id: "
	   "{%d , %d} Prev_Page_Id: {%d , %d} Used: %d ---\n\n",
	   pg_vpid->volid, pg_vpid->pageid,
	   node_type_to_string (node_type), node_header.node_level, key_cnt,
	   next_vpid.volid, next_vpid.pageid,
	   prev_vpid.volid, prev_vpid.pageid,
	   DB_PAGESIZE - spage_get_free_space (thread_p, page_ptr));
  fflush (fp);

  if (key_cnt < 0)
    {
      fprintf (fp,
	       "btree_dump_page: node key count underflow: %d\n", key_cnt);
      return;
    }

  if (dump_key == true)
    {
      /* output the content of each record */
      for (i = 1; i <= key_cnt; i++)
	{
	  (void) spage_get_record (page_ptr, i, &rec, PEEK);

	  fprintf (fp, "%5d ", i);

	  if (node_type == BTREE_LEAF_NODE)
	    {
	      btree_dump_leaf_record (thread_p, fp, btid, &rec, n);
	    }
	  else
	    {
	      btree_dump_non_leaf_record (thread_p, fp, btid, &rec, n, 1);
	    }

	  fprintf (fp, "\n");
	}

      if (node_type != BTREE_LEAF_NODE)
	{
	  /* print the last record of a non leaf page, it has no key */
	  (void) spage_get_record (page_ptr, key_cnt + 1, &rec, PEEK);

	  fprintf (fp, "%5d ", i);

	  btree_dump_non_leaf_record (thread_p, fp, btid, &rec, n, 0);
	  fprintf (fp, "(Last Rec, Key ignored)\n\n");
	}
    }

}

static const char *
node_type_to_string (short node_type)
{
  return (node_type == BTREE_LEAF_NODE) ? "LEAF" : "NON_LEAF";
}
