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
 * object_primitive.c - This file contains code for handling the values of
 *                      primitive types in memory and for conversion between
 *                      the disk representation.
 */

#ident "$Id$"

#include "config.h"


#include <stdlib.h>
#include <string.h>

#include "system_parameter.h"
#include "misc_string.h"
#include "db.h"
#if !defined (SERVER_MODE)
#include "work_space.h"
#include "schema_manager.h"
#endif /* !SERVER_MODE */
#include "object_primitive.h"
#include "object_representation.h"
#include "set_object.h"
#if !defined (SERVER_MODE)
#include "locator_cl.h"
#endif /* !SERVER_MODE */
#include "object_print.h"
#include "memory_alloc.h"
#include "intl_support.h"
#include "language_support.h"
#include "string_opfunc.h"
#include "object_accessor.h"
#if !defined (SERVER_MODE)
#include "transform_cl.h"
#endif /* !SERVER_MODE */
#include "server_interface.h"

#if defined (SERVER_MODE)
#include "thread.h"
#endif

/* this must be the last header file included!!! */
#include "dbval.h"

#if !defined(SERVER_MODE)
extern unsigned int db_on_server;
#endif

#define MR_CMP(d1, d2)                                     \
     ((d1) < (d2) ? DB_LT : (d1) > (d2) ? DB_GT : DB_EQ)

#define MR_CMP_RETURN_CODE(c)                              \
     ((c) < 0 ? DB_LT : (c) > 0 ? DB_GT : DB_EQ)

/*
 * MR_OID_SIZE
 * Hack so we don't have to conditionalize the type vector.
 * On the server we don't have the structure definition for this.
 */
#if !defined (SERVER_MODE)
#define MR_OID_SIZE sizeof(WS_MEMOID)
#else
#define MR_OID_SIZE 0
#endif

/*
 * PR_OID_PROMOTION_DEFAULT
 *
 * It is intended to allow some control over whether or not OIDs read
 * by "readval" are automatically promoted to MOPs or not.  This
 * is used to prevent cluttering up the workspace with MOPs when we
 * don't really need them.  When this is enabled, callers of "readval"
 * had better be prepared to see DB_VALUE structures containing unswizzled
 * OIDs rather than MOPs.  Not intended for use by by real applications.
 * Think about this more, this may also be the mechanism by which the server
 * can control reading of object values.
 *
 * Note that this makes sense only for the "readval" function, the "readmem
 * function must be building a workspace instance memory representation
 * and oid promotion cannot be controlled there.
 *
 */
#if defined (SERVER_MODE)
#define PR_INHIBIT_OID_PROMOTION_DEFAULT 1
#else
#define PR_INHIBIT_OID_PROMOTION_DEFAULT 0
#endif

/*
 * These are currently fixed length widets of length DB_NUMERIC_BUF_SIZE.
 * Ultimately they may be of variable size based on the precision and scale,
 * in antipication of that move, use the followign function for copying.
 */
#define OR_NUMERIC_SIZE(precision) DB_NUMERIC_BUF_SIZE
#define MR_NUMERIC_SIZE(precision) DB_NUMERIC_BUF_SIZE

#define STR_SIZE(prec, codeset)                                             \
     (((codeset) == INTL_CODESET_RAW_BITS) ? (BITS_TO_BYTES(prec) :		 \
      INTL_CODESET_MULT (codeset) * (prec))

/* left for future extension */
#define DO_CONVERSION_TO_SRVR_STR(codeset)  false
#define DO_CONVERSION_TO_SQLTEXT(codeset)   false


static void mr_initmem_string (void *mem, TP_DOMAIN * domain);
static int mr_setmem_string (void *memptr, TP_DOMAIN * domain,
			     DB_VALUE * value);
static int mr_getmem_string (void *memptr, TP_DOMAIN * domain,
			     DB_VALUE * value, bool copy);
static int mr_data_lengthmem_string (void *memptr, TP_DOMAIN * domain,
				     int disk);
static int mr_index_lengthmem_string (void *memptr, TP_DOMAIN * domain);
static void mr_data_writemem_string (OR_BUF * buf, void *memptr,
				     TP_DOMAIN * domain);
static void mr_data_readmem_string (OR_BUF * buf, void *memptr,
				    TP_DOMAIN * domain, int size);
static void mr_freemem_string (void *memptr);
static void mr_initval_string (DB_VALUE * value, int precision, int scale);
static int mr_setval_string (DB_VALUE * dest, const DB_VALUE * src,
			     bool copy);
static int mr_data_lengthval_string (const DB_VALUE * value, int disk);
static int mr_data_writeval_string (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_string (OR_BUF * buf, DB_VALUE * value,
				   TP_DOMAIN * domain, int size, bool copy);
static int mr_index_lengthval_string (const DB_VALUE * value);
static int mr_index_writeval_string (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_string (OR_BUF * buf, DB_VALUE * value,
				    int precision, int scale,
				    int collation_id, int size, bool copy);
static int mr_lengthval_string_internal (const DB_VALUE * value, int disk,
					 int align);
static int mr_writeval_string_internal (OR_BUF * buf, DB_VALUE * value,
					int align);
static int mr_readval_string_internal (OR_BUF * buf, DB_VALUE * value,
				       int precision, int scale,
				       int collation_id, int size, bool copy,
				       int align);
static int mr_index_cmpdisk_string (void *mem1, void *mem2, int precision,
				    int scale, int collation_id);
static int mr_data_cmpdisk_string (void *mem1, void *mem2, TP_DOMAIN * domain,
				   int do_coercion, int total_order);
static int mr_cmpval_string (DB_VALUE * value1, DB_VALUE * value2,
			     int do_coercion, int total_order, int collation);
static void mr_initmem_varbit (void *mem, TP_DOMAIN * domain);
static int mr_setmem_varbit (void *memptr, TP_DOMAIN * domain,
			     DB_VALUE * value);
static int mr_getmem_varbit (void *memptr, TP_DOMAIN * domain,
			     DB_VALUE * value, bool copy);
static int mr_data_lengthmem_varbit (void *memptr, TP_DOMAIN * domain,
				     int disk);
static int mr_index_lengthmem_varbit (void *memptr, TP_DOMAIN * domain);
static void mr_data_writemem_varbit (OR_BUF * buf, void *memptr,
				     TP_DOMAIN * domain);
static void mr_data_readmem_varbit (OR_BUF * buf, void *memptr,
				    TP_DOMAIN * domain, int size);
static void mr_freemem_varbit (void *memptr);
static void mr_initval_varbit (DB_VALUE * value, int precision, int scale);
static int mr_setval_varbit (DB_VALUE * dest, const DB_VALUE * src,
			     bool copy);
static int mr_data_lengthval_varbit (const DB_VALUE * value, int disk);
static int mr_data_writeval_varbit (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_varbit (OR_BUF * buf, DB_VALUE * value,
				   TP_DOMAIN * domain, int size, bool copy);
static int mr_index_lengthval_varbit (const DB_VALUE * value);
static int mr_index_writeval_varbit (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_varbit (OR_BUF * buf, DB_VALUE * value,
				    int precision, int scale,
				    int collation_id, int size, bool copy);
static int mr_lengthval_varbit_internal (const DB_VALUE * value, int disk,
					 int align);
static int mr_writeval_varbit_internal (OR_BUF * buf, const DB_VALUE * value,
					int align);
static int mr_readval_varbit_internal (OR_BUF * buf, DB_VALUE * value,
				       int precision, int scale,
				       int collation_id, int size, bool copy,
				       int align);
static int mr_index_cmpdisk_varbit (void *mem1, void *mem2, int precision,
				    int scale, int collation_id);
static int mr_data_cmpdisk_varbit (void *mem1, void *mem2, TP_DOMAIN * domain,
				   int do_coercion, int total_order);
static int mr_cmpval_varbit (DB_VALUE * value1, DB_VALUE * value2,
			     int do_coercion, int total_order, int collation);

static void mr_initmem_null (void *mem, TP_DOMAIN * domain);
static int mr_setmem_null (void *memptr, TP_DOMAIN * domain,
			   DB_VALUE * value);
static int mr_getmem_null (void *memptr, TP_DOMAIN * domain,
			   DB_VALUE * value, bool copy);
static void mr_data_writemem_null (OR_BUF * buf, void *memptr,
				   TP_DOMAIN * domain);
static void mr_data_readmem_null (OR_BUF * buf, void *memptr,
				  TP_DOMAIN * domain, int size);
static void mr_initval_null (DB_VALUE * value, int precision, int scale);
static int mr_setval_null (DB_VALUE * dest, const DB_VALUE * src, bool copy);
static int mr_data_writeval_null (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_null (OR_BUF * buf, DB_VALUE * value,
				 TP_DOMAIN * domain, int size, bool copy);
static int mr_data_cmpdisk_null (void *mem1, void *mem2, TP_DOMAIN * domain,
				 int do_coercion, int total_order);
static int mr_cmpval_null (DB_VALUE * value1, DB_VALUE * value2,
			   int do_coercion, int total_order, int collation);
static void mr_initmem_int (void *mem, TP_DOMAIN * domain);
static int mr_setmem_int (void *mem, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_int (void *mem, TP_DOMAIN * domain, DB_VALUE * value,
			  bool copy);
static void mr_data_writemem_int (OR_BUF * buf, void *mem,
				  TP_DOMAIN * domain);
static void mr_data_readmem_int (OR_BUF * buf, void *mem, TP_DOMAIN * domain,
				 int size);
static void mr_initval_int (DB_VALUE * value, int precision, int scale);
static int mr_setval_int (DB_VALUE * dest, const DB_VALUE * src, bool copy);
static int mr_data_writeval_int (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_int (OR_BUF * buf, DB_VALUE * value,
				TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_int (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_int (OR_BUF * buf, DB_VALUE * value,
				 int precision, int scale, int collation_id,
				 int size, bool copy);
static int mr_index_cmpdisk_int (void *mem1, void *mem2, int precision,
				 int scale, int collation_id);
static int mr_data_cmpdisk_int (void *mem1, void *mem2, TP_DOMAIN * domain,
				int do_coercion, int total_order);
static int mr_cmpval_int (DB_VALUE * value1, DB_VALUE * value2,
			  int do_coercion, int total_order, int collation);
static void mr_initmem_bigint (void *mem, TP_DOMAIN * domain);
static int mr_setmem_bigint (void *mem, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_bigint (void *mem, TP_DOMAIN * domain, DB_VALUE * value,
			     bool copy);
static void mr_data_writemem_bigint (OR_BUF * buf, void *mem,
				     TP_DOMAIN * domain);
static void mr_data_readmem_bigint (OR_BUF * buf, void *mem,
				    TP_DOMAIN * domain, int size);
static void mr_initval_bigint (DB_VALUE * value, int precision, int scale);
static int mr_setval_bigint (DB_VALUE * dest, const DB_VALUE * src,
			     bool copy);
static int mr_data_writeval_bigint (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_bigint (OR_BUF * buf, DB_VALUE * value,
				   TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_bigint (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_bigint (OR_BUF * buf, DB_VALUE * value,
				    int precision, int scale,
				    int collation_id, int size, bool copy);
static int mr_index_cmpdisk_bigint (void *mem1, void *mem2, int precision,
				    int scale, int collation_id);
static int mr_data_cmpdisk_bigint (void *mem1, void *mem2, TP_DOMAIN * domain,
				   int do_coercion, int total_order);
static int mr_cmpval_bigint (DB_VALUE * value1, DB_VALUE * value2,
			     int do_coercion, int total_order, int collation);
static void mr_initmem_double (void *mem, TP_DOMAIN * domain);
static int mr_setmem_double (void *mem, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_double (void *mem, TP_DOMAIN * domain,
			     DB_VALUE * value, bool copy);
static void mr_data_writemem_double (OR_BUF * buf, void *mem,
				     TP_DOMAIN * domain);
static void mr_data_readmem_double (OR_BUF * buf, void *mem,
				    TP_DOMAIN * domain, int size);
static void mr_initval_double (DB_VALUE * value, int precision, int scale);
static int mr_setval_double (DB_VALUE * dest, const DB_VALUE * src,
			     bool copy);
static int mr_data_writeval_double (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_double (OR_BUF * buf, DB_VALUE * value,
				   TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_double (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_double (OR_BUF * buf, DB_VALUE * value,
				    int precision, int scale,
				    int collation_id, int size, bool copy);
static int mr_index_cmpdisk_double (void *mem1, void *mem2, int precision,
				    int scale, int collation_id);
static int mr_data_cmpdisk_double (void *mem1, void *mem2, TP_DOMAIN * domain,
				   int do_coercion, int total_order);
static int mr_cmpval_double (DB_VALUE * value1, DB_VALUE * value2,
			     int do_coercion, int total_order, int collation);
static void mr_initmem_time (void *mem, TP_DOMAIN * domain);
static int mr_setmem_time (void *mem, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_time (void *mem, TP_DOMAIN * domain,
			   DB_VALUE * value, bool copy);
static void mr_data_writemem_time (OR_BUF * buf, void *mem,
				   TP_DOMAIN * domain);
static void mr_data_readmem_time (OR_BUF * buf, void *mem, TP_DOMAIN * domain,
				  int size);
static void mr_initval_time (DB_VALUE * value, int precision, int scale);
static int mr_setval_time (DB_VALUE * dest, const DB_VALUE * src, bool copy);
static int mr_data_writeval_time (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_time (OR_BUF * buf, DB_VALUE * value,
				 TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_time (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_time (OR_BUF * buf, DB_VALUE * value,
				  int precision, int scale, int collation_id,
				  int size, bool copy);
static int mr_index_cmpdisk_time (void *mem1, void *mem2, int precision,
				  int scale, int collation_id);
static int mr_data_cmpdisk_time (void *mem1, void *mem2, TP_DOMAIN * domain,
				 int do_coercion, int total_order);
static int mr_cmpval_time (DB_VALUE * value1, DB_VALUE * value2,
			   int do_coercion, int total_order, int collation);

static void mr_initmem_datetime (void *mem, TP_DOMAIN * domain);
static void mr_initval_datetime (DB_VALUE * value, int precision, int scale);
static int mr_setmem_datetime (void *mem, TP_DOMAIN * domain,
			       DB_VALUE * value);
static int mr_getmem_datetime (void *mem, TP_DOMAIN * domain,
			       DB_VALUE * value, bool copy);
static int mr_setval_datetime (DB_VALUE * dest, const DB_VALUE * src,
			       bool copy);
static void mr_data_writemem_datetime (OR_BUF * buf, void *mem,
				       TP_DOMAIN * domain);
static void mr_data_readmem_datetime (OR_BUF * buf, void *mem,
				      TP_DOMAIN * domain, int size);
static int mr_data_writeval_datetime (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_datetime (OR_BUF * buf, DB_VALUE * value,
				     TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_datetime (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_datetime (OR_BUF * buf, DB_VALUE * value,
				      int precision, int scale,
				      int collation_id, int size, bool copy);
static int mr_index_cmpdisk_datetime (void *mem1, void *mem2, int precision,
				      int scale, int collation_id);
static int mr_data_cmpdisk_datetime (void *mem1, void *mem2,
				     TP_DOMAIN * domain, int do_coercion,
				     int total_order);
static int mr_cmpval_datetime (DB_VALUE * value1, DB_VALUE * value2,
			       int do_coercion, int total_order,
			       int collation);

static void mr_initmem_date (void *mem, TP_DOMAIN * domain);
static int mr_setmem_date (void *mem, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_date (void *mem, TP_DOMAIN * domain,
			   DB_VALUE * value, bool copy);
static void mr_data_writemem_date (OR_BUF * buf, void *mem,
				   TP_DOMAIN * domain);
static void mr_data_readmem_date (OR_BUF * buf, void *mem, TP_DOMAIN * domain,
				  int size);
static void mr_initval_date (DB_VALUE * value, int precision, int scale);
static int mr_setval_date (DB_VALUE * dest, const DB_VALUE * src, bool copy);
static int mr_data_writeval_date (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_date (OR_BUF * buf, DB_VALUE * value,
				 TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_date (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_date (OR_BUF * buf, DB_VALUE * value,
				  int precision, int scale, int collation_id,
				  int size, bool copy);
static int mr_index_cmpdisk_date (void *mem1, void *mem2, int precision,
				  int scale, int collation_id);
static int mr_data_cmpdisk_date (void *mem1, void *mem2, TP_DOMAIN * domain,
				 int do_coercion, int total_order);
static int mr_cmpval_date (DB_VALUE * value1, DB_VALUE * value2,
			   int do_coercion, int total_order, int collation);
static void mr_null_oid (OID * oid);
static void mr_initmem_object (void *mem, TP_DOMAIN * domain);
static void mr_initval_object (DB_VALUE * value, int precision, int scale);
static int mr_setmem_object (void *memptr, TP_DOMAIN * domain,
			     DB_VALUE * value);
static int mr_getmem_object (void *memptr, TP_DOMAIN * domain,
			     DB_VALUE * value, bool copy);
static int mr_setval_object (DB_VALUE * dest, const DB_VALUE * src,
			     bool copy);
static int mr_data_lengthval_object (const DB_VALUE * value, int disk);
static void mr_data_writemem_object (OR_BUF * buf, void *memptr,
				     TP_DOMAIN * domain);
static void mr_data_readmem_object (OR_BUF * buf, void *memptr,
				    TP_DOMAIN * domain, int size);
static int mr_data_writeval_object (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_object (OR_BUF * buf, DB_VALUE * value,
				   TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_object (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_object (OR_BUF * buf, DB_VALUE * value,
				    int precision, int scale,
				    int collation_id, int size, bool copy);
static int mr_index_cmpdisk_object (void *mem1, void *mem2, int precision,
				    int scale, int collation_id);
static int mr_data_cmpdisk_object (void *mem1, void *mem2, TP_DOMAIN * domain,
				   int do_coercion, int total_order);
static int mr_cmpval_object (DB_VALUE * value1, DB_VALUE * value2,
			     int do_coercion, int total_order, int collation);

static void mr_initval_variable (DB_VALUE * value, int precision, int scale);
static int mr_setval_variable (DB_VALUE * dest, const DB_VALUE * src,
			       bool copy);
static int mr_data_lengthval_variable (const DB_VALUE * value, int disk);
static int mr_data_writeval_variable (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_variable (OR_BUF * buf, DB_VALUE * value,
				     TP_DOMAIN * domain, int size, bool copy);
static int mr_data_cmpdisk_variable (void *mem1, void *mem2,
				     TP_DOMAIN * domain,
				     int do_coercion, int total_order);
static int mr_cmpval_variable (DB_VALUE * value1, DB_VALUE * value2,
			       int do_coercion, int total_order,
			       int collation);
static void mr_initmem_sub (void *mem, TP_DOMAIN * domain);
static void mr_initval_sub (DB_VALUE * value, int precision, int scale);
static int mr_setmem_sub (void *mem, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_sub (void *mem, TP_DOMAIN * domain, DB_VALUE * value,
			  bool copy);
static int mr_setval_sub (DB_VALUE * dest, const DB_VALUE * src, bool copy);
static int mr_data_lengthmem_sub (void *mem, TP_DOMAIN * domain, int disk);
static void mr_data_writemem_sub (OR_BUF * buf, void *mem,
				  TP_DOMAIN * domain);
static void mr_data_readmem_sub (OR_BUF * buf, void *mem, TP_DOMAIN * domain,
				 int size);
static int mr_data_lengthval_sub (const DB_VALUE * value, int disk);
static int mr_data_writeval_sub (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_sub (OR_BUF * buf, DB_VALUE * value,
				TP_DOMAIN * domain, int size, bool copy);
static int mr_data_cmpdisk_sub (void *mem1, void *mem2, TP_DOMAIN * domain,
				int do_coercion, int total_order);
static int mr_cmpval_sub (DB_VALUE * value1, DB_VALUE * value2,
			  int do_coercion, int total_order, int collation);
static void mr_initmem_oid (void *memptr, TP_DOMAIN * domain);
static void mr_initval_oid (DB_VALUE * value, int precision, int scale);
static int mr_setmem_oid (void *memptr, TP_DOMAIN * domain, DB_VALUE * value);
static int mr_getmem_oid (void *memptr, TP_DOMAIN * domain,
			  DB_VALUE * value, bool copy);
static int mr_setval_oid (DB_VALUE * dest, const DB_VALUE * src, bool copy);
static void mr_data_writemem_oid (OR_BUF * buf, void *memptr,
				  TP_DOMAIN * domain);
static void mr_data_readmem_oid (OR_BUF * buf, void *memptr,
				 TP_DOMAIN * domain, int size);
static int mr_data_writeval_oid (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_oid (OR_BUF * buf, DB_VALUE * value,
				TP_DOMAIN * domain, int size, bool copy);
static int mr_index_writeval_oid (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_oid (OR_BUF * buf, DB_VALUE * value,
				 int precision, int scale, int collation_id,
				 int size, bool copy);
static int mr_index_cmpdisk_oid (void *mem1, void *mem2, int precision,
				 int scale, int collation_id);
static int mr_data_cmpdisk_oid (void *mem1, void *mem2, TP_DOMAIN * domain,
				int do_coercion, int total_order);
static int mr_cmpval_oid (DB_VALUE * value1, DB_VALUE * value2,
			  int do_coercion, int total_order, int collation);
static void mr_initmem_sequence (void *memptr, TP_DOMAIN * domain);
static int mr_setmem_sequence (void *memptr, TP_DOMAIN * domain,
			       DB_VALUE * value);
static int mr_setval_set_internal (DB_VALUE * dest, const DB_VALUE * src,
				   bool copy, DB_TYPE set_type);
static int mr_data_lengthmem_sequence (void *memptr, TP_DOMAIN * domain,
				       int disk);
static void mr_data_writemem_sequence (OR_BUF * buf, void *memptr,
				       TP_DOMAIN * domain);
static void mr_data_readmem_sequence (OR_BUF * buf, void *memptr,
				      TP_DOMAIN * domain, int size);
static int mr_data_lengthval_sequence (const DB_VALUE * value, int disk);
static int mr_data_writeval_sequence (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_sequence (OR_BUF * buf, DB_VALUE * value,
				     TP_DOMAIN * domain, int size, bool copy);
static void mr_freemem_sequence (void *memptr);
static void mr_initval_sequence (DB_VALUE * value, int precision, int scale);
static int mr_getmem_sequence (void *memptr, TP_DOMAIN * domain,
			       DB_VALUE * value, bool copy);
static int mr_setval_sequence (DB_VALUE * dest, const DB_VALUE * src,
			       bool copy);
static int mr_data_cmpdisk_sequence (void *mem1, void *mem2,
				     TP_DOMAIN * domain, int do_coercion,
				     int total_order);
static int mr_cmpval_sequence (DB_VALUE * value1, DB_VALUE * value2,
			       int do_coercion, int total_order,
			       int collation);
static void mr_initmem_numeric (void *memptr, TP_DOMAIN * domain);
static int mr_setmem_numeric (void *mem, TP_DOMAIN * domain,
			      DB_VALUE * value);
static int mr_getmem_numeric (void *mem, TP_DOMAIN * domain,
			      DB_VALUE * value, bool copy);
static int mr_data_lengthmem_numeric (void *mem, TP_DOMAIN * domain,
				      int disk);
static int mr_index_lengthmem_numeric (void *mem, TP_DOMAIN * domain);
static void mr_data_writemem_numeric (OR_BUF * buf, void *mem,
				      TP_DOMAIN * domain);
static void mr_data_readmem_numeric (OR_BUF * buf, void *mem,
				     TP_DOMAIN * domain, int size);
static void mr_initval_numeric (DB_VALUE * value, int precision, int scale);
static int mr_setval_numeric (DB_VALUE * dest, const DB_VALUE * src,
			      bool copy);
static int mr_data_lengthval_numeric (const DB_VALUE * value, int disk);
static int mr_data_writeval_numeric (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_numeric (OR_BUF * buf, DB_VALUE * value,
				    TP_DOMAIN * domain, int size, bool copy);
static int mr_index_lengthval_numeric (const DB_VALUE * value);
static int mr_index_writeval_numeric (OR_BUF * buf, const DB_VALUE * value);
static int mr_index_readval_numeric (OR_BUF * buf, DB_VALUE * value,
				     int precision, int scale,
				     int collation_id, int size, bool copy);
static int mr_index_cmpdisk_numeric (void *mem1, void *mem2, int precision,
				     int scale, int collation_id);
static int mr_data_cmpdisk_numeric (void *mem1, void *mem2,
				    TP_DOMAIN * domain, int do_coercion,
				    int total_order);
static int mr_cmpval_numeric (DB_VALUE * value1, DB_VALUE * value2,
			      int do_coercion, int total_order,
			      int collation);
static void mr_initmem_resultset (void *mem, TP_DOMAIN * domain);
static int mr_setmem_resultset (void *mem, TP_DOMAIN * domain,
				DB_VALUE * value);
static int mr_getmem_resultset (void *mem, TP_DOMAIN * domain,
				DB_VALUE * value, bool copy);
static void mr_data_writemem_resultset (OR_BUF * buf, void *mem,
					TP_DOMAIN * domain);
static void mr_data_readmem_resultset (OR_BUF * buf, void *mem,
				       TP_DOMAIN * domain, int size);
static void mr_initval_resultset (DB_VALUE * value, int precision, int scale);
static int mr_setval_resultset (DB_VALUE * dest, const DB_VALUE * src,
				bool copy);
static int mr_data_writeval_resultset (OR_BUF * buf, const DB_VALUE * value);
static int mr_data_readval_resultset (OR_BUF * buf, DB_VALUE * value,
				      TP_DOMAIN * domain, int size,
				      bool copy);
static int mr_data_cmpdisk_resultset (void *mem1, void *mem2,
				      TP_DOMAIN * domain, int do_coercion,
				      int total_order);
static int mr_cmpval_resultset (DB_VALUE * value1, DB_VALUE * value2,
				int do_coercion, int total_order,
				int collation);

int pr_Inhibit_oid_promotion = PR_INHIBIT_OID_PROMOTION_DEFAULT;

PR_TYPE tp_Null = {
  "*NULL*", DB_TYPE_NULL, 0, 0, 0, 0,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_null,
  mr_initval_null,
  mr_setmem_null,
  mr_getmem_null,
  mr_setval_null,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_null,
  mr_data_readmem_null,
  mr_data_writeval_null,
  mr_data_readval_null,
  NULL,				/* index_lenghmem */
  NULL,				/* index_lenghval */
  NULL,				/* index_writeval */
  NULL,				/* index_readval */
  NULL,				/* index_cmpdisk */
  NULL,				/* freemem */
  mr_data_cmpdisk_null,
  mr_cmpval_null
};

PR_TYPE *tp_Type_null = &tp_Null;

PR_TYPE tp_Integer = {
  "integer", DB_TYPE_INTEGER, 0, sizeof (int), sizeof (int), 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_int,
  mr_initval_int,
  mr_setmem_int,
  mr_getmem_int,
  mr_setval_int,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_int,
  mr_data_readmem_int,
  mr_data_writeval_int,
  mr_data_readval_int,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  mr_index_writeval_int,
  mr_index_readval_int,
  mr_index_cmpdisk_int,
  NULL,				/* freemem */
  mr_data_cmpdisk_int,
  mr_cmpval_int
};

PR_TYPE *tp_Type_integer = &tp_Integer;

#if 1				/* TODO - */
#define tp_Short_disksize (sizeof (short))
#endif

PR_TYPE tp_Bigint = {
  "bigint", DB_TYPE_BIGINT, 0, sizeof (DB_BIGINT), sizeof (DB_BIGINT), 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_bigint,
  mr_initval_bigint,
  mr_setmem_bigint,
  mr_getmem_bigint,
  mr_setval_bigint,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_bigint,
  mr_data_readmem_bigint,
  mr_data_writeval_bigint,
  mr_data_readval_bigint,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  mr_index_writeval_bigint,
  mr_index_readval_bigint,
  mr_index_cmpdisk_bigint,
  NULL,				/* freemem */
  mr_data_cmpdisk_bigint,
  mr_cmpval_bigint
};

PR_TYPE *tp_Type_bigint = &tp_Bigint;

PR_TYPE tp_Double = {
  "double", DB_TYPE_DOUBLE, 0, sizeof (double), sizeof (double), 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_double,
  mr_initval_double,
  mr_setmem_double,
  mr_getmem_double,
  mr_setval_double,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lenghtval */
  mr_data_writemem_double,
  mr_data_readmem_double,
  mr_data_writeval_double,
  mr_data_readval_double,
  NULL,				/* index_lenghtmem */
  NULL,				/* index_lenghtval */
  mr_index_writeval_double,
  mr_index_readval_double,
  mr_index_cmpdisk_double,
  NULL,				/* freemem */
  mr_data_cmpdisk_double,
  mr_cmpval_double
};

PR_TYPE *tp_Type_double = &tp_Double;

PR_TYPE tp_Time = {
  "time", DB_TYPE_TIME, 0, sizeof (DB_TIME), OR_TIME_SIZE, 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_time,
  mr_initval_time,
  mr_setmem_time,
  mr_getmem_time,
  mr_setval_time,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_time,
  mr_data_readmem_time,
  mr_data_writeval_time,
  mr_data_readval_time,
  NULL,				/* index_lenghmem */
  NULL,				/* index_lenghval */
  mr_index_writeval_time,
  mr_index_readval_time,
  mr_index_cmpdisk_time,
  NULL,				/* freemem */
  mr_data_cmpdisk_time,
  mr_cmpval_time
};

PR_TYPE *tp_Type_time = &tp_Time;

PR_TYPE tp_Datetime = {
  "datetime", DB_TYPE_DATETIME, 0, sizeof (DB_DATETIME), OR_DATETIME_SIZE, 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_datetime,
  mr_initval_datetime,
  mr_setmem_datetime,
  mr_getmem_datetime,
  mr_setval_datetime,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_datetime,
  mr_data_readmem_datetime,
  mr_data_writeval_datetime,
  mr_data_readval_datetime,
  NULL,				/* index_lenghmem */
  NULL,				/* index_lenghval */
  mr_index_writeval_datetime,
  mr_index_readval_datetime,
  mr_index_cmpdisk_datetime,
  NULL,				/* freemem */
  mr_data_cmpdisk_datetime,
  mr_cmpval_datetime
};

PR_TYPE *tp_Type_datetime = &tp_Datetime;

PR_TYPE tp_Date = {
  "date", DB_TYPE_DATE, 0, sizeof (DB_DATE), OR_DATE_SIZE, 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_date,
  mr_initval_date,
  mr_setmem_date,
  mr_getmem_date,
  mr_setval_date,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_date,
  mr_data_readmem_date,
  mr_data_writeval_date,
  mr_data_readval_date,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_index_writeval_date,
  mr_index_readval_date,
  mr_index_cmpdisk_date,
  NULL,				/* freemem */
  mr_data_cmpdisk_date,
  mr_cmpval_date
};

PR_TYPE *tp_Type_date = &tp_Date;

/*
 * tp_Object
 *
 * ALERT!!! We set the alignment for OIDs to 8 even though they only have an
 * int and two shorts.  This is done because the WS_MEMOID has a pointer
 * following the OID and it must be on an 8 byte boundary for the Alpha boxes.
 */

PR_TYPE tp_Object = {
  "object", DB_TYPE_OBJECT, 0, MR_OID_SIZE, OR_OID_SIZE, 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_object,
  mr_initval_object,
  mr_setmem_object,
  mr_getmem_object,
  mr_setval_object,
  NULL,				/* data_lengthmem */
  mr_data_lengthval_object,
  mr_data_writemem_object,
  mr_data_readmem_object,
  mr_data_writeval_object,
  mr_data_readval_object,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_index_writeval_object,
  mr_index_readval_object,
  mr_index_cmpdisk_object,
  NULL,				/* freemem */
  mr_data_cmpdisk_object,
  mr_cmpval_object
};

PR_TYPE *tp_Type_object = &tp_Object;

PR_TYPE tp_Variable = {
  "*variable*", DB_TYPE_VARIABLE, 1, sizeof (DB_VALUE), 0, 4,
  help_fprint_value,
  help_sprint_value,
  NULL,				/* initmem */
  mr_initval_variable,
  NULL,				/* setmem */
  NULL,				/* getmem */
  mr_setval_variable,
  NULL,				/* data_lengthmem */
  mr_data_lengthval_variable,
  NULL,				/* data_writemem */
  NULL,				/* data_readmem */
  mr_data_writeval_variable,
  mr_data_readval_variable,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  NULL,				/* index_writeval */
  NULL,				/* index_readval */
  NULL,				/* index_cmpdisk */
  NULL,				/* freemem */
  mr_data_cmpdisk_variable,
  mr_cmpval_variable
};

PR_TYPE *tp_Type_variable = &tp_Variable;

PR_TYPE tp_Substructure = {
  "*substructure*", DB_TYPE_SUB, 1, sizeof (void *), 0, 8,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_sub,
  mr_initval_sub,
  mr_setmem_sub,
  mr_getmem_sub,
  mr_setval_sub,
  mr_data_lengthmem_sub,
  mr_data_lengthval_sub,
  mr_data_writemem_sub,
  mr_data_readmem_sub,
  mr_data_writeval_sub,
  mr_data_readval_sub,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  NULL,				/* index_writeval */
  NULL,				/* index_readval */
  NULL,				/* index_cmpdisk */
  NULL,				/* freemem */
  mr_data_cmpdisk_sub,
  mr_cmpval_sub
};

PR_TYPE *tp_Type_substructure = &tp_Substructure;

/*
 * tp_Oid
 *
 * ALERT!!! We set the alignment for OIDs to 8 even though they only have an
 * int and two shorts.  This is done because the WS_MEMOID has a pointer
 * following the OID and it must be on an 8 byte boundary for the Alpha boxes.
 */
PR_TYPE tp_Oid = {
  "*oid*", DB_TYPE_OID, 0, sizeof (OID), OR_OID_SIZE, 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_oid,
  mr_initval_oid,
  mr_setmem_oid,
  mr_getmem_oid,
  mr_setval_oid,
  NULL,				/* lengthmem */
  NULL,				/* lengthval */
  mr_data_writemem_oid,
  mr_data_readmem_oid,
  mr_data_writeval_oid,
  mr_data_readval_oid,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  mr_index_writeval_oid,
  mr_index_readval_oid,
  mr_index_cmpdisk_oid,
  NULL,				/* freemem */
  mr_data_cmpdisk_oid,
  mr_cmpval_oid
};

PR_TYPE *tp_Type_oid = &tp_Oid;

PR_TYPE tp_Sequence = {
  "sequence", DB_TYPE_SEQUENCE, 1, sizeof (SETOBJ *), 0, 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_sequence,
  mr_initval_sequence,
  mr_setmem_sequence,
  mr_getmem_sequence,
  mr_setval_sequence,
  mr_data_lengthmem_sequence,
  mr_data_lengthval_sequence,
  mr_data_writemem_sequence,
  mr_data_readmem_sequence,
  mr_data_writeval_sequence,
  mr_data_readval_sequence,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  NULL,				/* index_writeval */
  NULL,				/* index_readval */
  NULL,				/* index_cmpdisk */
  mr_freemem_sequence,
  mr_data_cmpdisk_sequence,
  mr_cmpval_sequence
};

PR_TYPE *tp_Type_sequence = &tp_Sequence;

PR_TYPE tp_Numeric = {
  "numeric", DB_TYPE_NUMERIC, 0, 0, 0, 1,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_numeric,
  mr_initval_numeric,
  mr_setmem_numeric,
  mr_getmem_numeric,
  mr_setval_numeric,
  mr_data_lengthmem_numeric,
  mr_data_lengthval_numeric,
  mr_data_writemem_numeric,
  mr_data_readmem_numeric,
  mr_data_writeval_numeric,
  mr_data_readval_numeric,
  mr_index_lengthmem_numeric,
  mr_index_lengthval_numeric,
  mr_index_writeval_numeric,
  mr_index_readval_numeric,
  mr_index_cmpdisk_numeric,
  NULL,				/* freemem */
  mr_data_cmpdisk_numeric,
  mr_cmpval_numeric
};

PR_TYPE *tp_Type_numeric = &tp_Numeric;


/*
 * tp_Type_id_map
 *    This quickly maps a type identifier to a type structure.
 *    This is dependent on the ordering of the DB_TYPE union so take
 *    care when modifying either of these.  It would be safer to build
 *    this at run time.
 */
PR_TYPE *tp_Type_id_map[] = {
  &tp_Null,
  &tp_Integer,
  &tp_Null,
  &tp_Double,
  &tp_String,
  &tp_Object,
  &tp_Null,
  &tp_Null,
  &tp_Sequence,
  &tp_Null,
  &tp_Time,
  &tp_Null,
  &tp_Date,
  &tp_Null,
  &tp_Variable,
  &tp_Substructure,
  &tp_Null,
  &tp_Null,
  &tp_Null,
  &tp_Null,
  &tp_Oid,
  &tp_Null,			/* place holder for DB_TYPE_DB_VALUE */
  &tp_Numeric,
  &tp_Null,
  &tp_VarBit,
  &tp_Null,
  &tp_Null,
  &tp_Null,
  &tp_ResultSet,
  &tp_Null,
  &tp_Null,
  &tp_Bigint,
  &tp_Datetime
};

PR_TYPE tp_ResultSet = {
  "resultset", DB_TYPE_RESULTSET, 0, sizeof (DB_RESULTSET),
  sizeof (DB_RESULTSET), 4,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_resultset,
  mr_initval_resultset,
  mr_setmem_resultset,
  mr_getmem_resultset,
  mr_setval_resultset,
  NULL,				/* data_lengthmem */
  NULL,				/* data_lengthval */
  mr_data_writemem_resultset,
  mr_data_readmem_resultset,
  mr_data_writeval_resultset,
  mr_data_readval_resultset,
  NULL,				/* index_lengthmem */
  NULL,				/* index_lengthval */
  NULL,				/* index_writeval */
  NULL,				/* index_readval */
  NULL,				/* index_cmpdisk */
  NULL,				/* freemem */
  mr_data_cmpdisk_resultset,
  mr_cmpval_resultset
};

PR_TYPE *tp_Type_resultset = &tp_ResultSet;

/*
 * DB_VALUE MAINTENANCE
 */

/*
 * pr_make_value - create an internal value container
 *    return: new value container
 * Note:
 *    The value is allocated within the main workspace blocks so that
 *    it will not serve as a root for the garbage collector.
 */
DB_VALUE *
pr_make_value (void)
{
  DB_VALUE *value;

  value = (DB_VALUE *) malloc (sizeof (DB_VALUE));

  if (value != NULL)
    {
      db_value_domain_init (value, DB_TYPE_NULL, DB_DEFAULT_PRECISION,
			    DB_DEFAULT_SCALE);
    }

  return value;
}

/*
 * pr_make_ext_value - creates an external value container suitable for
 * passing beyond the interface layer to application programs.
 *    return: new value container
 */
DB_VALUE *
pr_make_ext_value (void)
{
  DB_VALUE *value;

  value = (DB_VALUE *) malloc (sizeof (DB_VALUE));
  if (value != NULL)
    {
      db_value_domain_init (value, DB_TYPE_NULL, DB_DEFAULT_PRECISION,
			    DB_DEFAULT_SCALE);
    }

  return value;
}


/*
 * pr_clear_value - clear an internal or external DB_VALUE and
 *                  initialize it to the NULL state.
 *    return: NO_ERROR
 *    value(in/out): value to initialize
 * Note:
 *    Any external allocations (strings, sets, etc) will be freed.
 *
 *    There is too much type specific stuff in here.  We need to create a
 *    "freeval" type vector function to do this work.
 */
int
pr_clear_value (DB_VALUE * value)
{
  unsigned char *data;
  DB_TYPE db_type;

  if (value == NULL || DB_IS_NULL (value))
    {
      return NO_ERROR;		/* do nothing */
    }

  db_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (db_type)
    {
    case DB_TYPE_OBJECT:
      /* we need to be sure to NULL the object pointer so that this
       * db_value does not cause garbage collection problems by
       * retaining an object pointer.
       */
      value->data.op = NULL;
      break;

    case DB_TYPE_SEQUENCE:
      set_free (db_get_set (value));
      value->data.set = NULL;
      break;

    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARBIT:
      data = (unsigned char *) value->data.ch.buf;
      if (data != NULL)
	{
	  if (value->need_clear)
	    {
	      free_and_init (data);
	    }
	  /*
	   * Ack, phfffft!!! why should we have to know about the
	   * internals here?
	   */
	  value->data.ch.buf = NULL;
	}
      break;

    default:
      break;
    }

  /* always make sure the value gets cleared */
  PRIM_SET_NULL (value);
  value->need_clear = false;

  return NO_ERROR;
}

/*
 * pr_free_value - free an internval value container any anything that it
 * references
 *    return: NO_ERROR if successful, error code otherwise
 *    value(in/out): value to clear & free
 */
int
pr_free_value (DB_VALUE * value)
{
  int error = NO_ERROR;

  if (value != NULL)
    {
      /* some redundant checking but I want the semantics isolated */
      error = pr_clear_value (value);
      free_and_init (value);
    }
  return error;
}

/*
 * pr_free_ext_value - free an external value container and anything that it
 * references.
 *    return:
 *    value(in/out): external value to clear & free
 * Note:
 *    Identical to pr_free_value.
 */
int
pr_free_ext_value (DB_VALUE * value)
{
  int error = NO_ERROR;

  if (value != NULL)
    {
      /* some redundant checking but I want the semantics isolated */
      error = pr_clear_value (value);
      free_and_init (value);
    }

  return error;
}

/*
 * pr_clone_value - copy the contents of one value container to another.
 *    return: none
 *    src(in): source value
 *    dest(out): destination value
 * Note:
 *    For types that contain external allocations (like strings).
 *    The contents are copied.
 */
int
pr_clone_value (const DB_VALUE * src, DB_VALUE * dest)
{
  PR_TYPE *type;
  DB_TYPE src_dbtype;

  if (dest != NULL)
    {
      assert (dest->need_clear == false);
      assert (DB_IS_NULL (dest));

      if (src == NULL)
	{
	  db_make_null (dest);
	}
      else
	{
	  src_dbtype = DB_VALUE_DOMAIN_TYPE (src);

	  if (DB_IS_NULL (src))
	    {
	      db_value_domain_init (dest, src_dbtype,
				    DB_VALUE_PRECISION (src),
				    DB_VALUE_SCALE (src));
	      if (dest != NULL
		  && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (dest)))
		{
		  db_string_put_cs_and_collation (dest,
						  DB_GET_STRING_COLLATION
						  (src));
		}
	    }
	  else if (src != dest)
	    {
	      type = PR_TYPE_FROM_ID (src_dbtype);
	      if (type != NULL)
		{
		  /*
		   * Formerly called "initval" here but that was removed as
		   * "setval" is supposed to properly initialize the
		   * destination domain.  No need to do it twice.
		   * Make sure the COPY flag is set in the setval call.
		   */
		  (*(type->setval)) (dest, src, true);
		}
	      else
		{
		  /*
		   * can only get here in error conditions, initialize to NULL
		   */
		  db_make_null (dest);
		}
	    }
	}
    }

  return NO_ERROR;
}

/*
 * pr_copy_value - This creates a new internal value container with a copy
 * of the contents of the source container.
 *    return: new value
 *    value(in): value to copy
 */
DB_VALUE *
pr_copy_value (DB_VALUE * value)
{
  DB_VALUE *new_ = NULL;

  if (value != NULL)
    {
      new_ = pr_make_value ();
      if (pr_clone_value (value, new_) != NO_ERROR)
	{
	  /*
	   * oh well, couldn't allocate storage for the clone.
	   * Note that pr_free_value can only return errors in the
	   * case where the value has been initialized so it won't
	   * stomp on the error code if we call it here
	   */
	  pr_free_value (new_);
	  new_ = NULL;
	}
    }
  return new_;
}

/*
 * TYPE NULL
 */

/*
 * This is largely a placeholder type that does nothing except assert
 * that the size of a NULL value is zero.
 * The "mem" functions are no-ops since NULL is not a valid domain type.
 * The "value" functions don't do much, they just make sure the value
 * domain is initialized.
 *
 */

/*
 * mr_initmem_null - dummy function
 *    return:
 *    memptr():
 */
static void
mr_initmem_null (UNUSED_ARG void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
}

/*
 * mr_setmem_null - dummy function
 *    return:
 *    memptr():
 *    domain():
 *    value():
 */
static int
mr_setmem_null (UNUSED_ARG void *memptr, UNUSED_ARG TP_DOMAIN * domain,
		UNUSED_ARG DB_VALUE * value)
{
  return NO_ERROR;
}

/*
 * mr_getmem_null - dummy function
 *    return:
 *    memptr():
 *    domain():
 *    value():
 *    copy():
 */
static int
mr_getmem_null (UNUSED_ARG void *memptr, UNUSED_ARG TP_DOMAIN * domain,
		DB_VALUE * value, UNUSED_ARG bool copy)
{
  db_make_null (value);
  return NO_ERROR;
}

/*
 * mr_writemem_null - dummy function
 *    return:
 *    buf():
 *    memptr():
 *    domain():
 */
static void
mr_data_writemem_null (UNUSED_ARG OR_BUF * buf, UNUSED_ARG void *memptr,
		       UNUSED_ARG TP_DOMAIN * domain)
{
}

/*
 * mr_readmem_null - dummy function
 *    return:
 *    buf():
 *    memptr():
 *    domain():
 *    size():
 */
static void
mr_data_readmem_null (UNUSED_ARG OR_BUF * buf, UNUSED_ARG void *memptr,
		      UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
}

static void
mr_initval_null (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_NULL, precision, scale);
}

/*
 * mr_setval_null - dummy function
 *    return:
 *    dest():
 *    src():
 *    copy():
 */
static int
mr_setval_null (DB_VALUE * dest, UNUSED_ARG const DB_VALUE * src,
		UNUSED_ARG bool copy)
{
  mr_initval_null (dest, 0, 0);
  return NO_ERROR;
}

/*
 * mr_writeval_null - dummy function
 *    return:
 *    buf():
 *    value():
 */
static int
mr_data_writeval_null (UNUSED_ARG OR_BUF * buf,
		       UNUSED_ARG const DB_VALUE * value)
{
  return NO_ERROR;
}

/*
 * mr_readval_null - dummy function
 *    return:
 *    buf():
 *    value():
 *    domain():
 *    size():
 *    copy():
 *    copy_buf():
 *    copy_buf_len():
 */
static int
mr_data_readval_null (UNUSED_ARG OR_BUF * buf, DB_VALUE * value,
		      UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
		      UNUSED_ARG bool copy)
{
  if (value)
    {
      db_make_null (value);
      value->need_clear = false;
    }
  return NO_ERROR;
}

/*
 * mr_cmpdisk_null - dummy function
 *    return:
 *    mem1():
 *    mem2():
 *    domain():
 *    do_coercion():
 *    total_order():
 */
static int
mr_data_cmpdisk_null (UNUSED_ARG void *mem1, UNUSED_ARG void *mem2,
		      UNUSED_ARG TP_DOMAIN * domain,
		      UNUSED_ARG int do_coercion, UNUSED_ARG int total_order)
{
  assert (domain != NULL);

  return DB_UNK;
}

/*
 * mr_cmpval_null - dummy function
 *    return:
 *    value1():
 *    value2():
 *    do_coercion():
 *    total_order():
 *    start_colp():
 */
static int
mr_cmpval_null (UNUSED_ARG DB_VALUE * value1, UNUSED_ARG DB_VALUE * value2,
		UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		UNUSED_ARG int collation)
{
  return DB_UNK;
}


/*
 * TYPE INTEGER
 *
 * Your basic 32 bit signed integral value.
 * At the storage level, we don't really care whether it is signed or unsigned.
 */

static void
mr_initmem_int (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(int *) mem = 0;
}

static int
mr_setmem_int (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  if (value != NULL)
    *(int *) mem = DB_GET_INTEGER (value);
  else
    mr_initmem_int (mem, domain);

  return NO_ERROR;
}

static int
mr_getmem_int (void *mem, UNUSED_ARG TP_DOMAIN * domain, DB_VALUE * value,
	       UNUSED_ARG bool copy)
{
  return db_make_int (value, *(int *) mem);
}

static void
mr_data_writemem_int (OR_BUF * buf, void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  or_put_int (buf, *(int *) mem);
}

static void
mr_data_readmem_int (OR_BUF * buf, void *mem, UNUSED_ARG TP_DOMAIN * domain,
		     UNUSED_ARG int size)
{
  int rc = NO_ERROR;

  if (mem == NULL)
    {
      or_advance (buf, tp_Integer.disksize);
    }
  else
    {
      *(int *) mem = or_get_int (buf, &rc);
    }
}

static void
mr_initval_int (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_INTEGER, precision, scale);
  db_make_int (value, 0);
}

static int
mr_setval_int (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  if (src && !DB_IS_NULL (src))
    {
      return db_make_int (dest, DB_GET_INTEGER (src));
    }
  else
    {
      return db_value_domain_init (dest, DB_TYPE_INTEGER,
				   DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
    }
}

static int
mr_data_writeval_int (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_int (buf, DB_GET_INTEGER (value));
}

static int
mr_data_readval_int (OR_BUF * buf, DB_VALUE * value,
		     UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
		     UNUSED_ARG bool copy)
{
  int temp_int, rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Integer.disksize);
    }
  else
    {
      temp_int = or_get_int (buf, &rc);
      if (rc == NO_ERROR)
	{
	  db_make_int (value, temp_int);
	}
      value->need_clear = false;
    }
  return rc;
}

static int
mr_index_writeval_int (OR_BUF * buf, const DB_VALUE * value)
{
  int i;

  i = DB_GET_INTEGER (value);

  return or_put_data (buf, (char *) (&i), tp_Integer.disksize);
}

static int
mr_index_readval_int (OR_BUF * buf, DB_VALUE * value,
		      UNUSED_ARG int precision, UNUSED_ARG int scale,
		      UNUSED_ARG int collation_id, UNUSED_ARG int size,
		      UNUSED_ARG bool copy)
{
  int i, rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Integer.disksize);
    }
  else
    {
      rc = or_get_data (buf, (char *) (&i), tp_Integer.disksize);
      if (rc == NO_ERROR)
	{
	  db_make_int (value, i);
	}
      value->need_clear = false;
    }
  return rc;
}

static int
mr_index_cmpdisk_int (void *mem1, void *mem2, UNUSED_ARG int precision,
		      UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  int i1, i2;

  COPYMEM (int, &i1, mem1);
  COPYMEM (int, &i2, mem2);

  return MR_CMP (i1, i2);
}

static int
mr_data_cmpdisk_int (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
		     UNUSED_ARG int do_coercion, UNUSED_ARG int total_order)
{
  int i1, i2;

  assert (domain != NULL);

  i1 = OR_GET_INT (mem1);
  i2 = OR_GET_INT (mem2);

  return MR_CMP (i1, i2);
}

static int
mr_cmpval_int (DB_VALUE * value1, DB_VALUE * value2,
	       UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
	       UNUSED_ARG int collation)
{
  int i1, i2;

  i1 = DB_GET_INTEGER (value1);
  i2 = DB_GET_INTEGER (value2);

  return MR_CMP (i1, i2);
}

/*
 * TYPE BIGINT
 *
 * Your basic 64 bit signed integral value.
 * At the storage level, we don't really care whether it is signed or unsigned.
 */

static void
mr_initmem_bigint (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(DB_BIGINT *) mem = 0;
}

static int
mr_setmem_bigint (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  if (value != NULL)
    *(DB_BIGINT *) mem = db_get_bigint (value);
  else
    mr_initmem_bigint (mem, domain);

  return NO_ERROR;
}

static int
mr_getmem_bigint (void *mem, UNUSED_ARG TP_DOMAIN * domain, DB_VALUE * value,
		  UNUSED_ARG bool copy)
{
  return db_make_bigint (value, *(DB_BIGINT *) mem);
}

static void
mr_data_writemem_bigint (OR_BUF * buf, void *mem,
			 UNUSED_ARG TP_DOMAIN * domain)
{
  or_put_bigint (buf, *(DB_BIGINT *) mem);
}

static void
mr_data_readmem_bigint (OR_BUF * buf, void *mem,
			UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
  int rc = NO_ERROR;

  if (mem == NULL)
    {
      or_advance (buf, tp_Bigint.disksize);
    }
  else
    {
      *(DB_BIGINT *) mem = or_get_bigint (buf, &rc);
    }
}

static void
mr_initval_bigint (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_BIGINT, precision, scale);
  db_make_bigint (value, 0);
}

static int
mr_setval_bigint (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  if (src && !DB_IS_NULL (src))
    {
      return db_make_bigint (dest, db_get_bigint (src));
    }
  else
    {
      return db_value_domain_init (dest, DB_TYPE_BIGINT,
				   DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
    }
}

static int
mr_data_writeval_bigint (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_bigint (buf, db_get_bigint (value));
}

static int
mr_data_readval_bigint (OR_BUF * buf, DB_VALUE * value,
			UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
			UNUSED_ARG bool copy)
{
  int rc = NO_ERROR;
  DB_BIGINT temp_int;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Bigint.disksize);
    }
  else
    {
      temp_int = or_get_bigint (buf, &rc);
      if (rc == NO_ERROR)
	{
	  db_make_bigint (value, temp_int);
	}
      value->need_clear = false;
    }
  return rc;
}

static int
mr_index_writeval_bigint (OR_BUF * buf, const DB_VALUE * value)
{
  DB_BIGINT bi;

  bi = db_get_bigint (value);

  return or_put_data (buf, (char *) (&bi), tp_Bigint.disksize);
}

static int
mr_index_readval_bigint (OR_BUF * buf, DB_VALUE * value,
			 UNUSED_ARG int precision, UNUSED_ARG int scale,
			 UNUSED_ARG int collation_id, UNUSED_ARG int size,
			 UNUSED_ARG bool copy)
{
  int rc = NO_ERROR;
  DB_BIGINT bi;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Bigint.disksize);
    }
  else
    {
      rc = or_get_data (buf, (char *) (&bi), tp_Bigint.disksize);
      if (rc == NO_ERROR)
	{
	  db_make_bigint (value, bi);
	}
      value->need_clear = false;
    }

  return rc;
}

static int
mr_index_cmpdisk_bigint (void *mem1, void *mem2, UNUSED_ARG int precision,
			 UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  DB_BIGINT i1, i2;

  COPYMEM (DB_BIGINT, &i1, mem1);
  COPYMEM (DB_BIGINT, &i2, mem2);

  return MR_CMP (i1, i2);
}

static int
mr_data_cmpdisk_bigint (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
			UNUSED_ARG int do_coercion,
			UNUSED_ARG int total_order)
{
  DB_BIGINT i1, i2;

  assert (domain != NULL);

  OR_GET_BIGINT (mem1, &i1);
  OR_GET_BIGINT (mem2, &i2);

  return MR_CMP (i1, i2);
}

static int
mr_cmpval_bigint (DB_VALUE * value1, DB_VALUE * value2,
		  UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		  UNUSED_ARG int collation)
{
  DB_BIGINT i1, i2;

  i1 = DB_GET_BIGINT (value1);
  i2 = DB_GET_BIGINT (value2);

  return MR_CMP (i1, i2);
}

/*
 * TYPE DOUBLE
 *
 * IEEE double precision floating vlaues.
 * Remember the pointer here isn't necessarily valid as a "double*"
 * because the value may be packed into the object such that it
 * doesn't fall on a double word boundary.
 *
 */

static void
mr_initmem_double (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  double d = 0.0;

  OR_MOVE_DOUBLE (&d, mem);
}

static int
mr_setmem_double (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  double d;

  if (value == NULL)
    mr_initmem_double (mem, domain);
  else
    {
      d = db_get_double (value);
      OR_MOVE_DOUBLE (&d, mem);
    }
  return NO_ERROR;
}

static int
mr_getmem_double (void *mem, UNUSED_ARG TP_DOMAIN * domain, DB_VALUE * value,
		  UNUSED_ARG bool copy)
{
  double d;

  OR_MOVE_DOUBLE (mem, &d);
  return db_make_double (value, d);
}

static void
mr_data_writemem_double (OR_BUF * buf, void *mem,
			 UNUSED_ARG TP_DOMAIN * domain)
{
  double d;

  OR_MOVE_DOUBLE (mem, &d);
  or_put_double (buf, d);
}

static void
mr_data_readmem_double (OR_BUF * buf, void *mem,
			UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
  double d;
  int rc = NO_ERROR;

  if (mem == NULL)
    {
      or_advance (buf, tp_Double.disksize);
    }
  else
    {
      d = or_get_double (buf, &rc);
      OR_MOVE_DOUBLE (&d, mem);
    }
}

static void
mr_initval_double (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_DOUBLE, precision, scale);
  db_make_double (value, 0.0);
}

static int
mr_setval_double (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  if (src && !DB_IS_NULL (src))
    {
      return db_make_double (dest, db_get_double (src));
    }
  else
    {
      return db_value_domain_init (dest, DB_TYPE_DOUBLE,
				   DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
    }
}

static int
mr_data_writeval_double (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_double (buf, db_get_double (value));
}

static int
mr_data_readval_double (OR_BUF * buf, DB_VALUE * value,
			UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
			UNUSED_ARG bool copy)
{
  double temp;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Double.disksize);
    }
  else
    {
      temp = or_get_double (buf, &rc);
      if (rc == NO_ERROR)
	{
	  db_make_double (value, temp);
	}
      value->need_clear = false;
    }

  return rc;
}

static int
mr_index_writeval_double (OR_BUF * buf, const DB_VALUE * value)
{
  double d;

  d = db_get_double (value);

  return or_put_data (buf, (char *) (&d), tp_Double.disksize);
}

static int
mr_index_readval_double (OR_BUF * buf, DB_VALUE * value,
			 UNUSED_ARG int precision, UNUSED_ARG int scale,
			 UNUSED_ARG int collation_id, UNUSED_ARG int size,
			 UNUSED_ARG bool copy)
{
  double d;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Double.disksize);
    }
  else
    {
      rc = or_get_data (buf, (char *) (&d), tp_Double.disksize);
      if (rc == NO_ERROR)
	{
	  db_make_double (value, d);
	}
      value->need_clear = false;
    }

  return rc;
}

static int
mr_index_cmpdisk_double (void *mem1, void *mem2, UNUSED_ARG int precision,
			 UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  double d1, d2;

  COPYMEM (double, &d1, mem1);
  COPYMEM (double, &d2, mem2);

  return MR_CMP (d1, d2);
}

static int
mr_data_cmpdisk_double (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
			UNUSED_ARG int do_coercion,
			UNUSED_ARG int total_order)
{
  double d1, d2;

  assert (domain != NULL);

  OR_GET_DOUBLE (mem1, &d1);
  OR_GET_DOUBLE (mem2, &d2);

  return MR_CMP (d1, d2);
}

static int
mr_cmpval_double (DB_VALUE * value1, DB_VALUE * value2,
		  UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		  UNUSED_ARG int collation)
{
  double d1, d2;

  d1 = DB_GET_DOUBLE (value1);
  d2 = DB_GET_DOUBLE (value2);

  return MR_CMP (d1, d2);
}

/*
 * TYPE TIME
 *
 * 32 bit seconds counter.  Interpreted as an offset within a given day.
 * Probably not general enough currently to be used an an interval type?
 *
 */

static void
mr_initmem_time (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(DB_TIME *) mem = 0;
}

static int
mr_setmem_time (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  if (value == NULL)
    mr_initmem_time (mem, domain);
  else
    *(DB_TIME *) mem = *db_get_time (value);

  return NO_ERROR;
}

static int
mr_getmem_time (void *mem, UNUSED_ARG TP_DOMAIN * domain, DB_VALUE * value,
		UNUSED_ARG bool copy)
{
  (void) db_value_put_encoded_time (value, (DB_TIME *) mem);
  value->need_clear = false;
  return NO_ERROR;
}

static void
mr_data_writemem_time (OR_BUF * buf, void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  or_put_time (buf, (DB_TIME *) mem);
}

static void
mr_data_readmem_time (OR_BUF * buf, void *mem, UNUSED_ARG TP_DOMAIN * domain,
		      UNUSED_ARG int size)
{
  if (mem == NULL)
    {
      or_advance (buf, tp_Time.disksize);
    }
  else
    {
      or_get_time (buf, (DB_TIME *) mem);
    }
}

static void
mr_initval_time (DB_VALUE * value, UNUSED_ARG int precision,
		 UNUSED_ARG int scale)
{
  DB_TIME tm = 0;

  db_value_put_encoded_time (value, &tm);
  value->need_clear = false;
}

static int
mr_setval_time (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  int error;

  if (src && !DB_IS_NULL (src))
    {
      error = db_value_put_encoded_time (dest, db_get_time (src));
    }
  else
    {
      error = db_value_domain_init (dest, DB_TYPE_TIME,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      assert (error == NO_ERROR);
    }
  return error;
}

static int
mr_data_writeval_time (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_time (buf, db_get_time (value));
}

static int
mr_data_readval_time (OR_BUF * buf, DB_VALUE * value,
		      UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
		      UNUSED_ARG bool copy)
{
  DB_TIME tm;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Time.disksize);
    }
  else
    {
      rc = or_get_time (buf, &tm);
      if (rc == NO_ERROR)
	{
	  db_value_put_encoded_time (value, &tm);
	}
      value->need_clear = false;
    }
  return rc;
}

static int
mr_index_writeval_time (OR_BUF * buf, const DB_VALUE * value)
{
  DB_TIME *tm;

  tm = db_get_time (value);

  return or_put_data (buf, (char *) tm, tp_Time.disksize);
}

static int
mr_index_readval_time (OR_BUF * buf, DB_VALUE * value,
		       UNUSED_ARG int precision, UNUSED_ARG int scale,
		       UNUSED_ARG int collation_id, UNUSED_ARG int size,
		       UNUSED_ARG bool copy)
{
  DB_TIME tm;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Time.disksize);
    }
  else
    {
      rc = or_get_data (buf, (char *) (&tm), tp_Time.disksize);
      if (rc == NO_ERROR)
	{
	  db_value_put_encoded_time (value, &tm);
	}
      value->need_clear = false;
    }

  return rc;
}

static int
mr_index_cmpdisk_time (void *mem1, void *mem2, UNUSED_ARG int precision,
		       UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  DB_TIME t1, t2;

  COPYMEM (DB_TIME, &t1, mem1);
  COPYMEM (DB_TIME, &t2, mem2);

  return MR_CMP (t1, t2);
}

static int
mr_data_cmpdisk_time (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
		      UNUSED_ARG int do_coercion, UNUSED_ARG int total_order)
{
  DB_TIME t1, t2;

  assert (domain != NULL);

  OR_GET_TIME (mem1, &t1);
  OR_GET_TIME (mem2, &t2);

  return MR_CMP (t1, t2);
}

static int
mr_cmpval_time (DB_VALUE * value1, DB_VALUE * value2,
		UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		UNUSED_ARG int collation)
{
  const DB_TIME *t1, *t2;

  t1 = DB_GET_TIME (value1);
  t2 = DB_GET_TIME (value2);

  return MR_CMP (*t1, *t2);
}

/*
 * TYPE DATETIME
 *
 */

static void
mr_initmem_datetime (void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  DB_DATETIME *mem = (DB_DATETIME *) memptr;

  mem->date = 0;
  mem->time = 0;
}

static void
mr_initval_datetime (DB_VALUE * value, UNUSED_ARG int precision,
		     UNUSED_ARG int scale)
{
  DB_DATETIME dt;

  mr_initmem_datetime (&dt, NULL);
  db_make_datetime (value, &dt);
  value->need_clear = false;
}

static int
mr_setmem_datetime (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  if (value == NULL)
    {
      mr_initmem_datetime (mem, domain);
    }
  else
    {
      *(DB_DATETIME *) mem = *db_get_datetime (value);
    }

  return NO_ERROR;
}

static int
mr_getmem_datetime (void *mem, UNUSED_ARG TP_DOMAIN * domain,
		    DB_VALUE * value, UNUSED_ARG bool copy)
{
  return db_make_datetime (value, (DB_DATETIME *) mem);
}

static int
mr_setval_datetime (DB_VALUE * dest, const DB_VALUE * src,
		    UNUSED_ARG bool copy)
{
  int error;

  if (src && !DB_IS_NULL (src))
    {
      error = db_make_datetime (dest, db_get_datetime (src));
    }
  else
    {
      error = db_value_domain_init (dest, DB_TYPE_DATETIME,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      assert (error == NO_ERROR);
    }
  return error;
}

static void
mr_data_writemem_datetime (OR_BUF * buf, void *mem,
			   UNUSED_ARG TP_DOMAIN * domain)
{
  or_put_datetime (buf, (DB_DATETIME *) mem);
}

static void
mr_data_readmem_datetime (OR_BUF * buf, void *mem,
			  UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
  if (mem == NULL)
    {
      or_advance (buf, tp_Datetime.disksize);
    }
  else
    {
      or_get_datetime (buf, (DB_DATETIME *) mem);
    }
}

static int
mr_data_writeval_datetime (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_datetime (buf, db_get_datetime (value));
}

static int
mr_data_readval_datetime (OR_BUF * buf, DB_VALUE * value,
			  UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
			  UNUSED_ARG bool copy)
{
  DB_DATETIME datetime;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Datetime.disksize);
    }
  else
    {
      rc = or_get_datetime (buf, &datetime);
      if (rc == NO_ERROR)
	{
	  db_make_datetime (value, &datetime);
	}
      value->need_clear = false;
    }
  return rc;
}

static int
mr_index_writeval_datetime (OR_BUF * buf, const DB_VALUE * value)
{
  DB_DATETIME *datetime;
  int rc = NO_ERROR;

  datetime = db_get_datetime (value);

  assert (tp_Datetime.disksize == (tp_Date.disksize + tp_Time.disksize));

  rc = or_put_data (buf, (char *) (&datetime->date), tp_Date.disksize);
  if (rc == NO_ERROR)
    {
      rc = or_put_data (buf, (char *) (&datetime->time), tp_Time.disksize);
    }

  return rc;
}

static int
mr_index_readval_datetime (OR_BUF * buf, DB_VALUE * value,
			   UNUSED_ARG int precision, UNUSED_ARG int scale,
			   UNUSED_ARG int collation_id, UNUSED_ARG int size,
			   UNUSED_ARG bool copy)
{
  DB_DATETIME datetime;
  int rc = NO_ERROR;

  assert (tp_Datetime.disksize == (tp_Date.disksize + tp_Time.disksize));

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Datetime.disksize);
    }
  else
    {
      rc = or_get_data (buf, (char *) (&datetime.date), tp_Date.disksize);
      if (rc == NO_ERROR)
	{
	  rc = or_get_data (buf, (char *) (&datetime.time), tp_Time.disksize);
	}

      if (rc == NO_ERROR)
	{
	  db_make_datetime (value, &datetime);
	}
      value->need_clear = false;
    }

  return rc;
}

static int
mr_index_cmpdisk_datetime (void *mem1, void *mem2, UNUSED_ARG int precision,
			   UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  int c;
  DB_DATETIME dt1, dt2;

  if (mem1 == mem2)
    {
      return DB_EQ;
    }

  COPYMEM (unsigned int, &dt1.date, (char *)mem1 + OR_DATETIME_DATE);
  COPYMEM (unsigned int, &dt1.time, (char *) mem1 + OR_DATETIME_TIME);
  COPYMEM (unsigned int, &dt2.date, (char *) mem2 + OR_DATETIME_DATE);
  COPYMEM (unsigned int, &dt2.time, (char *) mem2 + OR_DATETIME_TIME);

  if (dt1.date < dt2.date)
    {
      c = DB_LT;
    }
  else if (dt1.date > dt2.date)
    {
      c = DB_GT;
    }
  else
    {
      if (dt1.time < dt2.time)
	{
	  c = DB_LT;
	}
      else if (dt1.time > dt2.time)
	{
	  c = DB_GT;
	}
      else
	{
	  c = DB_EQ;
	}
    }

  return c;
}

static int
mr_data_cmpdisk_datetime (void *mem1, void *mem2,
			  UNUSED_ARG TP_DOMAIN * domain,
			  UNUSED_ARG int do_coercion,
			  UNUSED_ARG int total_order)
{
  int c;
  DB_DATETIME dt1, dt2;

  assert (domain != NULL);

  if (mem1 == mem2)
    {
      return DB_EQ;
    }

  OR_GET_DATETIME (mem1, &dt1);
  OR_GET_DATETIME (mem2, &dt2);

  if (dt1.date < dt2.date)
    {
      c = DB_LT;
    }
  else if (dt1.date > dt2.date)
    {
      c = DB_GT;
    }
  else
    {
      if (dt1.time < dt2.time)
	{
	  c = DB_LT;
	}
      else if (dt1.time > dt2.time)
	{
	  c = DB_GT;
	}
      else
	{
	  c = DB_EQ;
	}
    }

  return c;
}

static int
mr_cmpval_datetime (DB_VALUE * value1, DB_VALUE * value2,
		    UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		    UNUSED_ARG int collation)
{
  const DB_DATETIME *dt1, *dt2;
  int c;

  dt1 = DB_GET_DATETIME (value1);
  dt2 = DB_GET_DATETIME (value2);

  if (dt1->date < dt2->date)
    {
      c = DB_LT;
    }
  else if (dt1->date > dt2->date)
    {
      c = DB_GT;
    }
  else
    {
      if (dt1->time < dt2->time)
	{
	  c = DB_LT;
	}
      else if (dt1->time > dt2->time)
	{
	  c = DB_GT;
	}
      else
	{
	  c = DB_EQ;
	}
    }

  return c;
}


/*
 * TYPE DATE
 *
 * 32 bit day counter, commonly called a "julian" date.
 */

static void
mr_initmem_date (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(DB_DATE *) mem = 0;
}

static int
mr_setmem_date (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  if (value == NULL)
    mr_initmem_date (mem, domain);
  else
    *(DB_DATE *) mem = *db_get_date (value);

  return NO_ERROR;
}

static int
mr_getmem_date (void *mem, UNUSED_ARG TP_DOMAIN * domain, DB_VALUE * value,
		UNUSED_ARG bool copy)
{
  return db_value_put_encoded_date (value, (DB_DATE *) mem);
}

static void
mr_data_writemem_date (OR_BUF * buf, void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  or_put_date (buf, (DB_DATE *) mem);
}

static void
mr_data_readmem_date (OR_BUF * buf, void *mem, UNUSED_ARG TP_DOMAIN * domain,
		      UNUSED_ARG int size)
{
  if (mem == NULL)
    {
      or_advance (buf, tp_Date.disksize);
    }
  else
    {
      or_get_date (buf, (DB_DATE *) mem);
    }
}

static void
mr_initval_date (DB_VALUE * value, UNUSED_ARG int precision,
		 UNUSED_ARG int scale)
{
  db_value_put_encoded_date (value, 0);
  value->need_clear = false;
}

static int
mr_setval_date (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  int error;

  if (src && !DB_IS_NULL (src))
    {
      error = db_value_put_encoded_date (dest, db_get_date (src));
    }
  else
    {
      error = db_value_domain_init (dest, DB_TYPE_DATE,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      assert (error == NO_ERROR);
    }
  return error;
}

static int
mr_data_writeval_date (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_date (buf, db_get_date (value));
}

static int
mr_data_readval_date (OR_BUF * buf, DB_VALUE * value,
		      UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
		      UNUSED_ARG bool copy)
{
  DB_DATE dt;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Date.disksize);
    }
  else
    {
      rc = or_get_date (buf, &dt);
      if (rc == NO_ERROR)
	{
	  db_value_put_encoded_date (value, &dt);
	}
      value->need_clear = false;
    }
  return rc;
}

static int
mr_index_writeval_date (OR_BUF * buf, const DB_VALUE * value)
{
  DB_DATE *dt;

  dt = db_get_date (value);

  return or_put_data (buf, (char *) dt, tp_Date.disksize);
}

static int
mr_index_readval_date (OR_BUF * buf, DB_VALUE * value,
		       UNUSED_ARG int precision, UNUSED_ARG int scale,
		       UNUSED_ARG int collation_id, UNUSED_ARG int size,
		       UNUSED_ARG bool copy)
{
  DB_DATE dt;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Date.disksize);
    }
  else
    {
      rc = or_get_data (buf, (char *) (&dt), tp_Date.disksize);
      if (rc == NO_ERROR)
	{
	  db_value_put_encoded_date (value, &dt);
	}
      value->need_clear = false;
    }

  return rc;
}

static int
mr_index_cmpdisk_date (void *mem1, void *mem2, UNUSED_ARG int precision,
		       UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  DB_DATE d1, d2;

  COPYMEM (DB_DATE, &d1, mem1);
  COPYMEM (DB_DATE, &d2, mem2);

  return MR_CMP (d1, d2);
}

static int
mr_data_cmpdisk_date (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
		      UNUSED_ARG int do_coercion, UNUSED_ARG int total_order)
{
  DB_DATE d1, d2;

  assert (domain != NULL);

  OR_GET_DATE (mem1, &d1);
  OR_GET_DATE (mem2, &d2);

  return MR_CMP (d1, d2);
}

static int
mr_cmpval_date (DB_VALUE * value1, DB_VALUE * value2,
		UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		UNUSED_ARG int collation)
{
  const DB_DATE *d1, *d2;

  d1 = DB_GET_DATE (value1);
  d2 = DB_GET_DATE (value2);

  return MR_CMP (*d1, *d2);
}

/*
 * TYPE OBJECT
 */

/*
 * This is a bit different than the other primitive types in that the memory
 * value and the DB_VALUE representations are not the same.  The memory value
 * will be represented with a WS_MEMOID structure to avoid creating MOPs until
 * they are actually needed.
 *
 * These types are not available on the server since there is no workspace
 * over there.  Although in standalone mode, we could promote OIDs to MOPs
 * on both sides, use the db_on_server flag to control this so we make
 * both sides behave similarly even though we're in standalone mode.
 * The "mem" functions will in general be commented out since they
 * call things that don't exist on the server.  THe "val" functions will
 * exist so that things tagged as DB_TYPE_OBJECT can be read as OID values.
 *
 */


/*
 * mr_null_oid - This is used to set an OID to the NULL state.
 *    return:  void
 *    oid(out): oid to initialize
 * Note:
 *    SET_OID_NULL does the actual work by setting the volid to -1 but it
 *    leaves garbage in the other fields which can be stored on disk and
 *    generally looks alarming when you encounter it later.  Before
 *    calling SET_OID_NULL, initialize the fields to nice zero values.
 *    Should be an inline function.
 */
static void
mr_null_oid (OID * oid)
{
  oid->pageid = 0;
  oid->volid = 0;
  oid->slotid = 0;

  OID_SET_NULL (oid);
}

static void
mr_initmem_object (UNUSED_ARG void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  /* there is no use for initmem on the server */
#if !defined (SERVER_MODE)
  WS_MEMOID *mem = (WS_MEMOID *) memptr;

  mr_null_oid (&mem->oid);
  mem->pointer = NULL;
#endif
}

/*
 * Can get here on the server when dispatching from set element domains.
 * Always represent object values as OIDs on the server.
 */

static void
mr_initval_object (DB_VALUE * value, int precision, int scale)
{
  OID oid;

#if !defined (SERVER_MODE)
  if (db_on_server)
    {
      db_value_domain_init (value, DB_TYPE_OID, precision, scale);
      OID_SET_NULL (&oid);
      db_make_oid (value, &oid);
    }
  else
    {
      db_value_domain_init (value, DB_TYPE_OBJECT, precision, scale);
      db_make_object (value, NULL);
    }
#else /* SERVER_MODE */
  db_value_domain_init (value, DB_TYPE_OID, precision, scale);
  OID_SET_NULL (&oid);
  db_make_oid (value, &oid);
#endif /* !SERVER_MODE */
}

static int
mr_setmem_object (UNUSED_ARG void *memptr, UNUSED_ARG TP_DOMAIN * domain,
		  UNUSED_ARG DB_VALUE * value)
{
  /* there is no need for setmem on the server */
#if !defined (SERVER_MODE)
  WS_MEMOID *mem = (WS_MEMOID *) memptr;
  OID *oid;
  MOP op;

  if (value == NULL)
    {
      mr_null_oid (&mem->oid);
      mem->pointer = NULL;
    }
  else
    {
      op = db_get_object (value);
      if (op == NULL)
	{
	  mr_initmem_object (mem, domain);
	}
      else
	{
	  oid = WS_OID (op);
	  mem->oid.volid = oid->volid;
	  mem->oid.pageid = oid->pageid;
	  mem->oid.slotid = oid->slotid;
	  mem->pointer = op;
	}
    }
#endif /* !SERVER_MODE */
  return NO_ERROR;
}

static int
mr_getmem_object (UNUSED_ARG void *memptr, UNUSED_ARG TP_DOMAIN * domain,
		  UNUSED_ARG DB_VALUE * value, UNUSED_ARG bool copy)
{
  int error = NO_ERROR;

  /* there is no need for getmem on the server */
#if !defined (SERVER_MODE)
  WS_MEMOID *mem = (WS_MEMOID *) memptr;
  MOP op;

  op = mem->pointer;
  if (op == NULL)
    {
      if (!OID_ISNULL (&mem->oid))
	{
	  op = ws_mop (&mem->oid, NULL);
	  if (op != NULL)
	    {
	      mem->pointer = op;
	      error = db_make_object (value, op);
	    }
	  else
	    {
	      error = er_errid ();
	      (void) db_make_object (value, NULL);
	    }
	}
    }
  else
    error = db_make_object (value, op);
#endif /* !SERVER_MODE */

  return error;
}


static int
mr_setval_object (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  int error = NO_ERROR;
  OID *oid;

#if !defined (SERVER_MODE)
  if (DB_IS_NULL (src))
    {
      PRIM_SET_NULL (dest);
    }
  /* can get here on the server when dispatching through set element domains */
  else if (DB_VALUE_TYPE (src) == DB_TYPE_OID)
    {
      /* make sure that the target type is set properly */
      db_value_domain_init (dest, DB_TYPE_OID,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      oid = (OID *) db_get_oid (src);
      error = db_make_oid (dest, oid);
    }
  else if (DB_VALUE_TYPE (src) == DB_TYPE_OBJECT)
    {
      /* If we're logically on the server, we probably shouldn't have gotten
       * here but if we do, don't continue with the object representation,
       * de-swizzle it back to an OID.
       */
      if (db_on_server)
	{
	  DB_OBJECT *obj;
	  /* what should this do for ISVID mops? */
	  obj = db_pull_object (src);
	  db_value_domain_init (dest, DB_TYPE_OID,
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	  oid = WS_OID (obj);
	  error = db_make_oid (dest, oid);
	}
      else
	{
	  db_value_domain_init (dest, DB_TYPE_OBJECT,
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	  error = db_make_object (dest, db_get_object (src));
	}
    }
#else /* SERVER_MODE */
  /*
   * If we're really on the server, we can only get here when dispatching
   * through set element domains.  The value must contain an OID.
   */
  if (DB_IS_NULL (src) || DB_VALUE_TYPE (src) != DB_TYPE_OID)
    {
      PRIM_SET_NULL (dest);
    }
  else
    {
      oid = (OID *) db_get_oid (src);
      error = db_make_oid (dest, oid);
    }
#endif /* !SERVER_MODE */

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
static int
mr_index_lengthval_object (const DB_VALUE * value)
{
  return tp_Oid.disksize;
}
#endif

/*
 * mr_lengthval_object - checks if the object is virtual or not. and returns
 * property type size.
 *    return: DB_TYPE_OID
 *    value(in): value to get length
 *    disk(in): indicator that it is disk object
 */
static int
mr_data_lengthval_object (UNUSED_ARG const DB_VALUE * value, int disk)
{
  int size;

  if (disk)
    {
      size = OR_OID_SIZE;
    }
  else
    {
      size = MR_OID_SIZE;
    }

  return size;
}

static void
mr_data_writemem_object (OR_BUF * buf, UNUSED_ARG void *memptr,
			 UNUSED_ARG TP_DOMAIN * domain)
{
#if !defined (SERVER_MODE)	/* there is no need for writemem on the server */
  WS_MEMOID *mem = (WS_MEMOID *) memptr;
  const OID *oidp;

  oidp = NULL;
  if (mem != NULL)
    {
      oidp = &mem->oid;
    }

  if (oidp == NULL)
    {
      /* construct an unbound oid */
      oidp = &oid_Null_oid;
    }
  else if (OID_ISTEMP (oidp))
    {
      /* Temporary oid, must get a permanent one. */
      if ((mem->pointer == NULL) || (mem->pointer->deleted))
	{
	  oidp = &oid_Null_oid;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_MR_TEMP_OID_WITHOUT_MOP, 0);
	}
      else
	{
	  oidp = WS_OID (mem->pointer);
	  if (OID_ISTEMP (oidp))
	    {
	      oidp = NULL;

	      if (locator_assign_permanent_oid (mem->pointer) == NULL)
		{
		  /* this is serious */
		  or_abort (buf);
		}
	      else
		{
		  oidp = WS_OID (mem->pointer);
		}

	      if (oidp == NULL)
		{
		  /* normally would have used or_abort by now */
		  oidp = &oid_Null_oid;
		}
	    }
	}
    }
  else
    {
      /* normal OID check for deletion */
      if ((mem->pointer != NULL) && (mem->pointer->deleted))
	oidp = &oid_Null_oid;
    }

  or_put_oid (buf, oidp);

#else /* SERVER_MODE */
  /* should never get here but in case we do, dump a NULL OID into
   * the buffer.
   */
  printf ("mr_writemem_object: called on the server !\n");
  or_put_oid (buf, &oid_Null_oid);
#endif /* !SERVER_MODE */
}


static void
mr_data_readmem_object (OR_BUF * buf, UNUSED_ARG void *memptr,
			UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
#if !defined (SERVER_MODE)	/* there is no need for readmem on the server ??? */
  WS_MEMOID *mem = (WS_MEMOID *) memptr;

  if (mem != NULL)
    {
      or_get_oid (buf, &mem->oid);
      mem->pointer = NULL;
    }
  else
    {
      or_advance (buf, tp_Object.disksize);
    }
#else
  /* shouldn't get here but if we do, just skip over it */
  printf ("mr_readmem_object: called on the server !\n");
  or_advance (buf, tp_Object.disksize);
#endif

}

static int
mr_index_writeval_object (OR_BUF * buf, const DB_VALUE * value)
{
  return mr_index_writeval_oid (buf, value);
}

static int
mr_data_writeval_object (OR_BUF * buf, const DB_VALUE * value)
{
#if !defined (SERVER_MODE)
  MOP mop;
#endif
  const OID *oidp = NULL;

  int rc = NO_ERROR;

#if !defined (SERVER_MODE)
  if (db_on_server || pr_Inhibit_oid_promotion)
    {
      if (DB_VALUE_TYPE (value) == DB_TYPE_OID)
	{
	  oidp = db_get_oid (value);
	  rc = or_put_oid (buf, oidp);
	  return rc;
	}
      else
	{
	  return ER_FAILED;
	}
    }
  if (DB_VALUE_TYPE (value) == DB_TYPE_OBJECT)
    {
      mop = db_get_object (value);
      if ((mop == NULL) || (mop->deleted))
	{
	  rc = or_put_oid (buf, &oid_Null_oid);
	}
      else
	{
	  oidp = WS_OID (mop);
	  if (OID_ISTEMP (oidp))
	    {
	      oidp = NULL;

	      if (locator_assign_permanent_oid (mop) == NULL)
		{
		  /* this is serious */
		  or_abort (buf);
		}
	      else
		{
		  oidp = WS_OID (mop);
		}

	      if (oidp == NULL)
		{
		  /* normally would have used or_abort by now */
		  oidp = &oid_Null_oid;
		}
	    }
	  rc = or_put_oid (buf, oidp);
	}
    }
  else if (DB_VALUE_TYPE (value) == DB_TYPE_OID)
    {
      oidp = db_get_oid (value);
      rc = or_put_oid (buf, oidp);
    }
  else
    {
      /* should never get here ! */
      rc = or_put_oid (buf, &oid_Null_oid);
    }
#else /* SERVER_MODE */
  /* on the server, the value must contain an OID */
  oidp = db_get_oid (value);
  rc = or_put_oid (buf, oidp);
#endif /* !SERVER_MODE */
  return rc;
}



static int
mr_index_readval_object (OR_BUF * buf, DB_VALUE * value,
			 int precision, int scale, int collation_id, int size,
			 bool copy)
{
  return mr_index_readval_oid (buf, value, precision, scale, collation_id,
			       size, copy);
}

static int
mr_data_readval_object (OR_BUF * buf, DB_VALUE * value,
			UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
			UNUSED_ARG bool copy)
{
  OID oid;
  int rc = NO_ERROR;

#if !defined (SERVER_MODE)
  if (value == NULL)
    {
      rc = or_advance (buf, tp_Object.disksize);
    }
  else
    {
      if (db_on_server || pr_Inhibit_oid_promotion)
	{
	  /* basically the same as mr_readval_server_oid, don't promote OIDs */
	  db_value_domain_init (value, DB_TYPE_OID,
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	  rc = or_get_oid (buf, &oid);
	  db_make_oid (value, &oid);
	}
      else
	{
	  db_value_domain_init (value, DB_TYPE_OBJECT,
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);

	  rc = or_get_oid (buf, &oid);
	  /*
	   * if the OID is NULL, leave the value with the NULL bit set
	   * and don't bother to put the OID inside.
	   * I added this because it seemed logical, does it break anything ?
	   */
	  if (!OID_ISNULL (&oid))
	    {
	      db_make_object (value, ws_mop (&oid, NULL));
	      if (db_get_object (value) == NULL)
		{
		  or_abort (buf);
		  return ER_FAILED;
		}
	    }
	}
    }
#else /* SERVER_MODE */
  /* on the server, we only read OIDs */
  if (value == NULL)
    {
      rc = or_advance (buf, tp_Object.disksize);
    }
  else
    {
      db_value_domain_init (value, DB_TYPE_OID,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      rc = or_get_oid (buf, &oid);
      /* should we be checking for the NULL OID here ? */
      db_make_oid (value, &oid);
    }
#endif /* !SERVER_MODE */
  return rc;
}

static int
mr_index_cmpdisk_object (void *mem1, void *mem2, int precision, int scale,
			 int collation_id)
{
  return mr_index_cmpdisk_oid (mem1, mem2, precision, scale, collation_id);
}

static int
mr_data_cmpdisk_object (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
			UNUSED_ARG int do_coercion,
			UNUSED_ARG int total_order)
{
  int c;
  OID o1, o2;

  assert (domain != NULL);

  OR_GET_OID (mem1, &o1);
  OR_GET_OID (mem2, &o2);
  /* if we ever store virtual objects, this will need to be
   * changed. However, since its known the only disk representation
   * of objects is an OID, this is a valid optimization
   */

  c = oid_compare (&o1, &o2);
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

static int
mr_cmpval_object (DB_VALUE * value1, DB_VALUE * value2,
		  UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		  UNUSED_ARG int collation)
{
  int c;
#if defined (SERVER_MODE)
  const OID *o1, *o2;
  DB_OBJECT *obj;

  /*
   * we need to be careful here because even though the domain may
   * say object, it may really be an OID (especially on the server).
   */
  if (DB_VALUE_DOMAIN_TYPE (value1) == DB_TYPE_OID)
    {
      o1 = DB_PULL_OID (value1);
    }
  else
    {
      obj = DB_GET_OBJECT (value1);
      o1 = (obj) ? WS_OID (obj) : &oid_Null_oid;
    }

  if (DB_VALUE_DOMAIN_TYPE (value2) == DB_TYPE_OID)
    {
      o2 = DB_PULL_OID (value2);
    }
  else
    {
      obj = DB_GET_OBJECT (value2);
      o2 = (obj) ? WS_OID (obj) : &oid_Null_oid;
    }

  c = oid_compare (o1, o2);
#else /* !SERVER_MODE */
  /* on the client, we must also handle virtual db_object types */
  OID *o1 = NULL, *o2 = NULL;
  DB_OBJECT *mop1 = NULL, *mop2 = NULL;

  /*
   * we need to be careful here because even though the domain may
   * say object, it may really be an OID (especially on the server).
   */
  if (DB_VALUE_DOMAIN_TYPE (value1) == DB_TYPE_OID)
    {
      o1 = DB_PULL_OID (value1);
    }
  else
    {
      mop1 = DB_PULL_OBJECT (value1);
      o1 = WS_OID (mop1);
    }

  if (DB_VALUE_DOMAIN_TYPE (value2) == DB_TYPE_OID)
    {
      o2 = DB_PULL_OID (value2);
    }
  else
    {
      mop2 = DB_PULL_OBJECT (value2);
      o2 = WS_OID (mop2);
    }

  c = oid_compare (o1, o2);
#endif /* SERVER_MODE */

  c = MR_CMP_RETURN_CODE (c);

  return c;
}




#if !defined (SERVER_MODE)

/*
 * pr_write_mop - write an OID to a disk representation buffer given a MOP
 * instead of a WS_MEMOID.
 *    return:
 *    buf(): transformer buffer
 *    mop(): mop to transform
 * Note:
 *    mr_write_object can't be used because it takes a WS_MEMOID as is the
 *    case for object references in instances.
 *    This must stay in sync with mr_write_object above !
 */
void
pr_write_mop (OR_BUF * buf, MOP mop)
{
  DB_VALUE value;

  mr_initval_object (&value, 0, 0);
  db_make_object (&value, mop);
  mr_data_writeval_object (buf, &value);
}
#endif /* !SERVER_MODE */


/*
 * TYPE VARIABLE
 *
 * Currently this can only be used internally for class objects.  I think
 * this is useful enough to make a general purpose thing.
 * Implemented with the DB_VALUE (like set elements) which means that we
 * will always create MOPs for variable values that are object references.
 * If this gets to be a big deal, will need to define another union
 * like DB_MEMORY_VALUE that has a local OID cache like the attribute
 * values do.
 * These were once just stubs that didn't do anything since the class
 * transformer called the pr_write_va/rtype etc. functions directly.  If
 * they can be regular types for object attributes, we need to support
 * an mr_ interface as well.
 *
 * NOTE: These are still stubs, need to think about other ramifications
 * in the schema level before making these public.
 */

static void
mr_initval_variable (DB_VALUE * value, int precision, int scale)
{
  mr_initval_null (value, precision, scale);
}

static int
mr_setval_variable (DB_VALUE * dest, UNUSED_ARG const DB_VALUE * src,
		    UNUSED_ARG bool copy)
{
  assert (false);

  mr_initval_null (dest, 0, 0);
  return ER_FAILED;
}

static int
mr_data_lengthval_variable (UNUSED_ARG const DB_VALUE * value,
			    UNUSED_ARG int disk)
{
  assert (false);

  return 0;
}

static int
mr_data_writeval_variable (UNUSED_ARG OR_BUF * buf,
			   UNUSED_ARG const DB_VALUE * value)
{
  assert (false);

  return ER_FAILED;
}

static int
mr_data_readval_variable (UNUSED_ARG OR_BUF * buf,
			  UNUSED_ARG DB_VALUE * value,
			  UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
			  UNUSED_ARG bool copy)
{
  assert (false);

  return ER_FAILED;
}

static int
mr_data_cmpdisk_variable (UNUSED_ARG void *mem1, UNUSED_ARG void *mem2,
			  UNUSED_ARG TP_DOMAIN * domain,
			  UNUSED_ARG int do_coercion,
			  UNUSED_ARG int total_order)
{
  assert (false);

  return DB_UNK;
}

static int
mr_cmpval_variable (UNUSED_ARG DB_VALUE * value1,
		    UNUSED_ARG DB_VALUE * value2, UNUSED_ARG int do_coercion,
		    UNUSED_ARG int total_order, UNUSED_ARG int collation)
{
#if 1
  assert (false);
#endif
  return DB_UNK;
}

/*
 * TYPE SUBSTRUCTURE
 *
 * Only for meta objects.  Might want to extend.
 * This really only serves as a placeholder in the type table.  These
 * functions should never be referenced through the usual channels.
 */

static void
mr_initmem_sub (UNUSED_ARG void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
}

static void
mr_initval_sub (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_SUB, precision, scale);
}

static int
mr_setmem_sub (UNUSED_ARG void *mem, UNUSED_ARG TP_DOMAIN * domain,
	       UNUSED_ARG DB_VALUE * value)
{
  return NO_ERROR;
}

static int
mr_getmem_sub (UNUSED_ARG void *mem, UNUSED_ARG TP_DOMAIN * domain,
	       UNUSED_ARG DB_VALUE * value, UNUSED_ARG bool copy)
{
  return NO_ERROR;
}

static int
mr_setval_sub (UNUSED_ARG DB_VALUE * dest, UNUSED_ARG const DB_VALUE * src,
	       UNUSED_ARG bool copy)
{
  return NO_ERROR;
}

static int
mr_data_lengthmem_sub (UNUSED_ARG void *mem, UNUSED_ARG TP_DOMAIN * domain,
		       UNUSED_ARG int disk)
{
  return 0;
}

static int
mr_data_lengthval_sub (UNUSED_ARG const DB_VALUE * value, UNUSED_ARG int disk)
{
  return 0;
}

static void
mr_data_writemem_sub (UNUSED_ARG OR_BUF * buf, UNUSED_ARG void *mem,
		      UNUSED_ARG TP_DOMAIN * domain)
{
}

static void
mr_data_readmem_sub (UNUSED_ARG OR_BUF * buf, UNUSED_ARG void *mem,
		     UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
}

static int
mr_data_writeval_sub (UNUSED_ARG OR_BUF * buf,
		      UNUSED_ARG const DB_VALUE * value)
{
  return NO_ERROR;
}

static int
mr_data_readval_sub (UNUSED_ARG OR_BUF * buf, UNUSED_ARG DB_VALUE * value,
		     UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
		     UNUSED_ARG bool copy)
{
  return NO_ERROR;
}

static int
mr_data_cmpdisk_sub (UNUSED_ARG void *mem1, UNUSED_ARG void *mem2,
		     UNUSED_ARG TP_DOMAIN * domain,
		     UNUSED_ARG int do_coercion, UNUSED_ARG int total_order)
{
  assert (false);
  assert (domain != NULL);

  return DB_UNK;
}

static int
mr_cmpval_sub (UNUSED_ARG DB_VALUE * value1, UNUSED_ARG DB_VALUE * value2,
	       UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
	       UNUSED_ARG int collation)
{
  return DB_UNK;
}

/*
 * TYPE OID
 *
 * DB_TYPE_OID is not really a "domain" type, it is rather a physical
 * representation of an object domain.  Due to the way we dispatch
 * on DB_TYPE_ codes however, we need a fleshed out type vector
 * for this.
 *
 * This is used by the server where we have no DB_OBJECT handles.
 * It can also be used on the client in places where we defer
 * the "swizzling" of OID references (e.g. inside sets).
 *
 * We don't have to handle the case where values come in with DB_TYPE_OBJECT
 * as we do in the _object handlers, true ?
 *
 */

static void
mr_initmem_oid (void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  OID *mem = (OID *) memptr;

  mr_null_oid (mem);
}

static void
mr_initval_oid (DB_VALUE * value, int precision, int scale)
{
  OID oid;

  mr_null_oid (&oid);
  db_value_domain_init (value, DB_TYPE_OID, precision, scale);
  db_make_oid (value, &oid);
}

static int
mr_setmem_oid (void *memptr, TP_DOMAIN * domain, DB_VALUE * value)
{
  OID *mem = (OID *) memptr;
  OID *oid;

  if (value == NULL)
    {
      mr_initmem_oid (mem, domain);
    }
  else
    {
      oid = db_get_oid (value);
      if (oid)
	{
	  mem->volid = oid->volid;
	  mem->pageid = oid->pageid;
	  mem->slotid = oid->slotid;
	}
      else
	{
	  return ER_FAILED;
	}
    }
  return NO_ERROR;
}

static int
mr_getmem_oid (void *memptr, UNUSED_ARG TP_DOMAIN * domain, DB_VALUE * value,
	       UNUSED_ARG bool copy)
{
  OID *mem = (OID *) memptr;
  OID oid;

  oid.volid = mem->volid;
  oid.pageid = mem->pageid;
  oid.slotid = mem->slotid;
  oid.groupid = mem->groupid;
  return db_make_oid (value, &oid);
}

static int
mr_setval_oid (DB_VALUE * dest, const DB_VALUE * src, UNUSED_ARG bool copy)
{
  OID *oid;

  if (src && !DB_IS_NULL (src))
    {
      oid = (OID *) db_get_oid (src);
      return db_make_oid (dest, oid);
    }
  else
    {
      PRIM_SET_NULL (dest);
      return NO_ERROR;
    }
}

static void
mr_data_writemem_oid (OR_BUF * buf, void *memptr,
		      UNUSED_ARG TP_DOMAIN * domain)
{
  OID *mem = (OID *) memptr;

  or_put_oid (buf, mem);
}

static void
mr_data_readmem_oid (OR_BUF * buf, void *memptr,
		     UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
  OID *mem = (OID *) memptr;
  OID oid;

  if (mem != NULL)
    {
      or_get_oid (buf, mem);
    }
  else
    {
      or_get_oid (buf, &oid);	/* skip over it */
    }
}

static int
mr_data_writeval_oid (OR_BUF * buf, const DB_VALUE * value)
{
  return (or_put_oid (buf, db_get_oid (value)));
}

static int
mr_data_readval_oid (OR_BUF * buf, DB_VALUE * value,
		     UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
		     UNUSED_ARG bool copy)
{
  OID oid;
  int rc = NO_ERROR;

  if (value != NULL)
    {
      db_value_domain_init (value, DB_TYPE_OID,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      rc = or_get_oid (buf, &oid);
      db_make_oid (value, &oid);
    }
  else
    {
      rc = or_advance (buf, tp_Oid.disksize);
    }

  return rc;
}

static int
mr_index_writeval_oid (OR_BUF * buf, const DB_VALUE * value)
{
  OID *oidp = NULL;
  int rc = NO_ERROR;

  assert (DB_VALUE_TYPE (value) == DB_TYPE_OID
	  || DB_VALUE_TYPE (value) == DB_TYPE_OBJECT);

  oidp = db_get_oid (value);

  /* do not align here */
  rc = or_put_data (buf, (char *) (&oidp->pageid), tp_Integer.disksize);
  if (rc == NO_ERROR)
    {
      rc = or_put_data (buf, (char *) (&oidp->slotid), tp_Short_disksize);
    }
  if (rc == NO_ERROR)
    {
      rc = or_put_data (buf, (char *) (&oidp->volid), tp_Short_disksize);
    }
  if (rc == NO_ERROR)
    {
      rc = or_put_data (buf, (char *) (&oidp->groupid), tp_Integer.disksize);
    }

  return rc;
}

static int
mr_index_readval_oid (OR_BUF * buf, DB_VALUE * value,
		      UNUSED_ARG int precision, UNUSED_ARG int scale,
		      UNUSED_ARG int collation_id, UNUSED_ARG int size,
		      UNUSED_ARG bool copy)
{
  OID oid;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_Object.disksize);
    }
  else
    {
      /* do not align here */
      rc = or_get_data (buf, (char *) (&oid.pageid), tp_Integer.disksize);
      if (rc == NO_ERROR)
	{
	  rc = or_get_data (buf, (char *) (&oid.slotid), tp_Short_disksize);
	}
      if (rc == NO_ERROR)
	{
	  rc = or_get_data (buf, (char *) (&oid.volid), tp_Short_disksize);
	}
      if (rc == NO_ERROR)
	{
	  rc =
	    or_get_data (buf, (char *) (&oid.groupid), tp_Integer.disksize);
	}

      if (rc == NO_ERROR)
	{
	  db_value_domain_init (value, DB_TYPE_OID,
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	  db_make_oid (value, &oid);
	}
    }

  return rc;
}

static int
mr_index_cmpdisk_oid (void *mem1, void *mem2, UNUSED_ARG int precision,
		      UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  int c;
  OID o1, o2;

  COPYMEM (int, &o1.pageid, (char *) mem1 + OR_OID_PAGEID);
  COPYMEM (short, &o1.slotid, (char *) mem1 + OR_OID_SLOTID);
  COPYMEM (short, &o1.volid, (char *) mem1 + OR_OID_VOLID);

  COPYMEM (int, &o2.pageid, (char *) mem2 + OR_OID_PAGEID);
  COPYMEM (short, &o2.slotid, (char *) mem2 + OR_OID_SLOTID);
  COPYMEM (short, &o2.volid, (char *) mem2 + OR_OID_VOLID);

  c = oid_compare (&o1, &o2);
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

static int
mr_data_cmpdisk_oid (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
		     UNUSED_ARG int do_coercion, UNUSED_ARG int total_order)
{
  int c;
  OID o1, o2;

  assert (domain != NULL);

  OR_GET_OID (mem1, &o1);
  OR_GET_OID (mem2, &o2);

  c = oid_compare (&o1, &o2);
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

static int
mr_cmpval_oid (DB_VALUE * value1, DB_VALUE * value2,
	       UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
	       UNUSED_ARG int collation)
{
  int c;
  OID *oid1, *oid2;

  oid1 = DB_GET_OID (value1);
  oid2 = DB_GET_OID (value2);

  if (oid1 == NULL || oid2 == NULL)
    {
      return DB_UNK;
    }

  c = oid_compare (oid1, oid2);
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

static void
mr_initmem_sequence (void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  SETOBJ **mem = (SETOBJ **) memptr;

  *mem = NULL;
}

static int
mr_setmem_sequence (void *memptr, TP_DOMAIN * domain, DB_VALUE * value)
{
  SETOBJ **mem = (SETOBJ **) memptr;
  int error = NO_ERROR;
  SETOBJ *set;
  SETREF *ref;

  /*
   * NOTE: assumes ownership info has already been placed
   * in the set reference by the caller
   */
  if ((value != NULL) && ((ref = db_get_set (value)) != NULL))
    {
      set = ref->set;
      if (*mem != set)
	{
	  if (*mem != NULL)
	    {
	      error = setobj_release (*mem);
	    }
	  *mem = set;
	}
    }
  else
    {
      if (*mem != NULL)
	{
	  error = setobj_release (*mem);
	}
      mr_initmem_sequence (mem, domain);
    }
  return error;
}

static int
mr_setval_set_internal (DB_VALUE * dest, const DB_VALUE * src,
			bool copy, DB_TYPE set_type)
{
  int error = NO_ERROR;
  SETREF *src_ref, *ref;

  if (src != NULL && !DB_IS_NULL (src)
      && ((src_ref = db_get_set (src)) != NULL))
    {
      if (!copy)
	{
	  ref = src_ref;
	  /* must increment the reference count */
	  ref->ref_count++;
	}
      else
	{
	  assert (dest->need_clear == false);

	  /* need to check if we have a disk_image, if so we just copy it */
	  if (src_ref->disk_set)
	    {
	      ref = set_make_reference ();
	      if (ref == NULL)
		{
		  goto err_set;
		}
	      else
		{
		  /* Copy the bits into a freshly allocated buffer. */
		  ref->disk_set = (char *) malloc (src_ref->disk_size);
		  if (ref->disk_set == NULL)
		    {
		      goto err_set;
		    }
		  else
		    {
		      ref->need_clear = true;
		      ref->disk_size = src_ref->disk_size;
		      ref->disk_domain = src_ref->disk_domain;
		      memcpy (ref->disk_set, src_ref->disk_set,
			      src_ref->disk_size);
		    }
		}
	    }
	  else
	    {
	      ref = set_copy (src_ref);
	      if (ref == NULL)
		{
		  goto err_set;
		}
	    }
	}

      switch (set_type)
	{
	case DB_TYPE_SEQUENCE:
	  db_make_sequence (dest, ref);
	  break;
	default:
	  break;
	}
    }
  else
    {
      db_value_domain_init (dest, set_type, DB_DEFAULT_PRECISION,
			    DB_DEFAULT_SCALE);
    }
  return error;

err_set:
  /* couldn't allocate storage for set */
  error = er_errid ();
  switch (set_type)
    {
    case DB_TYPE_SEQUENCE:
      db_make_sequence (dest, NULL);
      break;
    default:
      break;
    }
  PRIM_SET_NULL (dest);
  return error;
}

static int
mr_data_lengthmem_sequence (void *memptr, UNUSED_ARG TP_DOMAIN * domain,
			    int disk)
{
  int size;

  if (!disk)
    {
      size = tp_Sequence.size;
    }
  else
    {
      SETOBJ **mem = (SETOBJ **) memptr;

      size = or_packed_set_length (*mem, 0);
    }

  return size;
}

static int
mr_data_lengthval_sequence (const DB_VALUE * value, int disk)
{
  SETREF *ref;
  SETOBJ *set;
  int size;
#if !defined (SERVER_MODE)
  int pin;
#endif

  size = 0;

  if (!disk)
    {
      size = sizeof (DB_SET *);
    }
  else
    {
      ref = db_get_set (value);
      if (ref != NULL)
	{
	  /* should have a set_ function for this ! */
	  if (ref->disk_set)
	    {
	      size = ref->disk_size;
	    }
	  else if (set_get_setobj (ref, &set, 0) == NO_ERROR)
	    {
	      if (set != NULL)
		{
		  /* probably no need to pin here but it can't hurt */
#if !defined (SERVER_MODE)
		  pin = ws_pin (ref->owner, 1);
#endif
		  size = or_packed_set_length (set, 1);
#if !defined (SERVER_MODE)
		  (void) ws_pin (ref->owner, pin);
#endif
		}
	    }
	}
    }
  return size;
}

static void
mr_data_writemem_sequence (OR_BUF * buf, void *memptr,
			   UNUSED_ARG TP_DOMAIN * domain)
{
  SETOBJ **mem = (SETOBJ **) memptr;

  if (*mem != NULL)
    {
      /* note that we don't have to pin the object here since that will have
       * been handled above this leve.
       */
      or_put_set (buf, *mem, 0);
    }
}

static int
mr_data_writeval_sequence (OR_BUF * buf, const DB_VALUE * value)
{
  SETREF *ref;
  SETOBJ *set;
  int size;
#if !defined (SERVER_MODE)
  int pin;
#endif
  int rc = NO_ERROR;

  ref = db_get_set (value);
  if (ref != NULL)
    {
      /* If we have a disk image of the set, we can just copy those bits
       * here.  This assumes very careful maintenance of the disk and memory
       * images.  Currently, we only have one or the other.  That is, when
       * we transform the disk image to memory, we clear the disk image.
       */
      if (ref->disk_set)
	{
	  /* check for overflow */
	  if ((((ptrdiff_t) (buf->endptr - buf->ptr)) <
	       (ptrdiff_t) ref->disk_size))
	    {
	      return or_overflow (buf);
	    }
	  else
	    {
	      memcpy (buf->ptr, ref->disk_set, ref->disk_size);
	      rc = or_advance (buf, ref->disk_size);
	    }
	}
      else if (set_get_setobj (ref, &set, 0) == NO_ERROR)
	{
	  if (set != NULL)
	    {
	      if (ref->owner == NULL)
		{
		  or_put_set (buf, set, 1);
		}
	      else
		{
#if !defined (SERVER_MODE)
		  pin = ws_pin (ref->owner, 1);
#endif
		  size = or_packed_set_length (set, 1);
		  /* remember the Windows pointer problem ! */
		  if (((ptrdiff_t) (buf->endptr -
				    buf->ptr)) < (ptrdiff_t) size)
		    {
		      /* unpin the owner before we abort ! */
#if !defined (SERVER_MODE)
		      (void) ws_pin (ref->owner, pin);
#endif
		      return or_overflow (buf);
		    }
		  else
		    {
		      /* the buffer is ok, do the transformation */
		      or_put_set (buf, set, 1);
		    }
#if !defined (SERVER_MODE)
		  (void) ws_pin (ref->owner, pin);
#endif
		}
	    }
	}
    }
  return rc;
}

static void
mr_data_readmem_sequence (OR_BUF * buf, void *memptr, TP_DOMAIN * domain,
			  int size)
{
  SETOBJ **mem = (SETOBJ **) memptr;
  SETOBJ *set;

  if (mem == NULL)
    {
      if (size >= 0)
	{
	  or_advance (buf, size);
	}
      else
	{
	  set = or_get_set (buf, domain);
	  if (set != NULL)
	    {
	      setobj_free (set);
	    }
	}
    }
  else
    {
      if (!size)
	{
	  *mem = NULL;
	}
      else
	{
	  set = or_get_set (buf, domain);
	  if (set != NULL)
	    {
	      *mem = set;
	    }
	  else
	    {
	      or_abort (buf);
	    }
	}
    }
}

static int
mr_data_readval_sequence (OR_BUF * buf, DB_VALUE * value,
			  TP_DOMAIN * domain, int size, bool copy)
{
  SETOBJ *set;
  SETREF *ref;
  int rc = NO_ERROR;

  if (value == NULL)
    {
      if (size == -1)
	{
	  /* don't know the true size, must unpack the set and throw it away */
	  set = or_get_set (buf, domain);
	  if (set != NULL)
	    {
	      setobj_free (set);
	    }
	  else
	    {
	      or_abort (buf);
	      return ER_FAILED;
	    }
	}
      else
	{
	  if (size)
	    {
	      rc = or_advance (buf, size);
	    }
	}
    }
  else
    {
      /* In some cases, like VOBJ reading, the domain passed is NULL here so
       * be careful when initializing the value.  If it is NULL, it will be
       * read when the multiset is unpacked.
       */
      if (!domain)
	{
	  db_value_domain_init (value, DB_TYPE_SEQUENCE,
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	}
      else
	{
	  db_value_domain_init (value, TP_DOMAIN_TYPE (domain),
				domain->precision, domain->scale);
	}

      /* If size is zero, we have nothing to do, if size is -1,
       * just go ahead and unpack the set.
       */
      if (!size)
	{
	  db_make_sequence (value, NULL);
	}
      else if (copy)
	{
	  set = or_get_set (buf, domain);
	  if (set == NULL)
	    {
	      or_abort (buf);
	      return ER_FAILED;
	    }
	  else
	    {
	      ref = setobj_get_reference (set);
	      if (ref == NULL)
		{
		  or_abort (buf);
		  return ER_FAILED;
		}
	      else
		{
		  switch (set_get_type (ref))
		    {
		    case DB_TYPE_SEQUENCE:
		      db_make_sequence (value, ref);
		      break;
		    default:
		      break;
		    }
		}
	    }
	}
      else
	{
	  /* copy == false, which means don't translate it into memory rep */
	  ref = set_make_reference ();
	  if (ref == NULL)
	    {
	      or_abort (buf);
	      return ER_FAILED;
	    }
	  else
	    {
	      int disk_size;
	      DB_TYPE set_type;

	      if (size != -1)
		{
		  char *set_st;
		  int num_elements, has_domain, bound_bits, offset_tbl,
		    el_tags;

		  disk_size = size;

		  /* unfortunately, we still need to look at the header to
		   * find out the set type.
		   */
		  set_st = buf->ptr;
		  or_get_set_header (buf, &set_type, &num_elements,
				     &has_domain, &bound_bits, &offset_tbl,
				     &el_tags, NULL);

		  /* reset the OR_BUF */
		  buf->ptr = set_st;
		}
	      else
		{
		  /* we have to go figure the size out */
		  disk_size = or_disk_set_size (buf, domain, &set_type);
		}

	      /* Record the pointer to the disk bits */
	      ref->disk_set = buf->ptr;
	      ref->need_clear = false;
	      ref->disk_size = disk_size;
	      ref->disk_domain = domain;

	      /* advance the buffer as if we had read the set */
	      rc = or_advance (buf, disk_size);

	      switch (set_type)
		{
		case DB_TYPE_SEQUENCE:
		  db_make_sequence (value, ref);
		  break;
		default:
		  break;
		}
	    }
	}
    }
  return rc;
}

static void
mr_freemem_sequence (void *memptr)
{
  /* since we aren't explicitly setting the set to NULL,
     we must set up the reference structures so they will get
     the new set when it is brought back in, this is the
     only primitive type for which the free function is
     semantically different than using the setmem function
     with a NULL value
   */

  SETOBJ **mem = (SETOBJ **) memptr;

  if (*mem != NULL)
    {
      setobj_free (*mem);	/* free storage, NULL references */
    }
}

/*
 * TYPE SEQUENCE
 */

static void
mr_initval_sequence (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_SEQUENCE, precision, scale);
  db_make_sequence (value, NULL);
}

static int
mr_getmem_sequence (void *memptr, UNUSED_ARG TP_DOMAIN * domain,
		    DB_VALUE * value, UNUSED_ARG bool copy)
{
  SETOBJ **mem = (SETOBJ **) memptr;
  int error = NO_ERROR;
  SETOBJ *set;
  SETREF *ref;

  set = *mem;
  if (set == NULL)
    {
      error = db_make_sequence (value, NULL);
    }
  else
    {
      ref = setobj_get_reference (set);
      if (ref)
	{
	  error = db_make_sequence (value, ref);
	}
      else
	{
	  error = er_errid ();
	  (void) db_make_sequence (value, NULL);
	}
    }
  /*
   * NOTE: assumes that ownership info will already have been set or will
   * be set by the caller
   */

  return error;
}

static int
mr_setval_sequence (DB_VALUE * dest, const DB_VALUE * src, bool copy)
{
  return mr_setval_set_internal (dest, src, copy, DB_TYPE_SEQUENCE);
}

static int
mr_data_cmpdisk_sequence (void *mem1, void *mem2, TP_DOMAIN * domain,
			  int do_coercion, int total_order)
{
  int c;
  SETOBJ *seq1 = NULL, *seq2 = NULL;

  assert (domain != NULL);

  /* is not index type */
  assert (!tp_valid_indextype (TP_DOMAIN_TYPE (domain)));

  (void) or_unpack_set ((char *) mem1, &seq1, domain);
  if (seq1 == NULL)
    {
      return DB_UNK;
    }

  (void) or_unpack_set ((char *) mem2, &seq2, domain);
  if (seq2 == NULL)
    {
      setobj_free (seq1);
      return DB_UNK;
    }

  c = setobj_compare_order (seq1, seq2, do_coercion, total_order);

  setobj_free (seq1);
  setobj_free (seq2);

  return c;
}

static int
mr_cmpval_sequence (DB_VALUE * value1, DB_VALUE * value2,
		    int do_coercion, int total_order,
		    UNUSED_ARG int collation)
{
  int c;

  c = set_seq_compare (db_get_set (value1), db_get_set (value2),
		       do_coercion, total_order);

  return c;
}

/*
 * TYPE IDXKEY
 */

int
pr_idxkey_compare (const DB_IDXKEY * key1, const DB_IDXKEY * key2,
		   const int num_index_term, int *start_colp)
{
  int c = DB_UNK;
  int i, last;
  bool can_compare = false;

  assert (key1 != NULL);
  assert (key2 != NULL);
  assert (key1->size == key2->size);

  /* safe guard */
  if (key1->size != key2->size)
    {
      return DB_UNK;
    }

  if (num_index_term > 0)
    {
      last = num_index_term;
    }
  else
    {
      last = key1->size;
    }
  assert (last > 0);

  c = DB_EQ;			/* init */

  for (i = 0; i < last; i++)
    {
      /* consume equal-value columns */
      if (start_colp != NULL && i < *start_colp)
	{
#if !defined (NDEBUG)
	  if (DB_IS_NULL (&(key1->vals[i])))
	    {
	      assert (DB_IS_NULL (&(key2->vals[i])));
	    }
	  else
	    {
	      assert (!DB_IS_NULL (&(key2->vals[i])));
	    }
#endif

	  continue;		/* skip and go ahead */
	}

      /* val1 or val2 is NULL */
      if (DB_IS_NULL (&(key1->vals[i])))
	{
	  if (DB_IS_NULL (&(key2->vals[i])))
	    {
	      continue;		/* skip and go ahead */
	    }
	  else
	    {
	      c = DB_LT;
	      break;
	    }
	}
      else if (DB_IS_NULL (&(key2->vals[i])))
	{
	  c = DB_GT;
	  break;
	}

      /* at here, val1 and val2 is non-NULL */

      assert (!DB_IS_NULL (&(key1->vals[i])));
      assert (!DB_IS_NULL (&(key2->vals[i])));

#if 0				/* TODO:[happy] remove me */
      assert (tp_valid_indextype (DB_VALUE_DOMAIN_TYPE (&(key1->vals[i]))));
      assert (tp_valid_indextype (DB_VALUE_DOMAIN_TYPE (&(key2->vals[i]))));
#endif

      /* coercion and comparision */

      c = tp_value_compare (&(key1->vals[i]), &(key2->vals[i]), 1, 1,
			    &can_compare);
      if (!can_compare)
	{
	  c = DB_UNK;
	}

      if (c != DB_EQ)
	{
	  break;		/* exit for-loop */
	}
    }				/* for */

  assert ((c == DB_EQ && i == last) || (c != DB_EQ && i < last));

  /* save the start position of non-equal-value column */
  if (start_colp != NULL)
    {
      *start_colp = i;
    }

  return c;
}

/*
 * TYPE NUMERIC
 */

static void
mr_initmem_numeric (void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  assert (domain->precision != TP_FLOATING_PRECISION_VALUE);

  memset (memptr, 0, MR_NUMERIC_SIZE (domain->precision));
}

/*
 * Due to the "within tolerance" domain comparison used during attribute
 * assignment validation, we may receive a numeric whose precision is less
 * then the actual precision of the attribute.  In that case we should be doing
 * an on-the-fly coercion here.
 */
static int
mr_setmem_numeric (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  int error = NO_ERROR;
  int src_precision, src_scale, byte_size;
  DB_C_NUMERIC num, src_num;

  if (value == NULL)
    {
      mr_initmem_numeric (mem, domain);
    }
  else
    {
      src_num = DB_GET_NUMERIC (value);

      src_precision = DB_VALUE_PRECISION (value);
      src_scale = DB_VALUE_SCALE (value);

      /* this should have been handled by now */
      if (src_num == NULL || src_precision != domain->precision
	  || src_scale != domain->scale)
	{
	  error = ER_OBJ_DOMAIN_CONFLICT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	}
      else
	{
	  num = (DB_C_NUMERIC) mem;
	  byte_size = MR_NUMERIC_SIZE (src_precision);
	  memcpy (num, src_num, byte_size);
	}
    }
  return error;
}

static int
mr_getmem_numeric (void *mem, TP_DOMAIN * domain, DB_VALUE * value,
		   UNUSED_ARG bool copy)
{
  int error = NO_ERROR;
  DB_C_NUMERIC num;

  if (value == NULL)
    {
      return error;
    }

  num = (DB_C_NUMERIC) mem;
  error = db_make_numeric (value, num, domain->precision, domain->scale);
  value->need_clear = false;

  return error;
}

static void
mr_data_writemem_numeric (OR_BUF * buf, void *mem,
			  UNUSED_ARG TP_DOMAIN * domain)
{
  int disk_size;

  disk_size = OR_NUMERIC_SIZE (domain->precision);
  or_put_data (buf, (char *) mem, disk_size);
}

static void
mr_data_readmem_numeric (OR_BUF * buf, void *mem,
			 UNUSED_ARG TP_DOMAIN * domain, int size)
{

  /* if stored size is unknown, the domain precision must be set correctly */
  if (size < 0)
    {
      size = OR_NUMERIC_SIZE (domain->precision);
    }

  if (mem == NULL)
    {
      if (size)
	{
	  or_advance (buf, size);
	}
    }
  else if (size)
    {
      if (size != OR_NUMERIC_SIZE (domain->precision))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_CORRUPTED, 0);
	  or_abort (buf);
	}
      else
	{
	  or_get_data (buf, (char *) mem, size);
	}
    }
}

static int
mr_index_lengthmem_numeric (void *mem, TP_DOMAIN * domain)
{
  return mr_data_lengthmem_numeric (mem, domain, 1);
}

static int
mr_data_lengthmem_numeric (UNUSED_ARG void *mem,
			   UNUSED_ARG TP_DOMAIN * domain, int disk)
{
  int len;

  /* think about caching this in the domain so we don't have to calculate it */
  if (disk)
    {
      len = OR_NUMERIC_SIZE (domain->precision);
    }
  else
    {
      len = MR_NUMERIC_SIZE (domain->precision);
    }

  return len;
}

static void
mr_initval_numeric (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_NUMERIC, precision, scale);
}

static int
mr_setval_numeric (DB_VALUE * dest, const DB_VALUE * src,
		   UNUSED_ARG bool copy)
{
  int error = NO_ERROR;
  int src_precision, src_scale;
  DB_C_NUMERIC src_numeric;

  if (src && !DB_IS_NULL (src)
      && (src_numeric = (DB_C_NUMERIC) DB_GET_NUMERIC (src)) != NULL)
    {
      src_precision = DB_VALUE_PRECISION (src);
      src_scale = DB_VALUE_SCALE (src);

      /*
       * Because numerics are stored in an inline buffer, there is no
       * difference between the copy and non-copy operations, this may
       * need to change.
       */
      error = db_make_numeric (dest, src_numeric, src_precision, src_scale);
    }
  else
    {
      error = db_value_domain_init (dest, DB_TYPE_NUMERIC,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      assert (error == NO_ERROR);
    }
  return error;
}

static int
mr_index_lengthval_numeric (const DB_VALUE * value)
{
  return mr_data_lengthval_numeric (value, 1);
}

static int
mr_data_lengthval_numeric (const DB_VALUE * value, int disk)
{
  int precision, len;

  len = 0;
  if (value != NULL)
    {
      /* better have a non-NULL value by the time writeval is called ! */
      precision = DB_VALUE_PRECISION (value);
      if (disk)
	{
	  len = OR_NUMERIC_SIZE (precision);
	}
      else
	{
	  len = MR_NUMERIC_SIZE (precision);
	}
    }
  return len;
}

static int
mr_index_writeval_numeric (OR_BUF * buf, const DB_VALUE * value)
{
  return mr_data_writeval_numeric (buf, value);
}

static int
mr_data_writeval_numeric (OR_BUF * buf, const DB_VALUE * value)
{
  DB_C_NUMERIC numeric;
  int precision, disk_size;
  int rc = NO_ERROR;

  if (value != NULL)
    {
      numeric = DB_GET_NUMERIC (value);
      if (numeric != NULL)
	{
	  precision = DB_VALUE_PRECISION (value);
	  disk_size = OR_NUMERIC_SIZE (precision);
	  rc = or_put_data (buf, (char *) numeric, disk_size);
	}
    }
  return rc;
}

static int
mr_data_readval_numeric_internal (OR_BUF * buf, DB_VALUE * value,
				  int precision, int scale,
				  UNUSED_ARG int collation_id, int size,
				  UNUSED_ARG bool copy)
{
  int rc = NO_ERROR;

  /*
   * If size is -1, the caller doesn't know the size and we must determine
   * it from the domain.
   */
  if (size == -1)
    {
      size = OR_NUMERIC_SIZE (precision);
    }

#if 1				/* TODO - ??? */
  if (size == 1)
    {
      size = OR_NUMERIC_SIZE (precision);
    }
#endif

  if (value == NULL)
    {
      if (size)
	{
	  rc = or_advance (buf, size);
	}
    }
  else
    {
      /*
       * the copy and no copy cases are identical because db_make_numeric
       * will copy the bits into its internal buffer.
       */
      (void) db_make_numeric (value, (DB_C_NUMERIC) buf->ptr,
			      precision, scale);
      value->need_clear = false;
      rc = or_advance (buf, size);
    }

  return rc;
}

static int
mr_index_readval_numeric (OR_BUF * buf, DB_VALUE * value,
			  int precision, int scale, int collation_id,
			  int size, bool copy)
{
  return mr_data_readval_numeric_internal (buf, value, precision, scale,
					   collation_id, size, copy);
}

static int
mr_data_readval_numeric (OR_BUF * buf, DB_VALUE * value,
			 TP_DOMAIN * domain, int size, bool copy)
{
  return mr_data_readval_numeric_internal (buf, value, domain->precision,
					   domain->scale,
					   domain->collation_id, size, copy);
}

static int
mr_data_cmpdisk_numeric_internal (void *mem1, void *mem2, int precision,
				  int scale, int collation_id,
				  UNUSED_ARG int total_order)
{
  int c = DB_UNK;
  OR_BUF buf;
  DB_VALUE value1, value2;
  DB_VALUE answer;
  int rc = NO_ERROR;

  or_init (&buf, (char *) mem1, 0);
  rc =
    mr_data_readval_numeric_internal (&buf, &value1, precision, scale,
				      collation_id, -1, 0);
  if (rc != NO_ERROR)
    {
      return DB_UNK;
    }

  or_init (&buf, (char *) mem2, 0);
  rc =
    mr_data_readval_numeric_internal (&buf, &value2, precision, scale,
				      collation_id, -1, 0);
  if (rc != NO_ERROR)
    {
      return DB_UNK;
    }

  rc = numeric_db_value_compare (&value1, &value2, &answer);
  if (rc != NO_ERROR)
    {
      return DB_UNK;
    }

  c = MR_CMP_RETURN_CODE (DB_GET_INTEGER (&answer));

  return c;
}

static int
mr_index_cmpdisk_numeric (void *mem1, void *mem2, int precision, int scale,
			  int collation_id)
{
  return mr_data_cmpdisk_numeric_internal (mem1, mem2, precision, scale,
					   collation_id, 1);
}

static int
mr_data_cmpdisk_numeric (void *mem1, void *mem2, TP_DOMAIN * domain,
			 UNUSED_ARG int do_coercion,
			 UNUSED_ARG int total_order)
{
  assert (domain != NULL);

  return mr_data_cmpdisk_numeric_internal (mem1, mem2, domain->precision,
					   domain->scale,
					   domain->collation_id, 1);
}

static int
mr_cmpval_numeric (DB_VALUE * value1, DB_VALUE * value2,
		   UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		   UNUSED_ARG int collation)
{
  int c = DB_UNK;
  DB_VALUE answer;

  if (numeric_db_value_compare (value1, value2, &answer) != NO_ERROR)
    {
      return DB_UNK;
    }

  if (DB_GET_INTEGER (&answer) < 0)
    {
      c = DB_LT;
    }
  else
    {
      if (DB_GET_INTEGER (&answer) > 0)
	{
	  c = DB_GT;
	}
      else
	{
	  c = DB_EQ;
	}
    }

  return c;
}

/*
 * PRIMITIVE TYPE SUPPORT ROUTINES
 */


/*
 * pr_type_from_id - maps a type identifier such as DB_TYPE_INTEGER into its
 * corresponding primitive type descriptor structures.
 *    return: type descriptor
 *    id(in): type identifier constant
 */
PR_TYPE *
pr_type_from_id (DB_TYPE id)
{
  PR_TYPE *type = NULL;

  if (id <= DB_TYPE_LAST && id != DB_TYPE_TABLE)
    {
      type = tp_Type_id_map[(int) id];
    }

  return type;
}


/*
 * pr_type_name - Returns the string type name associated with a type constant.
 *    return: type name
 *    id(in): type identifier constant
 * Note:
 *    The string must not be freed after use.
 */
const char *
pr_type_name (DB_TYPE id)
{
  const char *name = NULL;
  PR_TYPE *type;

  type = PR_TYPE_FROM_ID (id);
  assert (type != NULL);

  if (type != NULL)
    {
      name = type->name;
    }

  return name;
}


/*
 * pr_is_set_type - Test to see if a type identifier is one of the set types.
 *    return: non-zero if type is one of the set types
 *    type(in):
 * Note:
 *    Since there is an unfortunate amount of special processing for
 *    the set types, this takes care of comparing against all three types.
 */
int
pr_is_set_type (DB_TYPE type)
{
  int status = 0;

  if (TP_IS_SET_TYPE (type))
    {
      status = 1;
    }

  return status;
}


/*
 * pr_is_string_type - Test to see if a type identifier is one of the string
 * types.
 *    return: non-zero if type is one of the string types
 *    type(in):  type to check
 */
int
pr_is_string_type (DB_TYPE type)
{
  int status = 0;

  if (type == DB_TYPE_VARCHAR || type == DB_TYPE_VARBIT)
    {
      status = 1;
    }

  return status;
}

/*
 * pr_is_variable_type - determine whether or not a type is fixed or variable
 * width on disk.
 *    return: non-zero if this is a variable width type
 *    id(in): type id
 * Note:
 *    With the advent of parameterized types like CHAR(n), NUMERIC(p,s) etc.
 *    this doesn't mean that all values of this type will be the same size,
 *    it means that for any particular attribute of a class, they will all be
 *    the same size and the value will be stored in the "fixed" region of the
 *    disk representation.
 */
int
pr_is_variable_type (DB_TYPE id)
{
  PR_TYPE *type;
  int is_variable = 0;

  type = PR_TYPE_FROM_ID (id);
  if (type != NULL)
    {
      is_variable = type->variable_p;
    }

  return is_variable;
}


/*
 * pr_find_type - Locate a type descriptor given a name.
 *    return: type structure
 *    name(in): type name
 * Note:
 *    Called by the schema manager to map a domain name into a primitive
 *    type.
 *    This now recognizes some alias names for a few of the types.
 *    The aliases should be more centrally defined so the parser can
 *    check for them.
 *
 */
PR_TYPE *
pr_find_type (const char *name)
{
  PR_TYPE *type, *found;
  int i;

  if (name == NULL)
    {
      return NULL;
    }

  found = NULL;
  for (i = DB_TYPE_FIRST; i <= DB_TYPE_LAST && found == NULL; i++)
    {
      type = tp_Type_id_map[i];
      if (type->name != NULL)
	{
	  if (intl_mbs_casecmp (name, type->name) == 0)
	    {
	      found = type;
	    }
	}
    }

  /* alias kludge */
  if (found == NULL)
    {
      if (intl_mbs_casecmp (name, "int") == 0
	  || intl_mbs_casecmp (name, "short") == 0)
	{
	  found = tp_Type_integer;
	}
      else if (intl_mbs_casecmp (name, "string") == 0)
	{
	  found = tp_Type_string;
	}
      else if (intl_mbs_casecmp (name, "list") == 0)
	{
	  found = tp_Type_sequence;
	}
    }

  return found;
}

/*
 * SIZE CALCULATORS
 * These operation on the instance memory format of data values.
 */


/*
 * pr_mem_size - Determine the number of bytes required for the memory
 * representation of a particular type.
 *    return: memory size of type
 *    type(in): PR_TYPE structure
 * Note:
 *    This only determines the size for an attribute value in contiguous
 *    memory storage for an instance.
 *    It does not include the size of any reference memory (like strings.
 *    For strings, it returns the size of the pointer NOT the length
 *    of the string.
 *
 */
int
pr_mem_size (PR_TYPE * type)
{
  return type->size;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * pr_disk_size - Determine the number of bytes of disk storage required for
 * a value.
 *    return: disk size of an instance attribute
 *    type(in): type identifier
 *    mem(in): pointer to memory for value
 * Note:
 *    The value must be in instance memory format, NOT DB_VALUE format.
 *    If you have a DB_VALUE, use pr_value_disk_size.
 *    This is called by the transformer when calculating sizes and offset
 *    tables for instances.
 */
int
pr_disk_size (PR_TYPE * type, void *mem)
{
  int size;

  if (type->lengthmem != NULL)
    {
      size = (*type->lengthmem) (mem, NULL, 1);
    }
  else
    {
      size = type->disksize;
    }
  return size;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * pr_total_mem_size - returns the total amount of storage used for a memory
 * attribute including any external allocatons (for strings etc.).
 *    return: total memory size of type
 *    type(in): type identifier
 *    mem(in): pointer to memory for value
 * Note:
 *    The length function is not defined to accept a DB_VALUE so
 *    this had better be in memory format!
 *    Called by sm_object_size to calculate total size for an object.
 *
 */
int
pr_total_mem_size (PR_TYPE * type, void *mem)
{
  int size;

  if (type->data_lengthmem != NULL)
    {
      size = (*type->data_lengthmem) (mem, NULL, 0);
    }
  else
    {
      size = type->size;
    }

  return size;
}

/*
 * DB_VALUE TRANSFORMERS
 *
 * Used in the storage of class objects.
 * Need to fully extend this into the variabe type above so these can
 * be used at attribute values as well.
 *
 * Predicate processor must be able to understand these if we can issue
 * queries on these.
 *
 * This needs to be merged with the partially implemented support
 * for tp_Type_variable above.
 *
 * These functions will be called with a DB_DATA union NOT a pointer to
 * the memory representation of an attribute.
 */


/*
 * pr_value_mem_size - Returns the amount of storage necessary to hold the
 * contents of a DB_VALUE.
 *    return: byte size used by contents of DB_VALUE
 *    value(in): value to examine
 * Note:
 *    Does not include the amount of space necessary for the DB_VALUE.
 *    Used by some statistics modules that calculate memory sizes of strucures.
 */
int
pr_value_mem_size (DB_VALUE * value)
{
  PR_TYPE *type;
  int size;
  DB_TYPE dbval_type;

  size = 0;
  dbval_type = DB_VALUE_DOMAIN_TYPE (value);
  type = PR_TYPE_FROM_ID (dbval_type);
  if (type != NULL)
    {
      if (type->data_lengthval != NULL)
	{
	  size = (*type->data_lengthval) (value, 0);
	}
      else
	{
	  size = type->size;
	}
    }

  return size;
}

/*
 * pr_idxkey_init_boundbits() -
 *      return: int
 *  bufptr(in) :
 *  n_atts(in) :
 *
 */

int
pr_idxkey_init_boundbits (char *bufptr, int n_atts)
{
  unsigned char *bits;
  int i, nbytes;

  nbytes = OR_MULTI_BOUND_BIT_BYTES (n_atts);
  bits = (unsigned char *) bufptr;

  for (i = 0; i < nbytes; i++)
    {
      bits[i] = (unsigned char) 0;
    }

  return nbytes;
}

/*
 * pr_data_writeval_disk_size - returns the number of bytes that will be
 * written by the "writeval" type function for this value.
 *    return: byte size of disk representation
 *    value(in): db value
 * Note:
 *    It is generally used prior to writing the value to pre-calculate the
 *    required size.
 *    Formerly called pr_value_disk_size.
 *
 *    Note that "writeval" is used for the construction of disk objects,
 *    and it will leave space for fixed width types that are logically
 *    NULL.  If you need a compressed representation for random values,
 *    look at the or_put_value family of functions.
 */
int
pr_data_writeval_disk_size (DB_VALUE * value)
{
  PR_TYPE *type;
  DB_TYPE dbval_type;

  dbval_type = DB_VALUE_DOMAIN_TYPE (value);
  type = PR_TYPE_FROM_ID (dbval_type);

  assert (type != NULL);

  if (type)
    {
      if (type->data_lengthval == NULL)
	{
	  return type->disksize;
	}
      else
	{
	  return (*(type->data_lengthval)) (value, 1);
	}
    }

  return 0;
}

/*
 * pr_index_writeval_disk_size - returns the number of bytes that will be
 * written by the "index_write" type function for this value.
 *    return: byte size of disk representation
 *    value(in): db value
 * Note:
 */
int
pr_index_writeval_disk_size (const DB_VALUE * value)
{
  PR_TYPE *type;
  DB_TYPE dbval_type;

  dbval_type = DB_VALUE_DOMAIN_TYPE (value);
  assert (tp_valid_indextype (dbval_type));

  type = PR_TYPE_FROM_ID (dbval_type);
  assert (type != NULL);

  if (type)
    {
      if (type->index_lengthval == NULL)
	{
	  assert (!type->variable_p);

	  return type->disksize;
	}
      else
	{
	  return (*(type->index_lengthval)) (value);
	}
    }

  return 0;
}

void
pr_data_writeval (OR_BUF * buf, DB_VALUE * value)
{
  PR_TYPE *type;
  DB_TYPE dbval_type;

  dbval_type = DB_VALUE_DOMAIN_TYPE (value);
  type = PR_TYPE_FROM_ID (dbval_type);
  if (type == NULL)
    {
      type = tp_Type_null;	/* handle strange arguments with NULL */
    }
  (*(type->data_writeval)) (buf, value);
}

/*
 * MISCELLANEOUS TYPE-RELATED HELPER FUNCTIONS
 */


/*
 * pr_valstring - Take the value and formats it using the sptrfunc member of
 * the pr_type vector for the appropriate type.
 *    return: a freshly-malloc'ed string with a printed rep of "val" in it
 *    val(in): some DB_VALUE
 * Note:
 *    The caller is responsible for eventually freeing the memory via free_and_init.
 *
 *    This is really just a debugging helper; it probably doesn't do enough
 *    serious formatting for external use.  Use it to get printed DB_VALUE
 *    representations into error messages and the like.
 */
char *
pr_valstring (DB_VALUE * val)
{
  int str_size;
  char *str;
  PR_TYPE *pr_type;
  DB_TYPE dbval_type;

  const char null_str[] = "(null)";
  const char NULL_str[] = "NULL";

  if (val == NULL)
    {
      str = (char *) malloc (sizeof (null_str));
      if (str)
	{
	  strcpy (str, null_str);
	}
      return str;
    }

  if (DB_IS_NULL (val))
    {
      str = (char *) malloc (sizeof (NULL_str));
      if (str)
	{
	  strcpy (str, NULL_str);
	}
      return str;
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (val);
  pr_type = PR_TYPE_FROM_ID (dbval_type);

  if (pr_type == NULL)
    {
      return NULL;
    }

  /*
   * Guess a size; if we're wrong, we'll learn about it later and be told
   * how big to make the actual buffer.  Most things are pretty small, so
   * don't worry about this too much.
   */
  str_size = 32;

  str = (char *) malloc (str_size);
  if (str == NULL)
    {
      return NULL;
    }

  str_size = (*(pr_type->sptrfunc)) (val, str, str_size);
  if (str_size < 0)
    {
      /*
       * We didn't allocate enough slop.  However, the sprintf function
       * was kind enough to tell us how much room we really needed, so
       * we can reallocate and try again.
       */
      char *old_str;
      old_str = str;
      str_size = -str_size;
      str_size++;		/* for NULL */
      str = (char *) realloc (str, str_size);
      if (str == NULL)
	{
	  free_and_init (old_str);
	  return NULL;
	}
      if ((*pr_type->sptrfunc) (val, str, str_size) < 0)
	{
	  free_and_init (str);
	  return NULL;
	}
    }

  return str;
}

/*
 * TYPE RESULTSET
 */

static void
mr_initmem_resultset (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(int *) mem = 0;
}

static int
mr_setmem_resultset (void *mem, TP_DOMAIN * domain, DB_VALUE * value)
{
  if (value != NULL)
    {
      *(int *) mem = db_get_resultset (value);
    }
  else
    {
      mr_initmem_resultset (mem, domain);
    }

  return NO_ERROR;
}

static int
mr_getmem_resultset (void *mem, UNUSED_ARG TP_DOMAIN * domain,
		     DB_VALUE * value, UNUSED_ARG bool copy)
{
  return db_make_resultset (value, *(int *) mem);
}

static void
mr_data_writemem_resultset (OR_BUF * buf, void *mem,
			    UNUSED_ARG TP_DOMAIN * domain)
{
  or_put_int (buf, *(int *) mem);
}

static void
mr_data_readmem_resultset (OR_BUF * buf, void *mem,
			   UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size)
{
  int rc = NO_ERROR;

  if (mem == NULL)
    {
      or_advance (buf, tp_ResultSet.disksize);
    }
  else
    {
      *(int *) mem = or_get_int (buf, &rc);
    }
}

static void
mr_initval_resultset (DB_VALUE * value, int precision, int scale)
{
  db_value_domain_init (value, DB_TYPE_RESULTSET, precision, scale);
  db_make_resultset (value, 0);
}

static int
mr_setval_resultset (DB_VALUE * dest, const DB_VALUE * src,
		     UNUSED_ARG bool copy)
{
  if (src && !DB_IS_NULL (src))
    {
      return db_make_resultset (dest, db_get_resultset (src));
    }
  else
    {
      return db_value_domain_init (dest, DB_TYPE_RESULTSET,
				   DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
    }
}

static int
mr_data_writeval_resultset (OR_BUF * buf, const DB_VALUE * value)
{
  return or_put_int (buf, db_get_resultset (value));
}

static int
mr_data_readval_resultset (OR_BUF * buf, DB_VALUE * value,
			   UNUSED_ARG TP_DOMAIN * domain, UNUSED_ARG int size,
			   UNUSED_ARG bool copy)
{
  int temp_int, rc = NO_ERROR;

  if (value == NULL)
    {
      rc = or_advance (buf, tp_ResultSet.disksize);
    }
  else
    {
      temp_int = or_get_int (buf, &rc);
      db_make_resultset (value, temp_int);
      value->need_clear = false;
    }
  return rc;
}

static int
mr_data_cmpdisk_resultset (void *mem1, void *mem2,
			   UNUSED_ARG TP_DOMAIN * domain,
			   UNUSED_ARG int do_coercion,
			   UNUSED_ARG int total_order)
{
  int i1, i2;

  assert (domain != NULL);

  /* is not index type */
  assert (!tp_valid_indextype (TP_DOMAIN_TYPE (domain)));

  i1 = OR_GET_INT (mem1);
  i2 = OR_GET_INT (mem2);

  return MR_CMP (i1, i2);
}

static int
mr_cmpval_resultset (DB_VALUE * value1, DB_VALUE * value2,
		     UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		     UNUSED_ARG int collation)
{
  int i1, i2;

  i1 = DB_GET_RESULTSET (value1);
  i2 = DB_GET_RESULTSET (value2);

  return MR_CMP (i1, i2);
}


/*
 * TYPE STRING
 */

static void
mr_initmem_string (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(char **) mem = NULL;
}


/*
 * The main difference between "memory" strings and "value" strings is that
 * the length tag is stored as an in-line prefix in the memory block allocated
 * to hold the string characters.
 */
static int
mr_setmem_string (void *memptr, TP_DOMAIN * domain, DB_VALUE * value)
{
  int error = NO_ERROR;
  char *src, *cur, *new_, **mem;
  int src_precision, src_length, new_length;

  /* get the current memory contents */
  mem = (char **) memptr;
  cur = *mem;

  if (value == NULL || (src = DB_GET_STRING (value)) == NULL)
    {
      /* remove the current value */
      if (cur != NULL)
	{
	  free_and_init (cur);
	  mr_initmem_string (memptr, domain);
	}
    }
  else
    {
      /*
       * Get information from the value.  Ignore precision for the time being
       * since we really only care about the byte size of the value for varchar.
       * Whether or not the value "fits" should have been checked by now.
       */
      src_precision = DB_GET_STRING_PRECISION (value);
      src_length = DB_GET_STRING_SIZE (value);	/* size in bytes */

      if (src_length < 0)
	{
	  src_length = strlen (src);
	}

      /* Currently we NULL terminate the workspace string.
       * Could try to do the single byte size hack like we have in the
       * disk representation.
       */
      new_length = src_length + sizeof (int) + 1;
      new_ = (char *) malloc (new_length);
      if (new_ == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  if (cur != NULL)
	    {
	      free_and_init (cur);
	    }

	  /* pack in the length prefix */
	  *(int *) new_ = src_length;
	  cur = new_ + sizeof (int);
	  /* store the string */
	  memcpy (cur, src, src_length);
	  /* NULL terminate the stored string for safety */
	  cur[src_length] = '\0';
	  *mem = new_;
	}
    }

  return error;
}

static int
mr_getmem_string (void *memptr, TP_DOMAIN * domain, DB_VALUE * value,
		  bool copy)
{
  int error = NO_ERROR;
  int mem_length;
  char **mem, *cur, *new_;

  /* get to the current value */
  mem = (char **) memptr;
  cur = *mem;

  if (cur == NULL)
    {
      db_value_domain_init (value, DB_TYPE_VARCHAR, domain->precision, 0);
      value->need_clear = false;
    }
  else
    {
      /* extract the length prefix and the pointer to the actual string data */
      mem_length = *(int *) cur;
      cur += sizeof (int);

      if (!copy)
	{
	  db_make_varchar (value, domain->precision, cur, mem_length,
			   TP_DOMAIN_COLLATION (domain));
	  value->need_clear = false;
	}
      else
	{
	  /* return it with a NULL terminator */
	  new_ = (char *) malloc (mem_length + 1);
	  if (new_ == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      memcpy (new_, cur, mem_length);
	      new_[mem_length] = '\0';
	      db_make_varchar (value, domain->precision, new_, mem_length,
			       TP_DOMAIN_COLLATION (domain));
	      value->need_clear = true;
	    }
	}
    }

  return error;
}


/*
 * For the disk representation, we may be adding pad bytes to round up to a
 * word boundary.
 *
 * We are currently adding a NULL terminator to the disk representation
 * for some code on the server that still manufactures pointers directly into
 * the disk buffer and assumes it is a NULL terminated string.  This terminator
 * can be removed after the server has been updated.  The logic for maintaining
 * the terminator is actuall in the or_put_varchar, family of functions.
 */
static int
mr_data_lengthmem_string (void *memptr, UNUSED_ARG TP_DOMAIN * domain,
			  int disk)
{
  char **mem, *cur;
  int len;

  len = 0;
  if (!disk)
    {
      len = tp_String.size;
    }
  else if (memptr != NULL)
    {
      mem = (char **) memptr;
      cur = *mem;
      if (cur != NULL)
	{
	  len = *(int *) cur;
	  len = or_packed_varchar_length (len);
	}
    }

  return len;
}

static int
mr_index_lengthmem_string (void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  OR_BUF buf;
  int charlen;
  int rc = NO_ERROR;

  /* generally, index key-value is short enough
   */
  charlen = OR_GET_BYTE (memptr);
  if (charlen < 0xFF)
    {
      return or_varchar_length (charlen);
    }

  assert (charlen == 0xFF);

  or_init (&buf, memptr, -1);

  charlen = or_get_varchar_length (&buf, &rc);

  return or_varchar_length (charlen);
}

static void
mr_data_writemem_string (OR_BUF * buf, void *memptr,
			 UNUSED_ARG TP_DOMAIN * domain)
{
  char **mem, *cur;
  int len;

  mem = (char **) memptr;
  cur = *mem;
  if (cur != NULL)
    {
      len = *(int *) cur;
      cur += sizeof (int);
      or_packed_put_varchar (buf, cur, len);
    }
}


/*
 * The amount of memory requested is currently calculated based on the
 * stored size prefix.  If we ever go to a system where we avoid storing the
 * size, then we could use the size argument passed in to this function but
 * that may also include any padding byte that added to bring us up to a word
 * boundary.
 * Might want some way to determine which bytes at the end of a string are
 * padding.
 */
static void
mr_data_readmem_string (OR_BUF * buf, void *memptr,
			UNUSED_ARG TP_DOMAIN * domain, int size)
{
  char **mem, *cur, *new_;
  int len;
  int mem_length, pad;
  char *start;
  int rc = NO_ERROR;

  /*
   * we must have an explicit size here as it can't be determined from the
   * domain
   */
  if (size < 0)
    return;

  if (memptr == NULL)
    {
      if (size)
	or_advance (buf, size);
    }
  else
    {
      mem = (char **) memptr;
      cur = *mem;
      /* should we be checking for existing strings ? */
#if 0
      if (cur != NULL)
	free_and_init (cur);
#endif

      new_ = NULL;
      if (size)
	{
	  start = buf->ptr;

	  /* KLUDGE, we have some knowledge of how the thing is stored here in
	   * order have some control over the conversion between the packed
	   * length prefix and the full word memory length prefix.
	   * Might want to put this in another specialized or_ function.
	   */

	  /* Get just the length prefix. */
	  len = or_get_varchar_length (buf, &rc);

	  /*
	   * Allocate storage for this string, including our own full word size
	   * prefix and a NULL terminator.
	   */
	  mem_length = len + sizeof (int) + 1;

	  new_ = (char *) malloc (mem_length);
	  if (new_ == NULL)
	    or_abort (buf);
	  else
	    {
	      /* store the length in our memory prefix */
	      *(int *) new_ = len;
	      cur = new_ + sizeof (int);

	      /*
	       * read the string, INLCUDING the NULL terminator (which is
	       * expected)
	       */
	      or_get_data (buf, cur, len + 1);
	      /* align like or_get_varchar */
	      or_get_align32 (buf);
	    }

	  /* If we were given a size, check to see if for some reason this is
	   * larger than the already word aligned string that we have now
	   * extracted.  This shouldn't be the case but since we've got a
	   * length, we may as well obey it.
	   */
	  pad = size - (int) (buf->ptr - start);
	  if (pad > 0)
	    or_advance (buf, pad);
	}
      *mem = new_;
    }
}

static void
mr_freemem_string (void *memptr)
{
  char *cur;

  if (memptr != NULL)
    {
      cur = *(char **) memptr;
      if (cur != NULL)
	{
	  free_and_init (cur);
	}
    }
}

static void
mr_initval_string (DB_VALUE * value, int precision, UNUSED_ARG int scale)
{
  db_make_varchar (value, precision, NULL, 0, LANG_COERCIBLE_COLL);
  value->need_clear = false;
}

static int
mr_setval_string (DB_VALUE * dest, const DB_VALUE * src, bool copy)
{
  int error = NO_ERROR;
  int src_precision, src_length;
  char *src_str, *new_;

  if (src == NULL || (DB_IS_NULL (src) && DB_VALUE_PRECISION (src) == 0))
    {
      error =
	db_value_domain_init (dest, DB_TYPE_VARCHAR, DB_DEFAULT_PRECISION, 0);
      assert (error == NO_ERROR);
    }
  else if (DB_IS_NULL (src) || (src_str = db_get_string (src)) == NULL)
    {
      error =
	db_value_domain_init (dest, DB_TYPE_VARCHAR, DB_VALUE_PRECISION (src),
			      0);
      assert (error == NO_ERROR);
    }
  else
    {
      /* Get information from the value. */
      src_precision = DB_VALUE_PRECISION (src);
      src_length = db_get_string_size (src);
      if (src_length < 0)
	{
	  src_length = strlen (src_str);
	}

      /* should we be paying attention to this? it is extremely dangerous */
      if (!copy)
	{
	  error = db_make_varchar (dest, src_precision, src_str, src_length,
				   DB_GET_STRING_COLLATION (src));
	}
      else
	{
	  assert (dest->need_clear == false);

	  new_ = (char *) malloc (src_length + 1);
	  if (new_ == NULL)
	    {
	      db_value_domain_init (dest, DB_TYPE_VARCHAR, src_precision, 0);
	      error = er_errid ();
	    }
	  else
	    {
	      memcpy (new_, src_str, src_length);
	      new_[src_length] = '\0';
	      db_make_varchar (dest, src_precision, new_, src_length,
			       DB_GET_STRING_COLLATION (src));
	      dest->need_clear = true;
	    }
	}
    }

  return error;
}

static int
mr_index_lengthval_string (const DB_VALUE * value)
{
  return mr_lengthval_string_internal (value, 1, CHAR_ALIGNMENT);
}

static int
mr_index_writeval_string (OR_BUF * buf, const DB_VALUE * value)
{
  return mr_writeval_string_internal (buf, value, CHAR_ALIGNMENT);
}

static int
mr_index_readval_string (OR_BUF * buf, DB_VALUE * value,
			 int precision, int scale, int collation_id, int size,
			 bool copy)
{
  return mr_readval_string_internal (buf, value, precision, scale,
				     collation_id, size, copy,
				     CHAR_ALIGNMENT);
}

static int
mr_data_lengthval_string (const DB_VALUE * value, int disk)
{
  return mr_lengthval_string_internal (value, disk, INT_ALIGNMENT);
}

static int
mr_data_writeval_string (OR_BUF * buf, const DB_VALUE * value)
{
  return mr_writeval_string_internal (buf, value, INT_ALIGNMENT);
}

static int
mr_data_readval_string (OR_BUF * buf, DB_VALUE * value,
			TP_DOMAIN * domain, int size, bool copy)
{
  assert (domain != NULL);

  return mr_readval_string_internal (buf, value, domain->precision,
				     domain->scale, domain->collation_id,
				     size, copy, INT_ALIGNMENT);
}

/*
 * Ignoring precision as byte size is really the only important thing for
 * varchar.
 */
static int
mr_lengthval_string_internal (const DB_VALUE * value, int disk, int align)
{
  int len;
  const char *str;

  if (DB_IS_NULL (value))
    {
      return 0;
    }

  str = value->data.ch.buf;
  len = value->data.ch.size;

  if (str == NULL)
    {
      return 0;
    }
  if (len < 0)
    {
      len = strlen (str);
    }

  if (disk == 0)
    {
      return len;
    }
  else if (align == INT_ALIGNMENT)
    {
      return or_packed_varchar_length (len);
    }
  else
    {
      return or_varchar_length (len);
    }
}


/*
 * Ignoring precision as byte size is really the only important thing for
 * varchar.
 */
static int
mr_writeval_string_internal (OR_BUF * buf, DB_VALUE * value, int align)
{
  int src_length;
  char *str;
  int rc = NO_ERROR;

  if (value != NULL && (str = db_get_string (value)) != NULL)
    {
      src_length = db_get_string_size (value);	/* size in bytes */
      if (src_length < 0)
	{
	  src_length = strlen (str);
	}

      if (align == INT_ALIGNMENT)
	{
	  rc = or_packed_put_varchar (buf, str, src_length);
	}
      else
	{
	  rc = or_put_varchar (buf, str, src_length);
	}
    }
  return rc;
}


static int
mr_readval_string_internal (OR_BUF * buf, DB_VALUE * value,
			    int precision, UNUSED_ARG int scale,
			    int collation_id, int size, bool copy, int align)
{
  int pad;
  char *new_, *start = NULL;
  int str_length;
  int rc = NO_ERROR;

#if 1				/* TODO - trace */
  assert (value != NULL);
#endif

  if (value == NULL)
    {
      if (size == -1)
	{
	  rc = or_skip_varchar (buf, align);
	}
      else
	{
	  if (size)
	    {
	      rc = or_advance (buf, size);
	    }
	}
    }
  else
    {
      if (!copy)
	{
	  str_length = or_get_varchar_length (buf, &rc);
	  if (rc == NO_ERROR)
	    {
	      db_make_varchar (value, precision, buf->ptr, str_length,
			       collation_id);
	      value->need_clear = false;
	      rc = or_skip_varchar_remainder (buf, str_length, align);
	    }
	}
      else
	{
	  if (size == 0)
	    {
	      /* its NULL */
	      db_value_domain_init (value, DB_TYPE_VARCHAR, precision, 0);
	    }
	  else
	    {			/* size != 0 */
	      if (size == -1)
		{
		  /* Standard packed varchar with a size prefix */
		  ;		/* do nothing */
		}
	      else
		{		/* size != -1 */
		  /* Standard packed varchar within an area of fixed size,
		   * usually this means we're looking at the disk
		   * representation of an attribute.
		   * Just like the -1 case except we advance past the additional
		   * padding.
		   */
		  start = buf->ptr;
		}		/* size != -1 */

	      str_length = or_get_varchar_length (buf, &rc);
	      if (rc != NO_ERROR)
		{
		  return ER_FAILED;
		}

	      /*
	       * Allocate storage for the string including the kludge
	       * NULL terminator
	       */
	      new_ = (char *) malloc (str_length + 1);

	      if (new_ == NULL)
		{
		  /* need to be able to return errors ! */
		  db_value_domain_init (value, DB_TYPE_VARCHAR,
					TP_FLOATING_PRECISION_VALUE, 0);
		  or_abort (buf);
		  return ER_FAILED;
		}

	      if (align == INT_ALIGNMENT)
		{
		  /* read the kludge NULL terminator */
		  rc = or_get_data (buf, new_, str_length + 1);

		  /* round up to a word boundary */
		  if (rc == NO_ERROR)
		    {
		      rc = or_get_align32 (buf);
		    }
		}
	      else
		{
		  rc = or_get_data (buf, new_, str_length);
		}

	      if (rc != NO_ERROR)
		{
		  free_and_init (new_);
		  return ER_FAILED;
		}

	      new_[str_length] = '\0';	/* append the kludge NULL terminator */
	      db_make_varchar (value, precision, new_, str_length,
			       collation_id);
	      value->need_clear = true;

	      if (size == -1)
		{
		  /* Standard packed varchar with a size prefix */
		  ;		/* do nothing */
		}
	      else
		{		/* size != -1 */
		  /* Standard packed varchar within an area of fixed size,
		   * usually this means we're looking at the disk
		   * representation of an attribute. Just like the -1 case
		   * except we advance past the additional padding.
		   */
		  pad = size - (int) (buf->ptr - start);
		  if (pad > 0)
		    {
		      rc = or_advance (buf, pad);
		    }
		}		/* size != -1 */
	    }			/* size != 0 */
	}
    }
  return rc;
}

static int
mr_data_cmpdisk_string_internal (void *mem1, void *mem2, int collation_id,
				 UNUSED_ARG int total_order)
{
  int c = DB_UNK;
  char *str1, *str2;
  int str_length1, str_length2;
  OR_BUF buf1, buf2;
  int rc = NO_ERROR;

  str1 = (char *) mem1;
  str2 = (char *) mem2;

  /* generally, data is short enough
   */
  str_length1 = OR_GET_BYTE (str1);
  str_length2 = OR_GET_BYTE (str2);
  if (str_length1 < 0xFF && str_length2 < 0xFF)
    {
      str1 += OR_BYTE_SIZE;
      str2 += OR_BYTE_SIZE;
      c = QSTR_COMPARE (collation_id,
			(unsigned char *) str1, str_length1,
			(unsigned char *) str2, str_length2);
      c = MR_CMP_RETURN_CODE (c);
      return c;
    }

  assert (str_length1 == 0xFF || str_length2 == 0xFF);

  or_init (&buf1, str1, 0);
  str_length1 = or_get_varchar_length (&buf1, &rc);
  if (rc == NO_ERROR)
    {
      or_init (&buf2, str2, 0);
      str_length2 = or_get_varchar_length (&buf2, &rc);
      if (rc == NO_ERROR)
	{
	  c = QSTR_COMPARE (collation_id,
			    (unsigned char *) buf1.ptr, str_length1,
			    (unsigned char *) buf2.ptr, str_length2);
	  c = MR_CMP_RETURN_CODE (c);
	  return c;
	}
    }

  return DB_UNK;
}

static int
mr_index_cmpdisk_string (void *mem1, void *mem2, UNUSED_ARG int precision,
			 UNUSED_ARG int scale, int collation_id)
{
  return mr_data_cmpdisk_string_internal (mem1, mem2, collation_id, 1);
}

static int
mr_data_cmpdisk_string (void *mem1, void *mem2, TP_DOMAIN * domain,
			UNUSED_ARG int do_coercion,
			UNUSED_ARG int total_order)
{
  assert (domain != NULL);

  return mr_data_cmpdisk_string_internal (mem1, mem2, domain->collation_id,
					  1);
}

static int
mr_cmpval_string (DB_VALUE * value1, DB_VALUE * value2,
		  UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		  int collation)
{
  int c;
  unsigned char *string1, *string2;
  int size1, size2;

  string1 = (unsigned char *) DB_GET_STRING (value1);
  string2 = (unsigned char *) DB_GET_STRING (value2);

  if (string1 == NULL || string2 == NULL)
    {
      return DB_UNK;
    }

  size1 = (int) DB_GET_STRING_SIZE (value1);
  size2 = (int) DB_GET_STRING_SIZE (value2);

  if (size1 < 0)
    {
      size1 = strlen ((char *) string1);
    }

  if (size2 < 0)
    {
      size2 = strlen ((char *) string2);
    }

  if (collation == -1)
    {
      assert (false);
      return DB_UNK;
    }

  c = QSTR_COMPARE (collation, string1, size1, string2, size2);
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

PR_TYPE tp_String = {
  "character varying", DB_TYPE_VARCHAR, 1, sizeof (const char *), 0, 1,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_string,
  mr_initval_string,
  mr_setmem_string,
  mr_getmem_string,
  mr_setval_string,
  mr_data_lengthmem_string,
  mr_data_lengthval_string,
  mr_data_writemem_string,
  mr_data_readmem_string,
  mr_data_writeval_string,
  mr_data_readval_string,
  mr_index_lengthmem_string,
  mr_index_lengthval_string,
  mr_index_writeval_string,
  mr_index_readval_string,
  mr_index_cmpdisk_string,
  mr_freemem_string,
  mr_data_cmpdisk_string,
  mr_cmpval_string
};

PR_TYPE *tp_Type_string = &tp_String;

/*
 * TYPE VARBINARY
 */

static void
mr_initmem_varbit (void *mem, UNUSED_ARG TP_DOMAIN * domain)
{
  *(char **) mem = NULL;
}

/*
 * The main difference between "memory" strings and "value" strings is that
 * the length tag is stored as an in-line prefix in the memory block allocated
 * to hold the string characters.
 */
static int
mr_setmem_varbit (void *memptr, TP_DOMAIN * domain, DB_VALUE * value)
{
  int error = NO_ERROR;
  char *src, *cur, *new_, **mem;
  int src_precision, src_length, src_length_bits, new_length;

  /* get the current memory contents */
  mem = (char **) memptr;
  cur = *mem;

  if (value == NULL || (src = db_get_string (value)) == NULL)
    {
      /* remove the current value */
      if (cur != NULL)
	{
	  free_and_init (cur);
	  mr_initmem_varbit (memptr, domain);
	}
    }
  else
    {
      /*
       * Get information from the value.  Ignore precision for the time being
       * since we really only care about the byte size of the value for varbit.
       * Whether or not the value "fits" should have been checked by now.
       */
      src_precision = DB_VALUE_PRECISION (value);
      src_length = db_get_string_size (value);	/* size in bytes */
      src_length_bits = db_get_string_length (value);	/* size in bits */

      new_length = src_length + sizeof (int);
      new_ = (char *) malloc (new_length);
      if (new_ == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  if (cur != NULL)
	    {
	      free_and_init (cur);
	    }

	  /* pack in the length prefix */
	  *(int *) new_ = src_length_bits;
	  cur = new_ + sizeof (int);
	  /* store the string */
	  memcpy (cur, src, src_length);
	  *mem = new_;
	}
    }

  return error;
}

static int
mr_getmem_varbit (void *memptr, TP_DOMAIN * domain,
		  DB_VALUE * value, bool copy)
{
  int error = NO_ERROR;
  int mem_bit_length;
  char **mem, *cur, *new_;

  /* get to the current value */
  mem = (char **) memptr;
  cur = *mem;

  if (cur == NULL)
    {
      db_value_domain_init (value, DB_TYPE_VARBIT, domain->precision, 0);
      value->need_clear = false;
    }
  else
    {
      /* extract the length prefix and the pointer to the actual string data */
      mem_bit_length = *(int *) cur;
      cur += sizeof (int);

      if (!copy)
	{
	  db_make_varbit (value, domain->precision, cur, mem_bit_length);
	  value->need_clear = false;
	}
      else
	{
	  /* return it */
	  new_ = (char *) malloc (BITS_TO_BYTES (mem_bit_length) + 1);
	  if (new_ == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      memcpy (new_, cur, BITS_TO_BYTES (mem_bit_length));
	      db_make_varbit (value, domain->precision, new_, mem_bit_length);
	      value->need_clear = true;
	    }
	}
    }
  return error;
}


/*
 * For the disk representation, we may be adding pad bytes
 * to round up to a word boundary.
 */
static int
mr_data_lengthmem_varbit (void *memptr, UNUSED_ARG TP_DOMAIN * domain,
			  int disk)
{
  char **mem, *cur;
  int len;

  len = 0;
  if (!disk)
    len = tp_VarBit.size;
  else if (memptr != NULL)
    {
      mem = (char **) memptr;
      cur = *mem;
      if (cur != NULL)
	{
	  len = *(int *) cur;
	  len = or_packed_varbit_length (len);
	}
    }

  return len;
}

static int
mr_index_lengthmem_varbit (void *memptr, UNUSED_ARG TP_DOMAIN * domain)
{
  OR_BUF buf;
  int bitlen;
  int rc = NO_ERROR;

  or_init (&buf, memptr, -1);

  bitlen = or_get_varbit_length (&buf, &rc);

  return or_varbit_length (bitlen);
}

static void
mr_data_writemem_varbit (OR_BUF * buf, void *memptr,
			 UNUSED_ARG TP_DOMAIN * domain)
{
  char **mem, *cur;
  int bitlen;

  mem = (char **) memptr;
  cur = *mem;
  if (cur != NULL)
    {
      bitlen = *(int *) cur;
      cur += sizeof (int);
      or_packed_put_varbit (buf, cur, bitlen);
    }
}


/*
 * The amount of memory requested is currently calculated based on the
 * stored size prefix.  If we ever go to a system where we avoid storing the
 * size, then we could use the size argument passed in to this function but
 * that may also include any padding byte that added to bring us up to a word
 * boundary. Might want some way to determine which bytes at the end of a
 * string are padding.
 */
static void
mr_data_readmem_varbit (OR_BUF * buf, void *memptr,
			UNUSED_ARG TP_DOMAIN * domain, int size)
{
  char **mem, *cur, *new_;
  int bit_len;
  int mem_length, pad;
  char *start;
  int rc = NO_ERROR;

  /* Must have an explicit size here - can't be determined from the domain */
  if (size < 0)
    {
      return;
    }

  if (memptr == NULL)
    {
      if (size)
	{
	  or_advance (buf, size);
	}
    }
  else
    {
      mem = (char **) memptr;
      cur = *mem;
      /* should we be checking for existing strings ? */
#if 0
      if (cur != NULL)
	free_and_init (NULL, cur);
#endif

      new_ = NULL;
      if (size)
	{
	  start = buf->ptr;

	  /* KLUDGE, we have some knowledge of how the thing is stored here in
	   * order have some control over the conversion between the packed
	   * length prefix and the full word memory length prefix.
	   * Might want to put this in another specialized or_ function.
	   */

	  /* Get just the length prefix. */
	  bit_len = or_get_varbit_length (buf, &rc);

	  /*
	   * Allocate storage for this string, including our own full word size
	   * prefix.
	   */
	  mem_length = BITS_TO_BYTES (bit_len) + sizeof (int);

	  new_ = (char *) malloc (mem_length);
	  if (new_ == NULL)
	    {
	      or_abort (buf);
	    }
	  else
	    {
	      /* store the length in our memory prefix */
	      *(int *) new_ = bit_len;
	      cur = new_ + sizeof (int);

	      /* read the string */
	      or_get_data (buf, cur, BITS_TO_BYTES (bit_len));
	      /* align like or_get_varchar */
	      or_get_align32 (buf);
	    }

	  /*
	   * If we were given a size, check to see if for some reason this is
	   * larger than the already word aligned string that we have now
	   * extracted.  This shouldn't be the case but since we've got a
	   * length, we may as well obey it.
	   */
	  pad = size - (int) (buf->ptr - start);
	  if (pad > 0)
	    {
	      or_advance (buf, pad);
	    }
	}
      *mem = new_;
    }
}

static void
mr_freemem_varbit (void *memptr)
{
  char *cur;

  if (memptr != NULL)
    {
      cur = *(char **) memptr;
      if (cur != NULL)
	{
	  free_and_init (cur);
	}
    }
}

static void
mr_initval_varbit (DB_VALUE * value, int precision, UNUSED_ARG int scale)
{
  db_make_varbit (value, precision, NULL, 0);
  value->need_clear = false;
}

static int
mr_setval_varbit (DB_VALUE * dest, const DB_VALUE * src, bool copy)
{
  int error = NO_ERROR;
  int src_precision, src_length, src_bit_length;
  char *src_str, *new_;

  if (src == NULL || (DB_IS_NULL (src) && DB_VALUE_PRECISION (src) == 0))
    {
      error = db_value_domain_init (dest,
				    DB_TYPE_VARBIT, DB_DEFAULT_PRECISION, 0);
      assert (error == NO_ERROR);
    }
  else if (DB_IS_NULL (src) || (src_str = db_get_string (src)) == NULL)
    {
      error = db_value_domain_init (dest,
				    DB_TYPE_VARBIT, DB_VALUE_PRECISION (src),
				    0);
      assert (error == NO_ERROR);
    }
  else
    {
      /* Get information from the value. */
      src_precision = DB_VALUE_PRECISION (src);
      src_length = db_get_string_size (src);
      src_bit_length = db_get_string_length (src);

      /* should we be paying attention to this? it is extremely dangerous */
      if (!copy)
	{
	  error =
	    db_make_varbit (dest, src_precision, src_str, src_bit_length);
	}
      else
	{
	  assert (dest->need_clear == false);

	  new_ = (char *) malloc (src_length + 1);
	  if (new_ == NULL)
	    {
	      db_value_domain_init (dest, DB_TYPE_VARBIT, src_precision, 0);
	      error = er_errid ();
	    }
	  else
	    {
	      memcpy (new_, src_str, src_length);
	      error =
		db_make_varbit (dest, src_precision, new_, src_bit_length);
	      if (error != NO_ERROR)
		{
		  free_and_init (new_);
		  db_value_domain_init (dest, DB_TYPE_VARBIT, src_precision,
					0);
		}
	      else
		{
		  dest->need_clear = true;
		}
	    }
	}
    }

  return error;
}

static int
mr_index_lengthval_varbit (const DB_VALUE * value)
{
  return mr_lengthval_varbit_internal (value, 1, CHAR_ALIGNMENT);
}

static int
mr_index_writeval_varbit (OR_BUF * buf, const DB_VALUE * value)
{
  return mr_writeval_varbit_internal (buf, value, CHAR_ALIGNMENT);
}

static int
mr_index_readval_varbit (OR_BUF * buf, DB_VALUE * value,
			 int precision, int scale, int collation_id, int size,
			 bool copy)
{
  return mr_readval_varbit_internal (buf, value, precision, scale,
				     collation_id, size, copy,
				     CHAR_ALIGNMENT);
}

static int
mr_data_lengthval_varbit (const DB_VALUE * value, int disk)
{
  return mr_lengthval_varbit_internal (value, disk, INT_ALIGNMENT);
}

static int
mr_data_writeval_varbit (OR_BUF * buf, const DB_VALUE * value)
{
  return mr_writeval_varbit_internal (buf, value, INT_ALIGNMENT);
}

static int
mr_data_readval_varbit (OR_BUF * buf, DB_VALUE * value,
			TP_DOMAIN * domain, int size, bool copy)
{
  return mr_readval_varbit_internal (buf, value, domain->precision,
				     domain->scale, domain->collation_id,
				     size, copy, INT_ALIGNMENT);
}

static int
mr_lengthval_varbit_internal (const DB_VALUE * value, UNUSED_ARG int disk,
			      int align)
{
  int bit_length, len;
  const char *str;

  len = 0;
  if (value != NULL && (str = db_get_string (value)) != NULL)
    {
      bit_length = db_get_string_length (value);	/* size in bits */

      if (align == INT_ALIGNMENT)
	{
	  len = or_packed_varbit_length (bit_length);
	}
      else
	{
	  len = or_varbit_length (bit_length);
	}
    }
  return len;
}

static int
mr_writeval_varbit_internal (OR_BUF * buf, const DB_VALUE * value, int align)
{
  int src_bit_length;
  char *str;
  int rc = NO_ERROR;

  if (value != NULL && (str = db_get_string (value)) != NULL)
    {
      src_bit_length = db_get_string_length (value);	/* size in bits */

      if (align == INT_ALIGNMENT)
	{
	  rc = or_packed_put_varbit (buf, str, src_bit_length);
	}
      else
	{
	  rc = or_put_varbit (buf, str, src_bit_length);
	}
    }
  return rc;
}


/*
 * Size can come in as negative here to create a value with a pointer
 * directly to disk.
 *
 * Note that we have a potential conflict with this as -1 is a valid size
 * to use here when the string has been packed with a domain/length prefix
 * and we can determine the size from there.  In current practice, this
 * isn't a problem because due to word alignment, we'll never get a
 * negative size here that is greater than -4.
 */
static int
mr_readval_varbit_internal (OR_BUF * buf, DB_VALUE * value,
			    int precision, UNUSED_ARG int scale,
			    UNUSED_ARG int collation_id, int size, bool copy,
			    int align)
{
  int pad;
  int str_bit_length, str_length;
  char *new_, *start = NULL;
  int rc = NO_ERROR;

#if 1				/* TODO - trace */
  assert (value != NULL);
#endif

  if (value == NULL)
    {
      if (size == -1)
	{
	  rc = or_skip_varbit (buf, align);
	}
      else
	{
	  if (size)
	    rc = or_advance (buf, size);
	}
    }
  else
    {
      if (!copy)
	{
	  str_bit_length = or_get_varbit_length (buf, &rc);
	  if (rc == NO_ERROR)
	    {
	      db_make_varbit (value, precision, buf->ptr, str_bit_length);
	      value->need_clear = false;
	      rc = or_skip_varbit_remainder (buf, str_bit_length, align);
	    }
	}
      else
	{
	  if (size == 0)
	    {
	      /* its NULL */
	      db_value_domain_init (value, DB_TYPE_VARBIT, precision, 0);
	    }
	  else
	    {			/* size != 0 */
	      if (size == -1)
		{
		  /* Standard packed varbit with a size prefix */
		  ;		/* do nothing */
		}
	      else
		{		/* size != -1 */
		  /* Standard packed varbit within an area of fixed size,
		   * usually this means we're looking at the disk
		   * representation of an attribute.
		   * Just like the -1 case except we advance past the additional
		   * padding.
		   */
		  start = buf->ptr;
		}		/* size != -1 */

	      str_bit_length = or_get_varbit_length (buf, &rc);
	      if (rc != NO_ERROR)
		{
		  return ER_FAILED;
		}

	      /* get the string byte length */
	      str_length = BITS_TO_BYTES (str_bit_length);

	      /*
	       * Allocate storage for the string including the kludge NULL
	       * terminator
	       */
	      new_ = (char *) malloc (str_length + 1);

	      if (new_ == NULL)
		{
		  /* need to be able to return errors ! */
		  db_value_domain_init (value, DB_TYPE_VARBIT,
					TP_FLOATING_PRECISION_VALUE, 0);
		  or_abort (buf);
		  return ER_FAILED;
		}

	      /* do not read the kludge NULL terminator */
	      rc = or_get_data (buf, new_, str_length);
	      if (rc == NO_ERROR && align == INT_ALIGNMENT)
		{
		  /* round up to a word boundary */
		  rc = or_get_align32 (buf);
		}

	      if (rc != NO_ERROR)
		{
		  free_and_init (new_);
		  return ER_FAILED;
		}

	      new_[str_length] = '\0';	/* append the kludge NULL terminator */
	      db_make_varbit (value, precision, new_, str_bit_length);
	      value->need_clear = true;

	      if (size == -1)
		{
		  /* Standard packed varbit with a size prefix */
		  ;		/* do nothing */
		}
	      else
		{		/* size != -1 */
		  /* Standard packed varbit within an area of fixed size,
		   * usually this means we're looking at the disk
		   * representation of an attribute.
		   * Just like the -1 case except we advance past the
		   * additional padding.
		   */
		  pad = size - (int) (buf->ptr - start);
		  if (pad > 0)
		    {
		      rc = or_advance (buf, pad);
		    }
		}		/* size != -1 */
	    }			/* size != 0 */
	}

    }
  return rc;
}

static int
mr_index_cmpdisk_varbit (void *mem1, void *mem2, UNUSED_ARG int precision,
			 UNUSED_ARG int scale, UNUSED_ARG int collation_id)
{
  assert (false);		/* is impossible */

  return mr_data_cmpdisk_varbit (mem1, mem2, NULL, 1, 1);
}

static int
mr_data_cmpdisk_varbit (void *mem1, void *mem2, UNUSED_ARG TP_DOMAIN * domain,
			UNUSED_ARG int do_coercion,
			UNUSED_ARG int total_order)
{
  int bit_length1, bit_length2;
  int mem_length1, mem_length2, c;
  OR_BUF buf1, buf2;

  assert (domain != NULL);

  or_init (&buf1, (char *) mem1, 0);
  bit_length1 = or_get_varbit_length (&buf1, &c);
  mem_length1 = BITS_TO_BYTES (bit_length1);

  or_init (&buf2, (char *) mem2, 0);
  bit_length2 = or_get_varbit_length (&buf2, &c);
  mem_length2 = BITS_TO_BYTES (bit_length2);

  c = varbit_compare ((unsigned char *) buf1.ptr, mem_length1,
		      (unsigned char *) buf2.ptr, mem_length2);
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

static int
mr_cmpval_varbit (DB_VALUE * value1, DB_VALUE * value2,
		  UNUSED_ARG int do_coercion, UNUSED_ARG int total_order,
		  UNUSED_ARG int collation)
{
  int c;
  unsigned char *string1, *string2;

  string1 = (unsigned char *) DB_GET_STRING (value1);
  string2 = (unsigned char *) DB_GET_STRING (value2);

  if (string1 == NULL || string2 == NULL)
    {
      return DB_UNK;
    }

  c = varbit_compare (string1, (int) DB_GET_STRING_SIZE (value1),
		      string2, (int) DB_GET_STRING_SIZE (value2));
  c = MR_CMP_RETURN_CODE (c);

  return c;
}

PR_TYPE tp_VarBit = {
  "varbinary", DB_TYPE_VARBIT, 1, sizeof (const char *), 0, 1,
  help_fprint_value,
  help_sprint_value,
  mr_initmem_varbit,
  mr_initval_varbit,
  mr_setmem_varbit,
  mr_getmem_varbit,
  mr_setval_varbit,
  mr_data_lengthmem_varbit,
  mr_data_lengthval_varbit,
  mr_data_writemem_varbit,
  mr_data_readmem_varbit,
  mr_data_writeval_varbit,
  mr_data_readval_varbit,
  mr_index_lengthmem_varbit,
  mr_index_lengthval_varbit,
  mr_index_writeval_varbit,
  mr_index_readval_varbit,
  mr_index_cmpdisk_varbit,
  mr_freemem_varbit,
  mr_data_cmpdisk_varbit,
  mr_cmpval_varbit
};

PR_TYPE *tp_Type_varbit = &tp_VarBit;
