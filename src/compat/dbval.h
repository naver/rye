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




#ifndef _DBVAL_H_
#define _DBVAL_H_

#ident "$Id$"

#ifndef _DBTYPE_H_
#error "It looks like dbval.h is included before dbtype.h; don't do that."
#endif

#include "language_support.h"
#include "system_parameter.h"
#include "object_domain.h"

#if !defined(NDEBUG)		/* for debug build */

#include "db.h"

#define DB_GET_STRING_SAFE(v) (DB_IS_NULL (v) ? "" : DB_GET_STRING(v))
#define DB_PULL_STRING(v) DB_GET_STRING(v)
#define DB_PULL_VARBIT(v, l) DB_GET_VARBIT(v, l)
#define DB_PULL_OBJECT(v) DB_GET_OBJECT(v)
#define DB_PULL_OID(v) db_get_oid(v)
#define DB_PULL_SET(v) DB_GET_SET(v)
#define DB_PULL_MULTISET(v) DB_PULL_SET(v)
#define DB_PULL_LIST(v) DB_PULL_SET(v)
#define DB_PULL_SEQUENCE(v) DB_PULL_SET(v)
#define DB_PULL_IDXKEY(v) DB_GET_IDXKEY(v)
#define DB_PULL_ELO(v) DB_GET_ELO(v)
#define DB_PULL_NUMERIC(v) DB_GET_NUMERIC(v)

#define db_pull_string(v) DB_PULL_STRING(v)
#define db_pull_varbit(v, l) DB_PULL_VARBIT(v, l)
#define db_pull_object(v) DB_PULL_OBJECT(v)
#define db_pull_oid(v) DB_PULL_OID(v)
#define db_pull_set(v) DB_PULL_SET(v)
#define db_pull_numeric(v) DB_PULL_NUMERIC(v)

#else /* for non-debug build */

#undef DB_IS_NULL
#undef DB_IDXKEY_IS_NULL
#undef DB_VALUE_DOMAIN_TYPE
#undef DB_VALUE_TYPE
#undef DB_VALUE_SCALE
#undef DB_VALUE_PRECISION
#undef DB_GET_INTEGER
#undef DB_GET_BIGINT
#undef DB_GET_DOUBLE
#undef DB_GET_STRING
#undef DB_GET_VARBIT
#undef DB_GET_OBJECT
#undef DB_GET_OID
#undef DB_GET_SET
#undef DB_GET_MULTISET
#undef DB_GET_LIST
#undef DB_GET_IDXKEY
#undef DB_GET_ELO
#undef DB_GET_TIME
#undef DB_GET_DATE
#undef DB_GET_DATETIME
#undef DB_GET_POINTER
#undef DB_GET_ERROR
#undef DB_GET_NUMERIC
#undef DB_GET_STRING_SIZE
#undef DB_GET_RESULTSET
#undef DB_GET_STRING_COLLATION


#define DB_IS_NULL(v) \
    (((v) && (v)->domain.general_info.is_null == 0) ? false : true)

#if 1				/* TODO - */
#define DB_IDXKEY_IS_NULL(k) db_idxkey_is_null(k)
#endif

#define DB_VALUE_DOMAIN_TYPE(v)	\
    ((DB_TYPE) ((v)->domain.general_info.type))

#define DB_VALUE_TYPE(v) \
    (DB_IS_NULL(v) ? DB_TYPE_NULL : DB_VALUE_DOMAIN_TYPE(v))

#define DB_VALUE_SCALE(v) \
    ((DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_NUMERIC) ? \
      (v)->domain.numeric_info.scale : 0)

#define DB_VALUE_PRECISION(v) \
    ((DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_NUMERIC) \
       ? ((v)->domain.numeric_info.precision) : \
          ((DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_VARBIT \
	    || DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_VARCHAR) \
           ? ((v)->domain.char_info.length) : 0))

#define DB_GET_INTEGER(v) \
    ((assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_INTEGER)), (v)->data.i)

#define DB_GET_BIGINT(v) \
    ((assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_BIGINT)), (v)->data.bigint)

#define DB_GET_DOUBLE(v) \
    ((assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_DOUBLE)), (v)->data.d)

/* note : this will have to change when we start using the small and large
          string buffers. */
#define DB_GET_STRING(v) \
      (DB_IS_NULL (v) ? NULL \
       : ((assert (DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARCHAR \
		   || DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARBIT)), \
	  (v)->data.ch.buf))

#define DB_GET_STRING_SAFE(v) \
      (DB_IS_NULL (v) ? "" \
       : ((assert (DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARCHAR \
		   || DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARBIT)), \
	  (v)->data.ch.buf))

#define DB_PULL_STRING(v) \
      ((assert (DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARCHAR \
		|| DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARBIT)), \
       (v)->data.ch.buf)

/* note: this will have to change when we start using the small and large
         string buffers. */
#define DB_GET_VARBIT(v, l) \
      (DB_IS_NULL (v) ? \
       NULL : \
       (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_VARBIT), \
	((*(l)) = (v)->data.ch.size), (v)->data.ch.buf))

#define DB_PULL_VARBIT(v, l) \
      ((assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_VARBIT)), \
       ((*(l)) = (v)->data.ch.size), (v)->data.ch.buf)

#define DB_GET_OBJECT(v) \
      (DB_IS_NULL (v) ? \
       (DB_OBJECT *) (NULL) : (v)->data.op)

#define DB_PULL_OBJECT(v) \
      ((v)->data.op)

#define DB_GET_OID(v) \
      (DB_IS_NULL (v) ? \
       (OID *) (NULL) : \
       (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_OID), \
	&((v)->data.oid)))

#define DB_PULL_OID(v) \
      (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_OID), \
       &((v)->data.oid))

#define DB_GET_SET(v) \
      (DB_IS_NULL (v) ? \
       NULL : \
       (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_SEQUENCE), \
	(v)->data.set))

#define DB_PULL_SET(v) \
      (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_SEQUENCE), \
       (v)->data.set)

#define DB_GET_MULTISET(v) DB_GET_SET(v)
#define DB_PULL_MULTISET(v) DB_PULL_SET(v)

#define DB_GET_LIST(v) DB_GET_SET(v)
#define DB_PULL_LIST(v) DB_PULL_SET(v)

#define DB_PULL_SEQUENCE(v) DB_PULL_LIST(v)

#define DB_GET_TIME(v) \
      (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_TIME), \
       (DB_TIME *) (&(v)->data.time))

#define DB_GET_DATE(v) \
      (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_DATE), \
       (DB_DATE *) (&(v)->data.date))

#define DB_GET_DATETIME(v) \
      (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_DATETIME), \
       (DB_DATETIME *) (&(v)->data.datetime))

#define DB_GET_NUMERIC(v) \
      (DB_IS_NULL (v) ? \
       NULL : \
       (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_NUMERIC), \
	(v)->data.num.d.buf))

#define DB_PULL_NUMERIC(v) \
      (assert (DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_NUMERIC), \
       (v)->data.num.d.buf)

#define DB_GET_STRING_SIZE(v) \
      ((DB_VALUE_DOMAIN_TYPE (v) == DB_TYPE_VARBIT) \
	  ? (((v)->data.ch.size + 7) / 8) \
	  : (v)->data.ch.size)

#define DB_GET_RESULTSET(v) \
      ((v)->data.rset)

#define DB_GET_STRING_COLLATION(v) \
      ((assert (DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARCHAR \
                || DB_VALUE_DOMAIN_TYPE(v) == DB_TYPE_VARBIT)), \
       ((v)->domain.char_info.collation_id))

#define db_value_is_null(v) DB_IS_NULL(v)
#define db_value_type(v) DB_VALUE_TYPE(v)
#define db_value_scale(v) DB_VALUE_SCALE(v)
#define db_value_precision(v) DB_VALUE_PRECISION(v)
#define db_get_int(v) DB_GET_INTEGER(v)
#define db_get_double(v) DB_GET_DOUBLE(v)
#define db_get_string(v) DB_GET_STRING(v)
#define db_pull_string(v) DB_PULL_STRING(v)
#define db_get_varbit(v, l) DB_GET_VARBIT(v, l)
#define db_pull_varbit(v, l) DB_PULL_VARBIT(v, l)
#define db_get_object(v) DB_GET_OBJECT(v)
#define db_pull_object(v) DB_PULL_OBJECT(v)
#define db_get_oid(v) DB_GET_OID(v)
#define db_pull_oid(v) DB_PULL_OID(v)
#define db_get_set(v) DB_GET_SET(v)
#define db_pull_set(v) DB_PULL_SET(v)
#define db_get_idxkey(v) DB_GET_IDXKEY(v)
#define db_get_time(v) DB_GET_TIME(v)
#define db_get_date(v) DB_GET_DATE(v)
#define db_get_datetime(v) DB_GET_DATETIME(v)
#define db_get_error(v) DB_GET_ERROR(v)
#define db_get_numeric(v) DB_GET_NUMERIC(v)
#define db_pull_numeric(v) DB_PULL_NUMERIC(v)
#define db_get_string_size(v) DB_GET_STRING_SIZE(v)
#define db_get_resultset(v) DB_GET_RESULTSET(v)
#define db_get_string_collation(v) DB_GET_STRING_COLLATION(v)

#define db_make_null(v) \
    ((v)->domain.general_info.type = DB_TYPE_NULL, \
     (v)->domain.general_info.is_null = 1, \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_int(v, n) \
    ((v)->domain.general_info.type = DB_TYPE_INTEGER, \
     (v)->data.i = (n), \
     (v)->domain.general_info.is_null = 0, \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_bigint(v, n) \
    ((v)->domain.general_info.type = DB_TYPE_BIGINT, \
     (v)->data.bigint = (n), \
     (v)->domain.general_info.is_null = 0, \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_double(v, n) \
    ((v)->domain.general_info.type = DB_TYPE_DOUBLE, \
     (v)->data.d = (n), \
     (v)->domain.general_info.is_null = 0, \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_object(v, n) \
    ((v)->domain.general_info.type = DB_TYPE_OBJECT, \
     (v)->data.op = (n), \
     (v)->domain.general_info.is_null = ((n) ? 0 : 1), \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_db_char(v, coll, p, s) \
    ((v)->data.ch.size = (s), \
     (v)->data.ch.buf = (char *) (p), \
     (v)->domain.general_info.is_null = ((p) ? 0 : 1), \
     (v)->domain.char_info.collation_id = (coll), \
     (v)->domain.char_info.length = MAX((v)->domain.char_info.length, (v)->data.ch.size), \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_varbit(v, l, p, s) \
    ((v)->domain.char_info.length = ((l) == DB_DEFAULT_PRECISION) ? \
      DB_MAX_VARBIT_PRECISION : (l), \
     (v)->domain.general_info.type = DB_TYPE_VARBIT, \
     (v)->need_clear = false, \
     db_make_db_char((v), 0, (p), (s)), \
     NO_ERROR)

#define db_make_string(v, p) \
    ((v)->domain.char_info.length = DB_MAX_VARCHAR_PRECISION, \
     (v)->domain.general_info.type = DB_TYPE_VARCHAR, \
     (v)->need_clear = false, \
     db_make_db_char((v), LANG_COERCIBLE_COLL, \
		     (p), ((p) ? strlen(p) : 0)), \
     NO_ERROR)

#define db_make_varchar(v, l, p, s, col) \
    ((v)->domain.char_info.length = ((l) == DB_DEFAULT_PRECISION) ? \
      DB_MAX_VARCHAR_PRECISION : (l), \
     (v)->domain.general_info.type = DB_TYPE_VARCHAR, \
     (v)->need_clear = false, \
     db_make_db_char((v), (col), (p), (s)), \
     NO_ERROR)

#define db_make_resultset(v, n) \
    ((v)->domain.general_info.type = DB_TYPE_RESULTSET, \
     (v)->data.rset = (n), \
     (v)->domain.general_info.is_null = 0, \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_string_put_cs_and_collation(v, coll) \
     ((v)->domain.char_info.collation_id = (coll), \
     NO_ERROR)

#endif /* for non-debug build */

#define DB_GET_STRING_PRECISION(v) \
    ((v)->domain.char_info.length)

#define DB_GET_VARBIT_PRECISION(v) \
    ((v)->domain.char_info.length)

#define DB_GET_NUMERIC_PRECISION(val) \
    ((val)->domain.numeric_info.precision)

#define DB_GET_NUMERIC_SCALE(val) \
    ((val)->domain.numeric_info.scale)

#if 1				/* TODO - */
#define db_push_oid(v, n) \
    ((v)->domain.general_info.type = DB_TYPE_OID, \
     (v)->data.oid.pageid = (n)->pageid, \
     (v)->data.oid.slotid = (n)->slotid, \
     (v)->data.oid.volid = (n)->volid, \
     (v)->data.oid.groupid = (n)->groupid, \
     (v)->domain.general_info.is_null = OID_ISNULL(n), \
     (v)->need_clear = false, \
     NO_ERROR)

#define db_make_oid(v, n) \
    (((n) == NULL) ? ((v)->domain.general_info.is_null = 1, NO_ERROR) : \
     db_push_oid((v), (n)))

#endif

#endif /* _DBVAL_H_ */
