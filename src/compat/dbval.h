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
