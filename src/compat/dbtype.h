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
 * dbtype.h - Definitions related to the memory representations of database
 * attribute values. This is an application interface file. It should contain
 * only definitions available to Rye customer applications.
 */

#ifndef _DBTYPE_H_
#define _DBTYPE_H_

#ident "$Id$"

#include "config.h"

#include "dbdef.h"

#define BITS_IN_BYTE            (8)
#define HEX_IN_BYTE             (2)
#define BITS_IN_HEX             (4)

#define BITS_TO_BYTES(bit_cnt)	(((bit_cnt) + BITS_IN_BYTE-1) / BITS_IN_BYTE)
#define BITS_TO_HEX(bit_cnt)	(((bit_cnt) + BITS_IN_HEX-1) / BITS_IN_HEX)

/*
 * DB_MAX_IDENTIFIER_LENGTH -
 * This constant defines the maximum length of an identifier
 * in the database.  An identifier is anything that is passed as a string
 * to the db_ functions (other than user attribute values).  This
 * includes such things as class names, attribute names etc.  This
 * isn't strictly enforced right now but applications must be aware that
 * this will be a requirement.
 */
#define DB_MAX_IDENTIFIER_LENGTH 255

/* Maximum allowable user name.*/
#define DB_MAX_USER_LENGTH 32

#define DB_MAX_PASSWORD_LENGTH 8

/* Maximum allowable schema name. */
#define DB_MAX_SCHEMA_LENGTH DB_MAX_USER_LENGTH

/* Maximum allowable class name. */
#define DB_MAX_CLASS_LENGTH (DB_MAX_IDENTIFIER_LENGTH-DB_MAX_SCHEMA_LENGTH-4)

#define DB_MAX_SPEC_LENGTH       4096

/* This constant defines the maximum length of a character
   string that can be used as the value of an attribute. */
#define DB_MAX_STRING_LENGTH	0x3fffffff

/* This constant defines the maximum length of a bit string
   that can be used as the value of an attribute. */
#define DB_MAX_VARBIT_LENGTH (DB_MAX_STRING_LENGTH / BITS_IN_BYTE)

/* The maximum precision that can be specified for a numeric domain. */
#define DB_MAX_NUMERIC_PRECISION 38

/* The upper limit for a numeber that can be represented by a numeric type */
#define DB_NUMERIC_OVERFLOW_LIMIT 1e38

/* The lower limit for a number that can be represented by a numeric type */
#define DB_NUMERIC_UNDERFLOW_LIMIT 1e-38

/* The maximum precision that can be specified
   for a CHARACTER VARYING domain.*/
#define DB_MAX_VARCHAR_PRECISION DB_MAX_STRING_LENGTH

/* The maximum precision that can be specified for a BIT VARYING domain. */
#define DB_MAX_VARBIT_PRECISION DB_MAX_VARBIT_LENGTH

/* This constant indicates that the system defined default for
   determining the length of a string is to be used for a DB_VALUE. */
#define DB_DEFAULT_STRING_LENGTH -1

/* This constant indicates that the system defined default for
   precision is to be used for a DB_VALUE. */
#define DB_DEFAULT_PRECISION -1

/* This constant indicates that the system defined default for
   scale is to be used for a DB_VALUE. */
#define DB_DEFAULT_SCALE -1

/* This constant defines the default precision of DB_TYPE_NUMERIC. */
#define DB_DEFAULT_NUMERIC_PRECISION DB_MAX_NUMERIC_PRECISION

/* This constant defines the default scale of DB_TYPE_NUMERIC. */
#define DB_DEFAULT_NUMERIC_SCALE 0

/* This constant defines the default scale of result
   of numeric division operation */
#define DB_DEFAULT_NUMERIC_DIVISION_SCALE 9

/* These constants define the size of buffers within a DB_VALUE. */
#define DB_NUMERIC_BUF_SIZE	(2*sizeof(double))

/* This constant defines the default precision of DB_TYPE_BIGINT. */
#define DB_BIGINT_PRECISION      19

/* This constant defines the default precision of DB_TYPE_INTEGER. */
#define DB_INTEGER_PRECISION      10

/* This constant defines the default decimal precision of DB_TYPE_DOUBLE. */
#define DB_DOUBLE_DECIMAL_PRECISION      15

/* This constant defines the default precision of DB_TYPE_TIME. */
#define DB_TIME_PRECISION      8

/* This constant defines the default precision of DB_TYPE_DATE. */
#define DB_DATE_PRECISION      10

/* This constant defines the default precision of DB_TYPE_DATETIME. */
#define DB_DATETIME_PRECISION      23

/* This constant defines the default scale of DB_TYPE_DATETIME. */
#define DB_DATETIME_DECIMAL_SCALE      3

#define db_set db_collection

#define DB_MAKE_NULL(value) db_make_null(value)

#define DB_MAKE_NULL_NARGS(...) db_make_null_nargs(__VA_ARGS__)

#if 1				/* TODO - */
#define DB_IDXKEY_MAKE_NULL(key) db_idxkey_make_null(key)
#endif

#define DB_VALUE_CLONE_AS_NULL(src_value, dest_value)                   \
  do {                                                                  \
    if ((db_value_domain_init(dest_value,                               \
                              db_value_domain_type(src_value),          \
                              db_value_precision(src_value),            \
                              db_value_scale(src_value)))               \
        == NO_ERROR)                                                    \
      (void)db_value_put_null(dest_value);                              \
  } while (0)

#define DB_MAKE_INTEGER(value, num) db_make_int(value, num)

#define DB_MAKE_INT DB_MAKE_INTEGER

#define DB_MAKE_BIGINT(value, num) db_make_bigint(value, num)

#define DB_MAKE_BIGINTEGER DB_MAKE_BIGINT

#define DB_MAKE_DOUBLE(value, num) db_make_double(value, num)

#define DB_MAKE_OBJECT(value, obj) db_make_object(value, obj)

#define DB_MAKE_OBJ DB_MAKE_OBJECT

/* obsolete */

#define DB_MAKE_SEQUENCE(value, set) db_make_sequence(value, set)

#define DB_MAKE_LIST DB_MAKE_SEQUENCE

/* obsolete */
#define DB_MAKE_SEQ DB_MAKE_SEQUENCE

#if defined (ENABLE_UNUSED_FUNCTION)
/* new preferred interface */
#define DB_MAKE_COLLECTION(value, col) db_make_collection(value, col)
#endif

#define DB_MAKE_IDXKEY(value, midxkey) db_make_idxkey(value, midxkey)

#define DB_MAKE_TIME(value, hour, minute, second) \
    db_make_time(value, hour, minute, second)

#define DB_MAKE_ENCODED_TIME(value, time_value) \
    db_value_put_encoded_time(value, time_value)

#define DB_MAKE_DATE(value, month, day, year) \
    db_make_date(value, month, day, year)

#define DB_MAKE_ENCODED_DATE(value, date_value) \
    db_value_put_encoded_date(value, date_value)

#define DB_MAKE_DATETIME(value, datetime_value) \
    db_make_datetime(value, datetime_value)

#define DB_MAKE_NUMERIC(value, num, precision, scale) \
        db_make_numeric(value, num, precision, scale)

#define DB_MAKE_VARBIT(value, max_bit_length, bit_str, bit_str_bit_size)\
        db_make_varbit(value, max_bit_length, bit_str, bit_str_bit_size)

#define DB_MAKE_VARCHAR(value, max_char_length, str, char_str_byte_size, \
		        collation) \
        db_make_varchar(value, max_char_length, str, char_str_byte_size, \
			collation)

#define DB_MAKE_STRING(value, str) db_make_string(value, str)

#define DB_MAKE_RESULTSET(value, handle) db_make_resultset(value, handle)

#define db_get_collection db_get_set

#define DB_IS_NULL(value)               db_value_is_null(value)

#define DB_IS_NULL_NARGS(...)           db_value_is_null_nargs(__VA_ARGS__)

#if 1				/* TODO - */
#define DB_IDXKEY_IS_NULL(key)        db_idxkey_is_null(key)
#endif

#define DB_VALUE_DOMAIN_TYPE(value)     db_value_domain_type(value)

#define DB_VALUE_TYPE(value)            db_value_type(value)

#define DB_VALUE_PRECISION(value)       db_value_precision(value)

#define DB_VALUE_SCALE(value)           db_value_scale(value)

#define DB_GET_INTEGER(value)           db_get_int(value)

#define DB_GET_INT                      DB_GET_INTEGER

#define DB_GET_BIGINT(value)            db_get_bigint(value)

#define DB_GET_BIGINTEGER               DB_GET_BIGINT

#define DB_GET_DOUBLE(value)            db_get_double(value)

#define DB_GET_STRING(value)            db_get_string(value)

#define DB_GET_OBJECT(value)            db_get_object(value)

#define DB_GET_OBJ DB_GET_OBJECT

#define DB_GET_SET(value)               db_get_set(value)

#define DB_GET_MULTISET(value)          db_get_set(value)

/* obsolete */
#define DB_GET_MULTI_SET DB_GET_MULTISET

#define DB_GET_LIST(value)              db_get_set(value)

#define DB_GET_SEQUENCE DB_GET_LIST

/* obsolete */
#define DB_GET_SEQ DB_GET_SEQUENCE

/* new preferred interface */
#define DB_GET_COLLECTION(value)        db_get_set(value)

#define DB_GET_IDXKEY(value)           db_get_idxkey(value)

#define DB_GET_TIME(value)              db_get_time(value)

#define DB_GET_DATE(value)              db_get_date(value)

#define DB_GET_DATETIME(value)          db_get_datetime(value)

#define DB_GET_ERROR(value)             db_get_error(value)

#define DB_GET_NUMERIC(value)           db_get_numeric(value)

#define DB_GET_VARBIT(value, length)       db_get_varbit(value, length)

#define DB_GET_STRING_SIZE(value)       db_get_string_size(value)

#define DB_GET_RESULTSET(value)         db_get_resultset(value)

#define DB_GET_STRING_LENGTH(value) db_get_string_length(value)

#define DB_GET_STRING_COLLATION(value) db_get_string_collation(value)

#define DB_INT16_MIN   (-(DB_INT16_MAX)-1)
#define DB_INT16_MAX   0x7FFF
#define DB_UINT16_MAX  0xFFFFU
#define DB_INT32_MIN   (-(DB_INT32_MAX)-1)
#define DB_INT32_MAX   0x7FFFFFFF
#define DB_UINT32_MIN  0
#define DB_UINT32_MAX  0xFFFFFFFFU
#if (__WORDSIZE == 64) || defined(_WIN64)
#define DB_BIGINT_MAX  9223372036854775807L
#define DB_BIGINT_MIN  (-DB_BIGINT_MAX - 1L)
#else /* (__WORDSIZE == 64) || defined(_WIN64) */
#define DB_BIGINT_MAX  9223372036854775807LL
#define DB_BIGINT_MIN  (-DB_BIGINT_MAX - 1LL)
#endif /* (__WORDSIZE == 64) || defined(_WIN64) */

/* DB_DATE_MIN and DB_DATE_MAX are calculated by julian_encode function
   with arguments (1,1,1) and (12,31,9999) respectively. */
#define DB_DATE_ZERO       DB_UINT32_MIN	/* 0 means zero date */
#define DB_DATE_MIN        1721424
#define DB_DATE_MAX        5373484

#define DB_TIME_MIN        DB_UINT32_MIN
#define DB_TIME_MAX        DB_UINT32_MAX

#define DB_IS_DATETIME_DEFAULT_EXPR(v) ((v) == DB_DEFAULT_SYSDATE || \
    (v) == DB_DEFAULT_SYSDATETIME || (v) == DB_DEFAULT_UNIX_TIMESTAMP)

/* This defines the basic type identifier constants.  These are used in
   the domain specifications of attributes and
   as value type tags in the DB_VALUE structures. */
typedef enum
{
  DB_TYPE_FIRST = 0,		/* first for iteration   */
  DB_TYPE_UNKNOWN = 0,
  DB_TYPE_NULL = 0,
  DB_TYPE_INTEGER = 1,
#if 0
  DB_TYPE_FLOAT = 2,		/* unused */
#endif
  DB_TYPE_DOUBLE = 3,
  DB_TYPE_VARCHAR = 4,		/* SQL CHAR(n) VARYING values   */
  DB_TYPE_OBJECT = 5,
#if 0
  DB_TYPE_SET = 6,		/* unused */
  DB_TYPE_MULTISET = 7,		/* unused */
#endif
  DB_TYPE_SEQUENCE = 8,
#if 0
  DB_TYPE_ELO = 9,		/* unused */
#endif
  DB_TYPE_TIME = 10,
#if 0
  DB_TYPE_TIMESTAMP = 11,	/* unused *//* SQL TIMESTAMP  */
#endif
  DB_TYPE_DATE = 12,
#if 0
  DB_TYPE_MONETARY = 13,
#endif
  DB_TYPE_VARIABLE = 14,	/* internal use only */
  DB_TYPE_SUB = 15,		/* internal use only */
#if 0
  POINTER = 16,
  ERROR = 17,
  DB_TYPE_SMALLINT = 18,	/* unused *//* SQL SMALLINT           */
  DB_TYPE_VOBJ = 19,		/* unused *//* internal use only */
#endif
  DB_TYPE_OID = 20,		/* internal use only */
#if 0
  DB_TYPE_DB_VALUE = 21,	/* special for esql *//* unused */
#endif
  DB_TYPE_NUMERIC = 22,		/* SQL NUMERIC(p,s) values      */
  DB_TYPE_VARBIT = 24,		/* SQL BIT(n) VARYING values    */
#if 0
  DB_TYPE_CHAR = 25,		/* unused *//* SQL CHAR(n) values   */
  DB_TYPE_NCHAR = 26,		/* unused *//* SQL NATIONAL CHAR(n) values  */
  DB_TYPE_VARNCHAR = 27,	/* unused *//* SQL NATIONAL CHAR(n) VARYING values  */
#endif
  DB_TYPE_RESULTSET = 28,	/* internal use only */
#if 0
  DB_TYPE_IDXKEY = 29,		/* unused */
#endif
  DB_TYPE_TABLE = 30,		/* internal use only */
  DB_TYPE_BIGINT = 31,
  DB_TYPE_DATETIME = 32,
#if 0				/* unused */
  DB_TYPE_BLOB = 33,
  DB_TYPE_CLOB = 34,
  DB_TYPE_ENUMERATION = 35,
#endif

  DB_TYPE_LIST = DB_TYPE_SEQUENCE,
#if 0
  DB_TYPE_SHORT = DB_TYPE_SMALLINT,
#endif
  DB_TYPE_STRING = DB_TYPE_VARCHAR,

#if 0
  DB_TYPE_CHAR = DB_TYPE_VARCHAR,	/* SQL CHAR(n) */
  DB_TYPE_NCHAR = DB_TYPE_VARCHAR,	/* SQL NATIONAL CHAR(n) */
  DB_TYPE_VARNCHAR = DB_TYPE_VARCHAR,	/* SQL NATIONAL CHAR(n) VARYING */
  DB_TYPE_BIT = DB_TYPE_VARBIT,	/* SQL BIT(n) */
#endif

  DB_TYPE_LAST = DB_TYPE_DATETIME
} DB_TYPE;

/* Domain information stored in DB_VALUE structures. */
typedef union db_domain_info DB_DOMAIN_INFO;
union db_domain_info
{
  struct general_info
  {
    unsigned char is_null;
    unsigned char type;
  } general_info;
  struct numeric_info
  {
    unsigned char is_null;
    unsigned char type;
    unsigned char precision;
    unsigned char scale;
  } numeric_info;
  struct char_info
  {
    unsigned char is_null;
    unsigned char type;
    int length;
    int collation_id;
  } char_info;
};

/* types used for the representation of bigint values. */
typedef INT64 DB_BIGINT;

/* Structure used for the representation of time values. */
typedef unsigned int DB_TIME;

/* Structure used for the representation of date values. */
typedef unsigned int DB_DATE;

typedef struct db_datetime DB_DATETIME;
struct db_datetime
{
  unsigned int date;		/* date */
  unsigned int time;		/* time */
};

/* Structure used for the representation of numeric values. */
typedef struct db_numeric DB_NUMERIC;
struct db_numeric
{
  union
  {
    unsigned char *digits;
    unsigned char buf[DB_NUMERIC_BUF_SIZE];
  } d;
};

/* Definition for the collection descriptor structure. The structures for
 * the collection descriptors and the sequence descriptors are identical
 * internally but not all db_collection functions can be used with sequences
 * and no db_seq functions can be used with sets. It is advisable to
 * recognize the type of set being used, type it appropriately and only
 * call those db_ functions defined for that type.
 */
typedef struct db_collection DB_COLLECTION;
typedef DB_COLLECTION DB_MULTISET;
typedef DB_COLLECTION DB_SEQ;
typedef DB_COLLECTION DB_SET;

/*
 *  LOID and related definition which were in storage_common.h moved here.
 */

typedef struct vpid VPID;	/* REAL PAGE IDENTIFIER */
struct vpid
{
  INT32 pageid;			/* Page identifier */
  INT16 volid;			/* Volume identifier where the page reside */
};

typedef struct vfid VFID;	/* REAL FILE IDENTIFIER */
struct vfid
{
  INT32 fileid;			/* File identifier */
  INT16 volid;			/* Volume identifier where the file reside */
};
#define NULL_VFID_INITIALIZER { NULL_FILEID, NULL_VOLID }

typedef struct loid LOID;	/* LARGE OBJECT IDENTIFIER */
struct loid
{
  VPID vpid;			/* Real page identifier */
  VFID vfid;			/* Real file identifier */
};

/* This is the memory representation of an internal object
 * identifier.  It is in the API only for a few functions that
 * are not intended for general use.
 * An object identifier is NOT a fixed identifier; it cannot be used
 * reliably as an object identifier across database sessions or even
 * across transaction boundaries.  API programs are not allowed
 * to make assumptions about the contents of this structure.
 */
typedef struct db_identifier DB_IDENTIFIER;
struct db_identifier
{
  int pageid;
  short slotid;
  short volid;
  int groupid;
};

typedef DB_IDENTIFIER OID;

#define NULL_OID_INITIALIZER    {NULL_PAGEID, NULL_SLOTID, NULL_VOLID, NULL_GROUPID}

/* db_char.sm was formerly db_char.small.  small is an (undocumented)
 * reserved word on NT. */

typedef struct db_char DB_CHAR;
struct db_char
{
  int size;
  char *buf;
};

typedef DB_CHAR DB_NCHAR;
typedef DB_CHAR DB_BIT;

typedef int DB_RESULTSET;

/* A union of all of the possible basic type values.  This is used in the
 * definition of the DB_VALUE which is the fundamental structure used
 * in passing data in and out of the db_ function layer.
 */

typedef union db_data DB_DATA;
union db_data
{
  int i;
  DB_BIGINT bigint;
  double d;
  DB_OBJECT *op;
  DB_TIME time;
  DB_DATE date;
  DB_DATETIME datetime;
  DB_COLLECTION *set;
  DB_COLLECTION *collect;
  int error;
  DB_IDENTIFIER oid;
  DB_NUMERIC num;
  DB_CHAR ch;
  DB_RESULTSET rset;
};

/* This is the primary structure used for passing values in and out of
 * the db_ function layer. Values are always tagged with a datatype
 * so that they can be identified and type checking can be performed.
 */

typedef struct db_value DB_VALUE;
struct db_value
{
  DB_DOMAIN_INFO domain;
  DB_DATA data;
  bool need_clear;
};

/* This is used to chain DB_VALUEs into a list.  It is used as an argument
   to db_send_arglist. */
typedef struct db_value_array DB_VALUE_ARRAY;
struct db_value_array
{
  int size;
  DB_VALUE *vals;
};

#define MAX_INDEX_KEY_LIST_NUM  17	/* with rightmost OID type */

/* This is used to chain DB_VALUEs into a list.  It is used as an idxkey */
typedef struct db_idxkey DB_IDXKEY;
struct db_idxkey
{
  int size;
  DB_VALUE vals[MAX_INDEX_KEY_LIST_NUM];
};

/* This is used to gather stats about the workspace.
 * It contains the number of object descriptors used and
 * total number of object descriptors allocated
 */
typedef struct db_workspace_stats DB_WORKSPACE_STATS;
struct db_workspace_stats
{
  int obj_desc_used;		/* number of object descriptors used */
  int obj_desc_total;		/* total # of object descriptors allocated  */
};

/* This defines the C language type identifier constants.
 * These are used to describe the types of values used for setting
 * DB_VALUE contents or used to get DB_VALUE contents into.
 */
typedef enum
{
  DB_TYPE_C_DEFAULT = 0,
  DB_TYPE_C_FIRST = 100,	/* first for iteration */
  DB_TYPE_C_INT,
  DB_TYPE_C_LONG,
#if 0
  DB_TYPE_C_FLOAT,
#endif
  DB_TYPE_C_DOUBLE,
#if 0
  DB_TYPE_C_CHAR,
#endif
  DB_TYPE_C_VARCHAR,
  DB_TYPE_C_VARBIT,
  DB_TYPE_C_OBJECT,
  DB_TYPE_C_SET,
  DB_TYPE_C_ELO,
  DB_TYPE_C_TIME,
  DB_TYPE_C_DATE,
  DB_TYPE_C_NUMERIC,
  DB_TYPE_C_POINTER,
  DB_TYPE_C_ERROR,
  DB_TYPE_C_IDENTIFIER,
  DB_TYPE_C_DATETIME,
  DB_TYPE_C_BIGINT,

#if 0
  DB_TYPE_C_NCHAR = DB_TYPE_C_VARCHAR,
  DB_TYPE_C_VARNCHAR = DB_TYPE_C_VARCHAR,
  DB_TYPE_C_BIT = DB_TYPE_C_VARBIT,

  DB_TYPE_C_MONETARY = DB_TYPE_C_DOUBLE,
#endif

  DB_TYPE_C_LAST		/* last for iteration   */
} DB_TYPE_C;

typedef DB_BIGINT DB_C_BIGINT;
typedef int DB_C_INT;
typedef long DB_C_LONG;
typedef double DB_C_DOUBLE;
typedef char *DB_C_VARCHAR;
typedef char *DB_C_VARBIT;
typedef DB_OBJECT DB_C_OBJECT;
typedef DB_COLLECTION DB_C_SET;
typedef DB_COLLECTION DB_C_COLLECTION;
typedef struct db_c_time DB_C_TIME;
struct db_c_time
{
  int hour;
  int minute;
  int second;
};

typedef struct db_c_date DB_C_DATE;
struct db_c_date
{
  int year;
  int month;
  int day;
};

/* identifiers for the default expression */
typedef enum
{
  DB_DEFAULT_NONE = 0,
  DB_DEFAULT_SYSDATE = 1,
  DB_DEFAULT_SYSDATETIME = 2,
  DB_DEFAULT_UNIX_TIMESTAMP = 3,
  DB_DEFAULT_USER = 4,
  DB_DEFAULT_CURR_USER = 5
} DB_DEFAULT_EXPR_TYPE;

typedef DB_DATETIME DB_C_DATETIME;
typedef unsigned char *DB_C_NUMERIC;
typedef DB_IDENTIFIER DB_C_IDENTIFIER;

extern DB_VALUE *db_value_create (void);
extern DB_VALUE *db_value_copy (DB_VALUE * value);
extern int db_value_clone (const DB_VALUE * src, DB_VALUE * dest);
extern int db_value_clear (DB_VALUE * value);
extern int db_value_clear_nargs (int nargs, ...);
extern int db_value_free (DB_VALUE * value);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_value_clear_array (DB_VALUE_ARRAY * value_array);
#endif
extern void db_value_print (const DB_VALUE * value);
extern int db_value_coerce (const DB_VALUE * src,
			    DB_VALUE * dest,
			    const DB_DOMAIN * desired_domain);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_value_equal (const DB_VALUE * value1, const DB_VALUE * value2);
#endif
extern int db_value_compare (const DB_VALUE * value1,
			     const DB_VALUE * value2);
extern int db_value_domain_init (DB_VALUE * value, DB_TYPE type,
				 const int precision, const int scale);
extern int db_value_domain_min (DB_VALUE * value, DB_TYPE type,
				const int precision, const int scale,
				const int collation_id);
extern int db_value_domain_max (DB_VALUE * value, DB_TYPE type,
				const int precision, const int scale,
				const int collation_id);
extern int db_value_domain_default (DB_VALUE * value, const DB_TYPE type,
				    const int precision, const int scale,
				    const int collation_id);
extern int db_value_domain_zero (DB_VALUE * value, const DB_TYPE type,
				 const int precision, const int scale);
extern int db_string_truncate (DB_VALUE * value, const int max_precision);
extern DB_TYPE db_value_domain_type (const DB_VALUE * value);
extern DB_TYPE db_value_type (const DB_VALUE * value);
extern int db_value_precision (const DB_VALUE * value);
extern int db_value_scale (const DB_VALUE * value);
extern int db_value_put_null (DB_VALUE * value);
extern int db_value_put_numeric (DB_VALUE * value, const DB_TYPE_C c_type,
				 void *input, const int input_length);
#if defined (ENABLE_UNUSED_FUNCTION)
extern bool db_value_type_is_collection (const DB_VALUE * value);
#endif
extern bool db_value_type_is_numeric (const DB_VALUE * value);
extern bool db_value_type_is_bit (const DB_VALUE * value);
extern bool db_value_type_is_char (const DB_VALUE * value);
extern bool db_value_type_is_internal (const DB_VALUE * value);
extern bool db_value_is_null (const DB_VALUE * value);
extern bool db_value_is_null_nargs (int nargs, ...);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_value_get (DB_VALUE * value,
			 const DB_TYPE_C type,
			 void *buf,
			 const int buflen, int *transferlen, int *outputlen);
#endif
extern int db_value_size (const DB_VALUE * value, DB_TYPE_C type, int *size);
extern int db_value_char_size (const DB_VALUE * value, int *size);

/*
 * DB_MAKE_ value constructors.
 * These macros are provided to make the construction of DB_VALUE
 * structures easier.  They will fill in the fields from the supplied
 * arguments. It is not necessary to use these macros but is usually more
 * convenient.
 */
extern int db_make_null (DB_VALUE * value);
extern int db_make_int (DB_VALUE * value, const int num);
extern int db_make_double (DB_VALUE * value, const DB_C_DOUBLE num);
extern int db_make_object (DB_VALUE * value, DB_C_OBJECT * obj);
extern int db_make_sequence (DB_VALUE * value, DB_C_SET * set);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_make_collection (DB_VALUE * value, DB_C_SET * set);
#endif
extern int db_make_time (DB_VALUE * value,
			 const int hour, const int minute, const int second);
extern int db_value_put_encoded_time (DB_VALUE * value,
				      const DB_TIME * time_value);
extern int db_make_date (DB_VALUE * value,
			 const int month, const int day, const int year);
extern int db_value_put_encoded_date (DB_VALUE * value,
				      const DB_DATE * date_value);
extern int db_make_datetime (DB_VALUE * value, const DB_DATETIME * datetime);
extern int db_make_bigint (DB_VALUE * value, const DB_BIGINT num);
extern int db_make_string (DB_VALUE * value, const char *str);
extern int db_make_string_copy (DB_VALUE * value, const char *str);
extern int db_make_numeric (DB_VALUE * value,
			    const DB_C_NUMERIC num,
			    const int precision, const int scale);
extern int db_make_varbit (DB_VALUE * value, const int max_bit_length,
			   const DB_C_VARBIT bit_str,
			   const int bit_str_bit_size);
extern int db_make_varchar (DB_VALUE * value, const int max_char_length,
			    DB_C_VARCHAR str,
			    const int char_str_byte_size,
			    const int collation_id);

extern int db_make_resultset (DB_VALUE * value, const DB_RESULTSET handle);

extern int db_idxkey_clone (const DB_IDXKEY * src, DB_IDXKEY * dest);
extern int db_idxkey_clear (DB_IDXKEY * key);
extern void db_idxkey_print (const DB_IDXKEY * key);
extern bool db_idxkey_is_null (const DB_IDXKEY * key);
extern int db_make_null_nargs (int nargs, ...);
extern int db_idxkey_make_null (DB_IDXKEY * key);

/*
 * DB_GET_ accessor macros.
 * These macros can be used to extract a particular value from a
 * DB_VALUE structure. No type checking is done so you need to make sure
 * that the type is correct.
 */
extern int db_get_int (const DB_VALUE * value);
extern DB_BIGINT db_get_bigint (const DB_VALUE * value);
extern DB_C_VARCHAR db_get_string (const DB_VALUE * value);
extern DB_C_DOUBLE db_get_double (const DB_VALUE * value);
extern DB_OBJECT *db_get_object (const DB_VALUE * value);
extern DB_COLLECTION *db_get_set (const DB_VALUE * value);
extern DB_TIME *db_get_time (const DB_VALUE * value);
extern DB_DATETIME *db_get_datetime (const DB_VALUE * value);
extern DB_DATE *db_get_date (const DB_VALUE * value);
extern DB_C_NUMERIC db_get_numeric (const DB_VALUE * value);
extern DB_C_VARBIT db_get_varbit (const DB_VALUE * value, int *length);
extern int db_get_string_size (const DB_VALUE * value);

extern DB_RESULTSET db_get_resultset (const DB_VALUE * value);

extern int db_string_put_cs_and_collation (DB_VALUE * value,
					   const int collation_id);
extern int db_get_string_collation (const DB_VALUE * value);
extern int valcnv_convert_value_to_string (DB_VALUE * value);

#endif /* _DBTYPE_H_ */
