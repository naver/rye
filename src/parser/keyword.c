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
 * keyword.c - SQL keyword table
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>


#include "rsql_grammar.h"
#include "parser.h"
#include "intl_support.h"
#include "dbtype.h"
#include "string_opfunc.h"

/* It is not required for the keywords to be alphabetically sorted, as they
 * will be sorted when needed. See pt_find_keyword.
 */

static KEYWORD_RECORD keywords[] = {
  {ABSOLUTE_, "ABSOLUTE", 0},
  {ACCESS, "ACCESS", 1},
  {ACTION, "ACTION", 0},
  {ADD, "ADD", 0},
  {ADDDATE, "ADDDATE", 1},
  {AFTER, "AFTER", 0},
  {ALL, "ALL", 0},
  {ALLOCATE, "ALLOCATE", 0},
  {ALTER, "ALTER", 0},
  {ANALYZE, "ANALYZE", 1},
  {AND, "AND", 0},
  {ARCHIVE, "ARCHIVE", 1},
  {ARE, "ARE", 0},
  {AS, "AS", 0},
  {ASC, "ASC", 0},
  {ASSERTION, "ASSERTION", 0},
  {ASYNC, "ASYNC", 0},
  {AT, "AT", 0},
  {ATTRIBUTE, "ATTRIBUTE", 0},
  {AVG, "AVG", 0},
  {BEGIN_, "BEGIN", 0},
  {BETWEEN, "BETWEEN", 0},
  {BIGINT, "BIGINT", 0},
  {BIT_LENGTH, "BIT_LENGTH", 0},
  {BOOLEAN_, "BOOLEAN", 0},
  {BOTH_, "BOTH", 0},
  {BREADTH, "BREADTH", 0},
  {BY, "BY", 0},
  {CACHE, "CACHE", 1},
  {CAPACITY, "CAPACITY", 1},
  {CASCADE, "CASCADE", 0},
  {CASCADED, "CASCADED", 0},
  {CASE, "CASE", 0},
  {CAST, "CAST", 0},
  {CATALOG, "CATALOG", 0},
  {CHANGE, "CHANGE", 0},
  {CHAR_, "CHAR", 0},
  {CHARSET, "CHARSET", 1},
  {CHARACTER_SET_, "CHARACTER_SET", 1},
  {CHECK, "CHECK", 0},
  {CHR, "CHR", 1},
  {CLASS, "CLASS", 0},
  {CLASSES, "CLASSES", 0},
  {CLOSE, "CLOSE", 0},
  {COALESCE, "COALESCE", 0},
  {COLLATE, "COLLATE", 0},
  {COLLATION, "COLLATION", 1},
  {COLUMN, "COLUMN", 0},
  {COLUMNS, "COLUMNS", 1},
  {COMMIT, "COMMIT", 0},
  {COMMITTED, "COMMITTED", 1},
  {CONNECTION, "CONNECTION", 0},
  {CONSTRAINT, "CONSTRAINT", 0},
  {CONSTRAINTS, "CONSTRAINTS", 0},
  {CONTINUE, "CONTINUE", 0},
  {CONVERT, "CONVERT", 0},
  {CORRESPONDING, "CORRESPONDING", 0},
  {COST, "COST", 1},
  {COUNT, "COUNT", 0},
  {CREATE, "CREATE", 0},
  {CROSS, "CROSS", 0},
  {CURRENT_DATE, "CURRENT_DATE", 0},
  {CURRENT_TIME, "CURRENT_TIME", 0},
  {CURRENT_DATETIME, "CURRENT_DATETIME", 0},
  {CURRENT_USER, "CURRENT_USER", 0},
  {CURSOR, "CURSOR", 0},
  {CYCLE, "CYCLE", 0},
  {DATA, "DATA", 0},
  {DATA_TYPE, "DATA_TYPE___", 0},
  {DATABASE, "DATABASE", 0},
  {Date, "DATE", 0},
  {DATE_ADD, "DATE_ADD", 1},
  {DATE_SUB, "DATE_SUB", 1},
  {DAY_, "DAY", 0},
  {DAY_HOUR, "DAY_HOUR", 0},
  {DAY_MILLISECOND, "DAY_MILLISECOND", 0},
  {DAY_MINUTE, "DAY_MINUTE", 0},
  {DAY_SECOND, "DAY_SECOND", 0},
  {NUMERIC, "DEC", 0},
  {NUMERIC, "DECIMAL", 0},
  {DECLARE, "DECLARE", 0},
  {DECREMENT, "DECREMENT", 1},
  {DEFAULT, "DEFAULT", 0},
  {DEFERRABLE, "DEFERRABLE", 0},
  {DEFERRED, "DEFERRED", 0},
  {DELETE_, "DELETE", 0},
  {DEPTH, "DEPTH", 0},
  {DESC, "DESC", 0},
  {DESCRIPTOR, "DESCRIPTOR", 0},
  {DIAGNOSTICS, "DIAGNOSTICS", 0},
  {DIFFERENCE_, "DIFFERENCE", 0},
  {DISCONNECT, "DISCONNECT", 0},
  {DISTINCT, "DISTINCT", 0},
  {DIV, "DIV", 0},
  {Domain, "DOMAIN", 0},
  {Double, "DOUBLE", 0},
  {DROP, "DROP", 0},
  {DUPLICATE_, "DUPLICATE", 0},
  {EACH, "EACH", 0},
  {ELSE, "ELSE", 0},
  {ELSEIF, "ELSEIF", 0},
  {ELT, "ELT", 1},
  {ENCRYPT, "ENCRYPT", 1},
  {END, "END", 0},
  {EQUALS, "EQUALS", 0},
  {ESCAPE, "ESCAPE", 0},
  {EXCEPT, "EXCEPT", 0},
  {EXCEPTION, "EXCEPTION", 0},
  {EXEC, "EXEC", 0},
  {EXECUTE, "EXECUTE", 0},
  {EXISTS, "EXISTS", 0},
  {EXPLAIN, "EXPLAIN", 1},
  {EXTRACT, "EXTRACT", 0},
  {False, "FALSE", 0},
  {FETCH, "FETCH", 0},
  {File, "FILE", 0},
  {FIRST, "FIRST", 0},
  {FLOAT_, "FLOAT", 0},
  {For, "FOR", 0},
  {FOREIGN, "FOREIGN", 0},
  {FOUND, "FOUND", 0},
  {FROM, "FROM", 0},
  {FULL, "FULL", 0},
  {FULLSCAN, "FULLSCAN", 1},
  {GENERAL, "GENERAL", 0},
  {GET, "GET", 0},
  {GE_INF_, "GE_INF", 1},
  {GE_LE_, "GE_LE", 1},
  {GE_LT_, "GE_LT", 1},
  {GLOBAL, "GLOBAL", 0},
  {GO, "GO", 0},
  {GOTO, "GOTO", 0},
  {GRANT, "GRANT", 0},
  {GRANTS, "GRANTS", 1},
  {GROUP_, "GROUP", 0},
  {GROUPS, "GROUPS", 1},
  {GROUP_CONCAT, "GROUP_CONCAT", 1},
  {GT_INF_, "GT_INF", 1},
  {GT_LE_, "GT_LE", 1},
  {GT_LT_, "GT_LT", 1},
  {HAVING, "HAVING", 0},
  {HEADER, "HEADER", 1},
  {HEAP, "HEAP", 1},
  {HOUR_, "HOUR", 0},
  {HOUR_MINUTE, "HOUR_MINUTE", 0},
  {HOUR_MILLISECOND, "HOUR_MILLISECOND", 0},
  {HOUR_SECOND, "HOUR_SECOND", 0},
  {IF, "IF", 0},
  {IFNULL, "IFNULL", 1},
  {IMMEDIATE, "IMMEDIATE", 0},
  {IN_, "IN", 0},
  {INCREMENT, "INCREMENT", 1},
  {INDEX, "INDEX", 0},
  {INDEX_PREFIX, "INDEX_PREFIX", 1},
  {INDEXES, "INDEXES", 1},
  {INDICATOR, "INDICATOR", 0},
  {INF_LE, "INF_LE", 1},
  {INF_LT, "INF_LT", 1},
  {INITIALLY, "INITIALLY", 0},
  {INNER, "INNER", 0},
  {INOUT, "INOUT", 0},
  {INPUT_, "INPUT", 0},
  {INSERT, "INSERT", 0},
  {INSTANCES, "INSTANCES", 1},
  {INTEGER, "INT", 0},
  {INTEGER, "INTEGER", 0},
  {INTERSECT, "INTERSECT", 0},
  {INTERSECTION, "INTERSECTION", 0},
  {INTERVAL, "INTERVAL", 0},
  {INTO, "INTO", 0},
  {IS, "IS", 0},
  {ISNULL, "ISNULL", 1},
  {ISOLATION, "ISOLATION", 0},
  {JAVA, "JAVA", 1},
  {JOIN, "JOIN", 0},
  {JSON, "JSON", 1},
  {KEY, "KEY", 0},
  {KEYS, "KEYS", 1},
  {LANGUAGE, "LANGUAGE", 0},
  {LAST, "LAST", 0},
  {LCASE, "LCASE", 1},
  {LEADING_, "LEADING", 0},
  {LEAVE, "LEAVE", 0},
  {LEFT, "LEFT", 0},
  {LEVEL, "LEVEL", 0},
  {LIKE, "LIKE", 0},
  {LIMIT, "LIMIT", 0},
  {LIST, "LIST", 0},
  {LOCK_, "LOCK", 1},
  {LOG, "LOG", 1},
  {LOOP, "LOOP", 0},
  {LOWER, "LOWER", 0},
  {MATCH, "MATCH", 0},
  {Max, "MAX", 0},
  {MAXIMUM, "MAXIMUM", 1},
  {MEMBERS, "MEMBERS", 1},
  {Min, "MIN", 0},
  {MINUTE_, "MINUTE", 0},
  {MINUTE_MILLISECOND, "MINUTE_MILLISECOND", 0},
  {MINUTE_SECOND, "MINUTE_SECOND", 0},
  {MINVALUE, "MINVALUE", 1},
  {MOD, "MOD", 0},
  {MODIFY, "MODIFY", 0},
  {MODULE, "MODULE", 0},
  {MONTH_, "MONTH", 0},
  {NA, "NA", 0},
  {NAME, "NAME", 1},
  {NATIONAL, "NATIONAL", 0},
  {NATURAL, "NATURAL", 0},
  {NCHAR, "NCHAR", 0},
  {NEXT, "NEXT", 0},
  {NO, "NO", 0},
  {NOCACHE, "NOCACHE", 1},
  {NOMAXVALUE, "NOMAXVALUE", 1},
  {NOMINVALUE, "NOMINVALUE", 1},
  {NONE, "NONE", 0},
  {NOT, "NOT", 0},
  {Null, "NULL", 0},
  {NULLIF, "NULLIF", 0},
  {NULLS, "NULLS", 1},
  {NUMERIC, "NUMERIC", 0},
  {OBJECT, "OBJECT", 0},
  {OCTET_LENGTH, "OCTET_LENGTH", 0},
  {OF, "OF", 0},
  {OFFSET, "OFFSET", 1},
  {OFF_, "OFF", 0},
  {ON_, "ON", 0},
  {OPEN, "OPEN", 0},
  {OPTIMIZATION, "OPTIMIZATION", 0},
  {OPTION, "OPTION", 0},
  {OR, "OR", 0},
  {ORDER, "ORDER", 0},
  {OUT_, "OUT", 0},
  {OUTER, "OUTER", 0},
  {OUTPUT, "OUTPUT", 0},
  {OVERLAPS, "OVERLAPS", 0},
  {OWNER, "OWNER", 1},
  {PAGE, "PAGE", 1},
  {PARAMETERS, "PARAMETERS", 0},
  {PARTIAL, "PARTIAL", 0},
  {PASSWORD, "PASSWORD", 1},
  {PERSIST, "PERSIST", 0},
  {POSITION, "POSITION", 0},
  {PRECISION, "PRECISION", 0},
  {PRESERVE, "PRESERVE", 0},
  {PRIMARY, "PRIMARY", 0},
  {PRIVILEGES, "PRIVILEGES", 0},
  {QUARTER, "QUARTER", 1},
  {RANGE_, "RANGE", 1},
  {READ, "READ", 0},
  {RECURSIVE, "RECURSIVE", 0},
  {REF, "REF", 0},
  {REFERENCES, "REFERENCES", 0},
  {REFERENCING, "REFERENCING", 0},
  {REMOVE, "REMOVE", 1},
  {RENAME, "RENAME", 0},
  {REORGANIZE, "REORGANIZE", 1},
  {REPEATABLE, "REPEATABLE", 1},
  {REPLACE, "REPLACE", 0},
  {RESIGNAL, "RESIGNAL", 0},
  {RESTRICT, "RESTRICT", 0},
  {RETURN, "RETURN", 0},
  {RETURNS, "RETURNS", 0},
  {REVOKE, "REVOKE", 0},
  {RIGHT, "RIGHT", 0},
  {ROLE, "ROLE", 0},
  {ROLLBACK, "ROLLBACK", 0},
  {ROLLUP, "ROLLUP", 0},
  {ROUTINE, "ROUTINE", 0},
  {ROW, "ROW", 0},
  {ROWNUM, "ROWNUM", 0},
  {ROWS, "ROWS", 0},
  {SAVEPOINT, "SAVEPOINT", 0},
  {SCHEMA, "SCHEMA", 0},
  {SCOPE, "SCOPE___", 0},
  {SCROLL, "SCROLL", 0},
  {SEARCH, "SEARCH", 0},
  {SECOND_, "SECOND", 0},
  {SECOND_MILLISECOND, "SECOND_MILLISECOND", 0},
  {MILLISECOND_, "MILLISECOND", 0},
  {SECTION, "SECTION", 0},
  {SELECT, "SELECT", 0},
  {SENSITIVE, "SENSITIVE", 0},
  {SEPARATOR, "SEPARATOR", 1},
  {SEQUENCE, "SEQUENCE", 0},
  {SEQUENCE_OF, "SEQUENCE_OF", 0},
  {SERIALIZABLE, "SERIALIZABLE", 0},
  {SESSION, "SESSION", 0},
  {SESSION_USER, "SESSION_USER", 0},
  {SET, "SET", 0},
  {SHOW, "SHOW", 1},
  {SmallInt, "SHORT", 0},
  {SIGNAL, "SIGNAL", 0},
  {SIMILAR, "SIMILAR", 0},
  {SLOTTED, "SLOTTED", 1},
  {SLOTS, "SLOTS", 1},
  {STABILITY, "STABILITY", 1},
  {STATEMENT, "STATEMENT", 1},
  {STATISTICS, "STATISTICS", 0},
  {STATUS, "STATUS", 1},
  {STDDEV, "STDDEV", 1},
  {STDDEV_POP, "STDDEV_POP", 1},
  {STDDEV_SAMP, "STDDEV_SAMP", 1},
  {String, "STRING", 0},
  {STR_TO_DATE, "STR_TO_DATE", 1},
  {SUBDATE, "SUBDATE", 1},
  {SUBSTRING_, "SUBSTRING", 0},
  {SUM, "SUM", 0},
  {SYSTEM, "SYSTEM", 1},
  {SYSTEM_USER, "SYSTEM_USER", 0},
  {SYS_DATE, "SYSDATE", 0},
  {SYS_DATETIME, "SYSDATETIME", 0},
  {SYS_TIME_, "SYSTIME", 0},
  {SYS_DATE, "SYS_DATE", 0},
  {SYS_DATETIME, "SYS_DATETIME", 0},
  {SYS_TIME_, "SYS_TIME", 0},
  {TABLE, "TABLE", 0},
  {TABLES, "TABLES", 1},
  {TEMPORARY, "TEMPORARY", 0},
  {TEXT, "TEXT", 1},
  {THEN, "THEN", 0},
  {Time, "TIME", 0},
  {TIMEOUT, "TIMEOUT", 1},
  {DATETIME, "DATETIME", 0},
  {TIMEZONE_HOUR, "TIMEZONE_HOUR", 0},
  {TIMEZONE_MINUTE, "TIMEZONE_MINUTE", 0},
  {TO, "TO", 0},
  {TRACE, "TRACE", 1},
  {TRAILING_, "TRAILING", 0},
  {TRANSACTION, "TRANSACTION", 0},
  {TRANSLATE, "TRANSLATE", 0},
  {TRANSLATION, "TRANSLATION", 0},
  {TRIGGER, "TRIGGER", 0},
  {TRIM, "TRIM", 0},
  {TRUNCATE, "TRUNCATE", 0},
  {True, "TRUE", 0},
  {UCASE, "UCASE", 1},
  {UNCOMMITTED, "UNCOMMITTED", 1},
  {Union, "UNION", 0},
  {UNIQUE, "UNIQUE", 0},
  {UNKNOWN, "UNKNOWN", 0},
  {UPDATE, "UPDATE", 0},
  {UPPER, "UPPER", 0},
  {USAGE, "USAGE", 0},
  {USE, "USE", 0},
  {USER, "USER", 0},
  {USING, "USING", 0},
  {VALUE, "VALUE", 0},
  {VALUES, "VALUES", 0},
  {VARBINARY, "VARBINARY", 0},
  {VARCHAR, "VARCHAR", 0},
  {VARIANCE, "VARIANCE", 1},
  {VAR_POP, "VAR_POP", 1},
  {VAR_SAMP, "VAR_SAMP", 1},
  {VARYING, "VARYING", 0},
  {VIEW, "VIEW", 0},
  {VOLUME, "VOLUME", 1},
  {WEEK, "WEEK", 1},
  {WHEN, "WHEN", 0},
  {WHENEVER, "WHENEVER", 0},
  {WHERE, "WHERE", 0},
  {WHILE, "WHILE", 0},
  {WITH, "WITH", 0},
  {WITHOUT, "WITHOUT", 0},
  {WORK, "WORK", 0},
  {WRITE, "WRITE", 0},
  {XOR, "XOR", 0},
  {YEAR_, "YEAR", 0},
  {YEAR_MONTH, "YEAR_MONTH", 0},
  {ZONE, "ZONE", 0},
};

static KEYWORD_RECORD *pt_find_keyword (const char *text);
static int keyword_cmp (const void *k1, const void *k2);


static int
keyword_cmp (const void *k1, const void *k2)
{
  return strcmp (((KEYWORD_RECORD *) k1)->keyword,
		 ((KEYWORD_RECORD *) k2)->keyword);
}

/*
 * pt_find_keyword () -
 *   return: keyword record corresponding to keyword text
 *   text(in): text to test
 */
static KEYWORD_RECORD *
pt_find_keyword (const char *text)
{
  static bool keyword_sorted = false;
  KEYWORD_RECORD *result_key;
  KEYWORD_RECORD dummy;

  if (keyword_sorted == false)
    {
      qsort (keywords,
	     (sizeof (keywords) / sizeof (keywords[0])),
	     sizeof (keywords[0]), keyword_cmp);
      keyword_sorted = true;
    }

  if (!text)
    {
      return NULL;
    }

  if (strlen (text) >= MAX_KEYWORD_SIZE)
    {
      return NULL;
    }

  intl_identifier_upper (text, dummy.keyword);

  result_key = (KEYWORD_RECORD *) bsearch
    (&dummy, keywords,
     (sizeof (keywords) / sizeof (keywords[0])),
     sizeof (KEYWORD_RECORD), keyword_cmp);

  return result_key;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * pt_identifier_or_keyword () -
 *   return: token number of corresponding keyword
 *   text(in): text to test
 */
int
pt_identifier_or_keyword (const char *text)
{
  KEYWORD_RECORD *keyword_rec;

  keyword_rec = pt_find_keyword (text);

  if (keyword_rec)
    {
      return keyword_rec->value;
    }
  else
    {
      return IdName;
    }
}

/*
 * pt_is_reserved_word () -
 *   return: true if string is a keyword
 *   text(in): text to test
 */
bool
pt_is_reserved_word (const char *text)
{
  KEYWORD_RECORD *keyword_rec;

  keyword_rec = pt_find_keyword (text);

  if (!keyword_rec)
    {
      return false;
    }
  else if (keyword_rec->unreserved)
    {
      return false;
    }
  else
    {
      return true;
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * pt_is_keyword () -
 *   return:
 *   text(in):
 */
bool
pt_is_keyword (const char *text)
{
  KEYWORD_RECORD *keyword_rec;

  keyword_rec = pt_find_keyword (text);

  if (!keyword_rec)
    {
      return false;
    }
  else
    {
      return true;
    }

/*  else if (keyword_rec->value == NEW || keyword_rec->value == OLD)
    {
      return false;
    }
  else
    {
      return true;
    }
*/
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * pt_get_keyword_rec () -
 *   return: KEYWORD_RECORD
 *   rec_count(out): keywords record count
 */
KEYWORD_RECORD *
pt_get_keyword_rec (int *rec_count)
{
  *(rec_count) = sizeof (keywords) / sizeof (keywords[0]);

  return (KEYWORD_RECORD *) (keywords);
}
#endif /* ENABLE_UNUSED_FUNCTION */
