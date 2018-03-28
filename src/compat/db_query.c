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
 * db_query.c - QUERY INTERFACE MODULE (Client Side)
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include "error_manager.h"
#include "storage_common.h"
#include "object_representation.h"
#include "object_primitive.h"
#include "class_object.h"
#include "db.h"
#include "xasl_support.h"
#include "db_query.h"
#include "server_interface.h"
#include "system_parameter.h"
#include "xasl_generation.h"
#include "query_executor.h"
#include "network_interface_cl.h"
#include "schema_manager.h"

#include "dbval.h"              /* this must be the last header file included!!! */

#define DB_INVALID_INDEX(i,cnt) ((i) < 0 || (i) >= (cnt))
#define DB_INVALID_RESTYPE(t)                \
                       ((t) != T_SELECT && (t) != T_CALL)

/* A resource mechanism used to effectively handle memory allocation for the
   query result structures. */
struct alloc_resource
{
  int free_qres_cnt;            /* number of free query_result structures */
  int max_qres_cnt;             /* maximum number of free structures to keep */
  DB_QUERY_RESULT *free_qres_list;      /* list of free query entry structures */
};

static struct
{                               /* global query table variable */
  int qres_cnt;                 /* number of active query entries */
  int qres_closed_cnt;          /* number of closed query entries */
  int entry_cnt;                /* # of result list entries */
  DB_QUERY_RESULT **qres_list;  /* list of query result entries   */
  struct alloc_resource alloc_res;      /* allocation structure resource */
} Qres_table =
{
  0, 0, 0, (DB_QUERY_RESULT **) NULL,
  {
  0, 0, (DB_QUERY_RESULT *) NULL}
};                              /* query result table */

static const int QP_QRES_LIST_INIT_CNT = 10;
                               /* query result list initial cnt */
static const float QP_QRES_LIST_INC_RATE = 1.25f;
                           /* query result list increment ratio */

static DB_QUERY_RESULT *allocate_query_result (void);
static void free_query_result (DB_QUERY_RESULT * q_res);
static int query_compile_local (const char *RSQL_query, DB_QUERY_ERROR * query_error, DB_SESSION ** session);
static int query_execute_local (const char *RSQL_query, void *result, DB_QUERY_ERROR * query_error, int execute);
static DB_QUERY_TYPE *db_cp_query_type_helper (DB_QUERY_TYPE * src, DB_QUERY_TYPE * dest);
#if defined (ENABLE_UNUSED_FUNCTION)
static int or_packed_query_format_size (const DB_QUERY_TYPE * q, int *count);
static char *or_pack_query_format (char *buf, const DB_QUERY_TYPE * q, const int count);
static char *or_unpack_query_format (char *buf, DB_QUERY_TYPE ** q);
#endif

/*
 * allocate_query_result() - This function allocates a query_result structure
 *   from the free list of query_result structures if any, or by malloc to
 *   allocate a new structure.
 * return : DB_QUERY_RESULT pointer or NULL
 */
static DB_QUERY_RESULT *
allocate_query_result (void)
{
  DB_QUERY_RESULT *q_res;

  q_res = Qres_table.alloc_res.free_qres_list;
  if (q_res != NULL)
    {
      Qres_table.alloc_res.free_qres_list = q_res->next;
      Qres_table.alloc_res.free_qres_cnt--;
    }
  else
    {
      q_res = (DB_QUERY_RESULT *) malloc (DB_SIZEOF (DB_QUERY_RESULT));
      if (q_res == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (DB_QUERY_RESULT));
        }
    }

  return q_res;
}

/*
 * free_query_result() - This function frees the query_result structure by
 *    putting it to the free query_result structure list if there are not
 *   many in the list, or by calling db_free.
 * return : void
 * q_res(in): Query result structure to be freed.
 */
static void
free_query_result (DB_QUERY_RESULT * q_res)
{
  if (Qres_table.alloc_res.free_qres_cnt < Qres_table.alloc_res.max_qres_cnt)
    {
      q_res->next = Qres_table.alloc_res.free_qres_list;
      Qres_table.alloc_res.free_qres_list = q_res;
      Qres_table.alloc_res.free_qres_cnt++;
    }
  else
    {
      free_and_init (q_res);
    }
}

/*
 * db_free_query_format() - This function frees the query type area.
 * return : void
 * q(in): Pointer to the query type list
 *
 * note : p->domain is a pointer to a cached domain structure,
 *        and should no longer be freed.
 */
void
db_free_query_format (DB_QUERY_TYPE * q)
{
  DB_QUERY_TYPE *p, *n;

  n = q;
  while (n != NULL)
    {
      p = n;
      n = n->next;
      if (p->name != NULL)
        {
          free_and_init (p->name);
        }
      if (p->attr_name != NULL)
        {
          free_and_init (p->attr_name);
        }
      if (p->spec_name != NULL)
        {
          free_and_init (p->spec_name);
        }
      if (p->src_domain != NULL)
        {
          regu_free_domain (p->src_domain);
        }
      free_and_init (p);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * or_packed_query_format_size - calculate the size of the packed query format
 *    return: size
 *    columns(in): query format information
 *    count (out): will hold the count of columns when the function returns
 */
static int
or_packed_query_format_size (const DB_QUERY_TYPE * columns, int *count)
{
  int size = 0;
  int len = 0;
  int columns_cnt = 0;
  const DB_QUERY_TYPE *column = NULL;
  /* number of columns in the list */
  size = OR_INT_SIZE;
  if (columns == NULL)
    {
      /* only an integer containing the size (0) */
      return size;
    }

  for (column = columns; column != NULL; column = column->next)
    {
      /* column type */
      size += OR_INT_SIZE;
      /* column name */
      size += or_packed_string_length (column->name, &len);
      /* attribute name */
      size += or_packed_string_length (column->attr_name, &len);
      /* spec name */
      size += or_packed_string_length (column->spec_name, &len);
      /* column data type */
      size += OR_INT_SIZE;
      /* column data size */
      size += OR_INT_SIZE;
      /* column domain information */
      size += or_packed_domain_size (column->domain);
      /* column source domain information */
      size += or_packed_domain_size (column->src_domain);
      /* column user visible */
      size += OR_INT_SIZE;
      columns_cnt++;
    }
  *count = columns_cnt;
  return size;
}

/*
 * or_pack_query_format - pack a query format list
 *    return	  : advanced pointer
 *    buf (in)	  : buffer pointer
 *    columns (in): the query format list
 *    count (in)  : the count of query format contained in the list
 */
static char *
or_pack_query_format (char *buf, const DB_QUERY_TYPE * columns, const int count)
{
  char *ptr = NULL;
  int len = 0;
  const DB_QUERY_TYPE *column;

  if (count != 0)
    {
      /* sanity check */
      assert (columns != NULL);
    }
  /* pack the number of columns */
  ptr = or_pack_int (buf, count);
  if (count == 0)
    {
      return ptr;
    }

  for (column = columns; column != NULL; column = column->next)
    {
      /* column name */
      len = (column->name == NULL) ? 0 : strlen (column->name);
      ptr = or_pack_string_with_length (ptr, column->name, len);

      /* attribute name */
      len = (column->attr_name == NULL) ? 0 : strlen (column->attr_name);
      ptr = or_pack_string_with_length (ptr, column->attr_name, len);

      /* spec name */
      len = (column->spec_name == NULL) ? 0 : strlen (column->spec_name);
      ptr = or_pack_string_with_length (ptr, column->spec_name, len);

      /* column data type */
      ptr = or_pack_int (ptr, column->db_type);
      /* column domain information */
      ptr = or_pack_domain (ptr, column->domain, 1);
      /* column source domain information */
      ptr = or_pack_domain (ptr, column->src_domain, 1);
      /* column user visible */
      ptr = or_pack_int (ptr, column->visible_type);
    }

  return ptr;
}

/*
 * or_unpack_query_format - unpack a query format list
 *    return	      : advanced pointer
 *    buf (in)	      : buffer pointer
 *    columns (in/out): the columns list
 *    count (in/out)  : the count of columns contained in the list
 *
 * Note: This function allocates memory for all members of the query format
 * object. This memory is not allocated in the private heap of the current
 * thread and needs to be released using free_and_init.
 */
static char *
or_unpack_query_format (char *buf, DB_QUERY_TYPE ** columns)
{
  char *ptr = NULL;
  int size = 0, i = 0;
  DB_QUERY_TYPE *head = NULL, *current = NULL;
  TP_DOMAIN *tp_dom = NULL;

  ptr = or_unpack_int (buf, &size);
  for (i = 0; i < size; i++)
    {
      int tmp = 0;
      DB_QUERY_TYPE *column = (DB_QUERY_TYPE *) malloc (sizeof (DB_QUERY_TYPE));

      if (column == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_QUERY_TYPE));
          goto error_cleanup;
        }
      /* column name */
      ptr = or_unpack_string_alloc (ptr, &(column->name));
      /* attribute name */
      ptr = or_unpack_string_alloc (ptr, &(column->attr_name));
      /* spec name */
      ptr = or_unpack_string_alloc (ptr, &(column->spec_name));
      /* column data type */
      ptr = or_unpack_int (ptr, &tmp);
      column->db_type = (DB_TYPE) tmp;
      /* column domain information */
      ptr = or_unpack_domain (ptr, &tp_dom, NULL);
      column->domain = tp_dom;

      /* column source domain */
      ptr = or_unpack_domain (ptr, &tp_dom, NULL);
      if (tp_dom != NULL)
        {
          column->src_domain = regu_cp_domain (tp_dom);
        }
      else
        {
          column->src_domain = NULL;
        }

      /* column user visible */
      ptr = or_unpack_int (ptr, &tmp);
      column->visible_type = (COL_VISIBLE_TYPE) tmp;

      column->next = NULL;

      if (head == NULL)
        {
          head = column;
          current = head;
        }
      else
        {
          current->next = column;
          current = current->next;
        }
    }

  *columns = head;
  return ptr;

error_cleanup:
  while (head != NULL)
    {
      current = head;
      head = head->next;

      /* free name */
      free_and_init (current->name);
      /* free attribute name */
      free_and_init (current->attr_name);
      /* free spec name */
      free_and_init (current->spec_name);
      free_and_init (current);
    }
  return ptr;
}

/*
 * db_free_colname_list() - This function frees the column name list.
 * return : void
 * colname_list(in): list of column names
 * cnt(in): number of names
 */
void
db_free_colname_list (char **colname_list, int cnt)
{
  int i;

  if (colname_list == NULL)
    {
      return;
    }

  for (i = 0; i < cnt; i++)
    {
      if (colname_list[i] != NULL)
        {
          free_and_init (colname_list[i]);
        }
    }

  free_and_init (colname_list);
}

/*
 * db_free_domain_list() - This function frees the domain list.
 * return : void
 * domain_list(in): List of domain pointers
 * cnt(in): Number of domain pointers
 */
void
db_free_domain_list (SM_DOMAIN ** domain_list, int cnt)
{
  int i;

  if (domain_list == NULL)
    {
      return;
    }

  for (i = 0; i < cnt; i++)
    {
      if (domain_list[i] != NULL)
        {
          regu_free_domain (domain_list[i]);
        }
    }

  free_and_init (domain_list);
}
#endif

/*
 * db_free_query_result() - This function frees the areas allocated for the
 *    query result structure and also the supplied query result structure
 *    pointer r.
 * return : void
 * r(in): Query Result Structure pointer
 */
void
db_free_query_result (DB_QUERY_RESULT * r)
{
  if (r == NULL)
    {
      return;
    }

  /* disconnect query result from the query table */
#if defined(QP_DEBUG)
  if (Qres_table.qres_list[r->qtable_ind] != r)
    {
      (void) fprintf (stdout, "*WARNING*: Misconnection between the query" "result structure and query table.\n");
      return;
    }
#endif

  Qres_table.qres_list[r->qtable_ind] = (DB_QUERY_RESULT *) NULL;
  Qres_table.qres_cnt--;
  if (r->status == T_CLOSED && Qres_table.qres_closed_cnt > 0)
    {
      Qres_table.qres_closed_cnt--;
    }

  /* free type list */
  db_free_query_format (r->query_type);
  r->query_type = NULL;

  switch (r->type)
    {
    case T_SELECT:
      break;

    case T_CALL:
      db_value_free (r->res.c.val_ptr);
      break;

    default:
      break;
    }

  r->status = T_CLOSED;

  free_query_result (r);
}

/*
 * db_alloc_query_format() - This function allocates specified number of type
 *    list nodes. And query type pointer set to the beginning of allocated list
 *    is returned.
 * return : query type pointer or NULL on failure
 * cnt(in): number of nodes in the type list
 */
DB_QUERY_TYPE *
db_alloc_query_format (int cnt)
{
  DB_QUERY_TYPE *p, *q;
  int k;

  if (cnt == 0)
    {
      return NULL;
    }

  q = (DB_QUERY_TYPE *) malloc (DB_SIZEOF (DB_QUERY_TYPE));
  if (q == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (DB_QUERY_TYPE));
      return NULL;
    }

  /* initialize */
  q->name = (char *) NULL;
  q->attr_name = (char *) NULL;
  q->spec_name = (char *) NULL; /* fill it at pt_fillin_type_size() */
  q->db_type = DB_TYPE_NULL;
  q->domain = (SM_DOMAIN *) NULL;
  q->src_domain = (SM_DOMAIN *) NULL;
  q->visible_type = USER_COLUMN;

  for (k = 0, p = q, p->next = NULL; k < cnt - 1; k++, p = p->next, p->next = NULL)
    {
      p->next = (DB_QUERY_TYPE *) malloc (DB_SIZEOF (DB_QUERY_TYPE));
      if (p->next == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (DB_QUERY_TYPE));
          db_free_query_format (q);
          return NULL;
        }
      /* initialize */
      p->next->db_type = DB_TYPE_NULL;
      p->next->name = (char *) NULL;
      p->next->attr_name = (char *) NULL;
      p->next->spec_name = (char *) NULL;
      p->next->domain = (SM_DOMAIN *) NULL;
      p->next->src_domain = (SM_DOMAIN *) NULL;
      p->next->visible_type = USER_COLUMN;
    }

  return q;
}

/*
 * db_alloc_query_result() - This function allocates a query result structure
 *    for the indicated type and column count.
 * return : query result pointer or NULL on failure.
 * r_type(in): query Result Structure type
 * col_cnt(in): column count
 */
DB_QUERY_RESULT *
db_alloc_query_result (DB_RESULT_TYPE r_type, UNUSED_ARG int col_cnt)
{
  DB_QUERY_RESULT *r, **qres_ptr;
  int ind, k;
  int new_cnt;

#if defined(QP_DEBUG)
  if (DB_INVALID_RESTYPE (r_type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return (DB_QUERY_RESULT *) NULL;
    }
#endif

  /* first search query table result list to see if there is place */
  for (ind = 0, qres_ptr = Qres_table.qres_list; ind < Qres_table.entry_cnt && *qres_ptr != NULL; ind++, qres_ptr++)
    {
      ;                         /* NULL */
    }

  if (ind == Qres_table.entry_cnt)
    {
      /* query table is full, so enlarge the table */
      if (Qres_table.entry_cnt == 0)
        {                       /* first time allocation */
          Qres_table.qres_list = (DB_QUERY_RESULT **) malloc (QP_QRES_LIST_INIT_CNT * DB_SIZEOF (DB_QUERY_RESULT *));
          if (Qres_table.qres_list == NULL)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                      ER_OUT_OF_VIRTUAL_MEMORY, 1, QP_QRES_LIST_INIT_CNT * DB_SIZEOF (DB_QUERY_RESULT *));
              return (DB_QUERY_RESULT *) NULL;
            }
          Qres_table.entry_cnt = QP_QRES_LIST_INIT_CNT;

          /* initialize query_result allocation resource */
          Qres_table.alloc_res.free_qres_cnt = 0;
          Qres_table.alloc_res.max_qres_cnt = Qres_table.entry_cnt;
          Qres_table.alloc_res.free_qres_list = (DB_QUERY_RESULT *) NULL;
        }
      else
        {
          /* expand the existing table */
          new_cnt = (int) ((Qres_table.entry_cnt * QP_QRES_LIST_INC_RATE) + 1);
          Qres_table.qres_list =
            (DB_QUERY_RESULT **) realloc (Qres_table.qres_list, new_cnt * DB_SIZEOF (DB_QUERY_RESULT *));
          if (Qres_table.qres_list == NULL)
            {
              return (DB_QUERY_RESULT *) NULL;
            }
          Qres_table.entry_cnt = new_cnt;

          /* expand query result allocation resource */
          Qres_table.alloc_res.max_qres_cnt = Qres_table.entry_cnt;
        }

      /* initialize newly allocated entries */
      for (k = ind, qres_ptr =
           (DB_QUERY_RESULT **) Qres_table.qres_list + ind; k < Qres_table.entry_cnt; k++, qres_ptr++)
        {
          *qres_ptr = (DB_QUERY_RESULT *) NULL;
        }
    }
  qres_ptr = (DB_QUERY_RESULT **) Qres_table.qres_list + ind;

  *qres_ptr = allocate_query_result ();
  if (*qres_ptr == NULL)
    {
      return (DB_QUERY_RESULT *) NULL;
    }

  /* connect query result structure to the query table */
  r = *qres_ptr;
  r->qtable_ind = ind;
  Qres_table.qres_cnt++;

  /* allocation of query type list is done later */
  r->query_type = (DB_QUERY_TYPE *) NULL;
  r->type_cnt = 0;

  switch (r_type)
    {
    case T_SELECT:
      break;

    case T_CALL:
      r->res.c.val_ptr = (DB_VALUE *) NULL;
      break;

    default:
      break;
    }

  return r;
}

/*
 * db_init_query_result() - This function initializes the query result
 *    structure according to the type.
 * return : void
 * r(out): query Result Structure
 * r_type(in): query Result Structure Type
 */
void
db_init_query_result (DB_QUERY_RESULT * r, DB_RESULT_TYPE r_type)
{
  if (r == NULL)
    {
      return;
    }

  r->type = r_type;
  r->status = T_OPEN;
  r->col_cnt = 0;
  r->is_server_query_ended = false;

  switch (r->type)
    {
    case T_SELECT:
      {
        r->res.s.query_id = -1;
        r->res.s.stmt_type = (RYE_STMT_TYPE) 0;
      }
      break;

    case T_CALL:
      r->res.c.crs_pos = C_BEFORE;
      break;

    default:
      break;
    }
  r->next = (DB_QUERY_RESULT *) NULL;
}

#if defined (RYE_DEBUG)
/*
 * db_dump_query_result() - this function dumps the content of the query result
 *   structure to standard output.
 * return : void
 * r: Query Result Structure
 */
void
db_dump_query_result (DB_QUERY_RESULT * r)
{
  if (r == NULL)
    {
      return;
    }

#if defined(QP_DEBUG)
  if (DB_INVALID_RESTYPE (r->type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return;
    }
#endif

  fprintf (stdout, "\nQuery Result Structure: \n");
  fprintf (stdout, "Type: %s \n", (r->type == T_SELECT) ? "T_SELECT" : (r->type == T_CALL) ? "T_CALL" : "T_UNKNOWN");
  fprintf (stdout, "Status: %s \n", (r->status == T_OPEN) ? "T_OPEN" :
           (r->status == T_CLOSED) ? "T_CLOSED" : "T_UNKNOWN");
  fprintf (stdout, "Column Count: %d \n", r->col_cnt);
  fprintf (stdout, "\n");
  if (r->type == T_SELECT)
    {
      fprintf (stdout, "Query_id: %lld \n", (long long) r->res.s.query_id);
      fprintf (stdout, "Stmt_id: %d \n", r->res.s.stmt_id);
      fprintf (stdout, "Tuple Cnt: %d \n", r->res.s.cursor_id.list_id.tuple_cnt);
      fprintf (stdout, "Stmt_type: %d \n", r->res.s.stmt_type);
    }                           /* if */
  fprintf (stdout, "\n");
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_cp_colname_list() - This function forms a new column name list from the
 *    given one.
 * return : column name list. NULL on error
 * colname_list(in): List of column names
 * cnt(in): Number of columns
 *
 * note : The returned column name list must be freed with db_free_colname_list
 */
char **
db_cp_colname_list (char **colname_list, int cnt)
{
  char **newname_list;
  int size, i;

  if (colname_list == NULL)
    {
      return NULL;
    }

  newname_list = (char **) malloc (cnt * DB_SIZEOF (char *));
  if (newname_list == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, cnt * DB_SIZEOF (char *));
      return NULL;
    }

  for (i = 0; i < cnt; i++)
    {
      newname_list[i] = NULL;
    }

  for (i = 0; i < cnt; i++)
    {
      size = strlen (colname_list[i]) + 1;
      newname_list[i] = (char *) malloc (size);
      if (newname_list[i] == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
          db_free_colname_list (newname_list, cnt);
          return NULL;
        }
      memcpy (newname_list[i], colname_list[i], size);
    }

  return newname_list;
}

/*
 * db_cp_domain_list() - This function forms a new domain list pointer.
 * return : new domain list. NULL on error.
 * domain_list(in): List of domain pointers
 * cnt(in): Number of domain pointers
 *
 * note: The content of each domain pointer is NOT copied to a new area, only
 *       pointers are set to the contents. There the actual domain pointers
 *       must NOT be freed until qp_free_domain_ptr() is explicitly called.
 */
SM_DOMAIN **
db_cp_domain_list (SM_DOMAIN ** domain_list, int cnt)
{
  SM_DOMAIN **newdomain_list;
  int i;

  if (domain_list == NULL)
    {
      return NULL;
    }

  newdomain_list = (SM_DOMAIN **) malloc (cnt * DB_SIZEOF (SM_DOMAIN *));
  if (newdomain_list == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, cnt * DB_SIZEOF (SM_DOMAIN *));
      return NULL;
    }

  for (i = 0; i < cnt; i++)
    {
      newdomain_list[i] = NULL;
    }

  for (i = 0; i < cnt; i++)
    {
      newdomain_list[i] = regu_cp_domain (domain_list[i]);
      if (newdomain_list[i] == NULL)
        {
          db_free_domain_list (newdomain_list, cnt);
          return NULL;
        }
    }

  return newdomain_list;
}
#endif

/*
 * db_clear_client_query_result() - This function is called when a transaction
 *    commits/aborts on the client or when the server goes down, in order to
 *    close the existing the query result structures.
 * return : void
 * notify_server(in) :
 */
void
db_clear_client_query_result (int notify_server, bool end_holdable)
{
  DB_QUERY_RESULT **qres_ptr;
  int k;

  /* search query table result list and mark existing entries as closed */
  for (k = 0, qres_ptr = Qres_table.qres_list; k < Qres_table.entry_cnt; k++, qres_ptr++)
    {
      if (*qres_ptr == NULL)
        {
          continue;
        }
      if (((*qres_ptr)->type == T_SELECT && !(*qres_ptr)->res.s.holdable) || end_holdable)
        {
          /* if end_holdable is false, only end queries that are not
             holdable */
          db_query_end_internal (*qres_ptr, notify_server);
        }
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_final_client_query_result() - This function is called to finalize client
 *    side query result structures by deallocating the allocated areas.
 * return: void
 */
void
db_final_client_query_result (void)
{
  DB_QUERY_RESULT **qres_ptr;
  DB_QUERY_RESULT *q_res, *t_res;
  int k;

  /* search query table result list and free all existing entries */
  for (k = 0, qres_ptr = Qres_table.qres_list; k < Qres_table.entry_cnt; k++, qres_ptr++)
    {
      if (*qres_ptr != NULL)
        {
          if ((*qres_ptr)->type == T_SELECT)
            {
              cursor_free (&(*qres_ptr)->res.s.cursor_id);
            }
          db_free_query_result (*qres_ptr);
        }
    }

  if (Qres_table.qres_list != NULL)
    {
      free_and_init (Qres_table.qres_list);
      Qres_table.entry_cnt = 0;
    }

  /* free area allocated for query_result structures resource */
  q_res = Qres_table.alloc_res.free_qres_list;
  while (q_res != NULL)
    {
      t_res = q_res;
      q_res = q_res->next;
      free_and_init (t_res);
    }
  Qres_table.alloc_res.free_qres_list = (DB_QUERY_RESULT *) NULL;

  /* free the stuff allocated during xasl tree translation */
  xts_final ();
}
#endif

/*
 * db_cp_query_type_helper() - Copies the given type to a newly allocated type
 * return : dest or NULL on error.
 * src(in): query type to be copied
 * dest(in): query type newly allocated
 *
 * note : It is no longer necessary to use regu_cp_domain() to copy the
 *  domain field, since it is now a pointer to a cached domain structure.
 */
static DB_QUERY_TYPE *
db_cp_query_type_helper (DB_QUERY_TYPE * src, DB_QUERY_TYPE * dest)
{
  int size;

  dest->db_type = src->db_type;
  dest->domain = src->domain;

  dest->name = NULL;
  dest->attr_name = NULL;
  dest->spec_name = NULL;
  dest->src_domain = NULL;
  dest->visible_type = src->visible_type;

  if (src->name != NULL)
    {
      size = strlen (src->name) + 1;
      dest->name = (char *) malloc (size);
      if (dest->name == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
          return NULL;
        }
      memcpy ((char *) dest->name, src->name, size);
    }

  if (src->attr_name != NULL)
    {
      size = strlen (src->attr_name) + 1;
      dest->attr_name = (char *) malloc (size);
      if (dest->attr_name == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
          return NULL;
        }
      memcpy ((char *) dest->attr_name, src->attr_name, size);
    }

  if (src->spec_name != NULL)
    {
      size = strlen (src->spec_name) + 1;
      dest->spec_name = (char *) malloc (size);
      if (dest->spec_name == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
          return NULL;
        }
      memcpy ((char *) dest->spec_name, src->spec_name, size);
    }

  if (src->src_domain != NULL)
    {
      dest->src_domain = regu_cp_domain (src->src_domain);
    }

  return dest;
}

/*
 * db_cp_query_type() - This function copies the given type list into a newly
 *                      allocated type list.
 * return : new type list or NULL on error.
 * query_type(in): query type list to be copied
 * copy_only_user(in):
 */
DB_QUERY_TYPE *
db_cp_query_type (DB_QUERY_TYPE * query_type, int copy_only_user)
{
  DB_QUERY_TYPE *q;
  DB_QUERY_TYPE *ptr1, *ptr2;
  int cnt;

  /* find count of nodes to copy */
  for (cnt = 0, ptr1 = query_type; ptr1; ptr1 = ptr1->next)
    {
      if ((ptr1->visible_type != SYSTEM_ADDED_COLUMN) && (!copy_only_user || ptr1->visible_type == USER_COLUMN))
        {
          cnt++;
        }
    }

  q = db_alloc_query_format (cnt);
  if (q == NULL)
    {
      return NULL;
    }

  for (ptr1 = query_type, ptr2 = q; ptr1; ptr1 = ptr1->next)
    {
      if ((ptr1->visible_type != SYSTEM_ADDED_COLUMN) && (!copy_only_user || ptr1->visible_type == USER_COLUMN))
        {
          ptr2 = db_cp_query_type_helper (ptr1, ptr2);
          if (ptr2 == NULL)
            {
              db_free_query_format (q);
              return NULL;
            }

          ptr2 = ptr2->next;
        }
    }

  return q;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_get_query_type() - This function forms a query type list structure from
 *   the given parmeters. The cnt field refers to number of nodes in the
 *   provided lists which is actual column count of the query result list file.
 *   If oid_included is set to true, the first node of the lists which
 *   correspond to the first hidden oid column is eliminated from the formed
 *   type list.
 * return : DB_QUERY_TYPE pointer or NULL.
 * type_list(in): data type list
 * size_list(in): size list
 * colname_list(in): column name list(can be NULL)
 * attrname_list(in): attribute name list(can be NULL)
 * domain_list(in): domain list(can be NULL)
 * src_domain_list(in): source Domain list(can be NULL)
 * cnt(in): number of columns
 * oid_included(in): hidden first oid column included
 */
DB_QUERY_TYPE *
db_get_query_type (DB_TYPE * type_list, int *size_list,
                   char **colname_list, char **attrname_list,
                   SM_DOMAIN ** domain_list, SM_DOMAIN ** src_domain_list, int cnt, bool oid_included)
{
  DB_QUERY_TYPE *q, *type_ptr;
  DB_TYPE *typep;
  char **colnamep;
  char **attrnamep;
  SM_DOMAIN **domainp;
  SM_DOMAIN **src_domainp;
  int *sizep;
  int k, size;
  int type_cnt;

  CHECK_CONNECT_NULL ();

  if (type_list == NULL || size_list == NULL || cnt <= 0)
    {
      return NULL;
    }

  type_cnt = (oid_included) ? (cnt - 1) : cnt;

  q = db_alloc_query_format (type_cnt);
  if (q == NULL)
    {
      return NULL;
    }

  typep = type_list;
  sizep = size_list;
  colnamep = colname_list;
  attrnamep = attrname_list;
  domainp = domain_list;
  src_domainp = src_domain_list;
  type_ptr = q;
  for (k = 0; k < cnt; k++)
    {

      if (!(oid_included && k == 0))
        {
          type_ptr->db_type = *typep;
          type_ptr->size = *sizep;
          type_ptr->name = (char *) NULL;
          type_ptr->attr_name = (char *) NULL;
          type_ptr->spec_name = (char *) NULL;
          type_ptr->domain = (SM_DOMAIN *) NULL;
          type_ptr->src_domain = (SM_DOMAIN *) NULL;
          type_ptr->visible_type = USER_COLUMN;
          if (colname_list)
            {
              /* column names can NOT be NULL */
              size = strlen (*colnamep) + 1;
              type_ptr->name = (char *) malloc (size);
              if (type_ptr->name == NULL)
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
                  db_free_query_format (q);
                  return NULL;
                }
              memcpy ((char *) type_ptr->name, *colnamep, size);
            }
          if (attrname_list)
            {
              if (*attrnamep)   /* attribute names can be NULL */
                {
                  size = strlen (*attrnamep) + 1;
                  type_ptr->attr_name = (char *) malloc (size);
                  if (type_ptr->attr_name == NULL)
                    {
                      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
                      db_free_query_format (q);
                      return NULL;
                    }
                  memcpy ((char *) type_ptr->attr_name, *attrnamep, size);
                }
            }
          if (domain_list)
            {
              type_ptr->domain = regu_cp_domain (*domainp);
            }
          if (src_domain_list)
            {
              type_ptr->src_domain = regu_cp_domain (*src_domainp);
            }

          type_ptr = type_ptr->next;
        }
      typep++;
      sizep++;
      if (colname_list)
        {
          colnamep++;
        }
      if (attrname_list)
        {
          attrnamep++;
        }
      if (domain_list)
        {
          domainp++;
        }
      if (src_domain_list)
        {
          src_domainp++;
        }
    }

  return q;
}
#endif

/*
 * query_compile_local() - This function handles the query compilation part of
 *              query_execute_local.
 * return : error code.
 * RSQL_query (in) : query string to be executed
 * query_error(out): set to the error information, if any.
 * session    (out): query session handle
 * stmt_no    (out): statement number
 */
static int
query_compile_local (const char *RSQL_query, DB_QUERY_ERROR * query_error, DB_SESSION ** session)
{
  int error = NO_ERROR;         /* return code from funcs */
  DB_SESSION_ERROR *errs;

  CHECK_CONNECT_ERROR ();
  *session = db_open_buffer_local (RSQL_query);
  if (!(*session))
    {
      return (er_errid ());
    }

  /* compile the statement */
  db_compile_statement_local (*session);
  errs = db_get_errors (*session);
  if (errs != NULL)
    {
      int line, col;
      (void) db_get_next_error (errs, &line, &col);
      error = er_errid ();
      if (query_error)
        {
          query_error->err_lineno = line;
          query_error->err_posno = col;
        }
    }

  if (error < 0)
    {
      db_close_session_local (*session);
      *session = NULL;
      return (er_errid ());
    }

  if (error < 0)
    {
      db_close_session_local (*session);
      *session = NULL;
      return (er_errid ());
    }

  return (error);
}

/*
 * db_execute_with_values() - This function executes a dynamic sql select query
 *    with input values
 * return : error code
 *   RSQL_query  (IN) : query string to be executed
 *   result      (OUT): pointer to the query result structure
 *   query_error (OUT): set to the error information, if any.
 *   arg_count   (IN) : number of input values
 *   vals        (IN) : input values
 */
int
db_execute_with_values (const char *RSQL_query, DB_QUERY_RESULT ** result,
                        DB_QUERY_ERROR * query_error, int arg_count, DB_VALUE * vals)
{
  int error;
  DB_SESSION *session = NULL;

  /* compile the query */
  error = query_compile_local (RSQL_query, query_error, &session);
  if (session == NULL)
    {
      return error;
    }

  if (arg_count > 0)
    {
      db_push_values (session, arg_count, vals);
    }

  error = db_execute_and_keep_statement (session, result);

  db_close_session_local (session);

  return (error);
}

/*
 * query_execute_local() -
 * return : error code
 * RSQL_query(in): query string to be executed
 * result(out): Pointer to the query result structure
 * query_error(out): Set to the error information, if any.
 * execute(in):
 * exec_mode(in):
 */
static int
query_execute_local (const char *RSQL_query, void *result, DB_QUERY_ERROR * query_error, int execute)
{
  int error;
  DB_SESSION *session = NULL;

  if (result)
    {
      *(char **) result = NULL;
    }

  /* compile the query */
  error = query_compile_local (RSQL_query, query_error, &session);
  if (session == NULL)
    {
      return error;
    }

  if (execute && error == NO_ERROR)
    {
      error = db_execute_statement_local (session, (DB_QUERY_RESULT **) result);
    }
  else if (result)
    {
      *(DB_QUERY_TYPE **) result = db_get_query_type_list (session);
    }

  db_close_session_local (session);

  return (error);
}

/*
 * QUERY PRE-PROCESSING ROUTINES
 */

/*
 * db_query_format_next() - This function is used to scan the elements of the
 *    query type list.
 * return : Pointer to the next type list node, or NULL
 * query_type(in): Pointer to the current type list node
 *
 * note : Do not pass the result of this function to the
 *    db_query_format_free()function.
 */
DB_QUERY_TYPE *
db_query_format_next (DB_QUERY_TYPE * query_type)
{
  CHECK_1ARG_NULL (query_type);
  return query_type->next;
}

/*
 * db_query_format_name() - This function is used to get the name of a column
 *    in a query format descriptor list.
 * return : Column name of the current type list node
 * query_type(in): Pointer to the current type list node
 *
 * note : Do not free this string. it is freed with the descriptor in the
 *   db_query_format_free() function. If the column was derived from a
 *   constant expression, the column name is similarly derived.
 *   for example, x + 10.
 */
char *
db_query_format_name (DB_QUERY_TYPE * query_type)
{
  CHECK_1ARG_NULL (query_type);
  return ((char *) query_type->name);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_query_format_attr_name() - This function returns attribute name of the
 *    current query type list node.
 * return : attribute name of the current type list node
 * query_type(in): pointer to the current type list node
 */
char *
db_query_format_attr_name (DB_QUERY_TYPE * query_type)
{
  CHECK_1ARG_NULL (query_type);
  return ((char *) query_type->attr_name);
}
#endif

/*
 * db_query_format_domain() - This function returns domain information of
 *    current query type list node.
 * return : domain information of the current type list node
 * query_type(in): pointer to the current type list node
 */
SM_DOMAIN *
db_query_format_domain (DB_QUERY_TYPE * query_type)
{
  CHECK_1ARG_NULL (query_type);
  return query_type->domain;
}

/*
 * db_query_format_src_domain() - Returns source domain information of current
 *    query type list node.
 * return : source domain information of the current type node
 * query_type(in): Pointer to the current type list node
 *
 */
SM_DOMAIN *
db_query_format_src_domain (DB_QUERY_TYPE * query_type)
{
  CHECK_1ARG_NULL (query_type);
  return query_type->src_domain;
}

/*
 * db_query_format_type() - This function is used to get the basic type
 *    identifier of a column in a query format descriptor list.
 * return : basic type id of a column
 * query_type(in): Pointer to the current type list node
 *
 * note : Use the db_query_format_domain() function for non-primitive types.
 */
DB_TYPE
db_query_format_type (DB_QUERY_TYPE * query_type)
{
  CHECK_1ARG_RETURN_EXPR (query_type, DB_TYPE_NULL);
  return query_type->db_type;
}

/*
 * db_query_format_free() - This function frees a query format list.
 * return : void
 * query_type(in): pointer to the beginning of the query type list
 *
 * note : Make sure to pass the head of the list and not an element in the
 *    middle of the list. You must use the exact return value. Do not use the
 *    return value of the db_query_format_next() function.
 */
void
db_query_format_free (DB_QUERY_TYPE * query_type)
{
  if (query_type == NULL)
    {
      return;
    }

  db_free_query_format (query_type);
}

/*
 * db_query_format_class_name() - This function returns the name of the class
 *    which the current type list node belongs to.
 * return : name of the class which the current type list node belongs to.
 * query_type(in): Pointer to the current type list node
 */
const char *
db_query_format_class_name (DB_QUERY_TYPE * query_type)
{
  SM_DOMAIN *src_domain = NULL;

  CHECK_1ARG_NULL (query_type);

  src_domain = db_query_format_src_domain (query_type);
  if (src_domain == NULL)
    {
      return (const char *) NULL;
    }
  if (src_domain->class_mop == NULL)
    {
      return NULL;
    }

  return sm_class_name (src_domain->class_mop);
}

/*
 * db_query_format_is_non_null() - This function returns the nullability of
 *    current type list node.
 * return : nullability of current type list node
 * query_type(in): Pointer to the current type list node
 */
int
db_query_format_is_non_null (DB_QUERY_TYPE * query_type)
{
  SM_DOMAIN *src_domain = NULL;
  DB_ATTRIBUTE *attr = NULL;

  CHECK_1ARG_RETURN_EXPR (query_type, ER_OBJ_INVALID_ARGUMENT);

  src_domain = db_query_format_src_domain (query_type);
  if (src_domain && src_domain->class_mop && query_type->attr_name)
    {
      attr = db_get_attribute (src_domain->class_mop, query_type->attr_name);
      if (attr)
        {
          return db_attribute_is_non_null (attr);
        }
    }

  return ER_OBJ_INVALID_ATTRIBUTE;
}

/*
 * QUERY PROCESSING ROUTINES
 */

/*
 * db_execute() - This function is used to evaluate the RSQL_query statement(s)
 *    given in the string. The result descriptor is used with the cursor
 *    functions to access the individual tuple values. The return value is the
 *    row count (the number of rows) returned from the last statement executed.
 * return : integer, negative implies error.
 *          Positive is the count of qualified rows
 * RSQL_query(in): query string to be executed
 * result(out): Pointer to the query result structure
 * query_error(out): Set to the error information, if any.
 *
 * note : The DB_QUERY_RESULT structure obtained from db_execute(is invalidated
 *    at transaction boundaries (i.e., whenever db_commit_transaction()or
 *    db_abort_transaction() is executed).
 *    These structures should be closed with db_query_end()before committing or
 *    aborting the transaction. Any attempt to use such a structure after the
 *    end of the transaction in which it was created will result in an error.
 */
int
db_execute (const char *RSQL_query, DB_QUERY_RESULT ** result, DB_QUERY_ERROR * query_error)
{
  int retval;

  retval = query_execute_local (RSQL_query, result, query_error, 1);
  return (retval);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_query_execute_immediate() -
 * return :
 * RSQL_query(in):
 * result(out):
 * query_error(out):
 */
int
db_query_execute_immediate (const char *RSQL_query, DB_QUERY_RESULT ** result, DB_QUERY_ERROR * query_error)
{
  int r;

#if defined(RYE_DEBUG)
  fprintf (stdout, "db_query_execute_immediate is a deprecated function.\n");
  fprintf (stdout, "use teh equivilant function db_execute.\n");
#endif

  r = query_execute_local (RSQL_query, result, query_error, 1);

  return r;
}
#endif

/*
 * db_get_db_value_query_result() - This function forms a query result
 *    structure from the given db_value. The query result structure for the
 *    db_value is treated exactly like a list file of one_tuple, one_column.
 * return : DB_QUERY_RESULT*
 * val(in): Single Value
 */
DB_QUERY_RESULT *
db_get_db_value_query_result (DB_VALUE * val)
{
  DB_QUERY_RESULT *r;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (val);

  r = db_alloc_query_result (T_CALL, 1);
  if (r == NULL)
    {
      return NULL;
    }

  db_init_query_result (r, T_CALL);
  r->type = T_CALL;
  r->col_cnt = 1;

  /* allocate and initialize type list */
  r->type_cnt = 1;
  r->query_type = db_alloc_query_format (1);
  if (r->query_type == NULL)
    {
      db_free_query_result (r);
      return NULL;
    }                           /* if */
  r->query_type->db_type = DB_VALUE_TYPE (val);
  r->query_type->name = (char *) NULL;
  r->query_type->attr_name = (char *) NULL;
  r->query_type->spec_name = (char *) NULL;
  r->query_type->domain = (SM_DOMAIN *) NULL;
  r->query_type->src_domain = (SM_DOMAIN *) NULL;

  r->res.c.crs_pos = C_BEFORE;
  r->res.c.val_ptr = db_value_copy (val);
  if (r->res.c.val_ptr == NULL)
    {
      db_free_query_result (r);
      return NULL;
    }

  return r;
}

/*
 * db_query_next_tuple() - This function makes the next tuple in the LIST FILE
 *    referred by the query result structure the current active tuple of the
 *    query result cursor and returns DB_CURSOR_SUCCESS.
 * return : error code or cursor
 * result(in/out): Pointer to the query result structure
 */
int
db_query_next_tuple (DB_QUERY_RESULT * result)
{
  int retval;
  CURSOR_POSITION *c_pos;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      retval = cursor_next_tuple (&result->res.s.cursor_id);
      break;

    case T_CALL:
      c_pos = (CURSOR_POSITION *) & result->res.c.crs_pos;
      switch (*c_pos)
        {
        case C_BEFORE:
          *c_pos = C_ON;
          retval = DB_CURSOR_SUCCESS;
          break;
        case C_ON:
        case C_AFTER:
          *c_pos = C_AFTER;
          retval = DB_CURSOR_END;
          break;
        default:
          retval = ER_QPROC_UNKNOWN_CRSPOS;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
          break;
        }
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return (retval);
}

/*
 * db_query_prev_tuple() - This function makes the previous tuple in the LIST
 *    FILE referred by query result structure the current active tuple of the
 *    query result cursor and returns DB_CURSOR_SUCCESS.
 * return : error code or cursor
 * result(in/out): Pointer to the query result structure
 */
int
db_query_prev_tuple (DB_QUERY_RESULT * result)
{
  int retval;
  CURSOR_POSITION *c_pos;

  CHECK_CONNECT_ERROR ();

  CHECK_1ARG_ERROR (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      retval = cursor_prev_tuple (&result->res.s.cursor_id);
      break;

    case T_CALL:
      c_pos = (CURSOR_POSITION *) & result->res.c.crs_pos;
      switch (*c_pos)
        {
        case C_BEFORE:
        case C_ON:
          {
            *c_pos = C_BEFORE;
            retval = DB_CURSOR_END;
          }
          break;

        case C_AFTER:
          {
            *c_pos = C_ON;
            retval = DB_CURSOR_SUCCESS;
          }
          break;

        default:
          retval = ER_QPROC_UNKNOWN_CRSPOS;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
          break;
        }
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return (retval);
}

/*
 * db_query_first_tuple() - This function makes the first tuple in the LIST
 *    FILE referred by the query result structure the current active tuple
 *    of the query result cursor and DB_CURSOR_SUCCESS.
 * return : error code or cursor
 * result(in/out): Pointer to the query result structure
 */
int
db_query_first_tuple (DB_QUERY_RESULT * result)
{
  int retval;
  CHECK_CONNECT_ERROR ();

  CHECK_1ARG_ERROR (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      retval = cursor_first_tuple (&result->res.s.cursor_id);
      break;

    case T_CALL:
      result->res.c.crs_pos = C_ON;
      retval = DB_CURSOR_SUCCESS;
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return (retval);
}

/*
 * db_query_last_tuple() - This function is used to position the cursor
 *    directly to the last tuple within the query result.
 * return : error code or cursor
 * result(in/out): Pointer to the query result structure
 */
int
db_query_last_tuple (DB_QUERY_RESULT * result)
{
  int retval;
  CHECK_CONNECT_ERROR ();

  CHECK_1ARG_ERROR (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      retval = cursor_last_tuple (&result->res.s.cursor_id);
      break;

    case T_CALL:
      result->res.c.crs_pos = C_ON;
      retval = DB_CURSOR_SUCCESS;
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return (retval);
}

/*
 * db_query_seek_tuple() - This function set the cursor to a specific tuple
 *     according to value of the offset and seek_mode. Seek mode can only take
 *     DB_CURSOR_SEEK_SET, DB_CURSOR_SEEK_CUR, DB_CURSOR_SEEK_END values.
 *     If offset value is n and seek_mode value is:
 *              DB_CURSOR_SEEK_SET: The cursor is set to the nth tuple from
 *                                  the beginning. (absolute access)
 *              DB_CORSOR_SEEK_CUR: The cursor is set to its current position
 *                                  _plus_ n. (relative access)
 *              DB_CURSOR_SEEK_END: The cursor is set to the last tuple
 *                                  position _plus_ n. (relative access)
 *
 * return : On success, the function returns DB_CURSOR_SUCCESS.
 *
 *          On end_of_scan, the cursor position is changed, the function
 *          returns DB_CURSOR_END.
 *
 *          On failure, the cursor position remains unchanged the function
 *          returns corresponding error code.
 *
 * result(in/out): Query result structure
 * offset(in): Offset tuple count
 * seek_mode(in): Tuple seek mode
 */
int
db_query_seek_tuple (DB_QUERY_RESULT * result, int offset, int seek_mode)
{
  int scan;
  int rel1, rel2, rel3, rel_n;
  int curr_tplno, tpl_cnt;
  DB_QUERY_TPLPOS *tplpos;
  CURSOR_POSITION *c_pos;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (result);

#if 1                           /* TODO - */
  assert (seek_mode == DB_CURSOR_SEEK_SET);
#endif

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      {
        tplpos = db_query_get_tplpos (result);
        if (tplpos == NULL)
          {
            return er_errid ();
          }

        /* find the optimal relative position for the scan:
         * relative to the beginning, current tuple position or end.
         */
        curr_tplno = result->res.s.cursor_id.tuple_no;
        tpl_cnt = result->res.s.cursor_id.list_id.tuple_cnt;
        switch (seek_mode)
          {
          case DB_CURSOR_SEEK_SET:
            {
              rel1 = offset;    /* relative to beginning */
              rel2 = offset - curr_tplno;       /* relative to current tuple */
              rel3 = offset - (tpl_cnt - 1);    /* relative to end */
            }
            break;

          case DB_CURSOR_SEEK_CUR:
            {
              rel1 = curr_tplno + offset;
              rel2 = offset;
              rel3 = (curr_tplno + offset) - (tpl_cnt - 1);
            }
            break;

          case DB_CURSOR_SEEK_END:
            {
              rel1 = (tpl_cnt - 1) + offset;
              rel2 = (tpl_cnt - 1) + offset - curr_tplno;
              rel3 = offset;
            }
            break;

          default:
            {
              db_query_set_tplpos (result, tplpos);
              db_query_free_tplpos (tplpos);
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
            }
            return ER_GENERIC_ERROR;
          }

#if 1                           /* TODO - trace */
        assert (abs (rel1) < abs (rel2) && abs (rel1) < abs (rel3));
#endif
        if (abs (rel1) < abs (rel2) && abs (rel1) < abs (rel3))
          {
            /* move relative to the beginning */
            scan = db_query_first_tuple (result);
            if (scan != DB_CURSOR_SUCCESS)
              {
                if (scan != DB_CURSOR_END)
                  {
                    db_query_set_tplpos (result, tplpos);
                  }
                db_query_free_tplpos (tplpos);
                return (scan != DB_CURSOR_END) ? er_errid () : scan;
              }
            rel_n = rel1;
          }
        else if (abs (rel3) < abs (rel2))
          {
            /* move relative to the last */
            scan = db_query_last_tuple (result);
            if (scan != DB_CURSOR_SUCCESS)
              {
                if (scan != DB_CURSOR_END)
                  {
                    db_query_set_tplpos (result, tplpos);
                  }
                db_query_free_tplpos (tplpos);
                return (scan != DB_CURSOR_END) ? er_errid () : scan;
              }
            rel_n = rel3;
          }
        else
          {
            /* move relative to the current tuple */
            rel_n = rel2;
          }

        /* perform the actual scan operation in a relative manner */
        if (rel_n > 0)
          {
            while (rel_n--)
              {
                scan = db_query_next_tuple (result);
                if (scan != DB_CURSOR_SUCCESS)
                  {
                    if (scan != DB_CURSOR_END)
                      {
                        db_query_set_tplpos (result, tplpos);
                      }
                    db_query_free_tplpos (tplpos);
                    return (scan != DB_CURSOR_END) ? er_errid () : scan;
                  }
              }
          }
        else
          {
            while (rel_n++)
              {
                scan = db_query_prev_tuple (result);
                if (scan != DB_CURSOR_SUCCESS)
                  {
                    if (scan != DB_CURSOR_END)
                      {
                        db_query_set_tplpos (result, tplpos);
                      }
                    db_query_free_tplpos (tplpos);
                    return (scan != DB_CURSOR_END) ? er_errid () : scan;
                  }
              }
          }
        db_query_free_tplpos (tplpos);
      }
      break;

    case T_CALL:
      switch (seek_mode)
        {
        case DB_CURSOR_SEEK_SET:
        case DB_CURSOR_SEEK_END:
          c_pos = (CURSOR_POSITION *) & result->res.c.crs_pos;
          if (offset == 0)
            {
              *c_pos = C_ON;
              return DB_CURSOR_SUCCESS;
            }
          else if (offset > 0)
            {
              *c_pos = C_AFTER;
              return DB_CURSOR_END;
            }
          else
            {
              *c_pos = C_BEFORE;
              return DB_CURSOR_END;
            }

        case DB_CURSOR_SEEK_CUR:
          if (offset > 0)
            {
              return db_query_next_tuple (result);
            }
          else if (offset < 0)
            {
              return db_query_prev_tuple (result);
            }

        default:
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
          return ER_GENERIC_ERROR;
        }

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return ER_QPROC_INVALID_RESTYPE;
    }

  return DB_CURSOR_SUCCESS;
}

/*
 * db_query_get_tplpos() - This function returns the current tuple position
 *    information.
 * return : DB_QUERY_TPLPOS*, or NULL
 * result(in): Query result structure
 *
 * note: Even though. db_query_seek_tuple() routine can be used to position
 *       the cursor to a specific tuple, the combination of
 *       db_query_get_tplpos() and db_query_set_tplpos() provides a much
 *       faster way of accessing a specific tuple.
 */
DB_QUERY_TPLPOS *
db_query_get_tplpos (DB_QUERY_RESULT * result)
{
  DB_QUERY_TPLPOS *tplpos;

  CHECK_CONNECT_NULL ();

  CHECK_1ARG_NULL (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return NULL;
    }

  tplpos = (DB_QUERY_TPLPOS *) malloc (DB_SIZEOF (DB_QUERY_TPLPOS));
  if (tplpos == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (DB_QUERY_TPLPOS));
      return NULL;
    }

  switch (result->type)
    {
    case T_SELECT:
      tplpos->crs_pos = result->res.s.cursor_id.position;
      tplpos->vpid.pageid = result->res.s.cursor_id.current_vpid.pageid;
      tplpos->vpid.volid = result->res.s.cursor_id.current_vpid.volid;
      tplpos->tpl_no = result->res.s.cursor_id.current_tuple_no;
      tplpos->tpl_off = result->res.s.cursor_id.current_tuple_offset;
      break;

    case T_CALL:
      tplpos->crs_pos = result->res.c.crs_pos;
      break;

    default:
      free_and_init (tplpos);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return NULL;
    }

  return tplpos;
}

/*
 * db_query_set_tplpos() - This function set cursor to point to the indicated
 *    tuple position.
 * return : error code
 * result(in): query result structure
 * tplpos(out): tuple position information
 */
int
db_query_set_tplpos (DB_QUERY_RESULT * result, DB_QUERY_TPLPOS * tplpos)
{
  CHECK_1ARG_ERROR (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      /* reset cursor identifier */
      if (result->res.s.cursor_id.current_vpid.pageid != tplpos->vpid.pageid
          || result->res.s.cursor_id.current_vpid.volid != tplpos->vpid.volid)
        {
          /* needs to get another page */
          if (cursor_fetch_page_having_tuple (&result->res.s.cursor_id,
                                              &tplpos->vpid, tplpos->tpl_no, tplpos->tpl_off) != NO_ERROR)
            {
              return ER_FAILED;
            }
          result->res.s.cursor_id.current_vpid = tplpos->vpid;
        }
      result->res.s.cursor_id.position = tplpos->crs_pos;
      break;

    case T_CALL:
      result->res.c.crs_pos = tplpos->crs_pos;
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return ER_QPROC_INVALID_RESTYPE;
    }

  return NO_ERROR;
}

/*
 * db_query_free_tplpos() - This function frees the area pointed by the tplpos
 *    pointer.
 * return : void
 * tplpos(in): Tuple position information
 */
void
db_query_free_tplpos (DB_QUERY_TPLPOS * tplpos)
{
  free_and_init (tplpos);
}

/*
 * db_query_get_tuple_value() - This function is used to get the value for a
 *    column in the current tuple of a query result. The current tuple is
 *    specified by using the cursor control functions.
 * return : error code
 * result: pointer to the query result structure
 * index(in): position of the tuple value of interest (0 for the first one)
 * value(out): value container for column value
 */
int
db_query_get_tuple_value (DB_QUERY_RESULT * result, int index, DB_VALUE * value)
{
  int retval;
  DB_VALUE *valp;

  CHECK_CONNECT_ERROR ();

  CHECK_2ARGS_ERROR (result, value);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      if (DB_INVALID_INDEX (index, result->col_cnt))
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TPLVAL_INDEX, 1, index);
          return ER_QPROC_INVALID_TPLVAL_INDEX;
        }

      retval = cursor_get_tuple_value (&result->res.s.cursor_id, index, value);
      break;

    case T_CALL:
      valp = result->res.c.val_ptr;
      pr_clone_value (valp, value);
      retval = NO_ERROR;
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return (retval);
}

/*
 * db_query_get_tuple_valuelist() - This function can be used to get all of the
 *    column values for the current tuple of a query result. The current tuple
 *    is specified using the cursor control functions. The values for the
 *    columns up to the number specified by the size argument are copied into
 *    the value array.
 * return : error code.
 * result(in): Pointer to the query result structure
 * size(in): Number of values in the value list
 * value_list(out): an array of DB_VALUE structures
 */
int
db_query_get_tuple_valuelist (DB_QUERY_RESULT * result, int size, DB_VALUE * value_list)
{
  int retval;

  CHECK_CONNECT_ERROR ();

  CHECK_2ARGS_ERROR (result, value_list);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      if (DB_INVALID_INDEX (size - 1, result->col_cnt))
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TPLVAL_INDEX, 1, size - 1);
          return ER_QPROC_INVALID_TPLVAL_INDEX;
        }

      retval = cursor_get_tuple_value_list (&result->res.s.cursor_id, size, value_list);
      break;

    case T_CALL:
      retval = db_query_get_tuple_value (result, 0, value_list);
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return (retval);
}

/*
 * db_query_get_info() - This function returns information from a streaming
 *    query, including the current tuple count, as well as any existing error
 *    IDs and error strings. The tuple count in the result structure is also
 *    updated.
 * return : error code
 * result(in): the query result structure
 * done(out): set to true if the query has completed
 * count(out): address where the function will store the tuple count for
 *             the query
 * error(out): address where the last error of the query will be stored.
 * err_string(out): Pointer to the error string
 *
 * note : If the query has completed, the tuple count returned will be the
 *        total tuple count. Otherwise, it will be the number of tuples that
 *        can be retrieved without causing the client to block on the server.
 */
int
db_query_get_info (DB_QUERY_RESULT * result, int *done, int *count, int *error, char **err_string)
{
  int rc;

  CHECK_1ARG_MINUSONE (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return -1;
    }

  switch (result->type)
    {
    case T_SELECT:
      rc = qmgr_get_query_info (result, done, count, error, err_string);
      break;

    case T_CALL:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      rc = -1;
      break;
    }

  return rc;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_query_sync() - This function allows a client application either to wait
 *    for a streaming query to complete, or to interrupt the query. If wait is
 *    true(non zero), then the client will be blocked until the streaming query
 *    has completed. If wait is set to false(zero), then the streaming query
 *    will be terminated abnormally. This function does not affect a query that
 *    was not initiated as a streaming query.
 * return : error code
 * result(in): Pointer to the query result structure
 * wait(in): a signal to either wait for a streaming query to complete or to
 *           interrupt it
 */
int
db_query_sync (DB_QUERY_RESULT * result, int wait)
{
  int rc;

  CHECK_1ARG_RETURN_EXPR (result, ER_FAILED);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_FAILED;
    }

  switch (result->type)
    {
    case T_SELECT:
      rc = qmgr_sync_query (result, wait);
      break;

    case T_CALL:
    default:
      rc = NO_ERROR;
      break;
    }

  return (rc);
}
#endif

/*
 * db_query_tuple_count() - This function calculates the total number of result
 *    tuples in the query result.
 * return : number of tuples in the query result or -1 on error.
 * result(in): Pointer to the query result structure
 *
 * note : If an error is detected, the function returns -1 and the
 *    db_error_string() function can be used to see a description of the error.
 */
int
db_query_tuple_count (DB_QUERY_RESULT * result)
{
  int done, count;
  int error;
  int retval;

  CHECK_1ARG_MINUSONE (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return -1;
    }

  switch (result->type)
    {
    case T_SELECT:
      if (result->res.s.cursor_id.list_id.tuple_cnt == -1)
        {
#if 1                           /* TODO - trace */
          assert (false);
#endif
          db_query_get_info (result, &done, &count, &error, NULL);
        }
      retval = result->res.s.cursor_id.list_id.tuple_cnt;
      break;

    case T_CALL:
      retval = 1;
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      retval = -1;
      break;
    }

  return retval;
}

/*
 * db_query_column_count() - This function calculates the number of columns in
 *    each tuple of the query result.
 * return : number of columns.
 * result(in): Pointer to the query result structure
 */
int
db_query_column_count (DB_QUERY_RESULT * result)
{
  CHECK_1ARG_MINUSONE (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return -1;
    }

  if (DB_INVALID_RESTYPE (result->type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return -1;
    }

  return result->col_cnt;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_query_stmt_id() - This function returns the statement identifier for the
 *    query or -1 if the query is not select type.
 * return : statement id or -1
 * result(in): Pointer to the query result structure
 */
int
db_query_stmt_id (DB_QUERY_RESULT * result)
{
  CHECK_1ARG_MINUSONE (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return -1;
    }

  if (DB_INVALID_RESTYPE (result->type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return -1;
    }

  return result->res.s.stmt_id;
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_query_get_value_type() - This function returns the type of the specified
 *    result column, or DB_TYPE_NULL on error
 * return : DB_TYPE
 * result(in) : pointer to query result structure
 * index(in) : column index
 */
DB_TYPE
db_query_get_value_type (DB_QUERY_RESULT * result, int index)
{
  DB_QUERY_TYPE *typep;
  int k;

  CHECK_1ARG_RETURN_EXPR (result, DB_TYPE_NULL);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return DB_TYPE_NULL;
    }

  if (DB_INVALID_RESTYPE (result->type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return DB_TYPE_NULL;
    }

  if (DB_INVALID_INDEX (index, result->type_cnt))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TPLVAL_INDEX, 1, index);
      return DB_TYPE_NULL;
    }

  for (k = 0, typep = result->query_type; k < index && typep; k++, typep = typep->next)

    ;

  return typep ? typep->db_type : DB_TYPE_NULL;
}

/*
 * db_query_get_value_length() - This functionreturns the length of the
 *    specified result column
 * return : length of column or -1 on error
 * result(in) : pointer to query result structure
 * index(in) : which result column
 */
int
db_query_get_value_length (DB_QUERY_RESULT * result, int index)
{
  DB_QUERY_TYPE *typep;
  int k;

  CHECK_1ARG_MINUSONE (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return -1;
    }

  if (DB_INVALID_RESTYPE (result->type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      return -1;
    }

  if (DB_INVALID_INDEX (index, result->type_cnt))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TPLVAL_INDEX, 1, index);
      return -1;
    }

  for (k = 0, typep = result->query_type; k < index; k++, typep = typep->next)
    {
      ;                         /* NULL */
    }

  return typep ? typep->size : -1;
}
#endif

#if defined (RYE_DEBUG)
/*
 * db_sqlx_debug_print_result() - This function displays the result on
 *    standard output.
 * return : void
 * result(in): result to be displayed
 *
 * note: this function is only for DEBUGGING purpose. No product can use
 *	 this function to display the result.
 */
void
db_sqlx_debug_print_result (DB_QUERY_RESULT * result)
{
  if (result == NULL)
    {
      fprintf (stdout, "There is no result.\n\n");
      return;
    }

  switch (result->type)
    {
    case T_SELECT:
      cursor_print_list (result->res.s.query_id, &result->res.s.cursor_id.list_id);
      break;

    case T_CALL:
      db_value_print (result->res.c.val_ptr);
      break;

    default:
      (void) fprintf (stdout, "Invalid query result structure type: %d.\n", result->type);
      break;
    }

}
#endif

/*
 * QUERY POST-PROCESSING ROUTINES
 */

/*
 * db_query_end() - This function must be called when the application is
 *    finished with the query result descriptor that was returned by either
 *    the db_execute() function.
 *    This frees the descriptor and all storage related to the query results.
 *    Since query results can be of considerable size, it is important that
 *    they be freed as soon as they are no longer necessary.
 * return : error code
 * result(in): Pointer to the query result structure
 *
 * note : If db_query_end() is called on a currently executing streaming query,
 *    the query is terminated and all resources are cleaned up.
 */
int
db_query_end (DB_QUERY_RESULT * result)
{
  return db_query_end_internal (result, true);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_query_sync_end() -
 * return : error code
 * result(in): Pointer to the query result structure
 */
int
db_query_sync_end (DB_QUERY_RESULT * result)
{
  int rc = NO_ERROR;

  if (result->status == T_OPEN && result->type == T_SELECT)
    {
      rc = qmgr_sync_query (result, true);
    }
  if (rc == NO_ERROR)
    {
      rc = db_query_end_internal (result, true);
    }
  return rc;
}
#endif

/*
 * db_query_set_copy_tplvalue() -
 * return : error code
 * result(in/out):
 * copy(in):
 */
int
db_query_set_copy_tplvalue (DB_QUERY_RESULT * result, int copy)
{
  int retval = NO_ERROR;
  CHECK_CONNECT_ERROR ();

  CHECK_1ARG_ERROR (result);

  if (result->status == T_CLOSED)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OPR_ON_CLOSED_QRES, 0);
      return ER_QPROC_OPR_ON_CLOSED_QRES;
    }

  switch (result->type)
    {
    case T_SELECT:
      (void) cursor_set_copy_tuple_value (&result->res.s.cursor_id, copy ? true : false);
      break;

    case T_CALL:
      break;

    default:
      retval = ER_QPROC_INVALID_RESTYPE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_RESTYPE, 0);
      break;
    }

  return retval;
}

/*
 * db_query_end_internal() -
 * return :
 * result(in):
 * notify_server(in):
 */
int
db_query_end_internal (DB_QUERY_RESULT * result, bool notify_server)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();

  /* Silently return if the result structure has already been freed */
  if (result && result->status == T_CLOSED)
    {
      return NO_ERROR;
    }

  if (result)
    {
      if (result->type == T_SELECT)
        {
          if (notify_server && !(result->is_server_query_ended))
            {
              if (qmgr_end_query (result->res.s.query_id) != NO_ERROR)
                {
                  error = er_errid ();
                }
            }
          cursor_close (&result->res.s.cursor_id);
        }

      db_free_query_result (result);
    }

  return error;
}

/*
 * db_query_plan_dump_file() -
 * return :
 * filename(in):
 */
int
db_query_plan_dump_file (char *filename)
{
  if (query_Plan_dump_filename != NULL)
    {
      free (query_Plan_dump_filename);
    }

  query_Plan_dump_filename = NULL;

  if (filename != NULL)
    {
      query_Plan_dump_filename = strdup (filename);
    }

  return NO_ERROR;
}
