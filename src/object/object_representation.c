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
 * object_representation.c: Low level functions that manipulate
 *                          the disk representation of
 */

#ident "$Id$"

#include "config.h"

#include <string.h>
#include <setjmp.h>

#include "porting.h"
#if !defined (SERVER_MODE)
#include "locator_cl.h"
#endif /* !SERVER_MODE */
#include "object_representation.h"
#include "object_domain.h"
#include "error_manager.h"
#include "oid.h"
#include "storage_common.h"
#include "query_opfunc.h"
#include "class_object.h"
#include "db.h"
#include "set_object.h"
#if defined (SERVER_MODE)
#include "thread.h"
#endif /* !SERVER_MODE */

/* this must be the last header file included!!! */
#include "dbval.h"

static TP_DOMAIN *unpack_domain (OR_BUF * buf, int *is_null);
#if defined(ENABLE_UNUSED_FUNCTION)
static char *unpack_str_array (char *buffer, char ***string_array, int count);
#endif
static int or_put_varchar_internal (OR_BUF * buf, const char *string, int charlen, int align);
static int or_varbit_length_internal (int bitlen, int align);
static int or_varchar_length_internal (int charlen, int align);
static int or_put_varbit_internal (OR_BUF * buf, char *string, int bitlen, int align);
static char *or_unpack_var_table_internal (char *ptr, int nvars, OR_VARINFO * vars, int offset_size);

/*
 * classobj_get_prop - searches a property list for a value with the given name
 *    return: index of the found property, 0 iff not found
 *    properties(in): property sequence
 *    name(in): property name
 *    pvalue(out): value container for property value
 *
 * Note:
 * Remember to clear the value with pr_clear_value or equivalent.
 */
int
classobj_get_prop (DB_SEQ * properties, const char *name, DB_VALUE * pvalue)
{
  int error;
  int found, max, i;
  DB_VALUE value;
  char *tmp_str;

  error = NO_ERROR;
  found = 0;

  if (properties == NULL || name == NULL || pvalue == NULL)
    {
      goto error;
    }

  max = set_size (properties);

  for (i = 0; i < max && !found && error == NO_ERROR; i += 2)
    {
      error = set_get_element (properties, i, &value);
      if (error != NO_ERROR)
        {
          continue;
        }

      if (DB_VALUE_TYPE (&value) != DB_TYPE_VARCHAR || DB_GET_STRING (&value) == NULL)
        {
          error = ER_SM_INVALID_PROPERTY;
        }
      else
        {
          tmp_str = DB_GET_STRING (&value);
          if (tmp_str && strcmp (name, tmp_str) == 0)
            {
              if ((i + 1) >= max)
                {
                  error = ER_SM_INVALID_PROPERTY;
                }
              else
                {
                  error = set_get_element (properties, i + 1, pvalue);
                  if (error == NO_ERROR)
                    {
                      found = i + 1;
                    }
                }
            }
        }
      pr_clear_value (&value);
    }

error:
  if (error)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }

  return found;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_decompose_property_oid - parse oid string from buffer
 *    return: NO_ERROR or error code
 *
 *    buffer(in): buffer that contains oid string
 *    oid(out): OID
 *
 */
int
classobj_decompose_property_oid (const char *buffer, OID * oid)
{
  char *ptr;
  int result = 0;
  int val;

  if (buffer == NULL)
    {
      goto exit_on_error;
    }

  result = str_to_int32 (&val, &ptr, buffer, 10);
  oid->pageid = val;
  if (result != 0)
    {
      goto exit_on_error;
    }
  buffer = ptr + 1;

  result = str_to_int32 (&val, &ptr, buffer, 10);
  oid->slotid = val;
  if (result != 0)
    {
      goto exit_on_error;
    }
  buffer = ptr + 1;

  result = str_to_int32 (&val, &ptr, buffer, 10);
  oid->volid = val;
  if (result != 0)
    {
      goto exit_on_error;
    }

  return NO_ERROR;

exit_on_error:
  OID_SET_NULL (oid);

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
  return ER_GENERIC_ERROR;
}
#endif

/*
 * classobj_decompose_property_btid - parse btid string from buffer
 *    return: NO_ERROR or error code
 *
 *    buffer(in): buffer that contains oid string
 *    btid(out): BTID
 *
 */
int
classobj_decompose_property_btid (const char *buffer, BTID * btid)
{
  *btid = string_to_btid (buffer);
  if (BTID_IS_NULL (btid))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return ER_GENERIC_ERROR;
    }

  return NO_ERROR;
}

/*
 * RECDES DECODING FUNCTIONS
 */

/*
 * These are called primarily by the locator to get information
 * about the disk representation of an object stored in a disk
 * record descirptor (RECDES).
 */

/*
 * or_class_name - This is used to extract the class name from the disk
 * representation of a class object
 *    return: class name string pointer inside recode data
 *    record(in): disk record
 *
 * Note:
 *    To avoid a lot of dependencies with the schema code, we make the
 *    assumption that the name field is always maintained as the first
 *    variable attribute of class objects.
 *    This will also be true for the root class.
 *
 *    [PORTABILITY]
 *    This simply returns a pointer into the middle of the record.  We may
 *    need to copy the string out of the record for some architectures
 *    if there are weird alignment or character set problems.
 */
char *
or_class_name (RECDES * record)
{
  char *start, *name;
  int offset, len;

  /*
   * the first variable attribute for both classes and the rootclass
   * is the name - if this ever changes, we could check the class
   * OID which should be NULL for the root class and special case
   * from there
   */

  offset = OR_VAR_OFFSET (record->data, 0);
  start = &record->data[offset];

  /*
   * kludge kludge kludge
   * This is now an encoded "varchar" string, we need to skip over the length
   * before returning it.  Note that this also depends on the stored string
   * being NULL terminated.  This interface should be returning either a copy
   * or performing an extraction into a user supplied buffer !
   * Knowledge of the format of packed varchars should be in a different
   * or_ function.
   */
  len = (int) *((unsigned char *) start);
  if (len != 0xFF)
    {
      name = start + 1;
    }
  else
    {
      name = start + 1 + OR_INT_SIZE;
    }

  return name;
}

/*
 * or_rep_id - Extracts the representation id from the disk representation of
 * an object.
 *    return: representation of id of object. or NULL_REPRID for error
 *    record(in): disk record
 */
int
or_rep_id (RECDES * record)
{
  int rep = NULL_REPRID;

  if (record->length < OR_HEADER_SIZE)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_BUFFER_UNDERFLOW, 0);
    }
  else
    {
      rep = OR_GET_REPID (record->data);
    }

  return rep;
}

/*
 * or_set_rep_id () - set representation id for record
 * return : error code or NO_ERROR
 * record (in/out): record
 * repid (in)	  : new representation
 *
 * Note: This function changes the representation id of a record.
 */
int
or_set_rep_id (RECDES * record, int repid)
{
  OR_BUF orep, *buf;
  bool is_bound_bit = false;
  int offset_size = 0;
  unsigned int new_bits = 0;

  if (record->length < OR_HEADER_SIZE)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_BUFFER_UNDERFLOW, 0);
      return ER_FAILED;
    }

  OR_BUF_INIT2 (orep, record->data, record->area_size);
  buf = &orep;

  /* read REPR_ID flags */
  if (OR_GET_BOUND_BIT_FLAG (record->data))
    {
      is_bound_bit = true;
    }
  offset_size = OR_GET_OFFSET_SIZE (record->data);

  /* construct new REPR_ID element */
  new_bits = repid;
  if (is_bound_bit)
    {
      new_bits |= OR_BOUND_BIT_FLAG;
    }
  OR_SET_VAR_OFFSET_SIZE (new_bits, offset_size);

  /* write new REPR_ID to the record */
  or_put_int (buf + OR_REP_OFFSET, new_bits);

  return NO_ERROR;
}

/*
 * or_grp_id - extracts group id from the disk representation of an
 * object
 *    return: group id, or NULL_GROUPID for error
 *    record(in) : disk record
 */
int
or_grp_id (const RECDES * record)
{
  int grpid = NULL_GROUPID;

  if (record->length < OR_HEADER_SIZE)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_BUFFER_UNDERFLOW, 0);
    }
  else
    {
      grpid = OR_GET_GRPID (record->data);
    }

#if 0                           /* TODO - for debugging */
#if !defined(NDEBUG)
  if (grpid < 0)
    {
      int x = 1, y = 1;

      while (x > 0)
        {
          fprintf (stdout, "=== or_grp_id(): invalid grpid = %d === \n", grpid);
#if defined (SERVER_MODE)
          thread_sleep (1000);  /* 1000 msec */
#endif
          x = y;
        }
    }
#endif
#endif

  assert (grpid >= 0);

  return grpid;
}

#if defined(ENABLE_UNUSED_FUNCTION)
int
or_set_grp_id (RECDES * record, int grpid)
{
  char *p = NULL;

  if (record->length < OR_HEADER_SIZE)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_BUFFER_UNDERFLOW, 0);
      return ER_FAILED;
    }

  p = record->data;
  p += OR_GRPID_OFFSET;
  OR_PUT_INT (p, grpid);

  return NO_ERROR;
}
#endif

#if !defined (SERVER_MODE)
/*
 * BOUND BIT FUNCTIONS
 */
/*
 * These manipulate the bound-bit array which can be found in the headers
 * of objects and sets.
 *
 */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_get_bound_bit - Extracts the bound bit for a particular element.
 *    return: bound bit (0 or 1) for element
 *    bound_bits(in): pointer to bound bits on disk
 *    element(in): index into the bound bit table
 */
int
or_get_bound_bit (char *bound_bits, int element)
{
  return (OR_GET_BOUND_BIT (bound_bits, element));
}

/*
 * or_put_bound_bit - his sets the value of a bound bit
 *    return: void
 *    bound_bits(out): pointer to bound bit array
 *    element(in): bound bit table index
 *    bound(in): value to set in the table
 *
 * Note:
 * It assumes that the bound bit table is in memory format.
 */
void
or_put_bound_bit (char *bound_bits, int element, int bound)
{
  int mask;
  char *byte, value;

  /* this could be a macro, but its getting pretty confusing */
  mask = 1 << (element & 7);
  byte = bound_bits + (element >> 3);
  value = *byte;

  if (bound)
    {
      *byte = value | mask;
    }
  else
    {
      *byte = value & ~mask;
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */
#endif /* !SERVER_MODE */

/*
 * OR_BUF PACK/UNPACK FUNCTIONS
 */

/*
 * or_overflow - called by the or_put_ functions when there is not enough
 * room in the buffer to hold a particular value.
 *    return: ER_TF_BUFFER_OVERFLOW or long jump to buf->error_abort
 *    buf(in): translation state structure
 *
 * Note:
 *    Because of the recursive nature of the translation functions, we may
 *    be several levels deep so we can do a longjmp out to the top level
 *    if the user has supplied a jmpbuf.
 *    Because jmpbuf is not a pointer, we have to keep an additional flag
 *    called "error_abort" in the OR_BUF structure to indicate the validity
 *    of the jmpbuf.
 *    This is a fairly common ocurrence because the locator regularly calls
 *    the transformer with a buffer that is too small.  When overflow
 *    is detected, it allocates a larger one and retries the operation.
 *    Because of this, a system error is not signaled here.
 */
int
or_overflow (OR_BUF * buf)
{
  /*
   * since this is normal behavior, don't set an error condition, the
   * main transformer functions will need to test the status value
   * for ER_TF_BUFFER_OVERFLOW and know that this isn't an error condition.
   */

  if (buf->error_abort)
    {
      _longjmp (buf->env, ER_TF_BUFFER_OVERFLOW);
    }

  return ER_TF_BUFFER_OVERFLOW;
}

/*
 * or_underflow - This is called by the or_get_ functions when there is
 * not enough data in the buffer to extract a particular value.
 *    return: ER_TF_BUFFER_UNDERFLOW or long jump to buf->env
 *    buf(in): translation state structure
 *
 * Note:
 * Unlike or_overflow this is NOT a common ocurrence and indicates a serious
 * memory or disk corruption problem.
 */
int
or_underflow (OR_BUF * buf)
{
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_BUFFER_UNDERFLOW, 0);

  if (buf->error_abort)
    {
      _longjmp (buf->env, ER_TF_BUFFER_UNDERFLOW);
    }
  return ER_TF_BUFFER_UNDERFLOW;
}

/*
 * or_abort - This is called if there was some fundemtal error
 *    return: void
 *    buf(in): translation state structure
 *
 * Note:
 *    An appropriate error message should have already been set.
 */
void
or_abort (OR_BUF * buf)
{
  /* assume an appropriate error has already been set */
  if (buf->error_abort)
    {
      _longjmp (buf->env, er_errid ());
    }
}

/*
 * or_init - initialize the field of an OR_BUF
 *    return: void
 *    buf(in/out): or buffer to initialize
 *    data(in): buffer data
 *    length(in):  buffer data length
 */
void
or_init (OR_BUF * buf, char *data, int length)
{
  buf->buffer = data;
  buf->ptr = data;

  if (length == 0 || length == -1 || length == DB_INT32_MAX)
    {
      buf->endptr = (char *) OR_INFINITE_POINTER;
    }
  else
    {
      buf->endptr = data + length;
    }

  buf->error_abort = 0;
  buf->fixups = NULL;
}

/*
 * or_put_align32 - pad zero bytes round up to 4 byte bound
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 */
int
or_put_align32 (OR_BUF * buf)
{
  unsigned int bits;
  int rc = NO_ERROR;

  bits = (UINTPTR) buf->ptr & 3;
  if (bits)
    {
      rc = or_pad (buf, 4 - bits);
    }

  return rc;
}

/*
 * or_get_align32 - adnvance or buf pointer to next 4 byte alignment position
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 */
int
or_get_align32 (OR_BUF * buf)
{
  unsigned int bits;
  int rc = NO_ERROR;

  bits = (UINTPTR) (buf->ptr) & 3;
  if (bits)
    {
      rc = or_advance (buf, 4 - bits);
    }

  return rc;
}

/*
 * or_get_align64 - adnvance or buf pointer to next 8 byte alignment position
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 */
int
or_get_align64 (OR_BUF * buf)
{
  unsigned int bits;
  int rc = NO_ERROR;

  bits = (UINTPTR) (buf->ptr) & 7;
  if (bits)
    {
      rc = or_advance (buf, 8 - bits);
    }

  return rc;
}

/*
 * or_get_align - adnvance or buf pointer to next alignment position
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    align(in):
 */
int
or_get_align (OR_BUF * buf, int align)
{
  char *ptr;

  ptr = PTR_ALIGN (buf->ptr, align);
  if (ptr > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      buf->ptr = ptr;
      return NO_ERROR;
    }
}

/*
 * or_packed_varchar_length - returns length of place holder that can contain
 * package varchar length. Also ajust length up to 4 byte boundary.
 *    return: length of placeholder that can contain packed varchar length
 *    charlen(in): varchar length
 */
int
or_packed_varchar_length (int charlen)
{
  return or_varchar_length_internal (charlen, INT_ALIGNMENT);
}

/*
 * or_varchar_length - returns length of place holder that can contain
 * package varchar length.
 *    return: length of place holder that can contain packed varchar length
 *    charlen(in): varchar length
 */
int
or_varchar_length (int charlen)
{
  return or_varchar_length_internal (charlen, CHAR_ALIGNMENT);
}

#if defined (ENABLE_UNUSED_FUNCTION)
int
or_packed_recdesc_length (int length)
{
  return OR_INT_SIZE * 2 + or_packed_stream_length (length);
}
#endif

static int
or_varchar_length_internal (int charlen, int align)
{
  int len;

  if (charlen < 0xFF)
    {
      len = 1 + charlen;
    }
  else
    {
      len = 1 + OR_INT_SIZE + charlen;
    }

  if (align == INT_ALIGNMENT)
    {
      /* size of NULL terminator */
      len += 1;

      len = DB_ALIGN (len, INT_ALIGNMENT);
    }

  return len;
}

/*
 * or_put_varchar - put varchar to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    string(in): string to put into the or buffer
 *    charlen(in): string length
 */
int
or_put_varchar (OR_BUF * buf, char *string, int charlen)
{
  return or_put_varchar_internal (buf, string, charlen, CHAR_ALIGNMENT);
}

/*
 * or_packed_put_varchar - put varchar to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    string(in): string to put into the or buffer
 *    charlen(in): string length
 */
int
or_packed_put_varchar (OR_BUF * buf, const char *string, int charlen)
{
  return or_put_varchar_internal (buf, string, charlen, INT_ALIGNMENT);
}

static int
or_put_varchar_internal (OR_BUF * buf, const char *string, int charlen, int align)
{
  int net_charlen;
  char *start;
  int rc = NO_ERROR;

  start = buf->ptr;
  /* store the size prefix */
  if (charlen < 0xFF)
    {
      rc = or_put_byte (buf, charlen);
    }
  else
    {
      rc = or_put_byte (buf, 0xFF);

      if (rc == NO_ERROR)
        {
          OR_PUT_INT (&net_charlen, charlen);
          rc = or_put_data (buf, (char *) &net_charlen, OR_INT_SIZE);
        }
    }
  if (rc != NO_ERROR)
    {
      return rc;
    }

  /* store the string bytes */
  rc = or_put_data (buf, string, charlen);
  if (rc != NO_ERROR)
    {
      return rc;
    }

  if (align == INT_ALIGNMENT)
    {
      /* kludge, temporary NULL terminator */
      rc = or_put_byte (buf, 0);
      if (rc != NO_ERROR)
        {
          return rc;
        }

      /* round up to a word boundary */
      rc = or_put_align32 (buf);
    }

  return rc;
}

/*
 * or_packed_get_varchar - get varchar from or buffer
 *    return: varchar pointer read from the or buffer. or NULL for error
 *    buf(in/out): or buffer
 *    length_ptr(out): length of returned string
 */
char *
or_packed_get_varchar (OR_BUF * buf, int *length_ptr)
{
  int rc = NO_ERROR;
  int charlen;
  char *new_;

  charlen = or_get_varchar_length (buf, &rc);

  if (rc != NO_ERROR)
    {
      return NULL;
    }

  /* Allocate storage for the string including the kludge NULL terminator */
  new_ = (char *) malloc (charlen + 1);

  if (new_ == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 0);
      or_abort (buf);
      return NULL;
    }
  rc = or_get_data (buf, new_, charlen + 1);

  if (rc == NO_ERROR)
    {

      /* return the length */
      if (length_ptr != NULL)
        {
          *length_ptr = charlen;
        }

      /* round up to a word boundary */
      rc = or_get_align32 (buf);
    }
  if (rc != NO_ERROR)
    {
      db_private_free_and_init (NULL, new_);
      return NULL;
    }
  else
    {
      return new_;
    }
}

/*
 * or_get_varchar_length - get varchar length from or buffer
 *    return: length of varchar or 0 if error.
 *    buf(in/out): or buffer
 *    rc(out): status code
 */
int
or_get_varchar_length (OR_BUF * buf, int *rc)
{
  int net_charlen, charlen;

  /* unpack the size prefix */
  charlen = or_get_byte (buf, rc);

  if (*rc != NO_ERROR)
    {
      assert (charlen == 0);
      return charlen;
    }

  if (charlen == 0xFF)
    {
      *rc = or_get_data (buf, (char *) &net_charlen, OR_INT_SIZE);
      charlen = OR_GET_INT (&net_charlen);
    }

  return charlen;
}

/*
 * or_skip_varchar_remainder - skip varchar field of given length
 *    return: NO_ERROR if successful, error code otherwise
 *    buf(in/out): or buffer
 *    charlen(in): length of varchar field to skip
 *    align(in):
 */
int
or_skip_varchar_remainder (OR_BUF * buf, int charlen, int align)
{
  int rc = NO_ERROR;

  if (align == INT_ALIGNMENT)
    {
      rc = or_advance (buf, charlen + 1);
      if (rc == NO_ERROR)
        {
          rc = or_get_align32 (buf);
        }
    }
  else
    {
      rc = or_advance (buf, charlen);
    }

  return rc;
}

/*
 * or_skip_varchar - skip varchar field (length + data) from or buffer
 *    return: NO_ERROR or error code.
 *    buf(in/out): or buffer
 *    align(in):
 */
int
or_skip_varchar (OR_BUF * buf, int align)
{
  int charlen, rc = NO_ERROR;

  charlen = or_get_varchar_length (buf, &rc);

  if (rc == NO_ERROR)
    {
      return (or_skip_varchar_remainder (buf, charlen, align));
    }

  return rc;
}

/*
 * or_packed_varbit_length - returns packed varbit length of or buffer encoding
 *    return: packed varbit encoding length
 *    bitlen(in): varbit length
 */
int
or_packed_varbit_length (int bitlen)
{
  return or_varbit_length_internal (bitlen, INT_ALIGNMENT);
}

/*
 * or_packed_varbit_length - returns packed varbit length of or buffer encoding
 *    return: varbit encoding length
 *    bitlen(in): varbit length
 */
int
or_varbit_length (int bitlen)
{
  return or_varbit_length_internal (bitlen, CHAR_ALIGNMENT);
}

static int
or_varbit_length_internal (int bitlen, int align)
{
  int len;

  /* calculate size of length prefix */
  if (bitlen < 0xFF)
    {
      len = 1;
    }
  else
    {
      len = 1 + OR_INT_SIZE;
    }

  /* add in the string length in bytes */
  len += ((bitlen + 7) / 8);

  if (align == INT_ALIGNMENT)
    {
      /* round up to a word boundary */
      len = DB_ALIGN (len, INT_ALIGNMENT);
    }
  return len;
}

/*
 * or_put_varbit - put varbit into or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    string(in): string contains varbit value
 *    bitlen(in): length of varbit
 */
int
or_packed_put_varbit (OR_BUF * buf, char *string, int bitlen)
{
  return or_put_varbit_internal (buf, string, bitlen, INT_ALIGNMENT);
}

/*
 * or_put_varbit - put varbit into or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    string(in): string contains varbit value
 *    bitlen(in): length of varbit
 */
int
or_put_varbit (OR_BUF * buf, char *string, int bitlen)
{
  return or_put_varbit_internal (buf, string, bitlen, CHAR_ALIGNMENT);
}

static int
or_put_varbit_internal (OR_BUF * buf, char *string, int bitlen, int align)
{
  int net_bitlen;
  int bytelen;
  char *start;
  int status;
  int valid_buf;
  jmp_buf save_buf;

  if (buf->error_abort)
    {
      memcpy (&save_buf, &buf->env, sizeof (save_buf));
    }

  valid_buf = buf->error_abort;
  buf->error_abort = 1;
  status = _setjmp (buf->env);

  if (status == 0)
    {
      start = buf->ptr;
      bytelen = BITS_TO_BYTES (bitlen);

      /* store the size prefix */
      if (bitlen < 0xFF)
        {
          or_put_byte (buf, bitlen);
        }
      else
        {
          or_put_byte (buf, 0xFF);
          OR_PUT_INT (&net_bitlen, bitlen);
          or_put_data (buf, (char *) &net_bitlen, OR_INT_SIZE);
        }

      /* store the string bytes */
      or_put_data (buf, string, bytelen);

      if (align == INT_ALIGNMENT)
        {
          /* round up to a word boundary */
          or_put_align32 (buf);
        }
    }
  else
    {
      if (valid_buf)
        {
          memcpy (&buf->env, &save_buf, sizeof (save_buf));
          _longjmp (buf->env, status);
        }
    }

  if (valid_buf)
    {
      memcpy (&buf->env, &save_buf, sizeof (save_buf));
    }
  else
    {
      buf->error_abort = 0;
    }

  if (status == 0)
    {
      return NO_ERROR;
    }

  return status;

}

int
or_put_offset (OR_BUF * buf, int num)
{
  return or_put_offset_internal (buf, num, BIG_VAR_OFFSET_SIZE);
}

int
or_put_offset_internal (OR_BUF * buf, int num, int offset_size)
{
  if (offset_size == OR_BYTE_SIZE)
    {
      return or_put_byte (buf, num);
    }
  else if (offset_size == OR_SHORT_SIZE)
    {
      return or_put_short (buf, num);
    }
  else
    {
      assert (offset_size == BIG_VAR_OFFSET_SIZE);

      return or_put_int (buf, num);
    }
}

int
or_get_offset (OR_BUF * buf, int *error)
{
  return or_get_offset_internal (buf, error, BIG_VAR_OFFSET_SIZE);
}

int
or_get_offset_internal (OR_BUF * buf, int *error, int offset_size)
{
  if (offset_size == OR_BYTE_SIZE)
    {
      return or_get_byte (buf, error);
    }
  else if (offset_size == OR_SHORT_SIZE)
    {
      return or_get_short (buf, error);
    }
  else
    {
      assert (offset_size == BIG_VAR_OFFSET_SIZE);
      return or_get_int (buf, error);
    }
}

/*
 * or_get_varbit_length - get varbit length from or buffer
 *    return: length of varbit or 0 if error
 *    buf(in/out): or buffer
 *    rc(out): NO_ERROR or error code
 */
int
or_get_varbit_length (OR_BUF * buf, int *rc)
{
  int net_bitlen = 0, bitlen = 0;

  /* unpack the size prefix */
  bitlen = or_get_byte (buf, rc);

  if (*rc != NO_ERROR)
    {
      return bitlen;
    }

  if (bitlen == 0xFF)
    {
      *rc = or_get_data (buf, (char *) &net_bitlen, OR_INT_SIZE);
      bitlen = OR_GET_INT (&net_bitlen);
    }
  return bitlen;
}

/*
 * or_skip_varbit_remainder - skip varbit field of given length in or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    bitlen(in): bitlen to skip
 *    align(in):
 */
int
or_skip_varbit_remainder (OR_BUF * buf, int bitlen, int align)
{
  int rc = NO_ERROR;

  rc = or_advance (buf, BITS_TO_BYTES (bitlen));
  if (rc == NO_ERROR && align == INT_ALIGNMENT)
    {
      rc = or_get_align32 (buf);
    }
  return rc;
}

/*
 * or_skip_varbit - skip varbit in or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    align(in):
 */
int
or_skip_varbit (OR_BUF * buf, int align)
{
  int bitlen;
  int rc = NO_ERROR;

  bitlen = or_get_varbit_length (buf, &rc);
  if (rc == NO_ERROR)
    {
      return (or_skip_varbit_remainder (buf, bitlen, align));
    }
  return rc;
}

/*
 * NUMERIC DATA TRANSFORMS
 *    This set of functions handles the transformation of the
 *    numeric types byte, short, integer, float, and double.
 *
 */


/*
 * or_put_byte - put a byte to or buffer
 *    return: NO_ERROR or error code
 *    buf(out/out): or buffer
 *    num(in): byte value
 */
int
or_put_byte (OR_BUF * buf, int num)
{
  if ((buf->ptr + OR_BYTE_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_BYTE (buf->ptr, num);
      buf->ptr += OR_BYTE_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_byte - read a byte value from or buffer
 *    return: byte value read
 *    buf(in/out): or buffer
 *    error(out): NO_ERROR or error code
 */
int
or_get_byte (OR_BUF * buf, int *error)
{
  int value = 0;

  if ((buf->ptr + OR_BYTE_SIZE) > buf->endptr)
    {
      *error = or_underflow (buf);
      return 0;
    }
  else
    {
      value = OR_GET_BYTE (buf->ptr);
      buf->ptr += OR_BYTE_SIZE;
      *error = NO_ERROR;
    }
  return value;
}

/*
 * or_put_short - put a short value to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    num(in): short value to put
 */
int
or_put_short (OR_BUF * buf, int num)
{
  ASSERT_ALIGN (buf->ptr, SHORT_ALIGNMENT);

  if ((buf->ptr + OR_SHORT_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_SHORT (buf->ptr, num);
      buf->ptr += OR_SHORT_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_short - read a short value from or buffer
 *    return: short value read
 *    buf(in/out): or buffer
 *    error(out): NO_ERROR or error code
 */
int
or_get_short (OR_BUF * buf, int *error)
{
  int value = 0;

  ASSERT_ALIGN (buf->ptr, SHORT_ALIGNMENT);

  if ((buf->ptr + OR_SHORT_SIZE) > buf->endptr)
    {
      *error = or_underflow (buf);
      return 0;
    }
  else
    {
      value = OR_GET_SHORT (buf->ptr);
      buf->ptr += OR_SHORT_SIZE;
    }
  *error = NO_ERROR;
  return value;
}

/*
 * or_put_int - put int value to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    num(in): int value to put
 */
int
or_put_int (OR_BUF * buf, int num)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_INT_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_INT (buf->ptr, num);
      buf->ptr += OR_INT_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_int - get int value from or buffer
 *    return: int value read
 *    buf(in/out): or buffer
 *    error(out): NO_ERROR or error code
 */
int
or_get_int (OR_BUF * buf, int *error)
{
  int value = 0;

  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_INT_SIZE) > buf->endptr)
    {
      *error = or_underflow (buf);
    }
  else
    {
      value = OR_GET_INT (buf->ptr);
      buf->ptr += OR_INT_SIZE;
      *error = NO_ERROR;
    }
  return value;
}

/*
 * or_put_bigint - put bigint value to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    num(in): bigint value to put
 */
int
or_put_bigint (OR_BUF * buf, DB_BIGINT num)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_BIGINT_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_BIGINT (buf->ptr, &num);
      buf->ptr += OR_BIGINT_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_bigint - get bigint value from or buffer
 *    return: bigint value read
 *    buf(in/out): or buffer
 *    error(out): NO_ERROR or error code
 */
DB_BIGINT
or_get_bigint (OR_BUF * buf, int *error)
{
  DB_BIGINT value = 0;

  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_BIGINT_SIZE) > buf->endptr)
    {
      *error = or_underflow (buf);
    }
  else
    {
      OR_GET_BIGINT (buf->ptr, &value);
      buf->ptr += OR_BIGINT_SIZE;
      *error = NO_ERROR;
    }
  return value;
}

/*
 * or_put_double - put a double value to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    dnum(in): double value to put
 */
int
or_put_double (OR_BUF * buf, double dnum)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_DOUBLE_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_DOUBLE (buf->ptr, &dnum);
      buf->ptr += OR_DOUBLE_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_double - read a double value from or buffer
 *    return: double value read
 *    buf(in/out): or buffer
 *    error(out): NO_ERROR or error code
 */
double
or_get_double (OR_BUF * buf, int *error)
{
  double value = 0.0;

  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_DOUBLE_SIZE) > buf->endptr)
    {
      *error = or_underflow (buf);
    }
  else
    {
      OR_GET_DOUBLE (buf->ptr, &value);
      buf->ptr += OR_DOUBLE_SIZE;
      *error = NO_ERROR;
    }
  return value;
}

/*
 * EXTENDED TYPE TRANSLATORS
 *    This set of functions reads and writes the extended types time,
 *    utime, date.
 */

/*
 * or_put_time - write a DB_TIME to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    timeval(in): time value to write
 */
int
or_put_time (OR_BUF * buf, DB_TIME * timeval)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_TIME_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_TIME (buf->ptr, timeval);
      buf->ptr += OR_TIME_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_time - read a  DB_TIME from or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    timeval(out): pointer to DB_TIME value
 */
int
or_get_time (OR_BUF * buf, DB_TIME * timeval)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_TIME_SIZE) > buf->endptr)
    {
      return or_underflow (buf);
    }
  else
    {
      OR_GET_TIME (buf->ptr, timeval);
      buf->ptr += OR_TIME_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_put_date - write a DB_DATE value to or_buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    date(in): pointer to DB_DATE value
 */
int
or_put_date (OR_BUF * buf, DB_DATE * date)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_DATE_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_DATE (buf->ptr, date);
      buf->ptr += OR_DATE_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_date - read a DB_DATE value from or_buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    date(out): pointer to DB_DATE value
 */
int
or_get_date (OR_BUF * buf, DB_DATE * date)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_DATE_SIZE) > buf->endptr)
    {
      return or_underflow (buf);
    }
  else
    {
      OR_GET_DATE (buf->ptr, date);
      buf->ptr += OR_DATE_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_put_datetime - write a datetime value to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    datetimeval(in): pointer to datetime value
 */
int
or_put_datetime (OR_BUF * buf, DB_DATETIME * datetimeval)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_DATETIME_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      OR_PUT_DATETIME (buf->ptr, datetimeval);
      buf->ptr += OR_DATETIME_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_datetime - read a DB_DATETIME value from or_buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    date(out): pointer to DB_DATETIME value
 */
int
or_get_datetime (OR_BUF * buf, DB_DATETIME * datetime)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_DATETIME_SIZE) > buf->endptr)
    {
      return or_underflow (buf);
    }
  else
    {
      OR_GET_DATETIME (buf->ptr, datetime);
      buf->ptr += OR_DATETIME_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_put_oid - write content of an OID structure from or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    oid(in): pointer to OID
 */
int
or_put_oid (OR_BUF * buf, const OID * oid)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_OID_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      if (oid == NULL)
        {
          OR_PUT_NULL_OID (buf->ptr);
        }
      else
        {
          /* Cannot allow any temp oid's to be written */
          if (OID_ISTEMP (oid))
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
              or_abort (buf);
            }
          OR_PUT_OID (buf->ptr, oid);
        }
      buf->ptr += OR_OID_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_oid - read content of an OID structure from or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    oid(out): pointer to OID
 */
int
or_get_oid (OR_BUF * buf, OID * oid)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_OID_SIZE) > buf->endptr)
    {
      return or_underflow (buf);
    }
  else
    {
      OR_GET_OID (buf->ptr, oid);
      buf->ptr += OR_OID_SIZE;
    }
  return NO_ERROR;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_put_loid - write a long oid to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    loid(in): pointer to LOID
 */
int
or_put_loid (OR_BUF * buf, LOID * loid)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_LOID_SIZE) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      if (loid == NULL)
        {
          OR_PUT_NULL_LOID (buf->ptr);
        }
      else
        {
          OR_PUT_LOID (buf->ptr, loid);
        }
      buf->ptr += OR_LOID_SIZE;
    }
  return NO_ERROR;
}

/*
 * or_get_loid - read a long oid from or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    loid(out): pointer to LOID
 */
int
or_get_loid (OR_BUF * buf, LOID * loid)
{
  ASSERT_ALIGN (buf->ptr, INT_ALIGNMENT);

  if ((buf->ptr + OR_LOID_SIZE) > buf->endptr)
    {
      return or_underflow (buf);
    }
  else
    {
      OR_GET_LOID (buf->ptr, loid);
      buf->ptr += OR_LOID_SIZE;
    }
  return NO_ERROR;
}
#endif

/*
 * or_put_data - write an array of bytes to or buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    data(in): pointer to data
 *    length(in): length in bytes
 */
int
or_put_data (OR_BUF * buf, const char *data, int length)
{
  if ((buf->ptr + length) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      (void) memcpy (buf->ptr, data, length);
      buf->ptr += length;
    }
  return NO_ERROR;
}

/*
 * or_get_data - read an array of bytes from or buffer for given length
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    data(in): pointer to buffer to read data into
 *    length(in): length of read data
 */
int
or_get_data (OR_BUF * buf, char *data, int length)
{
  if ((buf->ptr + length) > buf->endptr)
    {
      return or_underflow (buf);
    }
  else
    {
      (void) memcpy (data, buf->ptr, length);
      buf->ptr += length;
    }
  return NO_ERROR;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_put_string - write string to or buf
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    str(in): string to wirte
 *
 * Note:
 *    Does byte padding on strings to bring them up to 4 byte boundary.
 *
 *    There is no or_get_string since this is the same as or_get_data.
 *    Since the workspace allocator (and most other Unix allocators) will
 *    keep track of the size of allocated blocks (and they will be
 *    in word multiples anyway), we can just include the disk padding
 *    bytes with the string when it is brought in from disk even though
 *    the total length may be more than that returned by strlen.
 */
int
or_put_string (OR_BUF * buf, char *str)
{
  int len, bits, pad;
  int rc = NO_ERROR;

  if (str == NULL)
    {
      return rc;
    }
  len = strlen (str) + 1;
  rc = or_put_data (buf, str, len);
  if (rc == NO_ERROR)
    {
      /* PAD */
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
          rc = or_pad (buf, pad);
        }
    }
  return rc;
}

/*
 * or_length_string - returns the number of bytes required to hold the disk
 * representation of a string
 *    return: number of bytes needed or 0 for error
 *    string(in): string for calculation
 *
 * Note:
 * This will include any padding bytes up to 4 byte boundary
 */
int
or_length_string (char *string)
{
  int len, bits;

  len = 0;
  if (string != NULL)
    {
      len = strlen (string) + 1;
      bits = len & 3;
      if (bits)
        {
          len += 4 - bits;
        }
    }
  return len;
}

/*
 * or_put_binary - Writes a binary array into the translation buffer.
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    binary(in): binary data
 *
 * Note:
 *    The length of the array is part of the binary data descriptor.
 *    This is similar to or_put_string in that it also must pad out the
 *    binary data to be on a word boundary.
 */
int
or_put_binary (OR_BUF * buf, DB_BINARY * binary)
{
  int header, len, bits, pad;
  int rc = NO_ERROR;

  if (binary != NULL && binary->length < OR_BINARY_MAX_LENGTH)
    {
      len = binary->length + OR_INT_SIZE;
      pad = 0;
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      header = binary->length | (pad << OR_BINARY_PAD_SHIFT);
      rc = or_put_int (buf, header);
      if (rc == NO_ERROR)
        {
          rc = or_put_data (buf, (char *) binary->data, binary->length);
          if (rc == NO_ERROR)
            {
              rc = or_pad (buf, pad);
            }
        }
    }
  return rc;
}

/*
 * or_length_binary - Calculates the number of bytes required for the disk
 * representaion of binary data.
 *    return: bytes required or 0 for error
 *    binary(in): binary data
 *
 * Note:
 * Included in this number are any padding bytes required to bring the data
 * up to a word boundary.
 */
int
or_length_binary (DB_BINARY * binary)
{
  int len, bits;

  len = 0;
  if (binary != NULL && binary->length < OR_BINARY_MAX_LENGTH)
    {
      len = binary->length + OR_INT_SIZE;       /* always a header word for sizes */
      bits = len & 3;
      if (bits)
        {
          len += 4 - bits;
        }
    }
  return len;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * or_pad - This advances the translation pointer and adds bytes of zero.
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    length(in): number of bytes to pad
 *
 * Note:
 *    This advances the translation pointer and adds bytes of zero.
 *    This is used add padding bytes to ensure proper allignment of
 *    some data types.
 */
int
or_pad (OR_BUF * buf, int length)
{
  if ((buf->ptr + length) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      (void) memset (buf->ptr, 0, length);
      buf->ptr += length;
    }
  return NO_ERROR;
}

/*
 * or_advance - This advances the translation pointer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 *    offset(in): number of bytes to skip
 */
int
or_advance (OR_BUF * buf, int offset)
{
  if ((buf->ptr + offset) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      buf->ptr += offset;
      return NO_ERROR;
    }
}

/*
 * or_seek - This sets the translation pointer directly to a certain byte in
 * the buffer.
 *    return: ERROR_SUCCESS or error code
 *    buf(in/out): or buffer
 *    psn(in): position within buffer
 */
int
or_seek (OR_BUF * buf, int psn)
{
  if ((buf->buffer + psn) > buf->endptr)
    {
      return (or_overflow (buf));
    }
  else
    {
      buf->ptr = buf->buffer + psn;
    }
  return NO_ERROR;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_unpack_var_table - Extracts a variable offset table from the disk
 * representation of an object and converts it into a memory structure
 *    return: advanced buffer pointer
 *    ptr(in): pointer into a disk representation
 *    nvars(in): number of variables expected
 *    vars(out): array of var table info
 *
 * Note:
 *    This is a little easier than dealing with the offset table in its raw
 *    disk format.  It assumes that you know the number of elements
 *    in the table and have previously allocated an array of OR_VARINFO
 *    structures that will be filled in.
 */
char *
or_unpack_var_table (char *ptr, int nvars, OR_VARINFO * vars)
{
  return or_unpack_var_table_internal (ptr, nvars, vars, BIG_VAR_OFFSET_SIZE);
}
#endif

/*
 * or_unpack_var_table_internal - Extracts a variable offset table from the disk
 * representation of an object and converts it into a memory structure
 *    return: advanced buffer pointer
 *    ptr(in): pointer into a disk representation
 *    nvars(in): number of variables expected
 *    vars(out): array of var table info
 *
 * Note:
 *    This is a little easier than dealing with the offset table in its raw
 *    disk format.  It assumes that you know the number of elements
 *    in the table and have previously allocated an array of OR_VARINFO
 *    structures that will be filled in.
 */
static char *
or_unpack_var_table_internal (char *ptr, int nvars, OR_VARINFO * vars, int offset_size)
{
  int i, offset, offset2;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (nvars == 0)
    {
      return ptr;
    }

  offset = OR_GET_OFFSET_INTERNAL (ptr, offset_size);
  ptr += offset_size;

  for (i = 0; i < nvars; i++)
    {
      offset2 = OR_GET_OFFSET_INTERNAL (ptr, offset_size);
      ptr += offset_size;
      vars[i].offset = offset;
      vars[i].length = offset2 - offset;
      offset = offset2;
    }
  ptr = PTR_ALIGN (ptr, INT_ALIGNMENT);

  return ptr;
}

/*
 * or_get_var_table - Extracts an array of OR_VARINFO structures from a
 * disk variable offset table
 *    return: array of OR_VARINFO structures
 *    buf(in/out): or buffer
 *    nvars(in): expected number of variables
 *    allocator(in): allocator for return value allocation
 */
OR_VARINFO *
or_get_var_table (OR_BUF * buf, int nvars, char *(*allocator) (int))
{
  return or_get_var_table_internal (buf, nvars, allocator, BIG_VAR_OFFSET_SIZE);
}

/*
 * or_get_var_table_internal - Extracts an array of OR_VARINFO structures from a
 * disk variable offset table
 *    return: array of OR_VARINFO structures
 *    buf(in/out): or buffer
 *    nvars(in): expected number of variables
 *    allocator(in): allocator for return value allocation
 */
OR_VARINFO *
or_get_var_table_internal (OR_BUF * buf, int nvars, char *(*allocator) (int), int offset_size)
{
  OR_VARINFO *vars;
  int length;

  vars = NULL;

  if (nvars == 0)
    {
      return NULL;
    }

  length = DB_ALIGN (offset_size * (nvars + 1), INT_ALIGNMENT);

  if ((buf->ptr + length) > buf->endptr)
    {
      or_underflow (buf);
      return NULL;
    }

  vars = (OR_VARINFO *) (*allocator) (sizeof (OR_VARINFO) * nvars);
  if (vars == NULL)
    {
      or_abort (buf);
      return NULL;
    }


  (void) or_unpack_var_table_internal (buf->ptr, nvars, vars, offset_size);

  buf->ptr += length;

  return vars;
}

/*
 * COMMUNICATION BUFFER PACK/UNPACK FUNCTIONS
 */


/*
 * NUMERIC DATA TYPE TRANSLATORS
 *    Translators for the numeric data types int, short, errcode, lock...
 */

/*
 * or_pack_int - write int value to ptr
 *    return: advanced buffer pointer
 *    ptr(out): out buffer
 *    number(in): int value
 */
char *
or_pack_int (char *ptr, int number)
{
  if (ptr == NULL)
    {
      return NULL;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_INT (ptr, number);
  return (ptr + OR_INT_SIZE);
}

/*
 * or_unpack_int - read a int value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    number(out): int value
 */
char *
or_unpack_int (char *ptr, int *number)
{
  if (ptr == NULL)
    {
      *number = 0;
      return NULL;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  *number = OR_GET_INT (ptr);
  return (ptr + OR_INT_SIZE);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_pack_bigint - write bigint value to ptr
 *    return: advanced buffer pointer
 *    ptr(out): out buffer
 *    number(in): bigint value
 */
char *
or_pack_bigint (char *ptr, DB_BIGINT number)
{
  return or_pack_int64 (ptr, number);
}

/*
 * or_unpack_bigint - read a bigint value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    number(out): bigint value
 */
char *
or_unpack_bigint (char *ptr, DB_BIGINT * number)
{
  return or_unpack_int64 (ptr, number);
}
#endif

/*
 * or_pack_int64 - write INT64 value to ptr
 *    return: advanced buffer pointer
 *    ptr(out): out buffer
 *    number(in): INT64 value
 */
char *
or_pack_int64 (char *ptr, INT64 number)
{
  ASSERT_ALIGN (ptr, MAX_ALIGNMENT);

  OR_PUT_INT64 (ptr, &number);
  return (ptr + OR_INT64_SIZE);
}

/*
 * or_unpack_int64 - read a INT64 value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    number(out): INT64 value
 */
char *
or_unpack_int64 (char *ptr, INT64 * number)
{
  ASSERT_ALIGN (ptr, MAX_ALIGNMENT);

  OR_GET_INT64 (ptr, number);
  return (ptr + OR_INT64_SIZE);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_pack_short - write a short value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    number(in): short value
 */
char *
or_pack_short (char *ptr, short number)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_INT (ptr, (int) number);
  return (ptr + OR_INT_SIZE);
}

/*
 * or_unpack_short - read a short value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    number(out): short value
 */
char *
or_unpack_short (char *ptr, short *number)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  *number = (short) OR_GET_INT (ptr);
  return (ptr + OR_INT_SIZE);
}
#endif

/*
 * or_pack_errcode - write a errcode value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    error(in): int value
 */
char *
or_pack_errcode (char *ptr, int error)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_INT (ptr, (int) error);
  return (ptr + OR_INT_SIZE);
}

/*
 * or_unpack_errcode - read a errcode value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    error(out): int value
 */
char *
or_unpack_errcode (char *ptr, int *error)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  *error = (int) OR_GET_INT (ptr);
  return (ptr + OR_INT_SIZE);
}


/*
 * or_pack_lock - write a LOCK value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    lock(in): LOCK value
 */
char *
or_pack_lock (char *ptr, LOCK lock)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_INT (ptr, (int) lock);
  return (ptr + OR_INT_SIZE);
}

/*
 * or_unpack_lock - read a LOCK value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    lock(out): LOCK value
 */
char *
or_unpack_lock (char *ptr, LOCK * lock)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  *lock = (LOCK) OR_GET_INT (ptr);
  return (ptr + OR_INT_SIZE);
}

/*
 * or_pack_float - write a float value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    number(in): float value
 */
char *
or_pack_float (char *ptr, float number)
{
  ASSERT_ALIGN (ptr, FLOAT_ALIGNMENT);

  OR_PUT_FLOAT (ptr, &number);
  return (ptr + OR_FLOAT_SIZE);
}

/*
 * or_unpack_float - read a float value
 *    return: advanced buffer pointer
 *    ptr(in): read buffer
 *    number(out): float value
 */
char *
or_unpack_float (char *ptr, float *number)
{
  ASSERT_ALIGN (ptr, FLOAT_ALIGNMENT);

  OR_GET_FLOAT (ptr, number);
  return (ptr + OR_FLOAT_SIZE);
}


/*
 * or_pack_double - write a double value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    number(in): double value
 */
char *
or_pack_double (char *ptr, double number)
{
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);

  OR_PUT_DOUBLE (ptr, &number);
  return (ptr + OR_DOUBLE_SIZE);
}

/*
 * or_unpack_double - read a double value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    number(out): double value
 */
char *
or_unpack_double (char *ptr, double *number)
{
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);

  OR_GET_DOUBLE (ptr, number);
  return (ptr + OR_DOUBLE_SIZE);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_pack_time - write a DB_TIME value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    time(in): DB_TIME value
 */
char *
or_pack_time (char *ptr, DB_TIME time)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_TIME (ptr, &time);
  return (ptr + OR_TIME_SIZE);
}

/*
 * or_unpack_time - read a DB_TIME value
 *    return:  advanced buffer pointer
 *    ptr(in): input buffer
 *    time(out): DB_TIME value
 */
char *
or_unpack_time (char *ptr, DB_TIME * time)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_TIME (ptr, time);
  return (ptr + OR_TIME_SIZE);
}

/*
 * or_pack_date - write a DB_DATE value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    date(in): DB_DATE value
 */
char *
or_pack_date (char *ptr, DB_DATE date)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_DATE (ptr, &date);
  return (ptr + OR_DATE_SIZE);
}

/*
 * or_unpack_date - read a DB_DATE value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    date(out): DB_DATE value
 */
char *
or_unpack_date (char *ptr, DB_DATE * date)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_DATE (ptr, date);
  return (ptr + OR_DATE_SIZE);
}

/*
 * or_unpack_int_array - extracts a array of integers from a buffer
 *    return: advanced buffer pointer
 *    ptr(in): current pointer in buffer
 *    n(in): array length
 *    number_array(out): result array
 */
char *
or_unpack_int_array (char *ptr, int n, int **number_array)
{
  int i;

  *number_array = (int *) malloc ((n * sizeof (int)));
  if (*number_array)
    {
      ASSERT_ALIGN (ptr, INT_ALIGNMENT);
      for (i = 0; i < n; i++)
        {
          ptr = or_unpack_int (ptr, &(*number_array)[i]);
        }
    }
  else
    {
      ptr = NULL;
    }

  return ptr;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * DISK IDENTIFIER TRANSLATORS
 *    Translators for the disk identifiers OID, LOID, HFID, BTID, EHID.
 */


/*
 * or_pack_oid - write a OID value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    oid(in): OID value
 */
char *
or_pack_oid (char *ptr, const OID * oid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (oid != NULL)
    {
      OR_PUT_OID (ptr, oid);
    }
  else
    {
      OR_PUT_NULL_OID (ptr);
    }
  return (ptr + OR_OID_SIZE);
}

/*
 * or_unpack_oid - read a OID value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    oid(out): OID value
 */
char *
or_unpack_oid (char *ptr, OID * oid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_OID (ptr, oid);
  return (ptr + OR_OID_SIZE);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * or_unpack_oid_array - read OID array values
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    n(in): number of OIDs to read
 *    oids(out): OID array
 */
char *
or_unpack_oid_array (char *ptr, int n, OID ** oids)
{
  int i;

  *oids = (OID *) malloc ((n * sizeof (OID)));

  if (*oids == NULL)
    {
      ptr = NULL;
      return ptr;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  for (i = 0; i < n; i++)
    {
      OR_GET_OID (ptr, &((*oids)[i]));
      ptr = ptr + OR_OID_SIZE;
    }

  return (ptr);
}
#endif

/*
 * or_pack_loid - write a LOID value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    loid(in): LOID value
 */
char *
or_pack_loid (char *ptr, const LOID * loid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (loid != NULL)
    {
      OR_PUT_LOID (ptr, loid);
    }
  else
    {
      OR_PUT_NULL_LOID (ptr);
    }

  return (ptr + OR_LOID_SIZE);
}

/*
 * or_unpack_loid - read a LOID value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    loid(out): LOID value
 */
char *
or_unpack_loid (char *ptr, LOID * loid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_LOID (ptr, loid);
  return (ptr + OR_LOID_SIZE);
}

/*
 * or_pack_hfid - write a HFID value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    hfid(in): HFID value
 */
char *
or_pack_hfid (char *ptr, const HFID * hfid)
{
  char *new_;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (hfid != NULL)
    {
      OR_PUT_HFID (ptr, hfid);
    }
  else
    {
      OR_PUT_NULL_HFID (ptr);
    }

  /* kludge, need to have all of these accept and return const args */
  new_ = (char *) ptr + OR_HFID_SIZE;
  return new_;
}

/*
 * or_unpack_hfid - read a HFID value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    hfid(out): HFID value
 */
char *
or_unpack_hfid (char *ptr, HFID * hfid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_HFID (ptr, hfid);
  return (ptr + OR_HFID_SIZE);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_pack_ehid - write a HFID value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    ehid(in): HFID value
 */
char *
or_pack_ehid (char *ptr, EHID * ehid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_PUT_EHID (ptr, ehid);

  return (ptr + OR_EHID_SIZE);
}

char *
or_pack_recdes (char *buf, RECDES * recdes)
{
  buf = or_pack_int (buf, recdes->length);
  buf = or_pack_short (buf, recdes->type);
  buf = or_pack_stream (buf, recdes->data, recdes->length);
  return buf;
}

/*
 * or_unpack_ehid - read a EHID value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    ehid(out): EHID value
 */
char *
or_unpack_ehid (char *ptr, EHID * ehid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_EHID (ptr, ehid);

  return (ptr + OR_EHID_SIZE);
}
#endif

/*
 * or_pack_btid - write a BTID value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    btid(in): BTID value
 */
char *
or_pack_btid (char *ptr, const BTID * btid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (btid)
    {
      OR_PUT_BTID (ptr, btid);
    }
  else
    {
      OR_PUT_NULL_BTID (ptr);
    }

#if !defined(NDEBUG)
  /* to make valgrind quiet */
  OR_PUT_SHORT (ptr + OR_BTID_SIZE, 0);
#endif

  return (ptr + OR_BTID_ALIGNED_SIZE);
}

/*
 * or_unpack_btid - read a BTID value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    btid(out): a BTID value
 */
char *
or_unpack_btid (char *ptr, BTID * btid)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_BTID (ptr, btid);

  return (ptr + OR_BTID_ALIGNED_SIZE);
}

/*
 * or_pack_log_lsa - write a LOG_LSA value
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    lsa(in): LOG_LSA value
 */
char *
or_pack_log_lsa (char *ptr, const LOG_LSA * lsa)
{
  char *new_;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (lsa != NULL)
    {
      OR_PUT_LOG_LSA (ptr, lsa);
    }
  else
    {
      OR_PUT_NULL_LOG_LSA (ptr);
    }

#if !defined(NDEBUG)
  /* to make valgrind quiet */
  OR_PUT_SHORT (ptr + OR_LOG_LSA_SIZE, 0);
#endif

  /* kludge, need to have all of these accept and return const args */
  new_ = (char *) ptr + OR_LOG_LSA_ALIGNED_SIZE;
  return new_;
}

/*
 * or_unpack_log_lsa - read a LOG_LSA value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    lsa(out): LOG_LSA value
 */
char *
or_unpack_log_lsa (char *ptr, LOG_LSA * lsa)
{
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_GET_LOG_LSA (ptr, lsa);
  return (ptr + OR_LOG_LSA_ALIGNED_SIZE);
}

/*
 * or_unpack_set - read a set
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    set(out): set value
 *    domain(in): domain of the set (can be NULL)
 */
char *
or_unpack_set (char *ptr, SETOBJ ** set, TP_DOMAIN * domain)
{
  OR_BUF orbuf;

  or_init (&orbuf, ptr, 0);
  *set = or_get_set (&orbuf, domain);

  return orbuf.ptr;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_unpack_setref - unpack a set and get set reference for the set
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    ref(out): reference for a set
 */
char *
or_unpack_setref (char *ptr, DB_SET ** ref)
{
  SETOBJ *set = NULL;

  ptr = or_unpack_set (ptr, &set, NULL);

  if (set != NULL)
    {
      *ref = setobj_get_reference (set);
    }
  else
    {
      *ref = NULL;
    }

  return ptr;
}
#endif

/*
 * or_pack_string - Puts a string into the buffer.
 *    return: advanced buffer pointer
 *    ptr(out): current buffer pointer
 *    string(in): string to pack
 * Note:
 *    The string will be padded with extra bytes so that the ending pointer
 *    is on a word boundary.
 *    NOTE:  This differs from or_put_string in that the length of the string
 *    is stored before the string data.  If the string is NULL the length
 *    is -1.  This is because comm buffers aren't formatted as objects
 *    and therefore don't have an offset table that holds the size
 *    of the string.
 */
char *
or_pack_string (char *ptr, const char *string)
{
  int len, bits, pad;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (string == NULL)
    {
      OR_PUT_INT (ptr, -1);
      ptr += OR_INT_SIZE;
    }
  else
    {
      len = strlen (string) + 1;
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      OR_PUT_INT (ptr, len + pad);
      ptr += OR_INT_SIZE;
      (void) memcpy (ptr, string, len);
      ptr += len;
      (void) memset (ptr, '\0', pad);
      ptr += pad;
    }
  return ptr;
}


/*
 * or_pack_string_with_null_padding - make stream to string and pack
 *    return: advanced buffer pointer
 *    ptr(out): current buffer pointer
 *    string(in): string to pack
 *    len(in): stream len
 */
char *
or_pack_string_with_null_padding (char *ptr, const char *string, size_t len)
{
  char *ret_ptr;

  ret_ptr = or_pack_stream (ptr, string, len + 1);

  ptr[OR_INT_SIZE + len] = '\0';        /* NULL Padding */

  return ret_ptr;
}

/*
 * or_pack_stream - Puts a stream into the buffer.
 *    return: advanced buffer pointer
 *    ptr(out): current buffer pointer
 *    stream(in): stream to pack
 */
char *
or_pack_stream (char *ptr, const char *stream, size_t len)
{
  int bits, pad;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (stream == NULL)
    {
      OR_PUT_INT (ptr, -1);
      ptr += OR_INT_SIZE;
    }
  else
    {
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      OR_PUT_INT (ptr, len + pad);
      ptr += OR_INT_SIZE;
      memcpy (ptr, stream, len);
      ptr += len;
      memset (ptr, '\0', pad);
      ptr += pad;
    }
  return ptr;
}

/*
 * or_pack_string_with_length - pack a string at most given length
 *    return: advanced buffer pointer
 *    ptr(out): output buffer
 *    string(in): string
 *    length(in): length
 */
char *
or_pack_string_with_length (char *ptr, const char *string, int length)
{
  int len, bits, pad;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  if (string == NULL)
    {
      OR_PUT_INT (ptr, -1);
      ptr += OR_INT_SIZE;
    }
  else
    {
      len = length + 1;
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      OR_PUT_INT (ptr, len + pad);
      ptr += OR_INT_SIZE;
      (void) memcpy (ptr, string, len);
      ptr += len;
      (void) memset (ptr, '\0', pad);
      ptr += pad;
    }

  return ptr;
}

/*
 * or_unpack_string - extracts a string from a buffer.
 *    return: advanced pointer
 *    ptr(in): current pointer
 *    string(out): return pointer
 */
char *
or_unpack_string (char *ptr, char **string)
{
  char *new_;
  int length;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  length = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  if (length == -1)
    {
      *string = NULL;
    }
  else
    {
      new_ = (char *) malloc (length);
      /* need to handle allocation errors */
      if (new_ == NULL)
        {
          assert (false);
          ptr += length;
        }
      else
        {
          (void) memcpy (new_, ptr, length);
          ptr += length;
        }
      *string = new_;
    }
  return ptr;
}

/*
 * or_unpack_stream - extracts a stream from a buffer.
 *    return: advanced pointer
 *    ptr(in): current pointer
 *    stream(out): return pointer
 */
char *
or_unpack_stream (char *ptr, char *stream, size_t len)
{
  int length;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  length = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  assert_release (length >= 0 && (size_t) length >= len);

  memcpy (stream, ptr, len);
  ptr += length;
  return ptr;
}

/*
 * or_unpack_string_alloc - extracts a string from a buffer.
 *    return: advanced pointer
 *    ptr(in): current pointer
 *    string(out): return pointer
 *
 * Note: Unlike or_unpack_string which uses db_private_alloc to allocate
 * memory for the resulting string, this function uses malloc and the string
 * has to be freed using free_and_init.
 */
char *
or_unpack_string_alloc (char *ptr, char **string)
{
  char *new_;
  int length;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  length = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  if (length == -1)
    {
      *string = NULL;
    }
  else
    {
      new_ = (char *) malloc (length * sizeof (char));
      /* need to handle allocation errors */
      if (new_ == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (length * sizeof (char)));
          ptr += length;
        }
      else
        {
          (void) memcpy (new_, ptr, length);
          ptr += length;
        }
      *string = new_;
    }
  return ptr;
}

/*
 * or_unpack_string_nocopy - extracts a string from a buffer.
 *    return: advanced pointer
 *    ptr(in): current pointer
 *    string(out): return pointer
 */
char *
or_unpack_string_nocopy (char *ptr, const char **string)
{
  int length;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  length = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  if (length == -1)
    {
      *string = NULL;
    }
  else
    {
      *string = ptr;
      ptr += length;
    }
  return ptr;
}

/*
 * or_packed_string_length - Determines the number of bytes required to hold
 * the packed representation of a string.
 *    return: length of packed string
 *    string(in): string to examine
 *    strlen(out): strlen(string)
 *
 * Note: This includes padding bytes necessary to bring the length up to a
 * word boundary and also includes a word for the string length which is
 * stored at the top.
 */
int
or_packed_string_length (const char *string, int *strlenp)
{
  int total, len, bits, pad;

  /* always have a length */
  total = OR_INT_SIZE;
  if (string != NULL)
    {
      if (strlenp != NULL)
        {
          len = (*strlenp = strlen (string)) + 1;
        }
      else
        {
          len = strlen (string) + 1;
        }
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      total += len + pad;
    }
  else
    {
      if (strlenp != NULL)
        {
          *strlenp = 0;
        }
    }
  return total;
}

/*
 * or_packed_stream_length - Determines the number of bytes required to hold
 * the packed representation of a stream.
 *    return: length of packed stream
 *    len(in): length of stream
 */
int
or_packed_stream_length (size_t len)
{
  int total, bits, pad;

  /* always have a length */
  total = OR_INT_SIZE;
  if (len > 0)
    {
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      total += len + pad;
    }
  return total;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * or_pack_bool_array - write a bool array to pointer
 *    return: advanced buffer pointer
 *    ptr(out): out buffer
 *    bools(in): bool array
 *    size(in): size of bool array
 */
char *
or_pack_bool_array (char *ptr, const bool * bools, int size)
{
  int bits, pad;
  if (ptr == NULL)
    {
      return NULL;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);
  if (bools == NULL)
    {
      OR_PUT_INT (ptr, -1);
      ptr += OR_INT_SIZE;
    }
  else
    {
      bits = size & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      OR_PUT_INT (ptr, size + pad);
      ptr += OR_INT_SIZE;
      (void) memcpy (ptr, bools, size);
      ptr += (size + pad);
    }
  return ptr;
}

/*
 * or_packed_bool_array_length - Determines the number of bytes required to
 *				 hold the packed representation of a bool
 *				 array.
 *    return: length of packed bool array
 *    bools(in): bool array
 *    size(in): the number of bool values in the array
 *
 * Note: This includes padding bytes necessary to bring the length up to a
 * word boundary and also includes a word for the array length which is
 * stored at the top.
 */
int
or_packed_bool_array_length (const bool * bools, int size)
{
  int total, bits, pad;

  /* always have a length */
  total = OR_INT_SIZE;
  if (bools != NULL)
    {
      bits = size & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      total += (size + pad);
    }

  return total;
}

/*
 * or_align_length - for a given length return aligned length
 *    return: aligned length
 *    length(in): given length
 */
int
or_align_length (int length)
{
  int total, len, bits, pad;

  /* always have a length */
  total = OR_INT_SIZE;

  if (length != 0)
    {
      len = length + 1;
      bits = len & 3;
      if (bits)
        {
          pad = 4 - bits;
        }
      else
        {
          pad = 0;
        }
      total += len + pad;
    }

  return total;
}

/*
 * or_encode - Encodes the source data into the buffer so that only ascii
 * characters appear in the buffer.
 *    return: void
 *    buffer(out): buffer to encode into
 *    source(in): source data
 *    size(in): size of source data
 */
void
or_encode (char *buffer, const char *source, int size)
{
  while (size--)
    {
      *buffer++ = ((*source & 0xf0) >> 4) + '@';
      *buffer++ = ((*source++ & 0xf) + '@');
    }
  *buffer = '\0';
}

/*
 * or_decode - Decodes the data in the buffer to dest.
 *    return:
 *    buffer(in): buffer to decode from
 *    dest(out): destination data
 *    size(in): size of destination data
 * Note: The buffer is assumed to be encoded by or_encode
 */
void
or_decode (const char *buffer, char *dest, int size)
{
  char c1, c2;

  while (size--)
    {
      c1 = ((*buffer++ - '@') << 4) & 0xf0;
      c2 = (*buffer++ - '@') & 0xf;
      *dest++ = c1 | c2;
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * DOMAIN PACKING
 */

/*
 * OR_DOMAIN_
 *    Constants used to pick apart the first word of a packed domain.
 */

/* note that this leaves room for only 63 type codes */
#define OR_DOMAIN_TYPE_MASK		(0x3F)
#define OR_DOMAIN_NEXT_FLAG		(0x80)  /* domain following this one */

#define OR_DOMAIN_NULL_FLAG		(0x40)  /* is a tagged NULL value */

#define OR_DOMAIN_SET_DOMAIN_FLAG	(0x100) /* for set types */
#define OR_DOMAIN_BUILTIN_FLAG		(0x100) /* for NULL type only */

#define OR_DOMAIN_SCALE_MASK		(0xFF00)
#define OR_DOMAIN_SCALE_SHIFT		(8)
#define OR_DOMAIN_SCALE_MAX		(0xFF)

#define OR_DOMAIN_CODSET_MASK		(0xFF00)
#define OR_DOMAIN_CODSET_SHIFT		(8)

#define OR_DOMAIN_PRECISION_MASK	(0xFFFF0000)
#define OR_DOMAIN_PRECISION_SHIFT	(16)
#define OR_DOMAIN_PRECISION_MAX		(0xFFFF)

#define OR_DOMAIN_COLLATION_MASK	(0x000000FF)
#if 0                           /* unused */
#define OR_DOMAIN_COLL_ENFORCE_FLAG	(0x80000000)
#define OR_DOMAIN_COLL_LEAVE_FLAG	(0x40000000)
#endif

/*
 * or_packed_domain_size - calcualte the necessary buffer size required
 * to hold a packed representation of a hierarchical domain structure.
 *    return: byte size of the packed domain
 *    domain(in): domain to pack
 *
 * Note: It is generally called before using or_pack_domain() to store the
 *    domain.
 */
int
or_packed_domain_size (TP_DOMAIN * domain)
{
  TP_DOMAIN *d;
  int size, precision, scale;
  DB_TYPE id;

  assert (domain != NULL);

  size = 0;

  /* hack, if this is a built-in domain, store a single word reference. */
  if (domain->built_in_index)
    {
      return OR_INT_SIZE;
    }

  /* now loop over the domains writing the disk representation */
  for (d = domain; d != NULL; d = d->next)
    {

      /* always have at least one word */
      size += OR_INT_SIZE;

      precision = 0;
      scale = 0;

      id = TP_DOMAIN_TYPE (d);

      switch (id)
        {
        case DB_TYPE_NUMERIC:
          precision = d->precision;
          scale = d->scale;
          /*
           * Safe guard for floating precision caused by incorrect type setting
           */
          if (precision <= TP_FLOATING_PRECISION_VALUE)
            {
              precision = DB_MAX_NUMERIC_PRECISION;
            }
          break;

        case DB_TYPE_VARCHAR:
          /* collation id */
          size += OR_INT_SIZE;
        case DB_TYPE_VARBIT:
          /*
           * Hack, if the precision is -1, it is a special value indicating
           * either the maximum precision for the varying types or a floating
           * precision for the fixed types.
           */
          if (d->precision != TP_FLOATING_PRECISION_VALUE)
            {
              precision = d->precision;
            }

          /*
           * Kludge, for temporary backward compatibility, treat varchar
           * types with the maximum precision as above. Need to change ourselves
           * to use -1 consistently for this after which this little
           * chunk of code can be removed.
           */
          if ((id == DB_TYPE_VARCHAR
               && d->precision == DB_MAX_VARCHAR_PRECISION)
              || (id == DB_TYPE_VARBIT && d->precision == DB_MAX_VARBIT_PRECISION))
            {
              precision = 0;
            }
          break;

        default:
          break;
        }

      if (precision >= OR_DOMAIN_PRECISION_MAX)
        {
          size += OR_INT_SIZE;
        }

      if (scale >= OR_DOMAIN_SCALE_MAX)
        {
          size += OR_INT_SIZE;
        }

      if (d->setdomain != NULL)
        {
          size += or_packed_domain_size (d->setdomain);
        }
    }

  return size;
}

/*
 * or_put_domain - creates the packed "disk" representation of a domain.
 *    return: NO_ERROR or error code
 *    buf(in/out): packing buffer
 *    domain(in): domain to pack
 *    is_null(in): use domain tags for "NULL" value
 * Note:
 *    The is_null flag was added recently to allow domain tags for "NULL"
 *    values without having to store something in addition to the domain
 *    to indicate that the value was NULL.
 *    This should only be on if the domain is being used to tag a value
 *    and the value is logically NULL.
 */
int
or_put_domain (OR_BUF * buf, TP_DOMAIN * domain, int is_null)
{
  unsigned int carrier, extended_precision, extended_scale;
  int precision, scale;
  int has_subdomain;
  bool has_collation;
  TP_DOMAIN *d;
  DB_TYPE id;
  int rc = NO_ERROR;
  unsigned int collation_storage;

  /*
   * Hack, if this is a built-in domain, store a single word reference.
   * This is only allowed for the top level domain.
   * Note that or_unpack_domain is probably not going to do the right
   * thing if we try to pack a builtin domain that is "inside" a hierarchical
   * domain.  This isn't supposed to happen and can't in general because
   * two hierarchical domains are going to have potentially different "next"
   * pointers in the sub-domains.
   */
  if (domain->built_in_index)
    {
      carrier =
        (DB_TYPE_NULL & OR_DOMAIN_TYPE_MASK) | OR_DOMAIN_BUILTIN_FLAG |
        (domain->built_in_index << OR_DOMAIN_PRECISION_SHIFT);
      if (is_null)
        {
          carrier |= OR_DOMAIN_NULL_FLAG;
        }
      return (or_put_int (buf, carrier));
    }

  /* must pack a full domain description */
  for (d = domain; d != NULL; d = d->next)
    {

      id = TP_DOMAIN_TYPE (d);

      /*
       * Initial word has type, precision, scale, & codeset to the extent that
       * they will fit.  High bit of the type byte is set if there
       * is another domain following this one. (e.g. for set or union domains).
       */
      carrier = id & OR_DOMAIN_TYPE_MASK;
      if (d->next != NULL)
        {
          carrier |= OR_DOMAIN_NEXT_FLAG;
        }
      if (is_null)
        {
          carrier |= OR_DOMAIN_NULL_FLAG;
        }

      precision = 0;
      scale = 0;
      extended_precision = 0;
      extended_scale = 0;
      has_subdomain = 0;
      has_collation = false;

      switch (id)
        {
        case DB_TYPE_NUMERIC:
          /* second byte contains scale, third & fourth bytes have precision */

          /* safe guard for scale */
          scale = d->scale;
          if (scale <= DB_DEFAULT_SCALE)
            {
              scale = 0;
            }

          if (scale < OR_DOMAIN_SCALE_MAX)
            {
              carrier |= scale << OR_DOMAIN_SCALE_SHIFT;
            }
          else
            {
              carrier |= OR_DOMAIN_SCALE_MAX << OR_DOMAIN_SCALE_SHIFT;
              extended_scale = d->scale;
            }
          /* handle all precisions the same way at the end */
          precision = d->precision;
          /*
           * Safe guard for floating precision caused by incorrect type setting
           */
          if (precision <= TP_FLOATING_PRECISION_VALUE)
            {
              precision = DB_MAX_NUMERIC_PRECISION;
            }
          break;

        case DB_TYPE_VARCHAR:
          has_collation = true;
        case DB_TYPE_VARBIT:
          carrier |= ((int) INTL_CODESET_UTF8) << OR_DOMAIN_CODSET_SHIFT;

          /*
           * Hack, if the precision is our special maximum/floating indicator,
           * store a zero in the precision field of the carrier.
           */
          if (d->precision != TP_FLOATING_PRECISION_VALUE)
            {
              precision = d->precision;
            }

          /*
           * Kludge, for temporary backward compatibility, treat varchar
           * types with the maximum precision as the -1 case.  See commentary
           * in or_packed_domain_size above.
           */
          if ((id == DB_TYPE_VARCHAR
               && d->precision == DB_MAX_VARCHAR_PRECISION)
              || (id == DB_TYPE_VARBIT && d->precision == DB_MAX_VARBIT_PRECISION))
            {
              precision = 0;
            }
          break;

        case DB_TYPE_SEQUENCE:
        case DB_TYPE_TABLE:
          /*
           * we need to recursively store the sub-domains following this one,
           * since sets can have empty domains we need a flag to indicate this.
           */
          if (d->setdomain != NULL)
            {
              carrier |= OR_DOMAIN_SET_DOMAIN_FLAG;
              has_subdomain = 1;
            }
          break;

        default:
          break;
        }

      /* handle the precision if this type wanted one */
      if (precision)
        {
          if (precision < OR_DOMAIN_PRECISION_MAX)
            {
              carrier |= precision << OR_DOMAIN_PRECISION_SHIFT;
            }
          else
            {
              carrier |= (unsigned int) OR_DOMAIN_PRECISION_MAX << OR_DOMAIN_PRECISION_SHIFT;
              extended_precision = precision;
            }
        }

      /* store the first word */
      rc = or_put_int (buf, carrier);
      if (rc != NO_ERROR)
        {
          return rc;
        }

      if (has_collation)
        {
          collation_storage = d->collation_id;
          rc = or_put_int (buf, collation_storage);
          if (rc != NO_ERROR)
            {
              return rc;
            }
        }

      /* do we require any extended precision words ? */
      if (extended_precision)
        {
          rc = or_put_int (buf, extended_precision);
          if (rc != NO_ERROR)
            {
              return rc;
            }
        }

      if (extended_scale)
        {
          rc = or_put_int (buf, extended_scale);
          if (rc != NO_ERROR)
            {
              return rc;
            }
        }

      /*
       * Recurse on the sub domains if necessary, note that we don't
       * pass the NULL bit down here because that applies only to the
       * top level domain.
       */
      if (has_subdomain)
        {
          rc = or_put_domain (buf, d->setdomain, 0);
          if (rc != NO_ERROR)
            {
              return rc;
            }
        }
    }
  return rc;
}


/*
 * unpack_domain_2 - read a TP_DOMAIN from a buffer
 *    return: TP_DOMAIN read
 *    buf(in): input buffer
 *    is_null(out): set 1 if NULL domain
 */
static TP_DOMAIN *
unpack_domain_2 (OR_BUF * buf, int *is_null)
{
  TP_DOMAIN *domain, *last, *d;
  unsigned int carrier, precision, scale, has_setdomain, collation_id, collation_storage;
  bool more, auto_precision, has_collation;
  DB_TYPE type;
  int index;
  int rc = NO_ERROR;

  domain = last = NULL;

  more = true;
  while (more)
    {

      carrier = or_get_int (buf, &rc);
      if (rc != NO_ERROR)
        {
          goto error;
        }

      type = (DB_TYPE) (carrier & OR_DOMAIN_TYPE_MASK);

      /* check for the special NULL bit */
      if (is_null != NULL)
        {
          *is_null = ((carrier & OR_DOMAIN_NULL_FLAG) != 0);
        }

      /* Hack, check for references to built-in domains. */
      if (type == DB_TYPE_NULL && (carrier & OR_DOMAIN_BUILTIN_FLAG))
        {
          index = (carrier & OR_DOMAIN_PRECISION_MASK) >> OR_DOMAIN_PRECISION_SHIFT;
          /*
           * Recall that the built-in domain indexes are 1 based rather
           * than zero based, must adjust prior to indexing the table.
           */
          if (index < 1)
            {
              goto error;
            }

          domain = tp_domain_resolve_default (index - 1);
          /* stop the loop */
          more = false;
        }
      else
        {
          /* unpack a real domain */

          more = (carrier & OR_DOMAIN_NEXT_FLAG) ? true : false;
          precision = 0;
          scale = 0;
          has_setdomain = 0;
          auto_precision = false;
          has_collation = false;

          switch (type)
            {
            case DB_TYPE_INTEGER:
            case DB_TYPE_BIGINT:
            case DB_TYPE_DOUBLE:
            case DB_TYPE_DATE:
            case DB_TYPE_TIME:
            case DB_TYPE_DATETIME:
              precision = tp_get_fixed_precision (type);
              break;

            case DB_TYPE_NUMERIC:
              precision = (carrier & OR_DOMAIN_PRECISION_MASK) >> OR_DOMAIN_PRECISION_SHIFT;
              scale = (carrier & OR_DOMAIN_SCALE_MASK) >> OR_DOMAIN_SCALE_SHIFT;
              break;

            case DB_TYPE_VARCHAR:
              has_collation = true;
            case DB_TYPE_VARBIT:
              precision = (carrier & OR_DOMAIN_PRECISION_MASK) >> OR_DOMAIN_PRECISION_SHIFT;

              if (precision == 0)
                {
                  precision = TP_FLOATING_PRECISION_VALUE;
                  auto_precision = true;

                  if (type == DB_TYPE_VARCHAR)
                    {
                      precision = DB_MAX_VARCHAR_PRECISION;
                    }
                  else if (type == DB_TYPE_VARBIT)
                    {
                      precision = DB_MAX_VARBIT_PRECISION;
                    }
                }
              break;

            case DB_TYPE_SEQUENCE:
            case DB_TYPE_TABLE:
              has_setdomain = carrier & OR_DOMAIN_SET_DOMAIN_FLAG;
              break;

            default:
              break;
            }

          if (has_collation)
            {
              collation_storage = or_get_int (buf, &rc);
              if (rc != NO_ERROR)
                {
                  goto error;
                }
              collation_id = collation_storage & OR_DOMAIN_COLLATION_MASK;
            }
          else
            {
              collation_id = 0;
            }

          /* do we have an extra precision word ? */
          if (precision == OR_DOMAIN_PRECISION_MAX && !auto_precision)
            {
              precision = or_get_int (buf, &rc);
            }

          if (rc != NO_ERROR)
            {
              goto error;
            }

          /* do we have an extra scale word ? */
          if (scale == OR_DOMAIN_SCALE_MAX)
            {
              scale = or_get_int (buf, &rc);
            }

          if (rc != NO_ERROR)
            {
              goto error;
            }

          /* start building a transient domain */
          d = tp_domain_construct (type, NULL, precision, scale, NULL);
          if (d == NULL)
            {
              goto error;
            }

          if (last == NULL)
            {
              domain = last = d;
            }
          else
            {
              last->next = d;
              last = last->next;
            }

          /* store the codset if we had one */
          d->collation_id = collation_id;

          /*
           * Recurse to get set sub-domains if there are any, note that
           * we don't pass the is_null flag down here since NULLness only
           * applies to the top level domain.
           */
          if (has_setdomain)
            {
              d->setdomain = unpack_domain_2 (buf, NULL);
              if (d->setdomain == NULL)
                {
                  goto error;
                }
            }
        }
    }

  return domain;

error:
  if (domain != NULL)
    {
      TP_DOMAIN *td, *next;
      for (td = domain, next = NULL; td != NULL; td = next)
        {
          next = td->next;
          tp_domain_free (td);
        }
    }
  return NULL;
}


/*
 * unpack_domain - unpack disk representation of domain
 *    return: TP_DOMAIN structure
 *    buf(in/out): or buffer
 *    is_null(out): OR_DOMAIN_NULL_FLAG was on th packed domain?
 * Note:
 *    OR_DOMAIN_NULL_FLAG will normally only be set if this domain was
 *    used as a tag for a packed value.
 */
static TP_DOMAIN *
unpack_domain (OR_BUF * buf, int *is_null)
{
  TP_DOMAIN *domain, *last, *dom;
  TP_DOMAIN *setdomain, *td, *next;
  DB_TYPE type;
  bool more;
  unsigned int carrier, index;
  unsigned int precision, scale, collation_id;
//  OID class_oid;
//  struct db_object *class_mop = NULL;
  int rc = NO_ERROR;
  unsigned int collation_storage;

  domain = last = dom = setdomain = NULL;
  precision = scale = 0;

  more = true;
  while (more)
    {
      carrier = or_get_int (buf, &rc);
      if (rc != NO_ERROR)
        {
          goto error;
        }

      type = (DB_TYPE) (carrier & OR_DOMAIN_TYPE_MASK);

      /* check for the special NULL bit */
      if (is_null != NULL)
        {
          *is_null = ((carrier & OR_DOMAIN_NULL_FLAG) != 0);
        }

      /* Hack, check for references to built-in domains. */
      if (type == DB_TYPE_NULL && (carrier & OR_DOMAIN_BUILTIN_FLAG))
        {
          index = (carrier & OR_DOMAIN_PRECISION_MASK) >> OR_DOMAIN_PRECISION_SHIFT;
          /* Recall that the builtin domain indexes are 1 based rather than zero
           * based,
           * must adjust prior to indexing the table.
           */
          domain = tp_domain_resolve_default (index - 1);
          if (domain == NULL)
            {
              goto error;
            }

          /* stop the loop */
          more = false;
        }
      else
        {
          /* unpack a real domain */
          more = (carrier & OR_DOMAIN_NEXT_FLAG) ? true : false;

          collation_id = 0;

          switch (type)         /* try to find */
            {
            case DB_TYPE_INTEGER:
            case DB_TYPE_BIGINT:
            case DB_TYPE_DOUBLE:
            case DB_TYPE_DATE:
            case DB_TYPE_TIME:
            case DB_TYPE_DATETIME:
              precision = tp_get_fixed_precision (type);

            case DB_TYPE_NULL:
#if 1                           /* TODO - trace */
              assert (false);
              goto error;
#else
              dom = tp_domain_resolve_default (type);
#endif
              break;

            case DB_TYPE_NUMERIC:
              /* get precision and scale */
              precision = (carrier & OR_DOMAIN_PRECISION_MASK) >> OR_DOMAIN_PRECISION_SHIFT;
              scale = (carrier & OR_DOMAIN_SCALE_MASK) >> OR_DOMAIN_SCALE_SHIFT;
              /* do we have an extra precision word ? */
              if (precision == OR_DOMAIN_PRECISION_MAX)
                {
                  precision = or_get_int (buf, &rc);
                  if (rc != NO_ERROR)
                    {
                      goto error;
                    }
                }
              /* do we have an extra scale word ? */
              if (scale == OR_DOMAIN_SCALE_MAX)
                {
                  scale = or_get_int (buf, &rc);
                  if (rc != NO_ERROR)
                    {
                      goto error;
                    }
                }
              dom = tp_domain_find_numeric (type, precision, scale);
              break;

            case DB_TYPE_VARCHAR:
              collation_storage = or_get_int (buf, &rc);
              if (rc != NO_ERROR)
                {
                  goto error;
                }
              collation_id = collation_storage & OR_DOMAIN_COLLATION_MASK;

            case DB_TYPE_VARBIT:
              precision = ((carrier & OR_DOMAIN_PRECISION_MASK) >> OR_DOMAIN_PRECISION_SHIFT);
              /* do we have an extra precision word ? */
              if (precision == OR_DOMAIN_PRECISION_MAX)
                {
                  precision = or_get_int (buf, &rc);
                  if (rc != NO_ERROR)
                    {
                      goto error;
                    }
                }
              if (precision == 0)
                {
                  /*
                   * Kludge, restore maximum precision for the types that
                   * aren't yet prepared for a -1.  This can be removed
                   * eventually, see commentary in the or_put_domain.
                   */
                  if (type == DB_TYPE_VARCHAR)
                    {
                      precision = DB_MAX_VARCHAR_PRECISION;
                    }
                  else if (type == DB_TYPE_VARBIT)
                    {
                      precision = DB_MAX_VARBIT_PRECISION;
                    }
                  else
                    {
                      precision = TP_FLOATING_PRECISION_VALUE;
                    }
                }
              dom = tp_domain_find_charbit (type, collation_id, precision);
              break;

            case DB_TYPE_OBJECT:
#if 1                           /* TODO - trace */
              assert (false);
              goto error;
#else
              OID_SET_NULL (&class_oid);
              class_mop = NULL;
              dom = tp_domain_find_object (type, &class_oid, class_mop);
#endif
              break;

            case DB_TYPE_SEQUENCE:
            case DB_TYPE_TABLE:
              if (carrier & OR_DOMAIN_SET_DOMAIN_FLAG)
                {
                  /* has setdomain */
                  setdomain = unpack_domain_2 (buf, NULL);
                  if (setdomain == NULL)
                    {
                      goto error;
                    }
                }
              else
                {
                  goto error;
                }

              dom = tp_domain_find_set (type, setdomain);
              if (dom)
                {
                  for (td = setdomain, next = NULL; td != NULL; td = next)
                    {
                      next = td->next;
                      tp_domain_free (td);
                    }
                }
              break;

            default:
              break;
            }

          if (dom == NULL)
            {
              /* not found. need to construct one */
              dom = tp_domain_construct (type, NULL, precision, scale, setdomain);
              if (dom == NULL)
                {
                  goto error;
                }

              switch (type)
                {
                case DB_TYPE_VARCHAR:
                  dom->collation_id = collation_id;
                case DB_TYPE_VARBIT:
                  break;
                case DB_TYPE_OBJECT:
#if 1                           /* TODO - trace */
                  assert (false);
                  goto error;
#else
                  COPY_OID (&dom->class_oid, &class_oid);
#if !defined (SERVER_MODE)
                  dom->class_mop = class_mop;
#endif /* !SERVER_MODE */
#endif
                  break;

                default:
                  break;
                }

              dom = tp_domain_cache (dom);
            }

          if (last == NULL)
            {
              domain = last = dom;
            }
          else
            {
              last->next = dom;
              last = last->next;
            }
        }
    }

  return domain;

error:
  if (domain != NULL)
    {
      for (td = domain, next = NULL; td != NULL; td = next)
        {
          next = td->next;
          tp_domain_free (td);
        }
    }
  return NULL;
}

/*
 * or_get_domain - unpacks a domain from a buffer and returns a cached domain.
 *    return: cached domain or NULL for error
 *    buf(in/out): or buffer
 *    caller_dom(in):
 *    is_null(out): OR_DOMAIN_NULL_FLAG was on in the packed domain?
 */
TP_DOMAIN *
or_get_domain (OR_BUF * buf, TP_DOMAIN * caller_dom, int *is_null)
{
  TP_DOMAIN *domain;

  if (caller_dom)
    {
      assert (caller_dom->is_cached);

      domain = unpack_domain_2 (buf, is_null);
      if (tp_domain_match (domain, caller_dom, TP_SET_MATCH))
        {
          tp_domain_free (domain);
          domain = caller_dom;
        }
      else if (domain != NULL && !domain->is_cached)
        {
          domain = tp_domain_cache (domain);
        }
    }
  else
    {
      domain = unpack_domain (buf, is_null);
      if (domain != NULL && !domain->is_cached)
        {
          domain = tp_domain_cache (domain);
        }
    }
  return domain;
}


/*
 * or_pack_domain - creates the packed "disk" representation of a domain
 *    return: advanced pointer
 *    ptr(out): output buffer
 *    domain(in): domain to pack
 *    is_null(in): use domain tags for "NULL" value
 *
 * Note:
 *    Alternate interface for or_put_domain, see that function
 *    for more information.
 */
char *
or_pack_domain (char *ptr, TP_DOMAIN * domain, int is_null)
{
  OR_BUF buf;
  int rc = 0;

  or_init (&buf, ptr, 0);
  rc = or_put_domain (&buf, domain, is_null);
  if (rc == NO_ERROR)
    {
      return buf.ptr;
    }
  else
    {
      return NULL;
    }
}

/*
 * or_unpack_domain - alternative interface to or_get_domain
 *    return: advanced buffer pointer
 *    ptr(in): pointer to buffer
 *    domain_ptr(out): pointer to domain
 *    is_null(out): use domain tags for "NULL" value
 */
char *
or_unpack_domain (char *ptr, struct tp_domain **domain_ptr, int *is_null)
{
  OR_BUF buf;
  TP_DOMAIN *domain;

  or_init (&buf, ptr, 0);

  domain = or_get_domain (&buf, NULL, is_null);
  if (domain_ptr != NULL)
    {
      *domain_ptr = domain;
    }

  return buf.ptr;
}

/*
 * or_put_sub_domain - put DB_TYPE_SUB field to buffer
 *    return: NO_ERROR or error code
 *    buf(in/out): or buffer
 * Note:
 *    This is a kludge until we have a more general mechanism for dealing
 *    with "substrutcure" domains.
 *    These are used in the disk representation of the class and indicate
 *    the presence of nested instances, similar in theory to the ADT concept.
 *    We've stored classes like this for some time, eventually this can
 *    be replaced by true ADT's when they come on-line but hopefully the
 *    representation will be identical.
 *    We use this function to avoid having to actually create a bunch of
 *    built-in domains for the meta classes though that wouldn't be all that
 *    hard to add to the tp_Domain array.
 */
int
or_put_sub_domain (OR_BUF * buf)
{
  unsigned int carrier;

  carrier = (DB_TYPE_SUB & OR_DOMAIN_TYPE_MASK);
  return (or_put_int (buf, carrier));
}

/*
 *  SET PACKING
 */
/*
 * or_packed_set_info - looks at the domain of the set and determines the
 * close to optimal storage configuration for it.
 *    return: void
 *    set_type(in): basic type of the set
 *    domain(in): full domain of the set (optional)
 *    include_domain(in): non-zero if the caller wants the domain in the set
 *    bound_bits(out): set to 1 if the set is homogeneous
 *    offset_table(out): set to the fixed width element size (-1 if variable)
 *    element_tags(out): set to 1 if bound bits required
 *    element_size(out): set to 1 if offset table is required
 *
 * Note:
 *    This looks at the domain of the set and determines the close to
 *    optimal storage configuration for it.  There is some room for
 *    interpretation here, and in fact, we can completely ignore the
 *    domain and store set in their most general representation all the
 *    time if there seems to be some problem dealing with compressed sets.
 *    This may not need to be a public function, it is only called by
 *    or_put_set and or_packed_set_size.
 */
void
or_packed_set_info (UNUSED_ARG DB_TYPE set_type,
                    TP_DOMAIN * domain,
                    UNUSED_ARG int include_domain,
                    int *bound_bits, int *offset_table, int *element_tags, int *element_size)
{
  TP_DOMAIN *element_domain;
  int homogeneous;


  /*
   * A set can be of fixed width only if the domain is fully specified and there
   * is only one fixed width data type in the set.
   * Note that for "attached" sets that may be fixed width, the domain must
   * have been assigned by now for us to determine this, if not, we punt
   * and assume its a variable width set.
   */

  /*
   * might only need bother with offset tables if this is an indexable
   * sequence ?
   */
  homogeneous = 0;
  *element_tags = 1;
  *element_size = -1;
  *bound_bits = 0;
  *offset_table = 0;

  if (domain != NULL)
    {
      element_domain = domain->setdomain;
      if (element_domain != NULL && element_domain->next == NULL)
        {
          /* set can contain only one type of thing */
          homogeneous = 1;
          /* returns -1 if this is a variable width thing */
          *element_size = tp_domain_disk_size (element_domain);
        }
    }

  if (homogeneous)
    {
      if (*element_size >= 0)
        {
          *bound_bits = 1;
        }
      else
        {
          *offset_table = 1;
        }
    }
  else
    {
      *offset_table = 1;
    }

  /*
   * Determine if we need to tag each value with its domain.
   * Normally, one would tag the elements if the domain is being excluded
   * from the set, but we'll allow it and assume that it will be passed
   * to the or_get_set function.
   */
  *element_tags = !homogeneous; /* || !include_domain */

  /*
   * If we have to have element tags, then don't bother with a bound
   * bit array.
   */
  if (*element_tags)
    {
      *bound_bits = 0;
    }
}

/*
 * or_put_set_header - write a set header containing the indicated information.
 *    return: NO_ERROR or return code
 *    buf(in/out): or buffer
 *    set_type(in): basic type of the set
 *    size(in): number of elements in the set
 *    domain(in): non-zero if a domain will be packed
 *    bound_bits(in): non-zero if a bound bit vector will be packed
 *    offset_table(in): non-zero if an offset table will be packed
 *    element_tags(in): non-zero if elements tags will be included
 *    common_sub_header(in): non-zero if substructure tags will be included
 * Note:
 *    This hides basically just hides the implementation of the header
 *    word and flag constants so we can control who gets to see this.
 *    Common sub_header is used only for class objects currently.
 *
 */
int
or_put_set_header (OR_BUF * buf, DB_TYPE set_type, int size,
                   int domain, int bound_bits, int offset_table, int element_tags, int common_sub_header)
{
  unsigned int header;
  int rc = NO_ERROR;

  header = set_type & 0xFF;

  if (offset_table)
    {
      header |= OR_SET_VARIABLE_BIT;
    }
  else if (bound_bits)
    {
      header |= OR_SET_BOUND_BIT;
    }

  if (domain)
    {
      header |= OR_SET_DOMAIN_BIT;
    }
  if (element_tags)
    {
      header |= OR_SET_TAG_BIT;
    }
  if (common_sub_header)
    {
      header |= OR_SET_COMMON_SUB_BIT;
    }
  rc = or_put_int (buf, header);

  if (rc == NO_ERROR)
    {
      rc = or_put_int (buf, size);
    }

  return rc;
}

/*
 * or_get_set_header - get set header from buffer
 *    return: ER_SUCCSSS or error code
 *    buf(in/out): or buffer
 *    set_type(out): set to the basic set type
 *    size(out): set to the element count
 *    domain(out): set non-zero if there will be a domain
 *    bound_bits(out): set non-zero if there will be bound bits
 *    offset_table(out): set non-zero if there will be an offset table
 *    element_tags(out): set non-zero if there will be element tags
 *    common_sub(out): set non-zero if there will be substructure tags
 */
int
or_get_set_header (OR_BUF * buf, DB_TYPE * set_type, int *size,
                   int *domain, int *bound_bits, int *offset_table, int *element_tags, int *common_sub)
{
  unsigned int header;
  int rc = NO_ERROR;

  header = or_get_int (buf, &rc);
  if (rc == NO_ERROR)
    {
      *set_type = (DB_TYPE) (header & OR_SET_TYPE_MASK);
      *domain = ((header & OR_SET_DOMAIN_BIT) != 0);
      *bound_bits = ((header & OR_SET_BOUND_BIT) != 0);
      *offset_table = ((header & OR_SET_VARIABLE_BIT) != 0);
      *element_tags = ((header & OR_SET_TAG_BIT) != 0);
      if (common_sub != NULL)
        {
          *common_sub = ((header & OR_SET_COMMON_SUB_BIT) != 0);
        }
      *size = or_get_int (buf, &rc);
    }
  return rc;
}

/*
 * or_skip_set_header - skip over the set header
 *    return: number of elements in set
 *    buf(in/out): or buffer
 *
 * Note:
 *    Used only by the class loader since it knows what the type
 *    of the set is.
 */
int
or_skip_set_header (OR_BUF * buf)
{
  DB_TYPE set_type;
  int count, length, rc = NO_ERROR;
  int domain, bound_bits, offset_table, element_tags, sub_header;

  or_get_set_header (buf, &set_type, &count, &domain, &bound_bits, &offset_table, &element_tags, &sub_header);

  if (offset_table)
    {
      or_advance (buf, OR_VAR_TABLE_SIZE (count));
    }

  else if (bound_bits)
    {
      or_advance (buf, OR_BOUND_BIT_BYTES (count));
    }

  if (domain)
    {
      length = or_get_int (buf, &rc);
      or_advance (buf, length);
    }

  if (sub_header)
    {
      or_advance (buf, OR_SUB_HEADER_SIZE);
    }

  return count;
}

/*
 * or_packed_set_length - Calculates the disk size of a set
 *    return: disk length of the set
 *    set(in): pointer to an internal set object (not a set reference)
 *    include_domain(in): non-zero if the caller wants the domain in the set
 *
 *    Note:
 *    the length returned here will match the representation
 *    created by or_put_set() only.  There are some other set packers
 *    floating around that represent sets as arrays of packed DB_VALUES,
 *    these are not necessarily the same size.
 *
 *    The include_domain flag is set if the set is to be packed with a full
 *    domain description.  This is normally off when the set is inside
 *    an object since the domain can be determined from the class.
 *    When "free" sets are packed in list files or as elements of nested
 *    sets, the domain should be included.
 *    This distinction may be somewhat difficult to make in the presence
 *    of nested sets.  Consider instead leaving the domain specified but
 *    don't pack the class OIDs of object domains.
 */
int
or_packed_set_length (SETOBJ * set, int include_domain)
{
  DB_VALUE *value = NULL;
  int len, element_size, bound_bits, offset_table, element_tags, i, bits;
  int set_size;
  TP_DOMAIN *set_domain;
  DB_TYPE set_type;
  int error;

  len = 0;
  if (set == NULL)
    {
      return 0;
    }

  set_size = setobj_size (set);
  set_type = setobj_type (set);
  set_domain = setobj_domain (set);

  /* Determine storage characteristics based on the domain */
  or_packed_set_info (set_type, set_domain, include_domain, &bound_bits, &offset_table, &element_tags, &element_size);

  len = OR_SET_HEADER_SIZE;

  if (offset_table)
    {
      len += OR_VAR_TABLE_SIZE (set_size);
    }
  else if (bound_bits)
    {
      len += OR_BOUND_BIT_BYTES (set_size);
    }

  if (set_domain != NULL && include_domain)
    {
      len += OR_INT_SIZE;
      len += or_packed_domain_size (set_domain);
    }

  /*
   * If we have a non-tagged fixed width set, can calculate the size without
   * mapping over the values.
   */
  if (bound_bits)
    {
      len += element_size * set_size;
    }
  else
    {

      for (i = 0; i < set_size; i++)
        {
          error = setobj_get_element_ptr (set, i, &value);

          /* Second argument indicates whether to "collapse_null" values into
           * nothing.
           *   - can do this only if there is an offset table.
           * Third argument indicates whether or not to include the domain which
           *   - we do if the values are tagged.
           * Fourth argument indicates the desire to pack class OIDs which we
           * never do since these are tag domains.
           */
          len += or_packed_value_size (value, offset_table, element_tags);
          if (offset_table)
            {
              bits = len & 3;
              if (bits)
                {
                  len += (4 - bits);
                }
            }
        }
    }

  /* always pad out a packed set to a word boundary */
  bits = len & 3;
  if (bits)
    {
      len += (4 - bits);
    }

  return len;
}

/*
 * or_put_set - primary function for building the disk representation
 * of a set.
 *    return: void
 *    buf(in/out): or buffer
 *    set(in): set object to encode
 *    include_domain(in): non-zero to store full set domain too
 */
void
or_put_set (OR_BUF * buf, SETOBJ * set, int include_domain)
{
  DB_VALUE *value = NULL;
  unsigned int bound_word;
  int element_tags, element_size, bound_bits, offset_table;
  char *set_start, *element_start, *offset_ptr, *bound_ptr;
  int i, offset, bit = 0, len, is_null, length, bits;
  TP_DOMAIN *set_domain;
  DB_TYPE set_type;
  int set_size;
  int error;

  if (set == NULL)
    {
      return;
    }

  /* only pay attention to this if we actually have a domain to store */
  set_domain = setobj_domain (set);
  set_type = setobj_type (set);
  set_size = setobj_size (set);

  if (set_domain == NULL)
    {
      include_domain = 0;
    }

  /* determine storage characteristics based on the domain */
  set_start = buf->ptr;
  or_packed_set_info (set_type, set_domain, include_domain, &bound_bits, &offset_table, &element_tags, &element_size);

  or_put_set_header (buf,
                     set_domain ? TP_DOMAIN_TYPE (set_domain) : set_type,
                     set_size, include_domain, bound_bits, offset_table, element_tags, 0);


  /* reserve space for the offset table or bound bit vector if necessary */
  offset_ptr = NULL;
  bound_ptr = NULL;
  bound_word = 0;
  if (set_size)
    {
      if (offset_table)
        {
          offset_ptr = buf->ptr;
          len = OR_VAR_TABLE_SIZE (set_size);
          or_advance (buf, len);
        }
      else if (bound_bits)
        {
          bound_ptr = buf->ptr;
          len = OR_BOUND_BIT_BYTES (set_size);
          or_advance (buf, len);
        }
    }

  /* write the domain if necessary, don't include the class OID */
  if (include_domain)
    {
      or_put_int (buf, or_packed_domain_size (set_domain));
      or_put_domain (buf, set_domain, 0);
    }

  /* stop if we don't have any elements */
  if (set_size)
    {

      /* calculate the offset to the first value (in case we're building an
         offset table) */
      offset = (int) (buf->ptr - set_start);

      /* iterate over the values */
      for (i = 0; i < set_size; i++)
        {
          error = setobj_get_element_ptr (set, i, &value);

          /*
           * make an entry in the offset table or bound bit array if we
           * have them
           */
          is_null = 0;
          if (offset_ptr != NULL)
            {
              /* offset table entry */
              OR_PUT_OFFSET (offset_ptr, offset);
              offset_ptr += BIG_VAR_OFFSET_SIZE;
            }
          else if (bound_ptr != NULL)
            {
              bit = i & 0x1F;
              if (value != NULL && DB_VALUE_TYPE (value) != DB_TYPE_NULL)
                {
                  bound_word |= 1L << bit;
                }
              else
                {
                  is_null = 1;
                }
              if (bit == 0x1F)
                {
                  OR_PUT_INT (bound_ptr, bound_word);
                  bound_ptr += OR_INT_SIZE;
                  bound_word = 0;
                }
            }

          /*
           * Write the value.  Be careful with NULLs in fixed width sets, need
           * to leave space.
           */
          element_start = buf->ptr;

          if (bound_ptr != NULL && is_null)
            {
              /*
               * Could just use or_advance here but lets be nice and
               * zero out the space for debugging.
               */
              or_pad (buf, element_size);
            }
          else
            {
              /* Third argument indicates whether to "collapse_null" values
               * into nothing.
               *   - can do this only if there is an offset table.
               * Fourth argument indicates whether or not to include the
               * domain which we do if the values are tagged.
               */
              or_put_value (buf, value, offset_table, element_tags);
            }

          if (offset_table)
            {
              length = CAST_BUFLEN (buf->ptr - element_start);
              bits = length & 3;
              if (bits)
                {
                  or_pad (buf, 4 - bits);
                }
            }

          offset += (int) (buf->ptr - element_start);
        }

      /* store the ending offset in the table if we're using one */
      if (offset_ptr != NULL)
        {
          OR_PUT_OFFSET (offset_ptr, offset);
        }

      if (bound_ptr != NULL && bit != 0x1f)
        {
          OR_PUT_INT (bound_ptr, bound_word);
        }
    }

  /* always pad out a packed set to a word boundary */
  length = CAST_BUFLEN (buf->ptr - set_start);
  bits = length & 3;
  if (bits)
    {
      or_pad (buf, 4 - bits);
    }
}

/*
 * or_get_set - eads the stored representation of a set and builds the
 *    corresponding memory represenation.
 *    return: internal set object
 *    buf(in/out): or buffer
 *    domain(in): expected domain (optional)
 * Note:
 *    The domain argument is required only if the domain set was packed
 *    explicitly without a domain and the elements are not tagged.  In that
 *    case, the supplied domain must be used to interpret the element
 *    format.  It better be right.  This is really only used for the
 *    stored values of attributes since we can always get the correct
 *    domain by looking in the catalog.
 */
SETOBJ *
or_get_set (OR_BUF * buf, TP_DOMAIN * domain)
{
  SETOBJ *set;
  DB_VALUE value;
  TP_DOMAIN *element_domain;
  DB_TYPE set_type;
  int size, fixed_element_size, element_size, offset, offset2, bit, i;
  int length, bits;
  unsigned int bound_word;
  char *offset_ptr, *bound_ptr;
  char *set_start;
  int has_domain, bound_bits, offset_table, element_tags;
  TP_DOMAIN *set_domain;
  int rc = NO_ERROR;

  set_start = buf->ptr;

  /* read the set header and decompose the various flags */
  or_get_set_header (buf, &set_type, &size, &has_domain, &bound_bits, &offset_table, &element_tags, NULL);

  set = setobj_create (set_type, size);
  if (set == NULL)
    {
      or_abort (buf);
      return NULL;
    }

  /*
   * If a domain was supplied, stick it in the set, probably should do a
   * sanity check in this and the doamin stored in the set.  The domain
   * MUST be passed if the set was packed with the "include_domain" domain
   * flag at zero.
   */
  if (domain != NULL)
    {
      setobj_put_domain (set, domain);
    }

  /* prepare for an offset table or bound bit array */
  offset_ptr = NULL;
  bound_ptr = NULL;
  offset = 0;
  bound_word = 0;
  if (offset_table)
    {
      offset_ptr = buf->ptr;
      or_advance (buf, OR_VAR_TABLE_SIZE (size));
    }
  else if (bound_bits)
    {
      bound_ptr = buf->ptr;
      or_advance (buf, OR_BOUND_BIT_BYTES (size));
    }

  if (has_domain)
    {
      (void) or_get_int (buf, &rc);     /* skip the domain size */
      /* the domain returned here will be cached */
      setobj_put_domain (set, or_get_domain (buf, domain, NULL));
      /* If this is stored and the caller has supplied one as an argument to
       * this function, they should be the same.  Might want to check this here.
       */
    }

  /*
   * Calculate the length of the fixed width elements if that's what we have.
   * This looks like it should be a little utilitiy function.
   */
  element_domain = NULL;
  fixed_element_size = -1;
  set_domain = setobj_domain (set);

  if (set_domain != NULL && set_domain->setdomain != NULL && set_domain->setdomain->next == NULL)
    {
      /* we only have one possible element domain */
      fixed_element_size = tp_domain_disk_size (set_domain->setdomain);
      element_domain = set_domain->setdomain;
    }

  if (size)
    {
      /* read the first offset or bound word */
      if (offset_table)
        {
          offset = OR_GET_OFFSET (offset_ptr);
          offset_ptr += BIG_VAR_OFFSET_SIZE;
        }
      else if (bound_bits)
        {
          bound_word = OR_GET_INT (bound_ptr);
        }

      /* loop over each element */
      for (i = 0; i < size; i++)
        {

          /* determine the length of the element if there is an offset table */
          element_size = -1;
          if (offset_ptr != NULL)
            {
              offset2 = OR_GET_OFFSET (offset_ptr);
              offset_ptr += BIG_VAR_OFFSET_SIZE;
              element_size = offset2 - offset;
              offset = offset2;
            }
          else if (bound_ptr != NULL)
            {
              element_size = fixed_element_size;        /* this must be fixed width! */
              bit = i & 0x1F;
              if ((bound_word & (1L << bit)) == 0)
                {
                  element_size = 0;     /* its NULL */
                }
              if (bit == 0x1F)
                {
                  bound_ptr += OR_INT_SIZE;
                  bound_word = OR_GET_INT (bound_ptr);
                }
            }

          /*
             8 element_size will now be 0 if NULL, the true size, or -1 if
             * variable or unknown.
           */

          /* Read the element. */
          if (element_size == 0)
            {
              /*
               * we have to initlaize the domain too, since a set can
               * have several possible domains, just pick the first one.
               * Actually,for wildcard sets, we won't have a domain to select.
               * Since I guess the NULL "domain" can logically be a part of all
               * sets, initialize the domain for NULL.
               */
              db_value_domain_init (&value, DB_TYPE_NULL, DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
              db_make_null (&value);
              /*
               * if this is a fixed width element array, skip over the null
               * data
               */
              if (bound_ptr != NULL)
                {
                  or_advance (buf, fixed_element_size);
                }
            }
          else
            {
              /*
               * read a packed value, pass the domain only if the
               * values are not tagged already tagged.
               */
              if (element_tags)
                {
                  or_get_value (buf, &value, NULL, element_size, true);
                }
              else
                {
                  or_get_value (buf, &value, element_domain, element_size, true);
                }
            }

          /*
           * This setobj interface function passes "ownership" of the memory
           * of value to the set. value need not be cleared after this call,
           * as its internal memory pointers are copied directly to the set
           * This function should only be used in construction of internal
           * setobj structure from temporary DB_VALUE's.
           */
          if (setobj_put_value (set, i, &value) != NO_ERROR)
            {
              /* PR9043 if value not added to set, clear it */
              pr_clear_value (&value);
            }
        }
    }

  /* sets are always paded, consume the padding */
  length = (int) (buf->ptr - set_start);
  bits = length & 3;
  if (bits)
    {
      or_advance (buf, 4 - bits);
    }

  return set;
}

/*
 * or_disk_set_size - determine set length.
 *    return: set length
 *    buf(in/out): or buffer
 *    set_domain(in/out):
 *    set_type(out): set type
 * Note:
 *    This routine will leave the OR_BUF unaltered (pointing to the beginning
 *    of the set.
 */
int
or_disk_set_size (OR_BUF * buf, TP_DOMAIN * set_domain, DB_TYPE * set_type)
{
  TP_DOMAIN *element_domain;
  DB_TYPE disk_set_type;
  int size, fixed_element_size, element_size, offset, offset2, bit;
  int length, bits, i, total_length;
  unsigned int bound_word;
  char *offset_ptr, *bound_ptr;
  char *set_start;
  int has_domain, bound_bits, offset_table, element_tags;
  int rc = NO_ERROR;

  total_length = 0;
  set_start = buf->ptr;

  /* read the set header and decompose the various flags */
  or_get_set_header (buf, &disk_set_type, &size, &has_domain, &bound_bits, &offset_table, &element_tags, NULL);

  if (set_type)
    {
      *set_type = disk_set_type;
    }

  /* prepare for an offset table or bound bit array */
  offset_ptr = NULL;
  bound_ptr = NULL;
  offset = 0;
  bound_word = 0;

  if (offset_table)
    {
      offset_ptr = buf->ptr;
      or_advance (buf, OR_VAR_TABLE_SIZE (size));
    }
  else if (bound_bits)
    {
      bound_ptr = buf->ptr;
      or_advance (buf, OR_BOUND_BIT_BYTES (size));
    }

  if (has_domain)
    {
      (void) or_get_int (buf, &rc);     /* skip the domain size */
      /* we have to unpack the domain */
      set_domain = or_get_domain (buf, set_domain, NULL);
    }

  /*
   * Calculate the length of the fixed width elements if that's what we have.
   * This looks like it should be a little utilitiy function.
   */
  element_domain = NULL;
  fixed_element_size = -1;
  if (set_domain != NULL && set_domain->setdomain != NULL && set_domain->setdomain->next == NULL)
    {
      /* we only have one possible element domain */
      fixed_element_size = tp_domain_disk_size (set_domain->setdomain);
      element_domain = set_domain->setdomain;
    }

  if (size)
    {
      /* read the first offset or bound word */
      if (offset_table)
        {
          offset = OR_GET_OFFSET (offset_ptr);
          offset_ptr += BIG_VAR_OFFSET_SIZE;
        }
      else if (bound_bits)
        {
          bound_word = OR_GET_INT (bound_ptr);
        }

      /* loop over each element */
      for (i = 0; i < size; i++)
        {
          /* determine the length of the element if there is an offset table */
          element_size = -1;
          if (offset_ptr != NULL)
            {
              offset2 = OR_GET_OFFSET (offset_ptr);
              offset_ptr += BIG_VAR_OFFSET_SIZE;
              element_size = offset2 - offset;
              offset = offset2;
            }
          else if (bound_ptr != NULL)
            {
              element_size = fixed_element_size;        /* this must be fixed width! */
              bit = i & 0x1F;
              if ((bound_word & (1L << bit)) == 0)
                {
                  element_size = 0;     /* its NULL */
                }

              if (bit == 0x1F)
                {
                  bound_ptr += OR_INT_SIZE;
                  bound_word = OR_GET_INT (bound_ptr);
                }
            }

          /*
           * element_size will now be 0 if NULL, the true size, or -1 if
           * variable or unknown.
           */

          /*
           * Skip the element, we may have to actually unpack the element
           * to do this (if the size is variable), but no storage should be
           * allocated.
           */
          if (element_size == 0)
            {
              /*
               * if this is a fixed width element array, skip over the null
               * data
               */
              if (bound_ptr != NULL)
                {
                  or_advance (buf, fixed_element_size);
                }
            }
          else if (element_size != -1)
            {
              /* in this case we can simply skip the element */
              or_advance (buf, element_size);
            }
          else
            {
              /* skip a packed value, pass the domain only if the
               * values are not already tagged.  The NULL db_value tells
               * the routine to skip the value rather than actually unpack
               * it into a db_value container.
               */
              if (element_tags)
                {
                  or_get_value (buf, NULL, NULL, element_size, true);
                }
              else
                {
                  or_get_value (buf, NULL, element_domain, element_size, true);
                }
            }
        }
    }

  /* sets are always paded, consume the padding */
  length = (int) (buf->ptr - set_start);
  bits = length & 3;
  if (bits)
    {
      or_advance (buf, 4 - bits);
    }

  total_length = (int) (buf->ptr - set_start);

  /* reset the OR_BUF so that it looks like we didn't do anything */
  buf->ptr = set_start;

  return total_length;
}

/*
 * VALUE PACKING
 */

/*
 * or_packed_value_size - calculating the size of the packed representation of
 * a value.
 *    return: packed size in bytes
 *    value(in): pointer to value
 *    collapse_null(in): non-zero to "collapse" null values
 *    include_domain(in): non-zero to include a domain tag
 */
int
or_packed_value_size (const DB_VALUE * value, int collapse_null, int include_domain)
{
  PR_TYPE *type;
  TP_DOMAIN *domain;
  int size = 0, bits;
  DB_TYPE dbval_type;

  if (value == NULL)
    {
      return 0;
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (value);
  type = PR_TYPE_FROM_ID (dbval_type);

  if (type == NULL)
    {
      return 0;
    }

  /* If the value is NULL, either pack nothing or pack the domain with the
   * special null flag enabled.
   */
  if (DB_VALUE_TYPE (value) == DB_TYPE_NULL)
    {
      if (!collapse_null || include_domain)
        {
          domain = tp_domain_resolve_value (value);
          if (domain != NULL)
            {
              size = or_packed_domain_size (domain);
            }
          else
            {
              size = or_packed_domain_size (&tp_Null_domain);
            }
        }
    }
  else
    {
      if (include_domain)
        {
          domain = tp_domain_resolve_value (value);
          if (domain != NULL)
            {
              size = or_packed_domain_size (domain);
            }
          else
            {
              /* shouldn't get here ! */
              size = or_packed_domain_size (&tp_Null_domain);
              return size;
            }
        }
      if (type->data_lengthval == NULL)
        {
          size += type->disksize;
        }
      else
        {
          size += (*(type->data_lengthval)) (value, 1);
        }
    }

  /* Values must as a unit be aligned to a word boundary.  We can't do this
   * inside the writeval function becaue that may be used to place data inside
   * disk structures that don't have alignment requirements.
   */
  if (include_domain)
    {
      bits = size & 3;
      if (bits)
        {
          size += (4 - bits);
        }
    }

  return size;
}


/*
 * or_put_value - pack a value
 *    return: error on overflow
 *    buf(out): packing buffer
 *    value(in): value to ponder
 *    collapse_null(in): non-zero to "collapse" null values
 *    include_domain(in): non-zero to include a domain tag
 */
int
or_put_value (OR_BUF * buf, const DB_VALUE * value, int collapse_null, int include_domain)
{
  PR_TYPE *type;
  TP_DOMAIN *domain;
  char *start, length, bits;
  int rc = NO_ERROR;
  DB_TYPE dbval_type;

  if (value == NULL)
    {
      return ER_FAILED;
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (value);
  type = PR_TYPE_FROM_ID (dbval_type);

  if (type == NULL)
    {
      return ER_FAILED;
    }

  start = buf->ptr;

  if (DB_VALUE_TYPE (value) == DB_TYPE_NULL)
    {
      if (!collapse_null || include_domain)
        {
          domain = tp_domain_resolve_value (value);
          if (domain != NULL)
            {
              rc = or_put_domain (buf, domain, 1);
            }
          else
            {
              /* shouldn't get here */
              rc = or_put_domain (buf, &tp_Null_domain, 1);
            }
        }
    }
  else
    {
      if (include_domain)
        {
          domain = tp_domain_resolve_value (value);
          if (domain != NULL)
            {
              rc = or_put_domain (buf, domain, 0);
            }
          else
            {
              /* shouldn't get here */
              assert (false);

              rc = or_put_domain (buf, &tp_Null_domain, 1);
              return NO_ERROR;
            }
        }
      /* probably should blow off writing the value if we couldn't determine the
       * domain ? */
      if (rc == NO_ERROR)
        {
          rc = (*(type->data_writeval)) (buf, value);
        }
    }

  /*
   * Values must as a unit be aligned to a word boundary.  We can't do this
   * inside the writeval function becaue that may be used to place data inside
   * disk structures that don't have alignment requirements.
   */
  if (rc == NO_ERROR)
    {
      if (include_domain)
        {
          length = (int) (buf->ptr - start);
          bits = length & 3;
          if (bits)
            {
              rc = or_pad (buf, 4 - bits);
            }
        }
    }
  return rc;
}


/*
 * or_get_value - extracts a packed DB_VALUE
 *    return: none
 *    buf(in/out): packing buffer
 *    value(out): value to ponder
 *    domain(out): domain to use (optional, only if the value is not tagged)
 *    expected(out): expected size of the value (optional, can be -1)
 *    copy(in):
 * Note:
 *    This extracts a packed DB_VALUE and whetever it contains.
 *    If the value is tagged with its own domain, the domain argument to
 *    this function must be NULL.  If the value is not tagged, the
 *    domain argument must be specified.
 *
 *    The expected size is probably not necessary but its been passed around
 *    for so long, I'm reluctant to yank it right now.  It can be -1 if
 *    we don't know the size but in that case we must be able to determine
 *    the packed size by looking at the domain or by the looking
 *    at the value header.
 *
 *    The value can be NULL, in which case, we will simply advance the
 *    OR_BUF past the current value.
 *
 */
int
or_get_value (OR_BUF * buf, DB_VALUE * value, TP_DOMAIN * domain, int expected, bool copy)
{
  char *start;
  int is_null, total, pad;
  int rc = NO_ERROR;

  is_null = 0;
  start = buf->ptr;

  /*
   * Always make sure this is properly initialized.
   * If the domain is given here, we could use that for further initialization ?
   */
  if (value)
    {
      db_make_null (value);
    }

  /* If size is zero, this must have been a "collapsed" NULL value. */
  if (expected == 0)
    {
      return NO_ERROR;
    }

  /*
   * If a domain was supplied, use it to decode the value, otherwise we
   * assume that the values must be tagged.
   */
  if (domain == NULL)
    {
      domain = or_get_domain (buf, NULL, &is_null);
      if (expected >= 0)
        {
          /* reduce the expected size by the amount consumed with the domain tag */
          expected -= CAST_BUFLEN (buf->ptr - start);
          start = buf->ptr;
        }
    }

  if (domain == NULL)
    {
      /* problems decoding the domain */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      or_abort (buf);
      return ER_FAILED;
    }
  else
    {
      if (value)
        {
          tp_init_value_domain (domain, value);
        }

      if (is_null && value)
        {
          /* this was a tagged NULL value, restore the domain but set the null flag */
          db_value_put_null (value);
          if (TP_IS_CHAR_TYPE (TP_DOMAIN_TYPE (domain)))
            {
              db_string_put_cs_and_collation (value, TP_DOMAIN_COLLATION (domain));
            }
        }
      else
        {
          if (value)
            {
              (*(domain->type->data_readval)) (buf, value, domain, expected, copy);
            }
          else
            {
              /* the NULL value, will cause readval to skip the value */
              (*(domain->type->data_readval)) (buf, NULL, domain, expected, false);
            }

          if (rc != NO_ERROR)
            {
              return rc;
            }
        }
    }

  /* Consume any padding bytes that may be left over.
   * If the expected size was given, use that to determine the amount of padding,
   * otherwise we'll have to assume that we just need to be brought up to
   * a word boundary.
   */
  total = (int) (buf->ptr - start);
  pad = 0;
  if (expected > 0)
    {
      pad = expected - total;
    }
  else if (expected < 0)
    {
      if (total & 3)
        pad = 4 - (total & 3);
    }
  if (pad > 0)
    {
      rc = or_advance (buf, pad);
    }
  return rc;
}


/*
 * LIST ID PACKING
 */

/*
 * or_pack_listid - packs a QFILE_LIST_ID descriptor
 *    return: advanced buffer pointer
 *    ptr(out): starting pointer
 *    listid_ptr(in): QFILE_LIST_ID pointer
 * Note:
 *    This packs a QFILE_LIST_ID descriptor.  The arguments are passed as void*
 *    so we can avoid unfortunate circular dependencies between query_list.h
 *    and or.h.  query_list.h is included at the top of this file so we have
 *    the information necessary for casting.
 *    The QFILE_LIST_ID doesn't have an OR_PUT macro because it is significantly
 *    more complex than the other types and may be of variable size.
 */
char *
or_pack_listid (char *ptr, const void *listid_ptr)
{
  const QFILE_LIST_ID *listid;
  int i;

  listid = (const QFILE_LIST_ID *) listid_ptr;

  OR_PUT_PTR (ptr, listid->query_id);
  ptr += OR_PTR_SIZE;
  OR_PUT_PTR (ptr, listid->tfile_vfid);
  ptr += OR_PTR_SIZE;

  OR_PUT_INT (ptr, listid->tuple_cnt);
  ptr += OR_INT_SIZE;

  OR_PUT_INT (ptr, listid->page_cnt);
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, listid->first_vpid.pageid);
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, listid->first_vpid.volid);
  ptr += OR_INT_SIZE;

  OR_PUT_INT (ptr, listid->last_vpid.pageid);
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, listid->last_vpid.volid);
  ptr += OR_INT_SIZE;

  OR_PUT_INT (ptr, listid->last_offset);
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, listid->lasttpl_len);
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, listid->type_list.type_cnt);
  ptr += OR_INT_SIZE;

  for (i = 0; i < listid->type_list.type_cnt; i++)
    {
      /* do we want to pack the class oids?? */
      ptr = or_pack_domain (ptr, listid->type_list.domp[i], 0);
    }

  return ptr;
}

#if defined(ENABLE_UNUSED_FUNCTION)
char *
or_unpack_recdes (char *buf, RECDES ** recdes)
{
  RECDES *tmp_recdes = NULL;
  int length;

  if (buf == NULL)
    {
      return NULL;
    }

  buf = or_unpack_int (buf, &length);
  tmp_recdes = (RECDES *) malloc (sizeof (RECDES) + length);
  if (tmp_recdes == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (RECDES) + length);
      return NULL;
    }

  tmp_recdes->area_size = length;
  tmp_recdes->length = length;
  tmp_recdes->data = ((char *) tmp_recdes) + sizeof (RECDES);

  buf = or_unpack_short (buf, &tmp_recdes->type);
  buf = or_unpack_stream (buf, tmp_recdes->data, length);

  *recdes = tmp_recdes;
  return buf;
}
#endif

/*
 * or_unpack_listid - This unpacks a QFILE_LIST_ID descriptor from a buffer.
 *    return: advanced buffer pointer
 *    ptr(in): starting pointer
 *    listid_ptr(out):
 * Note:
 *    This unpacks a QFILE_LIST_ID descriptor from a buffer.
 *    Kludge, the arguments are passed in as void* so we can avoid
 *    unfortunate circular dependencies between query_list.h and or.h
 *    query_list.h is included at the top of this file so we have the
 *    information necesary for casting.
 */
char *
or_unpack_listid (char *ptr, void **listid_ptr)
{
  QFILE_LIST_ID *listid;
  int count, i;

  /*
   * tuple_cnt 4, vfid.fileid 4, vfid.volid 2, attr_list.oid_flg 2,
   * attr_list.attr_cnt 4, attr_list.attr_id 4 * n
   */
  listid = (QFILE_LIST_ID *) db_private_alloc (NULL, sizeof (QFILE_LIST_ID));
  if (listid == NULL)
    {
      goto error;
    }
  QFILE_CLEAR_LIST_ID (listid);

  listid->query_id = OR_GET_PTR (ptr);
  ptr += OR_PTR_SIZE;

  listid->tfile_vfid = (struct qmgr_temp_file *) OR_GET_PTR (ptr);
  ptr += OR_PTR_SIZE;

  listid->tuple_cnt = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->page_cnt = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->first_vpid.pageid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  listid->first_vpid.volid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->last_vpid.pageid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  listid->last_vpid.volid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->last_offset = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  listid->lasttpl_len = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  count = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->type_list.type_cnt = count;
  listid->type_list.domp = NULL;

  if (count < 0)
    {
      goto error;
    }

  if (count > 0)
    {
      listid->type_list.domp = (TP_DOMAIN **) malloc (sizeof (TP_DOMAIN *) * count);

      if (listid->type_list.domp == NULL)
        {
          goto error;
        }
    }

  for (i = 0; i < count; i++)
    {
      ptr = or_unpack_domain (ptr, &listid->type_list.domp[i], NULL);
    }

  *listid_ptr = (void *) listid;
  return ptr;

error:
  if (listid)
    {
      db_private_free_and_init (NULL, listid);
    }
  return NULL;
}

/*
 * or_unpack_unbound_listid - This unpacks a QFILE_LIST_ID descriptor from a buffer.
 *    return: advanced buffer pointer
 *    ptr(in): starting pointer
 *    listid_ptr(out):
 * Note:
 *    This is a malloc used version for or_unpack_listid
 */
char *
or_unpack_unbound_listid (char *ptr, void **listid_ptr)
{
  QFILE_LIST_ID *listid;
  int count, i;

  /*
   * tuple_cnt 4, vfid.fileid 4, vfid.volid 2, attr_list.oid_flg 2,
   * attr_list.attr_cnt 4, attr_list.attr_id 4 * n
   */

  listid = (QFILE_LIST_ID *) malloc (sizeof (QFILE_LIST_ID));
  if (listid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (QFILE_LIST_ID));
      goto error;
    }
  QFILE_CLEAR_LIST_ID (listid);

  listid->query_id = OR_GET_PTR (ptr);
  ptr += OR_PTR_SIZE;

  listid->tfile_vfid = (struct qmgr_temp_file *) OR_GET_PTR (ptr);
  ptr += OR_PTR_SIZE;

  listid->tuple_cnt = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->page_cnt = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->first_vpid.pageid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  listid->first_vpid.volid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->last_vpid.pageid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  listid->last_vpid.volid = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->last_offset = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  listid->lasttpl_len = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  count = OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;

  listid->type_list.type_cnt = count;
  listid->type_list.domp = NULL;

  if (count < 0)
    {
      goto error;
    }

  if (count > 0)
    {
      listid->type_list.domp = (TP_DOMAIN **) malloc (sizeof (TP_DOMAIN *) * count);

      if (listid->type_list.domp == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, (sizeof (TP_DOMAIN *) * count));
          goto error;
        }
    }

  for (i = 0; i < count; i++)
    {
      ptr = or_unpack_domain (ptr, &listid->type_list.domp[i], NULL);
    }

  *listid_ptr = (void *) listid;
  return ptr;

error:
  if (listid)
    {
      free_and_init (listid);
    }
  return NULL;
}

/*
 * or_listid_length - Calculates the number of bytes required to store the
 * disk/comm representation of a QFILE_LIST_ID structure.
 *    return: length of the list representation in bytes
 *    listid_ptr(in): opaque pointer to QFILE_LIST_ID structure
 * Note:
 *    Calculates the number of bytes required to store the disk/comm
 *    representation of a QFILE_LIST_ID structure.  These are of variable size.
 *    Kludge, the arguments are passed in as void* so we can avoid
 *    unfortunate circular dependencies between query_list.h and or.h
 *    query_list.h is included at the top of this file so we have the
 *    information necesary for casting.
 */
int
or_listid_length (const void *listid_ptr)
{
  const QFILE_LIST_ID *listid;
  int length = 0;
  int i;

  listid = (const QFILE_LIST_ID *) listid_ptr;

  if (listid == NULL)
    {
      return length;
    }

  /* QFILE_LIST_ID 9 fixed item
   *  tuple_cnt
   *  page_cnt
   *  first_vpid.pageid
   *  first_vpid.volid
   *  last_vpid.pageid
   *  last_vpid.volid
   *  last_offset
   *  lasttpl_len
   *  type_list_type_cnt
   */
  length = OR_INT_SIZE * 9;

  for (i = 0; i < listid->type_list.type_cnt; i++)
    {
      length += or_packed_domain_size (listid->type_list.domp[i]);
    }

  length += OR_PTR_SIZE /* query_id */  + OR_PTR_SIZE;  /* tfile_vfid */
  return length;
}

/*
 * GENERIC DB_VALUE PACKING
 */

/*
 * or_pack_db_value - write a DB_VALUE
 *    return: advanced buffer pointer
 *    buffer(out): output buffer
 *    var(in): DB_VALUE
 */
char *
or_pack_db_value (char *buffer, const DB_VALUE * var)
{
  OR_BUF orbuf;
  char *aligned_buf;

  aligned_buf = PTR_ALIGN (buffer, MAX_ALIGNMENT);

  or_init (&orbuf, aligned_buf, 0);
  /* don't collapse nulls, include the domain, and include domain class oids */
  or_put_value (&orbuf, var, 0, 1);

  return orbuf.ptr;
}

/*
 * or_db_value_size - get the packed size of DB_VALUE
 *    return: packed size
 *    var(in): DB_VALUE
 */
int
or_db_value_size (const DB_VALUE * var)
{
  /* don't collapse nulls, include the domain */
  return or_packed_value_size (var, 0, 1);
}

/*
 * or_unpack_db_value - read a DB_VALUE
 *    return: advanced buffer pointer
 *    buffer(in): input buffer
 *    var(out): DB_VALUE
 */
char *
or_unpack_db_value (char *buffer, DB_VALUE * var)
{
  OR_BUF orbuf;

  buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);
  or_init (&orbuf, buffer, 0);
  or_get_value (&orbuf, var, NULL, -1, true);

  return orbuf.ptr;
}

/*
 * or_pack_db_idxkey - write a DB_IDXKEY
 *    return: advanced buffer pointer
 *    buffer(out): output buffer
 *    key(in): DB_IDXKEY
 */
char *
or_pack_db_idxkey (char *buffer, DB_IDXKEY * key)
{
  int i;

  assert (key != NULL);
  assert (key->size <= MAX_INDEX_KEY_LIST_NUM);

  buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);

  buffer = or_pack_int (buffer, key->size);

  for (i = 0; i < key->size; i++)
    {
      buffer = or_pack_db_value (buffer, &(key->vals[i]));
    }

  buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);

  return buffer;
}

/*
 * or_db_idxkey_size - get the packed size of DB_IDXKEY
 *    return: packed size
 *    key(in): DB_IDXKEY
 */
int
or_db_idxkey_size (DB_IDXKEY * key)
{
  int i;
  int size;

  assert (key != NULL);
  assert (key->size <= MAX_INDEX_KEY_LIST_NUM);

  size = OR_INT_SIZE;

  for (i = 0; i < key->size; i++)
    {
      size += MAX_ALIGNMENT;

      /* don't collapse nulls, exclude the domain */
      size += or_db_value_size (&(key->vals[i]));
    }

  size += MAX_ALIGNMENT;

  return size;
}

/*
 * or_unpack_db_idxkey - read a DB_IDXKEY
 *    return: advanced buffer pointer
 *    buffer(in): input buffer
 *    key(out): DB_IDXKEY
 */
char *
or_unpack_db_idxkey (char *buffer, DB_IDXKEY * key)
{
  int i;

  assert (key != NULL);

  buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);

  buffer = or_unpack_int (buffer, &(key->size));

  assert (key->size <= MAX_INDEX_KEY_LIST_NUM);

  for (i = 0; i < key->size; i++)
    {
      buffer = or_unpack_db_value (buffer, &(key->vals[i]));
    }

  buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);

  return buffer;
}

/*
 * or_pack_ptr - write pointer value to ptr
 *    return: advanced buffer pointer
 *    ptr(out): out buffer
 *    ptrval(in): pointer value
 */
char *
or_pack_ptr (char *ptr, UINTPTR ptrval)
{
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);

  OR_PUT_PTR (ptr, ptrval);
  return (ptr + OR_PTR_SIZE);
}

/*
 * or_unpack_ptr - read a pointer value
 *    return: advanced buffer pointer
 *    ptr(in): input buffer
 *    ptrval(out): pointer value
 */
char *
or_unpack_ptr (char *ptr, UINTPTR * ptrval)
{
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);

  *ptrval = OR_GET_PTR (ptr);
  return (ptr + OR_PTR_SIZE);
}

/*
 * or_pack_version -
 */
char *
or_pack_version (char *ptr, const RYE_VERSION * version)
{
  ptr = PTR_ALIGN (ptr, SHORT_ALIGNMENT);

  OR_PUT_SHORT (ptr, version->major);
  ptr += OR_SHORT_SIZE;
  OR_PUT_SHORT (ptr, version->minor);
  ptr += OR_SHORT_SIZE;
  OR_PUT_SHORT (ptr, version->patch);
  ptr += OR_SHORT_SIZE;
  OR_PUT_SHORT (ptr, version->build);
  ptr += OR_SHORT_SIZE;

  return ptr;
}

/*
 * or_unpack_version -
 */
char *
or_unpack_version (char *ptr, RYE_VERSION * version)
{
  ptr = PTR_ALIGN (ptr, SHORT_ALIGNMENT);

  version->major = OR_GET_SHORT (ptr);
  ptr += OR_SHORT_SIZE;
  version->minor = OR_GET_SHORT (ptr);
  ptr += OR_SHORT_SIZE;
  version->patch = OR_GET_SHORT (ptr);
  ptr += OR_SHORT_SIZE;
  version->build = OR_GET_SHORT (ptr);
  ptr += OR_SHORT_SIZE;

  return ptr;
}

/*
 * LITTLE ENDIAN TRANSFORMATION FUNCTIONS
 */

/*
 * little endian support functions.
 * Could just leave these in all the time.
 * Try to speed these up, consider making them inline.
 *
 */
#if OR_BYTE_ORDER == OR_LITTLE_ENDIAN

#if !defined (OR_HAVE_NTOHS)
unsigned short
ntohs (unsigned short from)
{
  unsigned short to;
  char *ptr, *vptr;

  ptr = (char *) &from;
  vptr = (char *) &to;
  vptr[0] = ptr[1];
  vptr[1] = ptr[0];

  return to;
}
#endif /* !OR_HAVE_NTOHS */

#if !defined (OR_HAVE_NTOHL)
unsigned int
ntohl (unsigned int from)
{
  unsigned int to;
  char *ptr, *vptr;

  ptr = (char *) &from;
  vptr = (char *) &to;
  vptr[0] = ptr[3];
  vptr[1] = ptr[2];
  vptr[2] = ptr[1];
  vptr[3] = ptr[0];

  return to;
}
#endif /* !OR_HAVE_NTOHL */

#if !defined (OR_HAVE_NTOHF)

void
ntohf (float *from, float *to)
{
  char *ptr, *vptr;

  ptr = (char *) from;
  vptr = (char *) to;
  vptr[0] = ptr[3];
  vptr[1] = ptr[2];
  vptr[2] = ptr[1];
  vptr[3] = ptr[0];
}
#endif /* !OR_HAVE_NTOHF */

#if !defined (OR_HAVE_NTOHD)

void
ntohd (double *from, double *to)
{
  char *ptr, *vptr;

  ptr = (char *) from;
  vptr = (char *) to;
  vptr[0] = ptr[7];
  vptr[1] = ptr[6];
  vptr[2] = ptr[5];
  vptr[3] = ptr[4];
  vptr[4] = ptr[3];
  vptr[5] = ptr[2];
  vptr[6] = ptr[1];
  vptr[7] = ptr[0];
}
#endif /* !OR_HAVE_NTOHD */

UINT64
ntohi64 (UINT64 from)
{
  UINT64 to;
  char *ptr, *vptr;

  ptr = (char *) &from;
  vptr = (char *) &to;
  vptr[0] = ptr[7];
  vptr[1] = ptr[6];
  vptr[2] = ptr[5];
  vptr[3] = ptr[4];
  vptr[4] = ptr[3];
  vptr[5] = ptr[2];
  vptr[6] = ptr[1];
  vptr[7] = ptr[0];

  return to;
}

#if !defined (OR_HAVE_HTONS)
unsigned short
htons (unsigned short from)
{
  return ntohs (from);
}
#endif /* !OR_HAVE_HTONS */

#if !defined (OR_HAVE_HTONL)
unsigned int
htonl (unsigned int from)
{
  return ntohl (from);
}
#endif /* !OR_HAVE_HTONL */

UINT64
htoni64 (UINT64 from)
{
  return ntohi64 (from);
}

#if !defined (OR_HAVE_HTONF)
void
htonf (float *to, float *from)
{
  float temp;
  char *ptr, *vptr;

  temp = *from;

  ptr = (char *) &temp;
  vptr = (char *) to;
  vptr[0] = ptr[3];
  vptr[1] = ptr[2];
  vptr[2] = ptr[1];
  vptr[3] = ptr[0];
}
#endif /* !OR_HAVE_HTONL */

#if !defined (OR_HAVE_NTOHD)
void
htond (double *to, double *from)
{
  double temp;
  char *ptr, *vptr;

  temp = *from;

  ptr = (char *) &temp;
  vptr = (char *) to;
  vptr[0] = ptr[7];
  vptr[1] = ptr[6];
  vptr[2] = ptr[5];
  vptr[3] = ptr[4];
  vptr[4] = ptr[3];
  vptr[5] = ptr[2];
  vptr[6] = ptr[1];
  vptr[7] = ptr[0];
}
#endif /* ! OR_HAVE_NTOHD */

#endif /* OR_BYTE_ORDER == OR_LITTLE_ENDIAN */
