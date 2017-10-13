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
 *	Crypt_opfunc.c
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <errno.h>

#include "thread.h"
#include "porting.h"
#include "error_code.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "crypt_opfunc.h"

#define GCRYPT_NO_MPI_MACROS
#define GCRYPT_NO_DEPRECATED

#include "gcrypt.h"

#define GCRYPT_SECURE_MEMORY_LEN (16*1024)

#define AES128_BLOCK_LEN (128/8)
#define AES128_KEY_LEN (128/8)

#if defined(SERVER_MODE)
static pthread_mutex_t gcrypt_init_mutex = PTHREAD_MUTEX_INITIALIZER;
GCRY_THREAD_OPTION_PTHREAD_IMPL;
#endif

static int gcrypt_initialized = 0;

typedef enum
{
  CRYPT_LIB_INIT_ERR = 0,
  CRYPT_LIB_OPEN_CIPHER_ERR,
  CRYPT_LIB_SET_KEY_ERR,
  CRYPT_LIB_CRYPT_ERR,
  CRYPT_LIB_UNKNOWN_ERR
} CRYPT_LIB_ERROR;

static const char *const crypt_lib_fail_info[] = {
  "Initialization failure!",
  "Open cipher failure!",
  "Set secret key failure!",
  "Encrypt/decrypt failure!",
  "Unknown error!"
};

static int init_gcrypt (void);
static char *str_to_hex (const char *src, int src_len, char **dest_p,
			 int *dest_len_p);

/*
 * init_gcrypt() -- Initialize libgcrypt
 *   return: Success, returns NO_ERROR.
 */
static int
init_gcrypt (void)
{
  /* if gcrypt init success, it doesn't return GPG_ERR_NO_ERROR. It's kind of weird! */
#define GCRYPT_INIT_SUCCESS gcry_error(GPG_ERR_GENERAL)

  gcry_error_t i_gcrypt_err;
  if (gcrypt_initialized == 0)
    {
#if defined(SERVER_MODE)
      pthread_mutex_lock (&gcrypt_init_mutex);

      if (gcrypt_initialized == 1)
	{
	  /* It means other concurrent thread has initialized gcrypt when
	   * the thread blocked by pthread_mutex_lock(&gcrypt_init_mutex). */
	  pthread_mutex_unlock (&gcrypt_init_mutex);
	  return NO_ERROR;
	}

      i_gcrypt_err =
	gcry_control (GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread);
      if (i_gcrypt_err != GPG_ERR_NO_ERROR)
	{
	  pthread_mutex_unlock (&gcrypt_init_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_ENCRYPTION_LIB_FAILED,
		  1, crypt_lib_fail_info[CRYPT_LIB_INIT_ERR]);
	  return ER_ENCRYPTION_LIB_FAILED;
	}
#endif
      gcry_check_version (NULL);

      /* allocate secure memory */
      gcry_control (GCRYCTL_SUSPEND_SECMEM_WARN);
      gcry_control (GCRYCTL_INIT_SECMEM, GCRYPT_SECURE_MEMORY_LEN, 0);
      gcry_control (GCRYCTL_RESUME_SECMEM_WARN);

      gcry_control (GCRYCTL_INITIALIZATION_FINISHED, 0);

      i_gcrypt_err = gcry_control (GCRYCTL_INITIALIZATION_FINISHED_P);
      if (i_gcrypt_err != GCRYPT_INIT_SUCCESS)
	{
#if defined(SERVER_MODE)
	  pthread_mutex_unlock (&gcrypt_init_mutex);
#endif
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_ENCRYPTION_LIB_FAILED,
		  1, crypt_lib_fail_info[CRYPT_LIB_INIT_ERR]);
	  return ER_ENCRYPTION_LIB_FAILED;
	}
      gcrypt_initialized = (i_gcrypt_err == GCRYPT_INIT_SUCCESS) ? 1 : 0;
#if defined(SERVER_MODE)
      pthread_mutex_unlock (&gcrypt_init_mutex);
#endif
      return NO_ERROR;
    }
  return NO_ERROR;
}


/*
 * str_to_hex() - convert a string to its hexadecimal expreesion string
 *   return:
 *   thread_p(in):
 *   src(in):
 *   src_len(in):
 *   dest_p(out):
 *   dest_len_p(out):
 * Note:
 */
static char *
str_to_hex (const char *src, int src_len, char **dest_p, int *dest_len_p)
{
  static const char hextable[] = "0123456789ABCDEF";
  int dest_len = 2 * src_len;
  int i = 0;
  unsigned char item_num = 0;
  char *dest;

  assert (src != NULL);

  dest = (char *) malloc (dest_len * sizeof (char));
  if (dest == NULL)
    {
      return NULL;
    }

  while (i < src_len)
    {
      item_num = (unsigned char) src[i];
      dest[2 * i] = hextable[item_num / 16];
      dest[2 * i + 1] = hextable[item_num % 16];
      i++;
    }

  *dest_p = dest;
  *dest_len_p = dest_len;
  return dest;
}

/*
 * sha_one() -
 *   return:
 *   thread_p(in):
 *   src(in):
 *   src_len(in):
 *   dest_len_p(out):
 * Note:
 */
int
sha_one (UNUSED_ARG THREAD_ENTRY * thread_p, const char *src, int src_len,
	 char **dest_p, int *dest_len_p)
{
  int hash_length;
  char *dest = NULL;
  char *dest_hex = NULL;
  int dest_len;
  int dest_hex_len;
  int error_status = NO_ERROR;

  assert (src != NULL);

  *dest_p = NULL;

  if (init_gcrypt () != NO_ERROR)
    {
      return ER_ENCRYPTION_LIB_FAILED;
    }

  hash_length = gcry_md_get_algo_dlen (GCRY_MD_SHA1);
  dest = (char *) malloc (hash_length);
  if (dest == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_and_free;
    }

  dest_len = hash_length;
  gcry_md_hash_buffer (GCRY_MD_SHA1, dest, src, src_len);
  dest_hex = str_to_hex (dest, dest_len, &dest_hex, &dest_hex_len);
  if (dest_hex == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_and_free;
    }

  *dest_p = dest_hex;
  *dest_len_p = dest_hex_len;

exit_and_free:
  if (dest != NULL)
    {
      free_and_init (dest);
    }
  return error_status;
}


/*
 * sha_two() -
 *   return:
 *   thread_p(in):
 *   src(in):
 *   src_len(in):
 *   need_hash_len(in):
 *   dest_p(out)
 *   dest_len_p(out):
 * Note:
 */
int
sha_two (UNUSED_ARG THREAD_ENTRY * thread_p, const char *src, int src_len,
	 int need_hash_len, char **dest_p, int *dest_len_p)
{
  int hash_length;
  int algo;
  char *dest = NULL;
  int dest_len;
  char *dest_hex = NULL;
  int dest_hex_len;
  int error_status = NO_ERROR;

  assert (src != NULL);

  *dest_p = NULL;

  switch (need_hash_len)
    {
    case 0:
    case 256:
      algo = GCRY_MD_SHA256;
      break;
    case 224:
      algo = GCRY_MD_SHA224;
      break;
    case 384:
      algo = GCRY_MD_SHA384;
      break;
    case 512:
      algo = GCRY_MD_SHA512;
      break;
    default:
      return NO_ERROR;
    }

  if (init_gcrypt () != NO_ERROR)
    {
      return ER_ENCRYPTION_LIB_FAILED;
    }

  hash_length = gcry_md_get_algo_dlen (algo);
  dest_len = hash_length;
  dest = (char *) malloc (hash_length);
  if (dest == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_and_free;
    }

  gcry_md_hash_buffer (algo, dest, src, src_len);
  dest_hex = str_to_hex (dest, dest_len, &dest_hex, &dest_hex_len);
  if (dest_hex == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_and_free;
    }

  *dest_p = dest_hex;
  *dest_len_p = dest_hex_len;

exit_and_free:
  if (dest != NULL)
    {
      free_and_init (dest);
    }
  return error_status;
}
