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
 * rye_shm.c -
 */

#ident "$Id$"

#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <assert.h>

#include "rye_master_shm.h"
#include "rye_shm.h"
#include "error_manager.h"
#include "system_parameter.h"

static RYE_SHM_TYPE rye_shm_check_header (const RYE_SHM_HEADER * shm_header, RYE_SHM_TYPE shm_type, bool check_status);

/*
 * rye_shm_create -
 *
 * return:
 * Note:
 */
void *
rye_shm_create (int shm_key, int size, RYE_SHM_TYPE shm_type)
{
  int mid;
  char err_msg[ER_MSG_SIZE];
  void *p;
  RYE_SHM_HEADER *shm_header;

  if (shm_key <= 0 || size <= 0)
    {
      assert (false);

      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "invalid shm key");

      return NULL;
    }

  mid = shmget (shm_key, size, IPC_CREAT | IPC_EXCL | 0644);
  if (mid == -1)
    {
      snprintf (err_msg, sizeof (err_msg), "error: shmget - key(%d), size(%d)", shm_key, size);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, err_msg);

      return NULL;
    }

  p = shmat (mid, (char *) 0, 0);
  if (p == (void *) -1)
    {
      snprintf (err_msg, sizeof (err_msg), "error: shmat - key(%d), size(%d)", shm_key, size);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, err_msg);

      return NULL;
    }

  memset (p, 0, size);

  shm_header = (RYE_SHM_HEADER *) p;

  assert (sizeof (shm_header->magic_string) > strlen (RYE_SHM_MAGIC_STR));
  strcpy (shm_header->magic_string, RYE_SHM_MAGIC_STR);
  shm_header->magic_number = RYE_SHM_MAGIC_NUMBER;
  shm_header->shm_type = shm_type;
  shm_header->status = RYE_SHM_CREATED;
  shm_header->shm_key = shm_key;
  shm_header->shm_version = rel_cur_version ();

  return p;
}

/*
 * rye_shm_is_used_key
 */
bool
rye_shm_is_used_key (int shm_key)
{
  int shmid;

  if (shm_key <= 0)
    {
      return true;
    }

  shmid = shmget (shm_key, 1, 0);
  if (shmid < 0)
    {
      if (errno == ENOENT)
        {
          return false;
        }
    }

  return true;
}


/*
 * rye_shm_check_shm
 */
RYE_SHM_TYPE
rye_shm_check_shm (int shm_key, RYE_SHM_TYPE shm_type, bool check_status)
{
  void *p;
  int ret_shm_type = RYE_SHM_TYPE_UNKNOWN;
  int shmid;

  if (shm_key <= 0)
    {
      return RYE_SHM_TYPE_UNKNOWN;
    }

  shmid = shmget (shm_key, sizeof (RYE_SHM_HEADER), 0);
  if (shmid < 0)
    {
      return RYE_SHM_TYPE_UNKNOWN;
    }

  p = shmat (shmid, (char *) 0, SHM_RDONLY);
  if (p == (void *) -1)
    {
      return RYE_SHM_TYPE_UNKNOWN;
    }

  ret_shm_type = rye_shm_check_header ((RYE_SHM_HEADER *) p, shm_type, check_status);

  shmdt (p);

  return ret_shm_type;
}

/*
 * rye_shm_attach -
 *
 * return:
 * Note:
 */
void *
rye_shm_attach (int shm_key, RYE_SHM_TYPE shm_type, bool is_monitoring)
{
  int mid;
  char err_msg[ER_MSG_SIZE];
  void *p;
  RYE_SHM_HEADER *shm_header;

  assert (shm_type > RYE_SHM_TYPE_UNKNOWN && shm_type <= RYE_SHM_TYPE_MAX);

  if (shm_key <= 0)
    {
      return NULL;
    }

  mid = shmget (shm_key, 0, (is_monitoring ? 0 : 0644));
  if (mid == -1)
    {
      snprintf (err_msg, sizeof (err_msg), "error: shmget - key(%d)", shm_key);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, err_msg);
      return NULL;
    }

  p = shmat (mid, (char *) 0, (is_monitoring ? SHM_RDONLY : 0));
  if (p == (void *) -1)
    {
      snprintf (err_msg, sizeof (err_msg), "error: shmat - key(%d)", shm_key);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, err_msg);
      return NULL;
    }

  shm_header = (RYE_SHM_HEADER *) p;
  if (rye_shm_check_header (shm_header, shm_type, true) == RYE_SHM_TYPE_UNKNOWN)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "rye_shm_check_header fail");

      rye_shm_detach (p);
      p = NULL;

      return NULL;
    }

  return p;
}

/*
 * rye_shm_detach -
 *
 * return:
 * Note:
 */
int
rye_shm_detach (void *shm_p)
{
  if (shm_p != NULL)
    {
      shmdt (shm_p);
    }

  return NO_ERROR;
}

/*
 * rye_shm_destroy -
 *
 * return:
 * Note:
 */
int
rye_shm_destroy (int shm_key)
{
  RYE_SHM_HEADER *shm_header;
  int mid;
  char err_msg[ER_MSG_SIZE];

  if (shm_key <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  mid = shmget (shm_key, 0, 0644);
  if (mid == -1)
    {
      snprintf (err_msg, sizeof (err_msg), "error: shmget - key(%d)", shm_key);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, err_msg);
      return ER_GENERIC_ERROR;
    }

  shm_header = (RYE_SHM_HEADER *) shmat (mid, (char *) 0, 0);
  if (shm_header == (RYE_SHM_HEADER *) (-1))
    {
      assert (false);
      return ER_FAILED;
    }
  shm_header->status = RYE_SHM_MARK_DELETED;

  (void) rye_shm_detach (shm_header);

  if (shmctl (mid, IPC_RMID, 0) == -1)
    {
      assert (false);
      return ER_FAILED;
    }

  return 0;
}

/*
 * rye_shared_mutex_init () -
 *    return: NO_ERROR or error code
 *
 *    mutex(out):
 */
int
rye_shm_mutex_init (pthread_mutex_t * mutex)
{
  int error = NO_ERROR;
  pthread_mutexattr_t mattr;

  error = pthread_mutexattr_init (&mattr);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEXATTR_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  error = pthread_mutexattr_setpshared (&mattr, PTHREAD_PROCESS_SHARED);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEXATTR_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  error = pthread_mutexattr_setrobust_np (&mattr, PTHREAD_MUTEX_ROBUST_NP);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEXATTR_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  error = pthread_mutex_init (mutex, &mattr);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "Invalid error code");
    }

  return error;
}

#if defined(RYE_SHM_UNUSED_FUNCTION)
/*
 * rye_shm_cond_init () -
 *    return: NO_ERROR or error code
 *
 *    cond(out):
 */
int
rye_shm_cond_init (pthread_cond_t * cond)
{
  pthread_condattr_t cattr;
  int error = NO_ERROR;

  if (pthread_condattr_init (&cattr) != 0)
    {
      error = ER_CSS_PTHREAD_COND_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto exit;
    }

  if (pthread_condattr_setpshared (&cattr, PTHREAD_PROCESS_SHARED) != 0)
    {
      error = ER_CSS_PTHREAD_COND_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto exit;
    }

  if (pthread_cond_init (cond, &cattr) != 0)
    {
      error = ER_CSS_PTHREAD_COND_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto exit;
    }

  return NO_ERROR;

exit:
  assert (error != NO_ERROR);

  return error;
}
#endif

/*
 * rye_shm_check_header () -
 *    return: true or false
 */
static RYE_SHM_TYPE
rye_shm_check_header (const RYE_SHM_HEADER * shm_header, RYE_SHM_TYPE shm_type, bool check_status)
{
  if (strcmp (shm_header->magic_string, RYE_SHM_MAGIC_STR) == 0)
    {
      if (check_status == false ||
          (shm_header->status == RYE_SHM_VALID && shm_header->magic_number == RYE_SHM_MAGIC_NUMBER))
        {
          if (shm_type == RYE_SHM_TYPE_UNKNOWN)
            {
              return shm_header->shm_type;
            }
          else if (shm_header->shm_type == shm_type)
            {
              return shm_type;
            }
        }
    }

  return RYE_SHM_TYPE_UNKNOWN;
}

/*
 * rye_shm_destroy_all_server_shm () -
 */
int
rye_shm_destroy_all_server_shm ()
{
  RYE_SHM_MASTER *shm_master;
  char *str_shm_key;
  int shm_key;
  int i;
  int num_keys;

  str_shm_key = prm_get_string_value (PRM_ID_RYE_SHM_KEY);
  parse_int (&shm_key, str_shm_key, 16);
  shm_master = rye_shm_attach (shm_key, RYE_SHM_TYPE_MASTER, false);
  if (shm_master == NULL)
    {
      return ER_FAILED;
    }

  assert (shm_master->num_shm <= MAX_NUM_SHM);
  num_keys = MIN (shm_master->num_shm, MAX_NUM_SHM);
  for (i = 0; i < num_keys; i++)
    {
      rye_shm_destroy (shm_master->shm_info[i].shm_key);
      shm_master->shm_info[i].shm_key = 0;
      shm_master->shm_info[i].type = RYE_SHM_TYPE_UNKNOWN;
    }
  shm_master->num_shm = 0;

  rye_shm_detach (shm_master);

  return NO_ERROR;
}
