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
 * broker_changer.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#include "broker_admin_pub.h"
#include "broker_config.h"
#include "broker_util.h"
#include "error_manager.h"
#include "language_support.h"

int
main (int argc, char *argv[])
{
  char *br_name;
  char *conf_name;
  char *conf_value;
  int shm_key_br_gl;
  int as_number = -1;

  if (argc == 2 && strcmp (argv[1], "--version") == 0)
    {
      printf ("VERSION %s\n", makestring (BUILD_NUMBER));
      return -1;
    }

  if (argc < 4)
    {
      printf ("%s <broker-name> [<cas-number>] <conf-name> <conf-value>\n", argv[0]);
      return -1;
    }

  sysprm_load_and_init (NULL);

  if (broker_config_read (NULL, NULL, &shm_key_br_gl, NULL, 0) < 0)
    {
      printf ("config file error\n");
      return -1;
    }

  (void) er_init (prm_get_string_value (PRM_ID_ER_LOG_FILE), prm_get_integer_value (PRM_ID_ER_EXIT_ASK));

  if (lang_init () != NO_ERROR)
    {
      assert (false);
      return -1;
    }

  ut_cd_work_dir ();

  br_name = argv[1];

  if (argc == 5)
    {
      int result;

      result = parse_int (&as_number, argv[2], 10);

      if (result != 0 || as_number < 0)
        {
          printf ("Invalid cas number\n");
          return -1;
        }

      conf_name = argv[3];
      conf_value = argv[4];
    }
  else
    {
      conf_name = argv[2];
      conf_value = argv[3];
    }

  admin_Err_msg[0] = '\0';

  if (admin_conf_change (shm_key_br_gl, br_name, conf_name, conf_value, as_number) < 0)
    {
      printf ("%s\n", admin_Err_msg);
      return -1;
    }

  if (admin_Err_msg[0] != '\0')
    {
      printf ("%s\n", admin_Err_msg);
    }

  printf ("OK\n");
  return 0;
}
