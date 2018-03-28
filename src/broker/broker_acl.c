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
 * broker_acl.c -
 */

#ident "$Id$"

#include <assert.h>
#include <errno.h>

#include "porting.h"
#include "broker_acl.h"
#include "cas_error.h"
#include "broker_util.h"
#include "error_manager.h"

#define ADMIN_ERR_MSG_SIZE	1024
#define ACCESS_FILE_DELIMITER ":"
#define IP_FILE_DELIMITER ","

#define ACL_MAX_BROKERS		20

static BR_ACL_INFO acl_Info[ACL_MAX_ITEM_COUNT];
static int num_Acl_info;
static int acl_Info_chn;
static unsigned char local_Host[4] = { 127, 0, 0, 1 };

static int acl_set_internal (const BROKER_ACL_CONF * br_acl_conf,
                             BR_ACL_INFO * br_acl_info,
                             int max_num_acl_info, const char *broker_name, char *admin_err_msg);
static int acl_check_ip (BR_ACL_INFO * acl_info, unsigned char *address);
static int find_acl_info (BR_ACL_INFO * acl_info, int num_acl_info, const char *dbname, const char *dbuser);
static BR_ACL_INFO *find_exact_acl_info (BR_ACL_INFO * acl_info,
                                         int num_acl_info, const char *dbname, const char *dbuser);
static const BR_ACL_IP_SECTION *find_acl_ip_sect (const BR_ACL_IP_SECTION *
                                                  br_sect, int num_br_sect, const char *ip_sect_name);
static int merge_acl_ip_info (ACL_IP_INFO * acl_ip_info1, int num_ip_info1,
                              const ACL_IP_INFO * acl_ip_info2, int num_ip_info2, char *admin_err_msg);
static int acl_info_sort_comp (const void *p1, const void *p2);
static char *acl_read_line (FILE * fp, char *buf, int bufsize);
static int check_acl_conf (BROKER_ACL_CONF * br_acl_conf, char *admin_err_msg);
static int set_group_info (BR_ACL_GROUP * acl_group, char *linebuf, char *admin_err_msg, const char *err_line_info);

int
br_acl_init_shm (T_SHM_APPL_SERVER * shm_appl, T_BROKER_INFO * br_info_p,
                 T_SHM_BROKER * shm_br, char *admin_err_msg, const BROKER_ACL_CONF * br_acl_conf)
{
  shm_appl->acl_chn = 0;

  if (br_sem_init (&shm_appl->acl_sem) < 0)
    {
      sprintf (admin_err_msg, "%s: cannot initialize acl semaphore", br_info_p->name);
      return -1;
    }

  if (br_acl_set_shm (shm_appl, NULL, br_acl_conf, shm_appl->broker_name, admin_err_msg) < 0)
    {
      return -1;
    }

  shm_appl->local_ip = shm_br->my_ip;

  return 0;
}

int
br_acl_set_shm (T_SHM_APPL_SERVER * shm_appl, BR_ACL_INFO * ret_acl_info,
                const BROKER_ACL_CONF * br_acl_conf, const char *broker_name, char *admin_err_msg)
{
  BR_ACL_INFO acl_info_buf[ACL_MAX_ITEM_COUNT];
  int num_acl_info;
  BR_ACL_INFO *acl_info;

  if (ret_acl_info == NULL)
    {
      acl_info = acl_info_buf;
    }
  else
    {
      acl_info = ret_acl_info;
    }

  num_acl_info = acl_set_internal (br_acl_conf, acl_info, ACL_MAX_ITEM_COUNT, broker_name, admin_err_msg);
  if (num_acl_info == 0)
    {
      num_acl_info = acl_set_internal (br_acl_conf, acl_info, ACL_MAX_ITEM_COUNT, "*", admin_err_msg);
    }
  if (num_acl_info < 0)
    {
      return -1;
    }

  qsort (acl_info, num_acl_info, sizeof (BR_ACL_INFO), acl_info_sort_comp);

  if (shm_appl != NULL)
    {
      br_sem_wait (&shm_appl->acl_sem);

      if (num_acl_info <= 0)
        {
          shm_appl->num_acl_info = 0;
        }
      else
        {
          memcpy (shm_appl->acl_info, acl_info, sizeof (BR_ACL_INFO) * num_acl_info);
          shm_appl->num_acl_info = num_acl_info;
        }

      shm_appl->acl_chn++;
      br_sem_post (&shm_appl->acl_sem);
    }

  return num_acl_info;
}

static FILE *
open_acl_file (const char *filename, bool make_default_acl_file)
{
  FILE *fp;

  if (strcmp (filename, "-") == 0)
    {
      return stdin;
    }

  fp = fopen (filename, "r");

  if (fp == NULL && make_default_acl_file && errno == ENOENT)
    {
      fp = fopen (filename, "w");
      if (fp != NULL)
        {
          fprintf (fp, "*:*:*:all_ip\n");
          fprintf (fp, "[all_ip]\n");
          fprintf (fp, "*\n");
          fclose (fp);
        }

      fp = fopen (filename, "r");
    }

  return fp;
}

int
br_acl_read_config_file (BROKER_ACL_CONF * br_acl_conf, const char *filename,
                         bool make_default_acl_file, char *admin_err_msg)
{
  FILE *fp;
  char buf[1024];
  int line = 0;
  bool ip_sect_started = false;
  BR_ACL_IP_SECTION *cur_ip_sect = NULL;
  ACL_IP_INFO *cur_ip_info = NULL;
  char err_line_info[64];

  memset (br_acl_conf, 0, sizeof (BROKER_ACL_CONF));

  fp = open_acl_file (filename, make_default_acl_file);
  if (fp == NULL)
    {
      sprintf (admin_err_msg, "cannot open access control file(%s)", filename);

      return -1;
    }

  while (acl_read_line (fp, buf, sizeof (buf)))
    {
      sprintf (err_line_info, "(line:%d)", ++line);

      if (buf[0] == '\0')
        {
          continue;
        }
      else if (ip_sect_started == false && buf[0] != '[')
        {
          if (br_acl_conf->num_acl_group == MAX_NUM_ACL_GROUP_CONF)
            {
              sprintf (admin_err_msg,
                       "error while loading access control file"
                       " - max broker list (%d) exceeded.", MAX_NUM_ACL_GROUP_CONF);
              goto error;
            }

          if (set_group_info
              (&br_acl_conf->acl_group[br_acl_conf->num_acl_group], buf, admin_err_msg, err_line_info) < 0)
            {
              goto error;
            }
          br_acl_conf->num_acl_group++;
        }
      else if (buf[0] == '[')
        {
          char *p = strchr (buf, ']');
          if (p == NULL)
            {
              sprintf (admin_err_msg, "Error while loading ip info file %s", err_line_info);
              goto error;
            }

          if (br_acl_conf->num_ip_sect >= MAX_NUM_IP_SECT_CONF)
            {
              sprintf (admin_err_msg,
                       "Error while loading ip info file %s"
                       " - max ip section(%d) exceeded.", err_line_info, MAX_NUM_IP_SECT_CONF);
              goto error;
            }

          cur_ip_sect = &br_acl_conf->ip_sect[br_acl_conf->num_ip_sect];
          br_acl_conf->num_ip_sect++;

          memcpy (cur_ip_sect->ip_sect_name, buf + 1, p - buf - 1);
          cur_ip_sect->num_acl_ip_info = 0;

          cur_ip_info = &cur_ip_sect->acl_ip_info[0];
          ip_sect_started = true;
        }
      else
        {
          if (cur_ip_sect->num_acl_ip_info >= ACL_MAX_IP_COUNT)
            {
              sprintf (admin_err_msg,
                       "Error while loading ip info file %s"
                       " - max ip count(%d) exceeded.", err_line_info, ACL_MAX_IP_COUNT);
              goto error;
            }

          if (cur_ip_info == NULL)
            {
              assert (0);
            }
          else
            {
              if (br_acl_conf_read_ip_addr (cur_ip_info, buf, admin_err_msg, err_line_info) < 0)
                {
                  goto error;
                }

              cur_ip_info++;
              cur_ip_sect->num_acl_ip_info++;
            }
        }
    }

  if (fp != stdin)
    {
      fclose (fp);
    }

  return check_acl_conf (br_acl_conf, admin_err_msg);

error:
  if (fp != stdin)
    {
      fclose (fp);
    }
  return -1;
}

int
br_acl_check_right (T_SHM_APPL_SERVER * shm_appl,
                    BR_ACL_INFO * acl_info, int num_acl_info,
                    const char *arg_dbname, const char *dbuser, unsigned char *address)
{
  char dbname[ACL_MAX_DBNAME_LENGTH];
  char *p;
  int acl_info_idx;
  int ip_info_idx = -1;
  unsigned char test_addr[3][ACL_IP_BYTE_COUNT];
  int test_addr_count = 0;

  if (strcmp (dbuser, SHARD_MGMT_DB_USER) == 0)
    {
      if (memcmp (address, local_Host, sizeof (local_Host)) == 0)
        {
          return 0;
        }
      else
        {
          return -1;
        }
    }

  p = strchr (arg_dbname, '@');
  if (p == NULL)
    {
      strncpy (dbname, arg_dbname, sizeof (dbname));
      dbname[sizeof (dbname) - 1] = '\0';
    }
  else
    {
      int len = MIN ((int) (p - arg_dbname), (int) (sizeof (dbname) - 1));
      strncpy (dbname, arg_dbname, len);
      dbname[len] = '\0';
    }

  if (acl_info == NULL)
    {
      assert (shm_appl);

      if (acl_Info_chn != shm_appl->acl_chn)
        {
          br_sem_wait (&shm_appl->acl_sem);

          memcpy (acl_Info, shm_appl->acl_info, sizeof (BR_ACL_INFO) * ACL_MAX_ITEM_COUNT);
          num_Acl_info = shm_appl->num_acl_info;
          acl_Info_chn = shm_appl->acl_chn;
          br_sem_post (&shm_appl->acl_sem);
        }

      acl_info = acl_Info;
      num_acl_info = num_Acl_info;
    }

  memcpy (test_addr[test_addr_count++], address, ACL_IP_BYTE_COUNT);

  if (memcmp (address, local_Host, sizeof (local_Host)) == 0 ||
      (shm_appl != NULL && memcmp (&shm_appl->local_ip, address, ACL_IP_BYTE_COUNT) == 0))
    {
      memcpy (test_addr[test_addr_count++], local_Host, ACL_IP_BYTE_COUNT);
      if (shm_appl != NULL)
        {
          memcpy (test_addr[test_addr_count++], &shm_appl->local_ip, ACL_IP_BYTE_COUNT);
        }
    }

  assert ((int) (sizeof (test_addr) / ACL_IP_BYTE_COUNT) >= test_addr_count);

  acl_info_idx = find_acl_info (acl_info, num_acl_info, dbname, dbuser);
  if (acl_info_idx >= 0)
    {
      int i;
      for (i = 0; i < test_addr_count; i++)
        {
          ip_info_idx = acl_check_ip (&acl_info[acl_info_idx], test_addr[i]);
          if (ip_info_idx >= 0)
            {
              break;
            }
        }
    }

  if (ip_info_idx < 0)
    {
      return -1;
    }
  else
    {
      if (shm_appl != NULL)
        {
          shm_appl->acl_info[acl_info_idx].acl_ip_info[ip_info_idx].ip_last_access_time = time (NULL);
        }

      return 0;
    }
}

static int
acl_set_internal (const BROKER_ACL_CONF * br_acl_conf,
                  BR_ACL_INFO * br_acl_info, int max_num_acl_info, const char *broker_name, char *admin_err_msg)
{
  int i;
  const BR_ACL_IP_SECTION *acl_ip_sect;
  int num_acl_info = 0;

  for (i = 0; i < br_acl_conf->num_acl_group; i++)
    {
      if (strcmp (br_acl_conf->acl_group[i].broker_name, broker_name) == 0)
        {
          acl_ip_sect = find_acl_ip_sect (br_acl_conf->ip_sect,
                                          br_acl_conf->num_ip_sect, br_acl_conf->acl_group[i].ip_sect_name);
          if (acl_ip_sect == NULL)
            {
              assert (0);
            }
          else
            {
              BR_ACL_INFO *update;
              update = find_exact_acl_info (br_acl_info, num_acl_info,
                                            br_acl_conf->acl_group[i].dbname, br_acl_conf->acl_group[i].dbuser);
              if (update == NULL)
                {
                  if (num_acl_info == max_num_acl_info)
                    {
                      sprintf (admin_err_msg,
                               "error while loading access control file"
                               " - max item count(%d) exceeded.", max_num_acl_info);
                      return -1;
                    }

                  strcpy (br_acl_info[num_acl_info].dbname, br_acl_conf->acl_group[i].dbname);
                  strcpy (br_acl_info[num_acl_info].dbuser, br_acl_conf->acl_group[i].dbuser);
                  br_acl_info[num_acl_info].num_acl_ip_info = 0;
                  update = &br_acl_info[num_acl_info];
                  num_acl_info++;
                }

              update->num_acl_ip_info =
                merge_acl_ip_info (update->acl_ip_info,
                                   update->num_acl_ip_info,
                                   acl_ip_sect->acl_ip_info, acl_ip_sect->num_acl_ip_info, admin_err_msg);
              if (update->num_acl_ip_info < 0)
                {
                  return -1;
                }
            }
        }
    }

  return num_acl_info;
}

static int
remove_duplicate_ip_info (ACL_IP_INFO * acl_ip_info, int num_acl_ip_info)
{
  int i, j;

  for (i = num_acl_ip_info - 1; i >= 0; i--)
    {
      for (j = 0; j < i; j++)
        {
          int iplen;

          iplen = MIN (acl_ip_info[i].ip_len, acl_ip_info[j].ip_len);
          assert (iplen <= ACL_IP_BYTE_COUNT);

          if (memcmp (acl_ip_info[i].ip_addr, acl_ip_info[j].ip_addr, iplen) == 0)
            {
              if (acl_ip_info[i].ip_len > acl_ip_info[j].ip_len)
                {
                  acl_ip_info[j] = acl_ip_info[i];
                }
              acl_ip_info[i] = acl_ip_info[num_acl_ip_info - 1];
              num_acl_ip_info--;
              break;
            }
        }
    }

  return num_acl_ip_info;
}

static char *
acl_read_line (FILE * fp, char *buf, int bufsize)
{
  char *p;

  if (fgets (buf, bufsize, fp) == NULL)
    {
      return NULL;
    }

  p = strchr (buf, '#');
  if (p != NULL)
    {
      *p = '\0';
    }

  trim (buf);
  return buf;
}

static int
set_group_info (BR_ACL_GROUP * acl_group, char *linebuf, char *admin_err_msg, const char *err_line_info)
{
  char *save = NULL;
  char *token[4];
  int i;

  save = NULL;
  for (i = 0; i < 4; i++)
    {
      token[i] = strtok_r (linebuf, ACCESS_FILE_DELIMITER, &save);
      if (token[i] == NULL)
        {
          sprintf (admin_err_msg, "error while loading access control file %s", err_line_info);
          return -1;
        }

      linebuf = NULL;
    }

  if (strlen (token[0]) >= BROKER_NAME_LEN ||
      strlen (token[1]) >= ACL_MAX_DBNAME_LENGTH ||
      strlen (token[2]) >= ACL_MAX_DBUSER_LENGTH || strlen (token[3]) >= MAX_IP_SECT_NAME)
    {
      sprintf (admin_err_msg, "error while loading access control file %s" " - id is too long", err_line_info);
      return -1;
    }

  strcpy (acl_group->broker_name, token[0]);
  strcpy (acl_group->dbname, token[1]);
  strcpy (acl_group->dbuser, token[2]);
  strcpy (acl_group->ip_sect_name, token[3]);

  return 0;
}

static int
merge_acl_ip_info (ACL_IP_INFO * acl_ip_info1, int num_ip_info1,
                   const ACL_IP_INFO * acl_ip_info2, int num_ip_info2, char *admin_err_msg)
{
  ACL_IP_INFO all_ip_info[ACL_MAX_IP_COUNT * 2];
  int all_count;

  memcpy (all_ip_info, acl_ip_info1, sizeof (ACL_IP_INFO) * num_ip_info1);
  memcpy (&all_ip_info[num_ip_info1], acl_ip_info2, sizeof (ACL_IP_INFO) * num_ip_info2);
  all_count = num_ip_info1 + num_ip_info2;

  all_count = remove_duplicate_ip_info (all_ip_info, all_count);

  if (all_count > ACL_MAX_IP_COUNT)
    {
      sprintf (admin_err_msg, "Error while loading ip info file " " - max ip count(%d) exceeded.", ACL_MAX_IP_COUNT);
      return -1;
    }

  memcpy (acl_ip_info1, all_ip_info, sizeof (ACL_IP_INFO) * all_count);
  return all_count;
}

static const BR_ACL_IP_SECTION *
find_acl_ip_sect (const BR_ACL_IP_SECTION * br_sect, int num_br_sect, const char *ip_sect_name)
{
  int i;
  for (i = 0; i < num_br_sect; i++)
    {
      if (strcmp (br_sect[i].ip_sect_name, ip_sect_name) == 0)
        {
          return &br_sect[i];
        }
    }

  return NULL;
}

static int
acl_info_sort_comp (const void *p1, const void *p2)
{
  const BR_ACL_INFO *info1 = p1;
  const BR_ACL_INFO *info2 = p2;
  int cmp;

  cmp = strcmp (info1->dbname, info2->dbname);
  if (cmp == 0)
    {
      goto comp_dbuser;
    }
  else if (info1->dbname[0] == '*')
    {
      return 1;
    }
  else if (info2->dbname[0] == '*')
    {
      return -1;
    }
  else
    {
      return cmp;
    }

comp_dbuser:
  cmp = strcmp (info1->dbuser, info2->dbuser);
  if (cmp == 0)
    {
      return 0;
    }
  else if (info1->dbuser[0] == '*')
    {
      return 1;
    }
  else if (info2->dbuser[0] == '*')
    {
      return -1;
    }
  else
    {
      return cmp;
    }
}

int
br_acl_conf_read_ip_addr (ACL_IP_INFO * ip_info, char *linebuf, char *admin_err_msg, const char *err_line_info)
{
  char *token;
  char *save = NULL;
  int iplen;

  for (iplen = 0; iplen < ACL_IP_BYTE_COUNT; iplen++)
    {
      token = strtok_r (linebuf, ".", &save);
      linebuf = NULL;

      if (token == NULL)
        {
          if (admin_err_msg != NULL)
            {
              sprintf (admin_err_msg, "Error while loading ip info file %s", err_line_info);
            }
          return -1;
        }

      if (strcmp (token, "*") == 0)
        {
          break;
        }
      else
        {
          int adr = 0, result;

          result = parse_int (&adr, token, 10);
          if (result != 0 || adr > 255 || adr < 0)
            {
              if (admin_err_msg != NULL)
                {
                  sprintf (admin_err_msg, "Error while loading ip info file %s", err_line_info);
                }
              return -1;
            }

          ip_info->ip_addr[iplen] = adr;
        }

    }

  if (strtok_r (NULL, ".", &save) != NULL)
    {
      if (admin_err_msg != NULL)
        {
          sprintf (admin_err_msg, "Error while loading ip info file %s", err_line_info);
        }
      return -1;
    }

  assert (iplen <= ACL_IP_BYTE_COUNT);
  ip_info->ip_len = iplen;
  ip_info->ip_last_access_time = 0;

  return 0;
}

static int
check_acl_conf (BROKER_ACL_CONF * br_acl_conf, char *admin_err_msg)
{
  int i;

  for (i = 0; i < br_acl_conf->num_ip_sect; i++)
    {
      br_acl_conf->ip_sect[i].num_acl_ip_info =
        remove_duplicate_ip_info (br_acl_conf->ip_sect[i].acl_ip_info, br_acl_conf->ip_sect[i].num_acl_ip_info);
    }

  for (i = 0; i < br_acl_conf->num_acl_group; i++)
    {
      if (find_acl_ip_sect (br_acl_conf->ip_sect,
                            br_acl_conf->num_ip_sect, br_acl_conf->acl_group[i].ip_sect_name) == NULL)
        {
          sprintf (admin_err_msg,
                   "error while loading access control file"
                   " - ip section (%s) not found", br_acl_conf->acl_group[i].ip_sect_name);
          return -1;
        }
    }

  return 0;
}

static int
find_acl_info (BR_ACL_INFO * acl_info, int num_acl_info, const char *dbname, const char *dbuser)
{
  int i;

  for (i = 0; i < num_acl_info; i++)
    {
      if (acl_info[i].dbname[0] == '*' || strncmp (acl_info[i].dbname, dbname, strlen (acl_info[i].dbname)) == 0)
        {
          if (acl_info[i].dbuser[0] == '*' || strcmp (acl_info[i].dbuser, dbuser) == 0)
            {
              return i;
            }
        }
    }

  return -1;
}

static BR_ACL_INFO *
find_exact_acl_info (BR_ACL_INFO * acl_info, int num_acl_info, const char *dbname, const char *dbuser)
{
  int i;

  for (i = 0; i < num_acl_info; i++)
    {
      if (strcmp (acl_info[i].dbname, dbname) == 0 && strcmp (acl_info[i].dbuser, dbuser) == 0)
        {
          return &acl_info[i];
        }
    }

  return NULL;
}

static int
acl_check_ip (BR_ACL_INFO * acl_info, unsigned char *address)
{
  int i;

  assert (acl_info && address);

  for (i = 0; i < acl_info->num_acl_ip_info; i++)
    {
      if (memcmp ((void *) acl_info->acl_ip_info[i].ip_addr, (void *) address, acl_info->acl_ip_info[i].ip_len) == 0)
        {
          return i;
        }
    }

  return -1;
}

static int
print_acl_ip_addr (char *str, ACL_IP_INFO * acl_ip_info)
{
  int len;
  int i;

  assert (acl_ip_info != NULL);
  assert (acl_ip_info->ip_len <= ACL_IP_BYTE_COUNT);

  len = 0;
  for (i = 0; i < acl_ip_info->ip_len && i < ACL_IP_BYTE_COUNT; i++)
    {
      len += sprintf (str + len, "%d%s", acl_ip_info->ip_addr[i], ((i != 3) ? "." : ""));
    }

  if (i != ACL_IP_BYTE_COUNT)
    {
      len += sprintf (str + len, "*");
    }

  return len;
}

void
br_acl_dump (FILE * fp, BR_ACL_INFO * acl_info)
{
  int i;
  char line_buf[LINE_MAX];
  char str[256];
  int len;
  struct timeval time_val;

  fprintf (fp, "%s:%s\n", acl_info->dbname, acl_info->dbuser);

  len = 0;
  len += sprintf (line_buf + len, "%16s ", "CLIENT IP");
  len += sprintf (line_buf + len, "%25s", "LAST ACCESS TIME");
  fprintf (fp, "%s\n", line_buf);

  for (i = 0; i < len; i++)
    {
      line_buf[i] = '=';
    }
  line_buf[i] = '\0';
  fprintf (fp, "%s\n", line_buf);

  for (i = 0; i < acl_info->num_acl_ip_info; i++)
    {
      print_acl_ip_addr (str, &acl_info->acl_ip_info[i]);

      len = sprintf (line_buf, "%16.16s ", str);

      if (acl_info->acl_ip_info[i].ip_last_access_time != 0)
        {
          time_val.tv_sec = acl_info->acl_ip_info[i].ip_last_access_time;
          time_val.tv_usec = 0;

          (void) er_datetime (&time_val, str, sizeof (str));

          sprintf (line_buf + len, "%s", str);
        }

      fprintf (fp, "%s\n", line_buf);
    }

  fprintf (fp, "\n");
}
