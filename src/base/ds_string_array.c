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
 * ds_string_array.c
 *
 */

#include <assert.h>
#include <sys/time.h>

#include "ds_string_array.h"

/*
 * Rye_split_string ()-
 *   return: The returned char** is null terminated char* array
 *           ex: "a,b" --> { "a", "b", NULL }
 *   str(in):
 *   delim(in):
 */
RSTR_ARRAY
Rye_split_string (const char *str, const char *delim)
{
  char *o;
  char *save = NULL, *v;
  char **r = NULL;
  int count = 1;

  if (str == NULL)
    {
      return NULL;
    }

  o = strdup (str);
  if (o == NULL)
    {
      return NULL;
    }

  for (v = strtok_r (o, delim, &save);
       v != NULL; v = strtok_r (NULL, delim, &save))
    {
      r = (char **) realloc (r, sizeof (char *) * (count + 1));
      if (r == NULL)
	{
	  free (o);
	  return NULL;
	}
      r[count - 1] = strdup (v);
      r[count] = NULL;
      count++;
    }

  free (o);
  return r;
}

/*
 * Rye_str_array_free () -
 *   return:
 *
 *   array(in/out):
 */
void
Rye_str_array_free (RSTR_ARRAY array)
{
  int i;

  for (i = 0; array[i] != NULL; i++)
    {
      free (array[i]);
    }
  free (array);
}

/*
 * Rye_str_array_get_length () -
 *   return: array length
 *
 *   array(in/out):
 */
int
Rye_str_array_get_length (RSTR_ARRAY array)
{
  int i;

  for (i = 0; array[i] != NULL; i++);

  return i;
}

/*
 * Rye_str_array_find () -
 *    return: index if value was found, -1 if value was not found.
 *
 *    array(in):
 *    value(in):
 */
int
Rye_str_array_find (RSTR_ARRAY array, const char *value)
{
  int i, found;

  found = -1;
  for (i = 0; array[i] != NULL; i++)
    {
      if (strcasecmp (array[i], value) == 0)
	{
	  found = i;
	  break;
	}
    }

  return found;
}

/*
 * Rye_str_array_find_substr () -
 *    return:
 *
 *    array(in):
 *    value(in):
 */
int
Rye_str_array_find_substr (RSTR_ARRAY array, const char *substr)
{
  int i, found;

  found = -1;
  for (i = 0; array[i] != NULL; i++)
    {
      if (strstr (array[i], substr) != NULL)
	{
	  found = i;
	  break;
	}
    }

  return found;
}

/*
 * Rye_str_array_shuffle ()-
 *   return:
 *
 *   array(in/out):
 */
void
Rye_str_array_shuffle (RSTR_ARRAY array)
{
  struct timeval t;
  int i, j;
  double r;
  struct drand48_data buf;
  char *temp;
  int count;

  gettimeofday (&t, NULL);

  srand48_r (t.tv_usec, &buf);

  count = Rye_str_array_get_length (array);

  /* Fisher-Yates shuffle */
  for (i = count - 1; i > 0; i--)
    {
      drand48_r (&buf, &r);
      j = (int) ((i + 1) * r);

      temp = array[j];
      array[j] = array[i];
      array[i] = temp;
    }
}
