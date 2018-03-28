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
 * transform.c: Definition of the meta-class information for class storage
 *              and catalog entries.
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include "error_manager.h"
#include "object_representation.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "transform.h"

/* server side only */
#if !defined(CS_MODE)
#include "intl_support.h"
#include "language_support.h"
#include "system_catalog.h"
#endif /* !CS_MODE */

/*
 * These define the structure of the meta class objects
 *
 * IMPORTANT
 * If you modify either the META_ATTRIBUTE or META_CLASS definitions
 * here, make sure you adjust the associated ORC_ constants in or.h.
 * Of particular importance are ORC_CLASS_VAR_ATT_COUNT and
 * ORC_ATT_VAR_ATT_COUNT.
 * If you don't know what these are, you shouldn't be making this change.
 *
 */
/* DOMAIN */
static META_ATTRIBUTE domain_atts[] = {
  {"type", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"precision", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"scale", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"codeset", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"collation_id", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"class", DB_TYPE_OBJECT, 1, META_CLASS_NAME, 0, 0, NULL},
  {"set_domain", DB_TYPE_SEQUENCE, 1, META_DOMAIN_NAME, 1, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_domain = { META_DOMAIN_NAME, {META_PAGE_DOMAIN, 0, META_VOLUME, GLOBAL_GROUPID}, 0,
0,
0,
&domain_atts[0]
};

/* ATTRIBUTE */
static META_ATTRIBUTE att_atts[] = {
  {"id", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"type", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"offset", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"order", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"class", DB_TYPE_OBJECT, 1, META_CLASS_NAME, 0, 0, NULL},
  {"flags", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"index_fileid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"index_root_pageid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"index_volid_key", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"name", DB_TYPE_VARCHAR, 1, NULL, 0, 0, NULL},
  {"value", DB_TYPE_VARCHAR, 1, NULL, 0, 0, NULL},
  {"original_value", DB_TYPE_VARCHAR, 0, NULL, 0, 0, NULL},
  {"domain", DB_TYPE_SEQUENCE, 1, META_DOMAIN_NAME, 0, 0, NULL},
  {"properties", DB_TYPE_SEQUENCE, 0, NULL, 0, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_attribute = { META_ATTRIBUTE_NAME, {META_PAGE_ATTRIBUTE, 0, META_VOLUME,
                                                            GLOBAL_GROUPID},
0, 0, 0,
&att_atts[0]
};

/* REPATTRIBUTE */
static META_ATTRIBUTE repatt_atts[] = {
  {"id", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"type", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"domain", DB_TYPE_SEQUENCE, 1, META_DOMAIN_NAME, 0, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_repattribute = { META_REPATTRIBUTE_NAME, {META_PAGE_REPATTRIBUTE, 0, META_VOLUME,
                                                                  GLOBAL_GROUPID}, 0, 0, 0,
&repatt_atts[0]
};

/* REPRESENTATION */
static META_ATTRIBUTE rep_atts[] = {
  {"id", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"fixed_count", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"variable_count", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"fixed_size", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"attributes", DB_TYPE_SEQUENCE, 0, META_REPATTRIBUTE_NAME, 1, 0, NULL},
  {"properties", DB_TYPE_SEQUENCE, 0, NULL, 0, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_representation = { META_REPRESENTATION_NAME, {META_PAGE_REPRESENTATION, 0, META_VOLUME,
                                                                      GLOBAL_GROUPID}, 0,
0, 0, &rep_atts[0]
};

/* CLASS */
static META_ATTRIBUTE class_atts[] = {
  {"attid_counter", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"unused", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"heap_fileid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"heap_volid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"heap_pageid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"current_repid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"fixed_count", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"variable_count", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"fixed_size", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"attribute_count", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"object_size", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"flags", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"class_type", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"owner", DB_TYPE_OBJECT, 1, "object", 0, 0, NULL},
  {"collation_id", DB_TYPE_INTEGER, 1, NULL, 0, 0, NULL},
  {"name", DB_TYPE_VARCHAR, 1, NULL, 0, 0, NULL},
  {"owner_name", DB_TYPE_VARCHAR, 1, NULL, 0, 0, NULL},
  {"loader_commands", DB_TYPE_VARCHAR, 1, NULL, 0, 0, NULL},
  {"representations", DB_TYPE_SEQUENCE, 0, META_REPRESENTATION_NAME, 1, 0,
   NULL},
  {"attributes", DB_TYPE_SEQUENCE, 1, META_ATTRIBUTE_NAME, 1, 0, NULL},
  {"query_spec", DB_TYPE_SEQUENCE, 1, META_QUERY_SPEC_NAME, 1, 0, NULL},
  {"properties", DB_TYPE_SEQUENCE, 0, META_CONSTRAINT_NAME, 1, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};
META_CLASS tf_Metaclass_class = { META_CLASS_NAME, {META_PAGE_CLASS, 0, META_VOLUME, GLOBAL_GROUPID}, 0, 0,
0,
&class_atts[0]
};

/* QUERY_SPEC */
static META_ATTRIBUTE query_spec_atts[] = {
  {"specification", DB_TYPE_VARCHAR, 1, NULL, 0, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_query_spec = { META_QUERY_SPEC_NAME, {META_PAGE_QUERY_SPEC, 0, META_VOLUME,
                                                              GLOBAL_GROUPID}, 0, 0, 0,
&query_spec_atts[0]
};

/* CONSTRAINT_ATTRIBUTE */
static META_ATTRIBUTE constraint_attribute_atts[] = {
  {"id", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"asc_desc", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"reserved", DB_TYPE_VARCHAR, 0, NULL, 0, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_constraint_attribute = { META_CONSTRAINT_ATTRIBUTE_NAME,
  {META_PAGE_CONSTRAINT_ATTRIBUTE, 0, META_VOLUME, GLOBAL_GROUPID}, 0, 0, 0,
  &constraint_attribute_atts[0]
};

/* CONSTRAINT */
static META_ATTRIBUTE constraint_atts[] = {
  {"constraint_type", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"status", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"constraint_name", DB_TYPE_VARCHAR, 0, NULL, 0, 0, NULL},
  {"btid", DB_TYPE_VARCHAR, 0, NULL, 0, 0, NULL},
  {"attributes", DB_TYPE_SEQUENCE, 0, META_CONSTRAINT_ATTRIBUTE_NAME, 1, 0,
   NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};

META_CLASS tf_Metaclass_constraint = {
  META_CONSTRAINT_NAME, {META_PAGE_CONSTRAINT, 0, META_VOLUME,
                         GLOBAL_GROUPID},
  0, 0, 0, &constraint_atts[0]
};

/* ROOT */
static META_ATTRIBUTE root_atts[] = {
  {"heap_fileid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"heap_volid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"heap_pageid", DB_TYPE_INTEGER, 0, NULL, 0, 0, NULL},
  {"name", DB_TYPE_VARCHAR, 0, NULL, 0, 0, NULL},
  {NULL, (DB_TYPE) 0, 0, NULL, 0, 0, NULL}
};
META_CLASS tf_Metaclass_root = { "rootclass", {META_PAGE_ROOT, 0, META_VOLUME, GLOBAL_GROUPID}, 0, 0, 0,
&root_atts[0]
};

/*
 * Meta_classes
 *    An array of pointers to each meta class.  This is used to reference
 *    the class structures after they have been compiled.
 */
static META_CLASS *Meta_classes[] = {
  &tf_Metaclass_root,
  &tf_Metaclass_class,
  &tf_Metaclass_representation,
  &tf_Metaclass_attribute,
  &tf_Metaclass_domain,
  &tf_Metaclass_repattribute,
  &tf_Metaclass_query_spec,
  &tf_Metaclass_constraint,
  &tf_Metaclass_constraint_attribute,
  NULL
};

#if !defined(CS_MODE)

static CT_ATTR ct_class_atts[] = {
  {"table_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"num_col", NULL_ATTRID, DB_TYPE_INTEGER},
  {"is_system_table", NULL_ATTRID, DB_TYPE_INTEGER},
  {"table_type", NULL_ATTRID, DB_TYPE_INTEGER},
  {"owner", NULL_ATTRID, DB_TYPE_OBJECT},
  {"collation_id", NULL_ATTRID, DB_TYPE_INTEGER},
  {"table_name", NULL_ATTRID, DB_TYPE_VARCHAR}, /* refer catcls_find_btid_of_class_name () */
  {"owner_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"cols", NULL_ATTRID, DB_TYPE_SEQUENCE},
  {"query_specs", NULL_ATTRID, DB_TYPE_SEQUENCE},
  {"indexes", NULL_ATTRID, DB_TYPE_SEQUENCE}
};

static CT_ATTR ct_attribute_atts[] = {
  {"table_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"table_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"col_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"data_type", NULL_ATTRID, DB_TYPE_INTEGER},
  {"def_order", NULL_ATTRID, DB_TYPE_INTEGER},
  {"is_nullable", NULL_ATTRID, DB_TYPE_INTEGER},
  {"is_shard_key", NULL_ATTRID, DB_TYPE_INTEGER},
  {"default_value", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"domains", NULL_ATTRID, DB_TYPE_SEQUENCE}
};

static CT_ATTR ct_attrid_atts[] = {
  {"id", NULL_ATTRID, DB_TYPE_INTEGER},
  {"name", NULL_ATTRID, DB_TYPE_VARCHAR}
};

static CT_ATTR ct_domain_atts[] = {
  {"object_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"table_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"col_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"data_type", NULL_ATTRID, DB_TYPE_INTEGER},
  {"prec", NULL_ATTRID, DB_TYPE_INTEGER},
  {"scale", NULL_ATTRID, DB_TYPE_INTEGER},
  {"code_set", NULL_ATTRID, DB_TYPE_INTEGER},
  {"collation_id", NULL_ATTRID, DB_TYPE_INTEGER},
  {"table_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"domain_table_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"set_domains", NULL_ATTRID, DB_TYPE_SEQUENCE}
};

static CT_ATTR ct_queryspec_atts[] = {
  {"table_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"table_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"spec", NULL_ATTRID, DB_TYPE_VARCHAR}
};

static CT_ATTR ct_index_atts[] = {
  {"table_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"table_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"index_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"is_unique", NULL_ATTRID, DB_TYPE_INTEGER},
  {"key_count", NULL_ATTRID, DB_TYPE_INTEGER},
  {"key_cols", NULL_ATTRID, DB_TYPE_SEQUENCE},
  {"is_primary_key", NULL_ATTRID, DB_TYPE_INTEGER},
  {"status", NULL_ATTRID, DB_TYPE_INTEGER}
};

static CT_ATTR ct_indexkey_atts[] = {
  {"index_of", NULL_ATTRID, DB_TYPE_OBJECT},
  {"table_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"index_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"key_col_name", NULL_ATTRID, DB_TYPE_VARCHAR},
  {"key_order", NULL_ATTRID, DB_TYPE_INTEGER},
  {"asc_desc", NULL_ATTRID, DB_TYPE_INTEGER}
};

CT_CLASS ct_Class = {
  CT_TABLE_NAME,
  NULL_OID_INITIALIZER,
  (sizeof (ct_class_atts) / sizeof (ct_class_atts[0])),
  ct_class_atts
};

CT_CLASS ct_Attribute = {
  CT_COLUMN_NAME,
  NULL_OID_INITIALIZER,
  (sizeof (ct_attribute_atts) / sizeof (ct_attribute_atts[0])),
  ct_attribute_atts
};

CT_CLASS ct_Attrid = {
  NULL,
  NULL_OID_INITIALIZER,
  (sizeof (ct_attrid_atts) / sizeof (ct_attrid_atts[0])),
  ct_attrid_atts
};

CT_CLASS ct_Domain = {
  CT_DOMAIN_NAME,
  NULL_OID_INITIALIZER,
  (sizeof (ct_domain_atts) / sizeof (ct_domain_atts[0])),
  ct_domain_atts
};

CT_CLASS ct_Queryspec = {
  CT_QUERYSPEC_NAME,
  NULL_OID_INITIALIZER,
  (sizeof (ct_queryspec_atts) / sizeof (ct_queryspec_atts[0])),
  ct_queryspec_atts
};

CT_CLASS ct_Index = {
  CT_INDEX_NAME,
  NULL_OID_INITIALIZER,
  (sizeof (ct_index_atts) / sizeof (ct_index_atts[0])),
  ct_index_atts
};

CT_CLASS ct_Indexkey = {
  CT_INDEXKEY_NAME,
  NULL_OID_INITIALIZER,
  (sizeof (ct_indexkey_atts) / sizeof (ct_indexkey_atts[0])),
  ct_indexkey_atts
};

CT_CLASS *ct_Classes[] = {
  &ct_Class,
  &ct_Attribute,
  &ct_Domain,
  &ct_Queryspec,
  &ct_Index,
  &ct_Indexkey,
  NULL
};

#endif /* !CS_MODE */

CATCLS_COLUMN columns_IndexStats[] = {
  {"table_name", "varchar(255)"}
  ,
  {"index_name", "varchar(255)"}
  ,
  {"pages", "int"}
  ,
  {"leafs", "int"}
  ,
  {"height", "int"}
  ,
  {"keys", "bigint"}
  ,
  {"leaf_space_free", "bigint"}
  ,
  {"leaf_pct_free", "double"}
  ,
  {"num_table_vpids", "int"}    /* Number of total pages for file table */
  ,
  {"num_user_pages_mrkdelete", "int"}   /* Num marked deleted pages */
  ,
  {"num_allocsets", "int"}      /* Number of volume arrays */
};

CATCLS_CONSTRAINT cons_IndexStats[] = {
  {
   DB_CONSTRAINT_PRIMARY_KEY, NULL, {"table_name", "index_name", NULL}
   }
  ,
  {
   DB_CONSTRAINT_INDEX, NULL, {"index_name", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"pages", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"leafs", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"height", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"keys", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"leaf_space_free", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"leaf_pct_free", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"num_table_vpids", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"num_user_pages_mrkdelete", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"num_allocsets", NULL}
   }
};

CATCLS_COLUMN columns_LogAnalyzer[] = {
  {"host_ip", "varchar(20)"}
  ,
  {"required_lsa", "bigint"}
  ,
  {"source_applied_time", "bigint"}
  ,
  {"creation_time", "bigint"}
};

CATCLS_CONSTRAINT cons_LogAnalyzer[] = {
  {
   DB_CONSTRAINT_PRIMARY_KEY, NULL, {"host_ip", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"required_lsa", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"source_applied_time", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"creation_time", NULL}
   }
};

CATCLS_COLUMN columns_LogApplier[] = {
  {"host_ip", "varchar(20)"}
  ,
  {"id", "int"}
  ,
  {"committed_lsa", "bigint"}
};

CATCLS_CONSTRAINT cons_LogApplier[] = {
  {
   DB_CONSTRAINT_PRIMARY_KEY, NULL, {"host_ip", "id", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"committed_lsa", NULL}
   }
};

CATCLS_COLUMN columns_ShardGidSkey[] = {
  {"gid", "int"}
  ,
  {"skey", "string"}
};

CATCLS_CONSTRAINT cons_ShardGidSkey[] = {
  {
   DB_CONSTRAINT_PRIMARY_KEY, NULL, {"gid", "skey", NULL}
   }
};

CATCLS_COLUMN columns_ShardGidRemoved[] = {
  {"gid", "int"}
  ,
  {"rem_dt", "datetime"}
};

CATCLS_CONSTRAINT cons_ShardGidRemoved[] = {
  {
   DB_CONSTRAINT_PRIMARY_KEY, NULL, {"gid", NULL}
   }
  ,
  {
   DB_CONSTRAINT_NOT_NULL, NULL, {"rem_dt", NULL}
   }
};

CATCLS_TABLE table_IndexStats = {
  CT_INDEX_STATS_NAME,
  (sizeof (columns_IndexStats) / sizeof (columns_IndexStats[0])),
  columns_IndexStats,
  (sizeof (cons_IndexStats) / sizeof (cons_IndexStats[0])),
  cons_IndexStats
};

CATCLS_TABLE table_LogAnalyzer = {
  CT_LOG_ANALYZER_NAME,
  (sizeof (columns_LogAnalyzer) / sizeof (columns_LogAnalyzer[0])),
  columns_LogAnalyzer,
  (sizeof (cons_LogAnalyzer) / sizeof (cons_LogAnalyzer[0])),
  cons_LogAnalyzer
};

CATCLS_TABLE table_LogApplier = {
  CT_LOG_APPLIER_NAME,
  (sizeof (columns_LogApplier) / sizeof (columns_LogApplier[0])),
  columns_LogApplier,
  (sizeof (cons_LogApplier) / sizeof (cons_LogApplier[0])),
  cons_LogApplier
};

CATCLS_TABLE table_ShardGidSkey = {
  CT_SHARD_GID_SKEY_INFO_NAME,
  (sizeof (columns_ShardGidSkey) / sizeof (columns_ShardGidSkey[0])),
  columns_ShardGidSkey,
  (sizeof (cons_ShardGidSkey) / sizeof (cons_ShardGidSkey[0])),
  cons_ShardGidSkey
};

CATCLS_TABLE table_ShardGidRemoved = {
  CT_SHARD_GID_REMOVED_INFO_NAME,
  (sizeof (columns_ShardGidRemoved) / sizeof (columns_ShardGidRemoved[0])),
  columns_ShardGidRemoved,
  (sizeof (cons_ShardGidRemoved) / sizeof (cons_ShardGidRemoved[0])),
  cons_ShardGidRemoved
};

/*
 * tf_compile_meta_classes - passes over the static meta class definitions
 * and fills in the missing fields that are too error prone to keep
 * calculating by hand.
 *    return: void
 * Note:
 *   Once this becomes reasonably static, this could be statically coded again.
 *   This is only used on the client but it won't hurt anything to have it on
 *   the server as well.
 */
void
tf_compile_meta_classes ()
{
  META_CLASS *class_;
  META_ATTRIBUTE *att;
  TP_DOMAIN *domain;
  int c, i;

  for (c = 0; Meta_classes[c] != NULL; c++)
    {
      class_ = Meta_classes[c];

      class_->n_variable = class_->fixed_size = 0;

      for (i = 0; class_->atts[i].name != NULL; i++)
        {
          att = &class_->atts[i];
          att->id = i;

          if (pr_is_variable_type (att->type))
            {
              class_->n_variable++;
            }
          else if (class_->n_variable)
            {
              /*
               * can't have fixed width attributes AFTER variable width
               * attributes
               */
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_INVALID_METACLASS, 0);
            }
          else
            {
              /*
               * need a domain for size calculations, since we don't use
               * any parameterized types this isn't necessary but we still must
               * have it to call tp_domain_isk_size().
               */
              domain = tp_domain_resolve_default (att->type);
              class_->fixed_size += tp_domain_disk_size (domain);
            }
        }
    }
}

#if !defined(CS_MODE)
/*
 * tf_install_meta_classes - dummy function
 *    return: NO_ERROR
 * Note:
 *    This is called during database formatting and generates the catalog
 *    entries for all the meta classes.
 */
int
tf_install_meta_classes ()
{
  /*
   * no longer making catalog entries, eventually build the meta-class object
   * here
   */
  return NO_ERROR;
}
#endif /* CS_MODE */
