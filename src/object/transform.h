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
 * transform.h: Definitions for the transformer shared between the client and
 *            server.
 */

#ifndef _TRANSFORM_H_
#define _TRANSFORM_H_

#ident "$Id$"

/*
 * META_ATTRIBUTE, META_CLASS
 *    These are the structure definitions for the meta class information.
 *    They will be built statically and used for the generation of the
 *    catalog information for class objects.
 */

typedef struct tf_meta_attribute
{
  const char *name;
  DB_TYPE type;
  int visible;
  const char *domain_string;
  int substructure;
  int id;
  void *extended_domain;	/* filled in on the client side */
} META_ATTRIBUTE;

typedef struct tf_meta_class
{
  const char *name;
  OID classoid;
  int repid;
  int n_variable;
  int fixed_size;
  META_ATTRIBUTE *atts;
} META_CLASS;

#if !defined(CS_MODE)
typedef struct tf_ct_attribute
{
  const char *name;
  int id;
  DB_TYPE type;
} CT_ATTR;

typedef struct tf_ct_class
{
  const char *name;
  OID classoid;
  int n_atts;
  CT_ATTR *atts;
} CT_CLASS;
#endif /* !CS_MODE */

typedef struct catcls_column CATCLS_COLUMN;
struct catcls_column
{
  const char *name;
  const char *type;
};

typedef struct catcls_constraint CATCLS_CONSTRAINT;
struct catcls_constraint
{
  DB_CONSTRAINT_TYPE type;
  const char *name;
  const char *atts[MAX_INDEX_KEY_LIST_NUM + 1];	/* +1 is end of atts */
};

typedef struct catcls_table CATCLS_TABLE;
struct catcls_table
{
  const char *name;
  int num_columns;
  CATCLS_COLUMN *columns;
  int num_constraints;
  CATCLS_CONSTRAINT *constraint;
};


/*
 * Meta OID information
 *    The meta-objects are given special system OIDs in the catalog.
 *    These don't map to actual physical locations but are used to
 *    tag the disk representations of classes with appropriate catalog
 *    keys.
 */

#define META_VOLUME			256

#define META_PAGE_CLASS			0
#define META_PAGE_ROOT			1
#define META_PAGE_REPRESENTATION	2
#define META_PAGE_RESOLUTION		3
#define META_PAGE_DOMAIN		4
#define META_PAGE_ATTRIBUTE		5
#define META_PAGE_METHARG		6
#define META_PAGE_METHSIG		7
#define META_PAGE_METHOD		8
#define META_PAGE_METHFILE		9
#define META_PAGE_REPATTRIBUTE	        10
#define META_PAGE_QUERY_SPEC		11
#define META_PAGE_CONSTRAINT            12
#define META_PAGE_CONSTRAINT_ATTRIBUTE  13

/*
 * Metaclass names
 *    Names for each of the meta classes.
 *    These can be used in query statements to query the schema.
 */

#define META_CLASS_NAME		        "sqlx_class"
#define META_ATTRIBUTE_NAME		"sqlx_attribute"
#define META_DOMAIN_NAME		"sqlx_domain"
#define META_METHFILE_NAME		"sqlx_method_file"
#define META_REPRESENTATION_NAME	"sqlx_representation"
#define META_REPATTRIBUTE_NAME	        "sqlx_repattribute"
#define META_QUERY_SPEC_NAME		"sqlx_query_spec"
#define META_CONSTRAINT_NAME            "sqlx_constraint"
#define META_CONSTRAINT_ATTRIBUTE_NAME  "sqlx_constraint_attribute"

/* catalog classes */
#define CT_TABLE_NAME              "db_table"
#define CT_COLUMN_NAME             "db_column"
#define CT_DOMAIN_NAME             "db_domain"
#define CT_QUERYSPEC_NAME          "db_query_spec"
#define CT_INDEX_NAME              "db_index"
#define CT_INDEXKEY_NAME           "db_index_key"
#define CT_AUTH_NAME		   "db_auth"
#define CT_DATATYPE_NAME           "db_data_type"
#define CT_INDEX_STATS_NAME        "db_index_stats"
#define CT_LOG_ANALYZER_NAME       "db_log_analyzer"
#define CT_LOG_APPLIER_NAME        "db_log_applier"
#define CT_COLLATION_NAME          "db_collation"
#define CT_USER_NAME               "db_user"
#define CT_ROOT_NAME               "db_root"
#define CT_SHARD_GID_SKEY_INFO_NAME	"db_shard_gid_skey_info"
#define CT_SHARD_GID_REMOVED_INFO_NAME	"db_shard_gid_removed_info"

#define CT_DBCOLL_COLL_ID_COLUMN	   "coll_id"
#define CT_DBCOLL_COLL_NAME_COLUMN	   "coll_name"
#define CT_DBCOLL_CHARSET_ID_COLUMN	   "charset_id"
#define CT_DBCOLL_BUILT_IN_COLUMN	   "built_in"
#define CT_DBCOLL_EXPANSIONS_COLUMN	   "expansions"
#define CT_DBCOLL_CONTRACTIONS_COLUMN	   "contractions"
#define CT_DBCOLL_UCA_STRENGTH		   "uca_strength"
#define CT_DBCOLL_CHECKSUM_COLUMN	   "checksum"

/*
 * Metaclass definitions
 *    Static definitions of the meta classes.
 */

extern META_CLASS tf_Metaclass_root;
extern META_CLASS tf_Metaclass_class;
extern META_CLASS tf_Metaclass_representation;
extern META_CLASS tf_Metaclass_attribute;
extern META_CLASS tf_Metaclass_domain;
extern META_CLASS tf_Metaclass_repattribute;
extern META_CLASS tf_Metaclass_query_spec;
extern META_CLASS tf_Metaclass_constraint;
extern META_CLASS tf_Metaclass_constraint_attribute;

#if !defined(CS_MODE)
extern CT_CLASS ct_Class;
extern CT_CLASS ct_Attribute;
extern CT_CLASS ct_Attrid;
extern CT_CLASS ct_Domain;
extern CT_CLASS ct_Queryspec;
extern CT_CLASS ct_Index;
extern CT_CLASS ct_Indexkey;
extern CT_CLASS *ct_Classes[];
#endif /* !CS_MODE */


extern CATCLS_TABLE table_IndexStats;
extern CATCLS_TABLE table_LogAnalyzer;
extern CATCLS_TABLE table_LogApplier;
extern CATCLS_TABLE table_ShardGidSkey;
extern CATCLS_TABLE table_ShardGidRemoved;


/* This fills in misc information missing from the static definitions */
extern void tf_compile_meta_classes (void);

/* This is available only on the server for catalog initialization */

#if !defined(CS_MODE)
extern int tf_install_meta_classes (void);
#endif /* !CS_MODE */

#endif /* _TRANSFORM_H_ */
