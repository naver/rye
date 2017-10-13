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
 * class_object.h - Definitions for structures used in the representation
 *                  of classes
 */

#ifndef _CLASS_OBJECT_H_
#define _CLASS_OBJECT_H_

#ident "$Id$"

#include "object_domain.h"
#include "work_space.h"
#include "object_primitive.h"
#include "storage_common.h"
#include "statistics.h"

/*
 *    This macro should be used whenever comparisons need to be made
 *    on the class or component names. Basically this will perform
 *    a case insensitive comparison
 */
#define SM_COMPARE_NAMES intl_identifier_casecmp

/*
 *    Shorthand macros for iterating over a component, attribute
 */

#define SM_IS_ATTFLAG_UNIQUE_FAMILY(c) \
        (((c) == SM_ATTFLAG_UNIQUE            || \
	  (c) == SM_ATTFLAG_PRIMARY_KEY)       \
          ? true : false)

#define SM_IS_ATTFLAG_INDEX_FAMILY(c) \
        ((SM_IS_ATTFLAG_UNIQUE_FAMILY(c)      || \
         (c) == SM_ATTFLAG_INDEX)        \
         ? true : false)

#define SM_MAP_INDEX_ATTFLAG_TO_CONSTRAINT(c) \
	((c) == SM_ATTFLAG_UNIQUE         ? SM_CONSTRAINT_UNIQUE : \
	 (c) == SM_ATTFLAG_PRIMARY_KEY    ? SM_CONSTRAINT_PRIMARY_KEY : \
	 (c) == SM_ATTFLAG_INDEX          ? SM_CONSTRAINT_INDEX : \
	                                    SM_CONSTRAINT_NOT_NULL)

#define SM_MAP_CONSTRAINT_ATTFLAG_TO_PROPERTY(c) \
	((c) == SM_ATTFLAG_UNIQUE         ? SM_PROPERTY_UNIQUE: \
	 (c) == SM_ATTFLAG_PRIMARY_KEY    ? SM_PROPERTY_PRIMARY_KEY: \
	 (c) == SM_ATTFLAG_INDEX          ? SM_PROPERTY_INDEX: \
	                                    SM_PROPERTY_NOT_NULL)

#define SM_MAP_CONSTRAINT_TO_ATTFLAG(c) \
	((c) == DB_CONSTRAINT_UNIQUE         ? SM_ATTFLAG_UNIQUE: \
	 (c) == DB_CONSTRAINT_PRIMARY_KEY    ? SM_ATTFLAG_PRIMARY_KEY: \
	 (c) == DB_CONSTRAINT_INDEX          ? SM_ATTFLAG_INDEX: \
	                                       SM_ATTFLAG_NON_NULL)

#define SM_MAP_DB_INDEX_CONSTRAINT_TO_SM_CONSTRAINT(c) \
	((c) == DB_CONSTRAINT_UNIQUE         ? SM_CONSTRAINT_UNIQUE: \
	 (c) == DB_CONSTRAINT_PRIMARY_KEY    ? SM_CONSTRAINT_PRIMARY_KEY: \
	 (c) == DB_CONSTRAINT_INDEX          ? SM_CONSTRAINT_INDEX: \
	                                       SM_CONSTRAINT_NOT_NULL)

#define SM_IS_CONSTRAINT_UNIQUE_FAMILY(c) \
        (((c) == SM_CONSTRAINT_UNIQUE          || \
	  (c) == SM_CONSTRAINT_PRIMARY_KEY)    \
          ? true : false )

#define SM_IS_CONSTRAINT_INDEX_FAMILY(c) \
        ((SM_IS_CONSTRAINT_UNIQUE_FAMILY(c)    || \
         (c) == SM_CONSTRAINT_INDEX)      \
         ? true : false )

#define SM_MAP_INDEX_ATTFLAG_TO_INDEX_TYPE_ERROR(c) \
        (SM_IS_ATTFLAG_UNIQUE_FAMILY(c) ? ER_SM_INVALID_UNIQUE_TYPE: \
         (c) == SM_ATTFLAG_INDEX         ? ER_SM_INVALID_INDEX_TYPE: \
                                            assert(false),ER_SM_INVALID_INDEX_TYPE)
#define SM_MAP_INDEX_ATTFLAG_TO_VCLASS_ERROR(c) \
        (SM_IS_ATTFLAG_UNIQUE_FAMILY(c) ? ER_SM_UNIQUE_ON_VCLASS: \
          (c) == SM_ATTFLAG_INDEX        ? ER_SM_UNIQUE_ON_VCLASS: \
                                            assert(false),ER_SM_UNIQUE_ON_VCLASS)

/*
 *    This constant defines the maximum size in bytes of a class name,
 *    attribute name, or any other named entity in the schema.
 */
#define SM_MAX_IDENTIFIER_LENGTH 255

/*
 *  c : constraint_type
 */
#define SM_GET_CONSTRAINT_STRING(c) \
	((c) == DB_CONSTRAINT_UNIQUE         ? "UNIQUE": \
	 (c) == DB_CONSTRAINT_PRIMARY_KEY    ? "PRIMARY KEY": \
	 (c) == DB_CONSTRAINT_INDEX          ? "INDEX": \
	 				       "NOT NULL")

typedef struct tp_domain SM_DOMAIN;

/*
 *    Bit field identifiers for attribute flags.  These could be defined
 *    with individual unsigned bit fields but this makes it easier
 *    to save them as a single integer.
 *    The "new" flag is used only at run time and shouldn't be here.
 *    Need to re-design the template functions to operate from a different
 *    memory structure during flattening.
 */
typedef enum
{
  SM_ATTFLAG_INDEX = 1,		/* attribute has an index 0x01 */
  SM_ATTFLAG_UNIQUE = 2,	/* attribute has UNIQUE constraint 0x02 */
  SM_ATTFLAG_NON_NULL = 4,	/* attribute has NON_NULL constraint 0x04 */
#if 0				/* unused */
  SM_ATTFLAG_VID = 8,		/* attribute is part of virtual object id 0x08 */
#endif
  SM_ATTFLAG_NEW = 16,		/* is a new attribute  0x10 */
  SM_ATTFLAG_PRIMARY_KEY = 32,	/* attribute has a primary key 0x20 */
  SM_ATTFLAG_SHARD_KEY = 64	/* attribute is the shard key 0x40 */
} SM_ATTRIBUTE_FLAG;

/* attribute constraint types */
typedef enum
{
  SM_CONSTRAINT_UNIQUE,
  SM_CONSTRAINT_INDEX,
  SM_CONSTRAINT_NOT_NULL,
  SM_CONSTRAINT_PRIMARY_KEY,
  SM_CONSTRAINT_LAST = SM_CONSTRAINT_PRIMARY_KEY
} SM_CONSTRAINT_TYPE;

/*
 *    These are used as tags in the SM_CLASS structure and indicates one
 *    of the several class types
 */
typedef enum
{
  SM_CLASS_CT,			/* default SQL/X class */
  SM_VCLASS_CT			/* component db virtual class */
} SM_CLASS_TYPE;

/*
 *    Flags for misc information about a class.  These must be defined
 *    as powers of two because they are stored packed in a single integer.
 */
typedef enum
{
  SM_CLASSFLAG_SYSTEM = 1,	/* a system defined class */
#if 0				/* unused */
  SM_CLASSFLAG_WITHCHECKOPTION = 2,	/* a view with check option */
  SM_CLASSFLAG_LOCALCHECKOPTION = 4,	/* view w/local check option */
  SM_CLASSFLAG_REUSE_OID = 8,	/* the class can reuse OIDs */
#endif
  SM_CLASSFLAG_SHARD_TABLE = 16	/* a shard table */
} SM_CLASS_FLAG;

/*
 *    These are used to tag the "meta" objects
 *    This type is used in the definition of SM_CLASS_HEADER
 */
typedef enum
{
  Meta_root,			/* the object is the root class */
  Meta_class			/* the object is a normal class */
} SM_METATYPE;


/*
 *    This is used at the top of all "meta" objects that are represented
 *    with C structures rather than in the usual instance memory format
 *    It serves as a convenient way to store common information
 *    for the class objects and the root class object and eliminates
 *    a lot of special case checking
 */
typedef struct sm_class_header SM_CLASS_HEADER;

struct sm_class_header
{
  SM_METATYPE type;		/* doesn't need to be a full word */
  const char *name;

  HFID heap;
};


/*
 *    Structure used to cache an attribute constraint.  Attribute constraints
 *    are maintained in the property list.  They are also cached using this
 *    structure for faster retrieval
 */
typedef struct sm_constraint SM_CONSTRAINT;

struct sm_constraint
{
  struct sm_constraint *next;

  char *name;
  SM_CONSTRAINT_TYPE type;
  int status;
  BTID index;
};

/*
 *    This structure is used as a header for attribute
 *    so they can be manipulated by generic functions written to
 *    operate on heterogeneous lists of attributes
 */

typedef struct sm_default_value SM_DEFAULT_VALUE;
struct sm_default_value
{
  DB_VALUE original_value;	/* initial default value; */
  DB_VALUE value;		/* current default/class value */
  DB_DEFAULT_EXPR_TYPE default_expr;	/* identifier for the default
					 * expression */
};

typedef struct sm_attribute SM_ATTRIBUTE;

/*
 *      NOTE:
 *      Regarding "original_value" and "value".
 *      "value" keeps the current value of default/class value,
 *      and "original_value" is used for default value only and keeps
 *      the first value given as the default value. "first" means that
 *      "adding new attribute with default value" or "setting a default
 *      value to an existing attribute which does not have it".
 *      "original_value" will be used to fetch records which do not have
 *      the attribute, in other words, their representations are different
 *      with the last representation. It will replace unbound value
 *      of the attribute. Therefore it should be always propagated to the
 *      last representation. See the following example.
 *
 *      create table x (i int);
 *      -- Insert a record with the first repr.
 *      insert into x values (1);
 *
 *      alter table x add attribute s varchar(32) default 'def1';
 *      -- The second repr has column "s" with default value 'def1'.
 *      -- 'def1' was copied to "original_value" and "value" of column "s".
 *
 *      select * from x;
 *         i  s
 *      ===================================
 *         1  'def1' <- This is from "original_value" of the last(=2nd) repr.
 *
 *      alter table x change column s default 'def2';
 *      -- At this point, the third repr also has a copy of "original_value"
 *      -- of the second repr.
 *
 *      insert into x values (2, default);
 *      select * from x;
 *         i  s
 *      ===================================
 *         1  'def1' <- This is from "original_value" of the last(=3rd) repr.
 *         2  'def2'
 *
 */
struct sm_attribute
{
  struct sm_attribute *next;

  const char *name;		/* name */
  PR_TYPE *type;		/* basic type */
  TP_DOMAIN *sma_domain;	/* allowable types */

  MOP class_mop;		/* origin class */

  int id;			/* unique id number */
  int offset;			/* memory offset */

  SM_DEFAULT_VALUE default_value;	/* default value */

  SM_CONSTRAINT *constraints;	/* cached constraint list */

  /* see tfcl and the discussion on attribute extensions */
  DB_SEQ *properties;		/* property list */

  unsigned int flags;		/* bit flags */
  int order;			/* definition order number */
  struct sm_attribute *order_link;	/* list in definition order */

  int storage_order;		/* storage order number */
};

typedef struct sm_class_constraint_attribute SM_DISK_CONSTRAINT_ATTRIBUTE;
struct sm_class_constraint_attribute
{
  struct sm_class_constraint_attribute *next;

  char *name;
  int att_id;
  int asc_desc;
};

typedef struct sm_class_constraint SM_CLASS_CONSTRAINT;
struct sm_class_constraint
{
  struct sm_class_constraint *next;

  const char *name;
  int num_atts;
  SM_ATTRIBUTE **attributes;
  int *asc_desc;		/* asc/desc info list */
  BTID index_btid;
  int index_status;
  SM_CONSTRAINT_TYPE type;
};

typedef struct sm_disk_constraint SM_DISK_CONSTRAINT;
struct sm_disk_constraint
{
  struct sm_disk_constraint *next;

  const char *name;
  BTID index_btid;
  int index_status;
  SM_CONSTRAINT_TYPE type;

  int num_atts;
  SM_DISK_CONSTRAINT_ATTRIBUTE *disk_info_of_atts;
};

/*
 *    This contains information about an instance attribute in an
 *    obsolete representation.  We need only keep the information required
 *    by the transformer to convert the old instances to the newest
 *    representation.
 */
typedef struct sm_repr_attribute SM_REPR_ATTRIBUTE;

struct sm_repr_attribute
{
  struct sm_repr_attribute *next;

  int attid;			/* old attribute id */
  DB_TYPE typeid_;		/* type id */
  TP_DOMAIN *domain;		/* full domain, think about merging with type id */
};

/*
 *    These contain information about old class representations so that
 *    obsolete objects can be converted to the latest representation as
 *    they are encountered.  Only the minimum amount of information required
 *    to do the conversion is kept.  Since
 *    class attributes do not effect the disk representation of instances,
 *    they are not part of the representation.
 */
typedef struct sm_representation SM_REPRESENTATION;

struct sm_representation
{
  struct sm_representation *next;

  SM_REPR_ATTRIBUTE *attributes;	/* list of attribute descriptions */
  int id;			/* unique identifier for this rep */
  int fixed_count;		/* number of fixed attributes */
  int variable_count;		/* number of variable attributes */
};

/*
 *    This is used in virtual and component class definitions.
 *    It represents in text form the query(s) which can instantiate a class.
 */
typedef struct sm_query_spec SM_QUERY_SPEC;

struct sm_query_spec
{
  struct sm_query_spec *next;

  const char *specification;
};

typedef struct sm_class SM_CLASS;
typedef struct sm_template *SMT;
typedef struct sm_template SM_TEMPLATE;
/*
 *    This is the primary class structure.  Most of the other
 *    structures in this file are attached to this at some level.
 */
struct sm_class
{
  SM_CLASS_HEADER header;

  SM_CLASS_TYPE class_type;	/* what kind of class variant is this? */
  int repid;			/* current representation id */

  SM_REPRESENTATION *representations;	/* list of old representations */

  int object_size;		/* memory size in bytes */
  int att_count;		/* number of instance attributes */
  SM_ATTRIBUTE *attributes;	/* list of instance attribute definitions */

  const char *loader_commands;	/* command string to the dynamic loader */

  int fixed_count;		/* number of fixed size attributes */
  int variable_count;		/* number of variable size attributes */
  int fixed_size;		/* byte size of fixed attributes */

  int att_ids;			/* attribute id counter */
  int unused;			/* formerly repid counter, delete */

  SM_QUERY_SPEC *query_spec;	/* virtual class query_spec information */
  SM_TEMPLATE *new_;		/* temporary structure */
  CLASS_STATS *stats;		/* server statistics, loaded on demand */

  MOP owner;			/* authorization object */
  int unused_02;		/* class collation, delete */
  void *auth_cache;		/* compiled cache */

  SM_ATTRIBUTE *ordered_attributes;	/* see classobj_fixup_loaded_class () */
  struct parser_context *virtual_query_cache;
  SM_CLASS_CONSTRAINT *constraints;	/* Constraint cache */
  SM_DISK_CONSTRAINT *disk_constraints;	/* Disk strucure of Constraint */

  unsigned int flags;
  unsigned int virtual_cache_schema_id;

  unsigned post_load_cleanup:1;	/* set if post load cleanup has occurred */

};



struct sm_template
{
  MOP op;			/* class MOP (if editing existing class) */
  SM_CLASS *current;		/* current class structure (if editing existing) */
  SM_CLASS_TYPE class_type;	/* what kind of class variant is this? */
  int tran_index;		/* transaction index when template was created */

  const char *name;		/* class name */

  SM_ATTRIBUTE *attributes;	/* instance attribute definitions */

  const char *loader_commands;	/* loader commands */

  SM_QUERY_SPEC *query_spec;	/* query_spec list */

  SM_ATTRIBUTE *instance_attributes;

  SM_DISK_CONSTRAINT *disk_constraints;
};


/*
 *    This is used for "browsing" functions that need to obtain a lot
 *    of information about the class but do not want to go through the full
 *    overhead of object de-referencing for each piece of information.
 *    It encapsulates a snapshot of a class definition that can be
 *    walked through as necessary.  Also since the copy is not part of
 *    an actual database object, we don't have to worry about swapping
 *    or GCing the structure out from under the caller.
 */
typedef struct sm_class_info SM_CLASS_INFO;
struct sm_class_info
{
  const char *name;
  DB_OBJECT *owner;		/* owner's user object */

  SM_CLASS_TYPE class_type;	/* what kind of class variant is this? */
  int att_count;		/* number of instance attributes */
  SM_ATTRIBUTE *attributes;	/* list of attribute definitions */
  SM_QUERY_SPEC *query_spec;	/* virtual class query_spec list */

  unsigned int flags;		/* persistent flags */
};


/*
 *    This structure is used to maintain a list of class/component mappings
 *    in an attribute descriptor.  Since the same descriptor
 *    can be applied an instance of any subclass, we dynamically cache
 *    up pointers to the subclass components as we need them.
 */

#if defined (ENABLE_UNUSED_FUNCTION)
typedef struct sm_descriptor_list SM_DESCRIPTOR_LIST;
struct sm_descriptor_list
{
  struct sm_descriptor_list *next;

  MOP classobj;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  unsigned write_access:1;
};
#endif

#if 0
typedef struct sm_validation SM_VALIDATION;
struct sm_validation
{
  DB_OBJECT *last_class;	/* DB_TYPE_OBJECT validation cache */
  DB_OBJLIST *validated_classes;

  DB_DOMAIN *last_setdomain;	/* DB_TYPE_COLLECTION validation cache */

  DB_TYPE last_type;		/* Other validation caches */
  int last_precision;
  int last_scale;
};
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 *    This structure is used as a "descriptor" for improved
 *    performance on repeated access to an attribute.
 */
typedef struct sm_descriptor SM_DESCRIPTOR;
struct sm_descriptor
{
  struct sm_descriptor *next;

  char *name;			/* component name */

  SM_DESCRIPTOR_LIST *map;	/* class/component map */
  SM_VALIDATION *valid;		/* validation cache */

  DB_OBJECT *class_mop;		/* root class */
};
#endif

extern const int SM_MAX_STRING_LENGTH;

/*
 * These are the names for the system defined properties on classes,
 * attributes.  For the built in properties, try
 * to use short names.  User properties if they are ever allowed
 * should have more descriptive names.
 *
 * Lets adopt the convention that names beginning with a '*' are
 * reserved for system properties.
 */
#define SM_PROPERTY_UNIQUE "*U"
#define SM_PROPERTY_INDEX "*I"
#define SM_PROPERTY_NOT_NULL "*N"
#define SM_PROPERTY_PRIMARY_KEY "*P"

#define SM_PROPERTY_NUM_INDEX_FAMILY         3

/*
 * index form : {status, "volid|pageid|fileid", ["attr", asc_desc]+ }
 *               {status, "volid|pageid|fileid", ["attr", asc_desc]+ }
 */
#define SM_PROP_INDEX_STATUS_POS                0
#define SM_PROP_INDEX_BTID_POS                  1
#define SM_PROP_INDEX_FIRST_ATTID_POS           2
#define SM_PROP_INDEX_FIRST_ASC_DESC_POS        3

#define INDEX_STATUS_INIT 0
#define INDEX_STATUS_IN_PROGRESS 1
#define INDEX_STATUS_COMPLETED 2

/* Threaded arrays */
extern DB_LIST *classobj_alloc_threaded_array (int size, int count);
extern void classobj_free_threaded_array (DB_LIST * array, LFREEER clear);

/* Property lists */
extern DB_SEQ *classobj_make_prop (void);
extern int classobj_copy_props (DB_SEQ * properties, DB_SEQ ** new_);
extern void classobj_free_prop (DB_SEQ * properties);
extern int classobj_get_prop (DB_SEQ * properties, const char *name,
			      DB_VALUE * pvalue);
extern int classobj_put_prop (DB_SEQ * properties, const char *name,
			      DB_VALUE * pvalue);
extern int classobj_drop_prop (DB_SEQ * properties, const char *name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int classobj_find_prop_constraint (DB_SEQ * properties,
					  const char *prop_name,
					  const char *cnstr_name,
					  DB_VALUE * cnstr_val);
#endif
extern int classobj_get_cached_index_family (SM_CONSTRAINT * constraints,
					     SM_CONSTRAINT_TYPE type,
					     BTID * id);
extern bool classobj_has_unique_constraint (SM_CONSTRAINT * constraints);
extern int classobj_decompose_property_btid (const char *buffer, BTID * btid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int classobj_decompose_property_oid (const char *buffer, OID * oid);
#endif
extern int classobj_btid_from_property_value (DB_VALUE * value, BTID * btid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int classobj_oid_from_property_value (DB_VALUE * value, OID * oid);
#endif

/* Constraints */
extern bool classobj_cache_constraints (SM_CLASS * class_);

extern void
classobj_free_disk_constraint_attribute (SM_DISK_CONSTRAINT_ATTRIBUTE *
					 cons_att);
extern void classobj_free_class_constraint (SM_CLASS_CONSTRAINT * cons);
extern void classobj_decache_class_constraints (SM_CLASS * class_);
#if 1				/* TODO:[happy] */
extern int classobj_cache_class_constraints (SM_CLASS * class_);
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
extern SM_CLASS_CONSTRAINT
  * classobj_find_class_constraint (SM_CLASS_CONSTRAINT * constraints,
				    SM_CONSTRAINT_TYPE type,
				    const char *name);
#endif
extern SM_CLASS_CONSTRAINT *classobj_find_class_index (SM_CLASS * class_,
						       const char *name);
extern SM_CLASS_CONSTRAINT
  * classobj_find_constraint_by_name (SM_CLASS_CONSTRAINT * cons_list,
				      const char *name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void classobj_remove_class_constraint_node (SM_CLASS_CONSTRAINT **
						   constraints,
						   SM_CLASS_CONSTRAINT *
						   node);
#endif

extern int classobj_make_class_constraints (SM_CLASS_CONSTRAINT ** new_list,
					    SM_ATTRIBUTE * attributes,
					    SM_DISK_CONSTRAINT *
					    disk_cons_list);

/* Disk Constraints */
extern SM_DISK_CONSTRAINT *classobj_make_disk_constraint (void);
extern void classobj_free_disk_constraint (SM_DISK_CONSTRAINT * disk_cons);
extern SM_DISK_CONSTRAINT *classobj_find_disk_constraint (SM_DISK_CONSTRAINT *
							  disk_cons_list,
							  const char *name);

#if defined (ENABLE_UNUSED_FUNCTION)
extern bool classobj_class_has_indexes (SM_CLASS * class_);
#endif

/* Attribute */
extern SM_ATTRIBUTE *classobj_make_attribute (const char *name,
					      PR_TYPE * type);
extern SM_ATTRIBUTE *classobj_copy_attribute (SM_ATTRIBUTE * src,
					      const char *alias);
extern int classobj_copy_att_ordered_list (SM_ATTRIBUTE * attlist,
					   SM_ATTRIBUTE ** copy_ptr);

extern void classobj_free_attribute (SM_ATTRIBUTE * att);

/* Representation attribute */
extern SM_REPR_ATTRIBUTE *classobj_make_repattribute (int attid,
						      DB_TYPE typeid_,
						      TP_DOMAIN * domain);

/* Representation */
extern SM_REPRESENTATION *classobj_make_representation (void);
extern void classobj_free_representation (SM_REPRESENTATION * rep);

/* Query_spec */
extern SM_QUERY_SPEC *classobj_make_query_spec (const char *);
extern SM_QUERY_SPEC *classobj_copy_query_spec (SM_QUERY_SPEC *);
extern void classobj_free_query_spec (SM_QUERY_SPEC *);

/* constraint */
extern SM_CLASS_CONSTRAINT *classobj_make_class_constraint (void);
extern SM_DISK_CONSTRAINT_ATTRIBUTE
  * classobj_make_disk_constraint_attribute (void);
extern SM_DISK_CONSTRAINT_ATTRIBUTE
  * classobj_copy_disk_constraint_attribute (SM_DISK_CONSTRAINT_ATTRIBUTE *
					     src);
extern int classobj_put_disk_constraint (SM_DISK_CONSTRAINT ** disk_cons_list,
					 SM_CLASS_CONSTRAINT * src_cons);
/* Editing template */
extern SM_TEMPLATE *classobj_make_template (const char *name, MOP op,
					    SM_CLASS * class_);
extern SM_TEMPLATE *classobj_make_template_like (const char *name,
						 SM_CLASS * class_);
extern void classobj_free_template (SM_TEMPLATE * template_ptr);
/* disk constraint */
extern SM_DISK_CONSTRAINT *classobj_copy_disk_constraint (SM_DISK_CONSTRAINT *
							  src);

/* Class */
extern SM_CLASS *classobj_make_class (const char *name);
extern void classobj_free_class (SM_CLASS * class_);
extern int classobj_class_size (SM_CLASS * class_);

extern int classobj_install_template (SM_CLASS * class_, SM_TEMPLATE * flat,
				      int saverep);

#if defined (ENABLE_UNUSED_FUNCTION)
extern SM_REPRESENTATION *classobj_find_representation (SM_CLASS * class_,
							int id);
#endif

extern void classobj_fixup_loaded_class (SM_CLASS * class_);

extern SM_ATTRIBUTE *classobj_find_attribute (SM_ATTRIBUTE * attlist,
					      const char *name);
extern SM_ATTRIBUTE *classobj_find_attribute_id (SM_ATTRIBUTE * attlist,
						 int id);


extern const char **classobj_point_at_att_names (SM_CLASS_CONSTRAINT *
						 constraint, int *count_ref);
/* Descriptors */
#if defined (ENABLE_UNUSED_FUNCTION)
extern SM_DESCRIPTOR *classobj_make_descriptor (MOP class_mop,
						SM_CLASS * classobj,
						SM_ATTRIBUTE * att,
						int write_access);
extern SM_DESCRIPTOR_LIST *classobj_make_desclist (MOP class_mop,
						   SM_CLASS * classobj,
						   SM_ATTRIBUTE * att,
						   int write_access);

extern void classobj_free_desclist (SM_DESCRIPTOR_LIST * dl);
extern void classobj_free_descriptor (SM_DESCRIPTOR * desc);
#endif

/* Debug */
#if defined (RYE_DEBUG)
extern void classobj_print (SM_CLASS * class_);
#endif
/* primary key */
extern SM_CLASS_CONSTRAINT *classobj_find_class_primary_key (SM_CLASS *
							     class_);
extern SM_CLASS_CONSTRAINT
  * classobj_find_cons_primary_key (SM_CLASS_CONSTRAINT * cons_list);

#if defined (ENABLE_UNUSED_FUNCTION)
extern const char *classobj_map_constraint_to_property (SM_CONSTRAINT_TYPE
							constraint);
#endif
extern int classobj_check_index_exist (SM_CLASS_CONSTRAINT * constraints,
				       const char *class_name,
				       const char *constraint_name);
SM_ATTRIBUTE *classobj_find_shard_key_column (SM_CLASS * class_);
#endif /* _CLASS_OBJECT_H_ */
