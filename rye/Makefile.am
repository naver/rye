
#
# Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
#
#   This program is free software; you can redistribute it and/or modify 
#   it under the terms of the GNU General Public License as published by 
#   the Free Software Foundation; either version 2 of the License, or 
#   (at your option) any later version. 
#
#  This program is distributed in the hope that it will be useful, 
#  but WITHOUT ANY WARRANTY; without even the implied warranty of 
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
#  GNU General Public License for more details. 
#
#  You should have received a copy of the GNU General Public License 
#  along with this program; if not, write to the Free Software 
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#


SRC = $(top_srcdir)/src
COMPAT_DIR = $(SRC)/compat
THREAD_DIR = $(SRC)/thread
BASE_DIR = $(SRC)/base
HEAPLAYER_DIR = $(SRC)/heaplayers
CONNECTION_DIR = $(SRC)/connection
COMMUNICATION_DIR = $(SRC)/communication
PARSER_DIR = $(SRC)/parser
OPTIMIZER_DIR = $(SRC)/optimizer
QUERY_DIR = $(SRC)/query
OBJECT_DIR = $(SRC)/object
TRANSACTION_DIR = $(SRC)/transaction
REPLICATION_DIR = $(SRC)/replication
STORAGE_DIR = $(SRC)/storage
EXECUTABLE_DIR = $(SRC)/executables
BROKER_DIR = $(SRC)/broker
CCI_DIR = $(SRC)/cci
DBMT_DIR = $(SRC)/dbmt
QATOOLS_DIR = $(SRC)/qatools
SESSION_DIR = $(SRC)/session

AM_CPPFLAGS = @SRC_INC@ -I$(HEAPLAYER_DIR)/util @EXT_INC@ \
	@SYS_DEFS@ @SERVER_DEFS@ @COMMON_DEFS@ @VERSION_DEFS@ @DIR_DEFS@

#AM_CFLAGS = @CFLAGS@ -Werror
#AM_CXXFLAGS = @CXXFLAGS@ -Werror
AM_CFLAGS = @CFLAGS@
AM_CXXFLAGS = @CXXFLAGS@

#BUILT_SOURCES = cnvlex.c
#CLEANFILES = cnvlex.c

#AM_LFLAGS = -8 -L -S$(COMPAT_DIR)/flex.skel -t
#cnvlex.c: $(COMPAT_DIR)/cnvlex.l
#	$(LEX) $(LFLAGS) $(AM_LFLAGS) $< > $@


include_HEADERS = \
	$(COMPAT_DIR)/dbi.h

COMPAT_SOURCES = \
	$(COMPAT_DIR)/cnv.c \
	$(COMPAT_DIR)/cnvlex.c \
	$(COMPAT_DIR)/db_date.c \
	$(COMPAT_DIR)/db_macro.c \
	$(COMPAT_DIR)/db_set.c

THREAD_SOURCES = \
	$(THREAD_DIR)/thread.c \
	$(THREAD_DIR)/critical_section.c

BASE_SOURCES = \
	$(BASE_DIR)/porting.c \
        $(BASE_DIR)/fixed_alloc.c \
        $(BASE_DIR)/memory_alloc.c \
	$(BASE_DIR)/databases_file.c \
	$(BASE_DIR)/md5.c \
	$(BASE_DIR)/base64.c \
	$(BASE_DIR)/error_manager.c \
	$(BASE_DIR)/language_support.c \
	$(BASE_DIR)/memory_hash.c \
	$(BASE_DIR)/perf_monitor.c \
	$(BASE_DIR)/ini_parser.c \
	$(BASE_DIR)/system_parameter.c \
	$(BASE_DIR)/ds_string_array.c \
	$(BASE_DIR)/fault_injection.c \
	$(BASE_DIR)/release_string.c \
	$(BASE_DIR)/stack_dump.c \
	$(BASE_DIR)/message_catalog.c \
	$(BASE_DIR)/object_representation_sr.c \
	$(BASE_DIR)/ieeefp.h \
	$(BASE_DIR)/mprec.h \
	$(BASE_DIR)/mprec.c \
	$(BASE_DIR)/dtoa.c \
	$(BASE_DIR)/adjustable_array.c \
	$(BASE_DIR)/chartype.c \
	$(BASE_DIR)/util_func.c \
	$(BASE_DIR)/intl_support.c \
	$(BASE_DIR)/environment_variable.c \
	$(BASE_DIR)/misc_string.c \
	$(BASE_DIR)/getopt_long.c \
	$(BASE_DIR)/rye_shm.c \
	$(BASE_DIR)/rye_master_shm.c \
	$(BASE_DIR)/rye_server_shm.c \
	$(BASE_DIR)/binaryheap.c \
	$(BASE_DIR)/event_log.c

EXECUTABLE_SOURCES = \
	$(EXECUTABLE_DIR)/util_common.c

HEAPLAYER_SOURCES = \
	$(HEAPLAYER_DIR)/customheaps.cpp \
	$(HEAPLAYER_DIR)/lea_heap.c

CONNECTION_SOURCES = \
	$(CONNECTION_DIR)/connection_sr.c \
	$(CONNECTION_DIR)/connection_globals.c \
	$(CONNECTION_DIR)/tcp.c \
	$(CONNECTION_DIR)/server_support.c \
	$(CONNECTION_DIR)/connection_support.c \
	$(CONNECTION_DIR)/heartbeat.c 

COMMUNICATION_SOURCES = \
	$(COMMUNICATION_DIR)/network_interface_sr.c \
	$(COMMUNICATION_DIR)/network_sr.c \
	$(COMMUNICATION_DIR)/network.c

PARSER_SOURCES = \
	$(PARSER_DIR)/parse_tree.c

QUERY_SOURCES = \
	$(QUERY_DIR)/arithmetic.c \
	$(QUERY_DIR)/query_dump.c \
	$(QUERY_DIR)/query_evaluator.c \
	$(QUERY_DIR)/fetch.c \
	$(QUERY_DIR)/list_file.c \
	$(QUERY_DIR)/numeric_opfunc.c \
	$(QUERY_DIR)/query_manager.c \
	$(QUERY_DIR)/scan_manager.c \
	$(QUERY_DIR)/string_opfunc1.c \
	$(QUERY_DIR)/string_opfunc2.c \
	$(QUERY_DIR)/string_opfunc3.c \
	$(QUERY_DIR)/query_executor.c \
	$(QUERY_DIR)/query_opfunc.c \
	$(QUERY_DIR)/stream_to_xasl.c \
	$(QUERY_DIR)/crypt_opfunc.c

OBJECT_SOURCES = \
	$(OBJECT_DIR)/object_print.c \
	$(OBJECT_DIR)/set_object.c \
	$(OBJECT_DIR)/object_representation.c \
	$(OBJECT_DIR)/object_primitive.c \
	$(OBJECT_DIR)/transform.c \
	$(OBJECT_DIR)/object_domain.c

TRANSACTION_SOURCES = \
	$(TRANSACTION_DIR)/log_compress.c \
	$(TRANSACTION_DIR)/boot_sr.c \
	$(TRANSACTION_DIR)/locator.c	\
	$(TRANSACTION_DIR)/locator_sr.c \
	$(TRANSACTION_DIR)/lock_table.c \
	$(TRANSACTION_DIR)/lock_manager.c \
	$(TRANSACTION_DIR)/log_manager.c \
	$(TRANSACTION_DIR)/log_global.c \
	$(TRANSACTION_DIR)/log_page_buffer.c \
	$(TRANSACTION_DIR)/log_prior_lsa.c \
	$(TRANSACTION_DIR)/log_recovery.c \
	$(TRANSACTION_DIR)/log_tran_table.c \
	$(TRANSACTION_DIR)/log_comm.c \
	$(TRANSACTION_DIR)/recovery.c \
	$(TRANSACTION_DIR)/transaction_sr.c \
	$(TRANSACTION_DIR)/wait_for_graph.c

REPLICATION_SOURCES = \
	$(REPLICATION_DIR)/repl_log.c \
	$(REPLICATION_DIR)/repl_common.c \
	$(REPLICATION_DIR)/repl_log_writer_sr.c

STORAGE_SOURCES = \
	$(STORAGE_DIR)/btree.c \
	$(STORAGE_DIR)/btree_dump.c \
	$(STORAGE_DIR)/btree_insert.c \
	$(STORAGE_DIR)/btree_load.c \
	$(STORAGE_DIR)/btree_stats.c \
	$(STORAGE_DIR)/system_catalog.c \
	$(STORAGE_DIR)/catalog_class.c \
	$(STORAGE_DIR)/disk_manager.c \
	$(STORAGE_DIR)/storage_common.c \
	$(STORAGE_DIR)/extendible_hash.c \
	$(STORAGE_DIR)/file_manager.c \
	$(STORAGE_DIR)/heap_file.c \
	$(STORAGE_DIR)/heap_ovf.c \
	$(STORAGE_DIR)/heap_scancache.c \
	$(STORAGE_DIR)/file_io.c \
	$(STORAGE_DIR)/file_io_lock.c \
	$(STORAGE_DIR)/backup.c \
	$(STORAGE_DIR)/backup_sr.c \
	$(STORAGE_DIR)/oid.c \
	$(STORAGE_DIR)/overflow_file.c \
	$(STORAGE_DIR)/page_buffer.c \
	$(STORAGE_DIR)/statistics_sr.c \
	$(STORAGE_DIR)/slotted_page.c \
	$(STORAGE_DIR)/external_sort.c

SESSION_SOURCES = \
	$(SESSION_DIR)/session.c \
	$(SESSION_DIR)/session_sr.c

lib_LTLIBRARIES = librye.la
librye_la_SOURCES = \
	$(COMPAT_SOURCES) \
	$(THREAD_SOURCES) \
	$(BASE_SOURCES) \
	$(HEAPLAYER_SOURCES) \
	$(CONNECTION_SOURCES) \
	$(COMMUNICATION_SOURCES) \
	$(PARSER_SOURCES) \
	$(QUERY_SOURCES) \
	$(OBJECT_SOURCES) \
	$(TRANSACTION_SOURCES) \
	$(REPLICATION_SOURCES) \
	$(STORAGE_SOURCES) \
	$(ES_SOURCES) \
	$(EXECUTABLE_SOURCES) \
	$(SESSION_SOURCES)
librye_la_LDFLAGS = -version-number @LT_VERSION@
librye_la_LIBADD = @EXT_LIBS@ @SYS_LIBS@ $(LIBADD_DL)
librye_la_LINK = $(LINK)

install-data-hook:
