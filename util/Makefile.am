
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
STORAGE_DIR = $(SRC)/storage
EXECUTABLE_DIR = $(SRC)/executables
BROKER_DIR = $(SRC)/broker
CCI_DIR = $(SRC)/cci
DBMT_DIR = $(SRC)/dbmt
REBALANCE_DIR = $(SRC)/rebalance
REPLICATION_DIR = $(SRC)/replication

FLEX = flex

AM_CPPFLAGS = @SRC_INC@ @EXT_INC@ \
	@SYS_DEFS@ @COMMON_DEFS@ @VERSION_DEFS@ \
	-DPRODUCE_ANSI_CODE -DUCI_TEMPORARY
#AM_CFLAGS = -Werror
#AM_CXXFLAGS = -Werror
#AM_LDADD = @SYS_LIBS@
LIBS += @SYS_LIBS@


bin_PROGRAMS = \
	rsql rye_master rye_server rye rye_admin_sa \
	rye_rel rye_migrator rye_repl rye_gc rye_test

rye_server_SOURCES = $(EXECUTABLE_DIR)/server.c
rye_server_LDADD = @RYE_LIB@

rye_master_SOURCES = \
	$(EXECUTABLE_DIR)/master.c \
	$(EXECUTABLE_DIR)/master_request.c \
	$(EXECUTABLE_DIR)/master_util.c \
	$(EXECUTABLE_DIR)/master_heartbeat.c \
	$(BASE_DIR)/ds_list.c \
	$(BASE_DIR)/ds_stack.c \
	$(BASE_DIR)/ds_queue.c \
	$(BROKER_DIR)/broker_admin.c \
	$(BROKER_DIR)/broker_admin_pub.c \
	$(BROKER_DIR)/broker_acl.c \
	$(BROKER_DIR)/broker_config.c \
	$(BROKER_DIR)/broker_util.c \
	$(BROKER_DIR)/broker_filename.c \
	$(BROKER_DIR)/broker_shm.c
rye_master_LDADD = @CS_LIB@ @CCI_LIB@

rye_rel_SOURCES = \
	$(EXECUTABLE_DIR)/rye_version.c
rye_rel_LDADD = @SA_LIB@

rye_SOURCES = \
	$(EXECUTABLE_DIR)/util_service.c \
	$(EXECUTABLE_DIR)/util_support.c \
	$(EXECUTABLE_DIR)/util_admin.c \
	$(EXECUTABLE_DIR)/commdb.c \
	$(EXECUTABLE_DIR)/master_util.c\
        $(BROKER_DIR)/broker_config.c \
	$(BROKER_DIR)/broker_admin.c \
	$(BROKER_DIR)/broker_shm.c \
	$(BROKER_DIR)/broker_util.c \
	$(BROKER_DIR)/broker_admin_pub.c \
	$(BROKER_DIR)/broker_max_heap.c \
	$(BROKER_DIR)/broker_filename.c \
	$(BROKER_DIR)/broker_acl.c \
	$(BROKER_DIR)/broker_log_util.c \
	$(BROKER_DIR)/log_top_string.c \
	$(BROKER_DIR)/broker_monitor.c \
	$(BROKER_DIR)/broker_tester.c
rye_LDADD = @CS_LIB@ @CCI_LIB@

rye_admin_sa_SOURCES = \
	$(EXECUTABLE_DIR)/util_admin_sa.c \
	$(EXECUTABLE_DIR)/util_admin.c \
	$(EXECUTABLE_DIR)/util_support.c \
	$(BASE_DIR)/getopt_long.c \
	$(BASE_DIR)/porting.c
rye_admin_sa_LDADD = @SA_LIB@


rsql_SOURCES = $(EXECUTABLE_DIR)/rsql_launcher.c \
			   $(EXECUTABLE_DIR)/util_support.c \
			   $(BASE_DIR)/porting.c \
			   $(BASE_DIR)/getopt_long.c

noinst_PROGRAMS = gencat
gencat_SOURCES = \
	$(BASE_DIR)/porting.c \
	$(EXECUTABLE_DIR)/gencat.c

rye_migrator_SOURCES = \
	$(REBALANCE_DIR)/rbl_move_group.c \
	$(REBALANCE_DIR)/rbl_sync_log.c \
	$(REBALANCE_DIR)/rbl_sync_query.c \
	$(REBALANCE_DIR)/rbl_copy_schema.c \
	$(REBALANCE_DIR)/rbl_conf.c \
	$(REBALANCE_DIR)/rbl_error_log.c \
	$(REBALANCE_DIR)/rbl_gc.c \
	$(REBALANCE_DIR)/rbl_table_info.c \
	$(REBALANCE_DIR)/rbl_migrator.c
rye_migrator_LDADD = @CS_LIB@ @CCI_LIB@

rye_repl_SOURCES = \
	$(REPLICATION_DIR)/repl_analyzer.c \
	$(REPLICATION_DIR)/repl_applier.c \
	$(REPLICATION_DIR)/repl_writer.c \
	$(REPLICATION_DIR)/repl_catalog.c \
	$(REPLICATION_DIR)/repl_page_buffer.c \
	$(REPLICATION_DIR)/repl_queue.c \
	$(REPLICATION_DIR)/repl.c \
	$(BASE_DIR)/ds_list.c \
	$(BASE_DIR)/ds_stack.c \
	$(BASE_DIR)/ds_queue.c \
	$(BROKER_DIR)/broker_admin.c \
	$(BROKER_DIR)/broker_admin_pub.c \
	$(BROKER_DIR)/broker_acl.c \
	$(BROKER_DIR)/broker_config.c \
	$(BROKER_DIR)/broker_util.c \
	$(BROKER_DIR)/broker_filename.c \
	$(BROKER_DIR)/broker_shm.c
rye_repl_LDADD = @CS_LIB@ @CCI_LIB@ @EXT_LIBS@ 

rye_test_SOURCES = \
	$(REPLICATION_DIR)/cci_test.c
rye_test_LDADD = @CCI_LIB@

rye_gc_SOURCES = \
	$(REBALANCE_DIR)/rbl_gc.c \
	$(REBALANCE_DIR)/rbl_table_info.c \
	$(REBALANCE_DIR)/rbl_error_log.c \
	$(REBALANCE_DIR)/rbl_gc_runner.c
rye_gc_LDADD = @CS_LIB@ @CCI_LIB@

