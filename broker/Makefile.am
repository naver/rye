
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

compatdir=@RYE_COMPATDIR@

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
STORAGE_DIR = $(SRC)/storage
EXECUTABLE_DIR = $(SRC)/executables
BROKER_DIR = $(SRC)/broker
CCI_DIR = $(SRC)/cci
DBMT_DIR = $(SRC)/dbmt
QATOOLS_DIR = $(SRC)/qatools

AM_CPPFLAGS = @SRC_INC@ @EXT_INC@ \
	@SYS_DEFS@ @VERSION_DEFS@ -DASYNC_MODE @XOPEN_DEFS@ -D_BSD_SOURCE \
	-DDIAG_DEVEL -DCAS_BROKER @DIR_DEFS@
AM_CFLAGS = -Werror
AM_CXXFLAGS = -Werror

bin_PROGRAMS = rye_broker rye_cas broker_changer \
	broker_log_top broker_monitor_npot

#bin_PROGRAMS = rye_broker rye_cas broker_changer \
#	broker_log_converter broker_log_runner broker_log_top rye_replay

include_HEADERS = 

dist_compat_SCRIPTS = uc

rye_broker_SOURCES = \
	$(BROKER_DIR)/broker.c \
	$(BROKER_DIR)/broker_shm.c \
	$(BROKER_DIR)/broker_util.c \
	$(BROKER_DIR)/broker_process_size.c \
	$(BROKER_DIR)/broker_max_heap.c \
	$(BROKER_DIR)/broker_send_fd.c \
	$(BROKER_DIR)/broker_recv_fd.c \
	$(BROKER_DIR)/broker_filename.c \
	$(BROKER_DIR)/shard_mgmt.c \
	$(BROKER_DIR)/local_mgmt.c \
	$(BROKER_DIR)/broker_log.c \
	$(BROKER_DIR)/cas_protocol.c
rye_broker_LDADD = @CS_LIB@ @CCI_LIB@ 

broker_monitor_npot_SOURCES = \
	$(BASE_DIR)/porting.c \
	$(BASE_DIR)/rye_shm.c \
	$(BASE_DIR)/rye_master_shm.c \
	$(BASE_DIR)/rye_server_shm.c \
	$(BASE_DIR)/perf_monitor.c \
	$(BROKER_DIR)/broker_monitor_npot.c \
	$(BROKER_DIR)/broker_shm.c
broker_monitor_npot_LDADD = @CCI_LIB@
broker_monitor_npot_LDFLAGS = @LDFLAGS@ -static

broker_changer_SOURCES = \
	$(BROKER_DIR)/broker_changer.c \
	$(BROKER_DIR)/broker_admin_pub.c \
	$(BROKER_DIR)/broker_shm.c \
	$(BROKER_DIR)/broker_process_size.c \
	$(BROKER_DIR)/broker_config.c \
	$(BROKER_DIR)/broker_util.c \
	$(BROKER_DIR)/broker_max_heap.c \
	$(BROKER_DIR)/broker_filename.c \
	$(BROKER_DIR)/broker_acl.c
broker_changer_LDADD = @CS_LIB@

rye_cas_SOURCES = \
	$(BROKER_DIR)/cas.c \
	$(BROKER_DIR)/cas_protocol.c \
	$(BROKER_DIR)/cas_network.c \
	$(BROKER_DIR)/cas_net_buf.c \
	$(BROKER_DIR)/cas_function.c \
	$(BROKER_DIR)/cas_execute.c \
	$(BROKER_DIR)/cas_handle.c \
	$(BROKER_DIR)/broker_util.c \
	$(BROKER_DIR)/broker_recv_fd.c \
	$(BROKER_DIR)/cas_log.c \
	$(BROKER_DIR)/broker_shm.c \
	$(BROKER_DIR)/broker_process_size.c \
	$(BROKER_DIR)/broker_filename.c \
	$(BROKER_DIR)/cas_error.c \
	$(BROKER_DIR)/broker_acl.c \
	$(BROKER_DIR)/cas_util.c 
rye_cas_CPPFLAGS = $(AM_CPPFLAGS)
rye_cas_LDADD = @CS_LIB@ -lrt

#broker_log_converter_SOURCES = \
#	$(BROKER_DIR)/broker_log_converter.c \
#	$(BROKER_DIR)/broker_log_util.c \
#	$(BROKER_DIR)/log_top_string.c 
#broker_log_converter_CPPFLAGS = $(AM_CPPFLAGS) -DBROKER_LOG_CONVERTER
#broker_log_converter_LDADD = @CS_LIB@

#broker_log_runner_SOURCES = \
#	$(BROKER_DIR)/cas_runner.c \
#	$(BROKER_DIR)/broker_log_util.c \
#	$(BROKER_DIR)/log_top_string.c
#broker_log_runner_CPPFLAGS = $(AM_CPPFLAGS) -DBROKER_LOG_RUNNER
#broker_log_runner_LDADD = @CS_LIB@ @CCI_LIB@

broker_log_top_SOURCES = \
	$(BROKER_DIR)/broker_log_top.c \
	$(BROKER_DIR)/log_top_query_info.c \
	$(BROKER_DIR)/broker_log_util.c \
	$(BROKER_DIR)/log_top_string.c \
	$(BROKER_DIR)/broker_log_top_tran.c
broker_log_top_CPPFLAGS = $(AM_CPPFLAGS) -DBROKER_LOG_TOP
broker_log_top_LDADD = @CS_LIB@

#rye_replay_SOURCES = \
#	$(BROKER_DIR)/broker_log_replay.c \
#	$(BROKER_DIR)/broker_log_util.c \
#	$(BROKER_DIR)/log_top_string.c 
#rye_replay_CPPFLAGS = $(AM_CPPFLAGS)
#rye_replay_LDADD = @CS_LIB@ @CCI_LIB@

lib_LTLIBRARIES =

VERSIONED_FILES = \
	$(BROKER_DIR)/broker_admin.c \
	$(BROKER_DIR)/broker_changer.c \
	$(BROKER_DIR)/broker_monitor.c \
	$(BROKER_DIR)/broker_admin_so.c

$(VERSIONED_FILES): $(top_srcdir)/BUILD_NUMBER
	touch $@
