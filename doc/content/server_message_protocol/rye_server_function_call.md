### 2. FUNCTION call
####  NET_SERVER_PING
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | client_val | int |  |

  * reply data0

    | description | type | note |
    | -------- | -------- | -------- |
    | server_val | int | 0 |

#### NET_SERVER_BO_INIT_SERVER
  not used
  
#### NET_SERVER_BO_REGISTER_CLIENT
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | client_info | string |  |
    | db_name | string |  |
    | db_user | string |  |
    | db_password | string |  |
    | program_name | string |  |
    | login_name | string |  |
    | host_name | string |  |
    | process_id | int |  |
    | client_lock_wait | int |  |

  * response data0
  
    | description | type | note |
    | -------- | -------- | -------- |
    | data1_size | int |  |

  * response data1

    | description | type | note |
    | -------- | -------- | -------- |
    | tran_index | int |  |
    | tran_state | int |  |
    | db_name | string |  |
    | host_name | string |  |
    | process_id | string |  |
    | root_class_oid | oid |  |
    | root_class_hfid | hfid |  |
    | page_size | int |  |
    | log_page_size | int |  |
    | disk_compatibility | float |  |
    | server_start_time | int |  |
    | db_lang | string |  |

#### NET_SERVER_BO_UNREGISTER_CLIENT
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | tran_index | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | success | int |  |

#### NET_SERVER_BO_ADD_VOLEXT
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | fullname | string |  |
    | writesize_in_sec | int |  |
    | purpose | int |  |
    | overwrite | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | volid | int |  |

#### NET_SERVER_BO_FIND_NPERM_VOLS
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | nvols | int |  |

#### NET_SERVER_BO_FIND_NTEMP_VOLS
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | nvols | int |  |

#### NET_SERVER_BO_FIND_LAST_TEMP
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | nvols | int |  |

#### NET_SERVER_BO_FIND_NBEST_ENTRIES
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | nbest | int |  |

#### NET_SERVER_BO_GET_SERVER_STATE
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | server_state | int |  |

#### NET_SERVER_BO_NOTIFY_HA_APPLY_STATE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | host_ip | string |  |
    | state | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |

#### NET_SERVER_BO_GET_LOCALES_INFO
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | data1_size | int |  |
    | error | int | |

  * response data1

    | description | type | note |
    | -------- | -------- | -------- |
    | server_coll_count | int |  |
    | server_locales_count | int |  |
    | server_coll_info(0..n) |  |  |
    | server_locale_info(0..n) |  |  |

    * server_coll_info

	  | description | type | note |
      | -------- | -------- | -------- |
      | coll_id | int |  |
      | coll_name | string |  |
      | code_set | int |  |
      | checksum | string |  |

    * server_locale_info

      | description | type | note |
      | -------- | -------- | -------- |
      | lang_name | string |  |
      | codeset | int |  |
      | checksum | string |  |

#### NET_SERVER_TM_SERVER_COMMIT
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | tran_state | int |  |

#### NET_SERVER_TM_SERVER_ABORT
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | tran_state | int |  |

#### NET_SERVER_TM_SERVER_SAVEPOINT
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | savepoint_name | string |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | success | int |  |
    | savepoint_lsa | LOG_LSA | |

#### NET_SERVER_TM_SERVER_PARTIAL_ABORT
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | savepoint_name | string |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | tran_state | int |  |
    | savepoint_lsa | LOG_LSA | |

#### NET_SERVER_TM_SERVER_HAS_UPDATED
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | has_updated | int |  |

#### NET_SERVER_TM_SERVER_ISACTIVE_AND_HAS_UPDATED
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | is_active_and_has_updated | int |  |

#### NET_SERVER_TM_WAIT_SERVER_ACTIVE_TRANS
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |

#### NET_SERVER_LC_FETCH
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | oid | OID |  |
    | lock | LOCK |  |
    | class_oid | OID |  |
    | prefetch | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_objs | int |  |
    | packed_desc_size | int |  |
    | content_size | int |  |
    | success | int |  |

  * response data1: packed_desc
  * response data2: content

#### NET_SERVER_LC_FETCHALL
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | hfid | HFID |  |
    | lock | LOCK |  |
    | class_oid | OID |  |
    | nobjects | int64 |  |
    | nfetched | int64 |  |
    | last_oid | OID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_objs | int64 |  |
    | packed_desc_size | int |  |
    | content_size | int |  |
    | lock | LOCK |  |
    | nobjects | int64 |  |
    | nfetched | int64 |  |
    | last_oid | OID |  |
    | success | int |  |

  * response data1: packed_desc
  * response data2: content

#### NET_SERVER_LC_FETCH_LOCKSET
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | send_size | int |  |

  * request data1: packed_lockset

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | recv_packed_size | int |  |
    | num_objs | int |  |
    | packed_desc_size | int |  |
    | content_size | int |  |
    | success | int | |

  * response data1: packed_lockset
  * response data2: packed_desc

  * response data3: content

#### NET_SERVER_LC_FIND_CLASSOID
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | class_name | string |  |
    | class_oid | OID |  |
    | lock | LOCK |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | found | int |  |
    | class_oid | OID |  |

#### NET_SERVER_LC_FORCE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_objs | int |  |
    | desc_size | int |  |
    | content_size | int |  |

  * request data1: packed_desc
  * request data2: content

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

  * response data1: packed_desc

#### NET_SERVER_LC_RESERVE_CLASSNAME
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_classes | int |  |
    | class_info(0..n) |  |  |

    * class_info

      | description | type | note |
      | -------- | -------- | -------- |
      | class_name | string |  |
      | oid | OID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | reserved | int |  |

#### NET_SERVER_LC_DELETE_CLASSNAME
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | class_name | string |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | deleted | int |  |

#### NET_SERVER_LC_RENAME_CLASSNAME
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | old_name | string |  |
    | new_name | string |  |
    | class_oid | OID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | updated | int |  |

#### NET_SERVER_LC_ASSIGN_OID
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | hfid | HFID |  |
    | expected_length | int |  |
    | class_oid | OID |  |
    | class_name | string |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | success | int |  |
    | perm_oid | OID | |

#### NET_SERVER_LC_FIND_LOCKHINT_CLASSOIDS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_classes | int |  |
    | quit_on_error | int |  |
    | class_info(0..n) |  |  |

    * class_info

      | description | type | note |
      | -------- | -------- | -------- |
      | class_name | int |  |
      | lock | LOCK |  |
      | class_oid | OID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | packed_size | int |  |
    | num_objs | int |  |
    | packed_desc_size | int |  |
    | content_size | int |  |
    | all_find | int |  |

  * response data1: packed lockhint
  * response data2: packed_desc
  * response data3: content

#### NET_SERVER_LC_FETCH_LOCKHINT_CLASSES
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | send_size | int |  |

  * request data1: packed_lockhint

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | packed_size | int |  |
    | num_objs | int |  |
    | packed_desc_size | int |  |
    | content_size | int |  |
    | success | int |  |

  * response data1: packed_lockhint
  * response data2: packed_desc
  * response data3: content

#### NET_SERVER_HEAP_CREATE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | hfid | HFID |  |
    | class_oid | OID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |
    | hfid | HFID |  |

#### NET_SERVER_HEAP_DESTROY
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | hfid | HFID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_HEAP_GET_CLASS_NOBJS_AND_NPAGES
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | hfid | HFID |  |
    | approximation | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_objs | int64 |  |
    | status | int |  |
    | num_pages | int |  |

#### NET_SERVER_LOG_RESET_WAIT_MSECS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | wait_msecs | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | wait | int |  |

#### NET_SERVER_LOG_SET_INTERRUPT
  NOT USED

#### NET_SERVER_LOG_GETPACK_TRANTB
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | include_query_exec_info | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | size | int |  |
    | error | int |  |

  * response data1: packed_tran_table

#### NET_SERVER_BTREE_ADD_INDEX
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | btid | BTID |  |
    | num_atts | int |  |
    | atts(0..n) | int * [num_atts] |  |
    | class_oid | OID |  |
    | attr_id | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |
    | btid | BTID |  |

#### NET_SERVER_BTREE_DEL_INDEX
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | btid | BTID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |

#### NET_SERVER_BTREE_LOAD_DATA
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | btid | BTID |  |
    | class_oid | OID | |
    | hfid | HFID | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_BTREE_FIND_UNIQUE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | key | DB_IDXKEY |  |
    | btid | BTID |  |
    | class_oid | OID |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |
    | oid | OID |  |

#### NET_SERVER_DISK_GET_PURPOSE_AND_SPACE_INFO
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | volid | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | vol_purpose | int |  |
    | space_info | VOL_SPACE_INFO | |
    | volid | int | |

#### NET_SERVER_DISK_VLABEL
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | volid | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | area_size | int |  |

  * response data1: vol_fullname

#### NET_SERVER_QST_GET_STATISTICS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | oid | OID |  |
    | timestamp | int | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | length | int |  |

  * response data1: area

#### NET_SERVER_QST_UPDATE_STATISTICS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | oid | OID |  |
    | update_stats | int | |
    | with_fullscan | int | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_QST_UPDATE_ALL_STATISTICS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | update_stats | int | |
    | with_fullscan | int | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_QM_QUERY_PREPARE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | sql_hash_text | string |  |
    | sql_plan_text | string |  |
    | sql_user_text | string |  |
    | user_oid | OID |  |
    | xasl_stream_size | int |  |
    | get_xasl_header | int |  |

  * request data1: xasl_stream

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | reply_buffer_size | int |  |
    | error | int |  |
    | xasl_id | XASL_ID |  |

  * response data1: XASL_NODE_HEADER

#### NET_SERVER_QM_QUERY_EXECUTE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | xasl_id | XASL_ID |  |
    | dbval_cnt | int |  |
    | senddata_size | int |  |
    | flag | int |  |
    | query_timeout | int |  |
    | shard_groupid | int |  |
    | shard_keyval_size | int |  |

  * request data1: db_value array
  * request data2: shard_key_value string

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | server_request | int |  |
    | reply_size_listid | int |  |
    | reply_size_page | int |  |
    | reply_size_plan | int |  |
    | query_idp | ptr |  |
    | qe_status_flag | int |  |

  * response data1: listid
  
  * response data2: page

  * response data3: plan

#### NET_SERVER_QM_QUERY_END
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | query_id | ptr |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |

#### NET_SERVER_QM_QUERY_DROP_PLAN
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | qstmt | string |  |
    | user_oid | OID | |
    | xasl_id | XASL_ID | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |

#### NET_SERVER_QM_QUERY_DROP_ALL_PLANS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | dummy | int | 0 |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |

#### NET_SERVER_QM_GET_QUERY_INFO
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | query_id | ptr |  |

  * response data0: int
  * respinse data1: query_info

#### NET_SERVER_LS_GET_LIST_FILE_PAGE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | query_id | ptr |  |
    | volid | int | |
    | pageid | int | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | buffer_size | ptr |  |
    | error | int | |

  * response data1: page

#### NET_SERVER_MNT_SERVER_COPY_STATS
  * request data0: none

  * response data0: stats (int64 * MNT_SIZE_OF_SERVER_EXEC_STATS)

#### NET_SERVER_CT_CAN_ACCEPT_NEW_REPR
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | class_oid | OID |  |
    | hfid | HFID | |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | status | int |  |
    | accept | int | |

#### NET_SERVER_CSS_KILL_TRANSACTION
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | kill_tran_index | int |  |
    | kill_user | string |  |
    | kill_host | string |  |
    | kill_pid | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | success | int |  |

#### NET_SERVER_QPROC_GET_SERVER_INFO
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | info_bits | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | val_size | int |  |
    | status | int | |

  * response data1: db_value array

#### NET_SERVER_PRM_SET_PARAMETERS
  * request data0: prm_count(int) + (prm_id(int) + 0(int) + prm_value) * prm_count

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | rc | int |  |

#### NET_SERVER_PRM_GET_PARAMETERS
  * request data0: prm_count(int) + (prm_id(int) + 0(int) + prm_value) * prm_count

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | receive_size | int |  |
    | rc | int | |

  * reponse data1: prm data

#### NET_SERVER_PRM_GET_FORCE_PARAMETERS
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | receive_size | int |  |
    | rc | int | |

  * reponse data1: prm data

#### NET_SERVER_REPL_INFO
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | request | int | REPL_INFO_TYPE_SCHEMA |
    | statement_type | int |  |
    | online_ddl_type | int |  |
    | repl_schema_name | string |  |
    | repl_schema_ddl | string |  |
    | repl_schema_db_user | string |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | success | int |  |

#### NET_SERVER_REPL_LOG_GET_EOF_LSA
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | dummy | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | lsa | LOG_LSA |  |

#### NET_SERVER_LOGWR_GET_LOG_PAGES
  * repeat loop

    * request data0
      first request: COMMAND_TYPE packet,  later request: DATA_TYPE packet

      | description | type | note |
      | -------- | -------- | -------- |
      | first_pageid_to_recv | int64 |  |
      | mode | int |  |
      | last_error | int |  |
      | compressed | int |  |

    * response data0

      | description | type | note |
      | -------- | -------- | -------- |
      | server_request | int | GET_NEXT_LOG_PAGES, END_CALLBACK  |
      | length | int | if server_request == GET_NEXT_LOG_PAGES |
      | pageid | int64 | if server_request == GET_NEXT_LOG_PAGES |
      | num_page | int | if server_request == GET_NEXT_LOG_PAGES |
      | file_status | int | if server_request == GET_NEXT_LOG_PAGES |
      | server_status | int | if server_request == GET_NEXT_LOG_PAGES |
      | error | int | if server_request == END_CALLBACK |

    * response data1: log_page (if server_request == GET_NEXT_LOG_PAGES)

#### NET_SERVER_SHUTDOWN
   NOT USED

#### NET_SERVER_MNT_SERVER_COPY_GLOBAL_STATS
  * request data0

  * response data0: stats (int64 * MNT_SIZE_OF_SERVER_EXEC_STATS)

#### NET_SERVER_LOG_CHECKPOINT
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int | always NO_ERROR  |

#### NET_SERVER_LOG_SET_SUPPRESS_REPL_ON_TRANSACTION
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | set | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_SES_CHECK_SESSION
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | session_id | int |  |
    | session_key | stream | SERVER_SESSION_KEY_SIZE |
    | db_user | string |  |
    | host | string |  |
    | program_name | string |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | area_size | int |  |
    | error | stream | SERVER_SESSION_KEY_SIZE |

  * response data1: session_id(int) + session_key(SERVER_SESSION_KEY_SIZE)

#### NET_SERVER_SES_END_SESSION
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | session_id | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_ACL_RELOAD
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_LC_REPL_FORCE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_objs | int |  |
    | desc_size | int |  |
    | content_size | int |  |

  * reqeust data1: desc
  * request data2: content

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_objs | int |  |
    | desc_size | int |  |
    | content_size | int |  |
    | error | int |  |

  * response data1: desc
  * response data2: content

#### NET_SERVER_LC_LOCK_SYSTEM_DDL_LOCK
  * request data0: none

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_MIGRATOR_GET_LOG_PAGES
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | last_recv_pageid | int64 |  |
    | compressed_protocol | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | length | int64 |  |

  * response data1: log_page

#### NET_SERVER_UPDATE_GROUP_ID
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | migrator_id | int |  |
    | group_id | int |  |
    | traget | int |  |
    | on_off | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_BLOCK_GLOBAL_DML
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | start_or_end | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### NET_SERVER_BK_PREPARE_BACKUP
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | num_threads | int |  |
    | do_compress | int |  |
    | sleep_msecs | int |  |
    | make_slave | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | area_size | int |  |

  * response data1

    | description | type | note |
    | -------- | -------- | -------- |
    | bkup_hdr db_version | RYE_VERSION |  |
    | ... | ... | |

#### NET_SERVER_BK_BACKUP_VOLUME
  * request data0

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | area_size | int |  |

  * response data1

    | description | type | note |
    | -------- | -------- | -------- |
    | iopageid | int |  |
    | magic | int |  |
    | db_compatibility | float |  |
    | bk_hdr_version | int |  |
    | db_creation | int64 |  |
    | start_time | int64 |  |
    | db_release | string |  |
    | db_name | string |  |
    | db_iopagesize | int |  |
    | chkpt_lsa | LOG_LSA |  |
    | bkpagesize | int |  |
    | first_arv_needed | int |  |
    | nxchkpt_atpageid | int |  |
    | num_perm_vols | int |  |

#### NET_SERVER_BK_BACKUP_LOG_VOLUME
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | bkup_iosize | int |  |
    | delete_unneeded_logarchives | int |  |

  * repeat packets until BK_PACKET_LOGS_BACKUP_END is received

    * response1 data0

      | description | type | note |
      | -------- | -------- | -------- |
      | packet_type | int | BK_PACKET_VOL_START <br> BK_PACKET_DATA <br> BK_PACKET_VOL_END <br> BK_PACKET_VOLS_BACKUP_END <br> BK_PACKET_LOGS_BACKUP_END |
      | unzip_bytes | int |  |
      | backuptime_lsa | LOG_LSA | type == BK_PACKET_LOGS_BACKUP_END |
      | backup_endtime | int64 | type == BK_PACKET_LOGS_BACKUP_END |

    * response2 data0

      | description | type | note |
      | -------- | -------- | -------- |
      | backup | stream |  |

#### DUMP functions
NET_SERVER_LOG_DUMP_STAT <br> NET_SERVER_LOG_DUMP_TRANTB <br> NET_SERVER_LK_DUMP <br> NET_SERVER_ACL_DUMP <br> NET_SERVER_CSS_DUMP_SERVER_STAT <br> NET_SERVER_CSS_DUMP_CS_STAT <br> NET_SERVER_PRM_DUMP_PARAMETERS <br> NET_SERVER_QM_QUERY_DUMP_PLANS
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | reply_stream_size | int |  |

  * response data0

    | description | type | note |
    | -------- | -------- | -------- |
    | total_size | int |  |

  * repeat read data packet
    * response data0

      | description | type | note |
      | -------- | -------- | -------- |
      | stream | char | [reply_stream_size] |

