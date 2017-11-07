## 2. CAS Request Message Protocol

### 2.1 CAS Request Message Protocol

  * CAS request

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | message_header |  | 20 |  | 
    | message |  | [message_size in header] |  | 

    * message_header

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | Message_size | INT | 4 |  | 
      | CAS_STATUS | CHAR | 16 |  | 

    * CAS_STATUS

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | status | CHAR | 1 | 0: inactive 1: active | 
      | server_nodeid | SHORT | 2 |  | 
      | shard_info_version | INT64 | 8 |  | 
      | RESERVED |  | 5 |  | 

  * CAS response

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | message_header |  | 20 |  | 
    | result_message |  | [message_size in header] |  | 

    * result_message - ERROR

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | result_code | CHAR | 1 | 0x00 | 
      | error_indicator | INT | 4 |  | 
      | error_code | INT | 4 |  | 
      | error_message_length | INT | 4 |  | 
      | error_message | CHAR | error_message_length] |  | 

    * result_message - SUCCESS

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | result_code | CHAR | 1 | 0x01 | 
      | result | | | defined in each request |

### 2.2 CAS function request

  * CAS request message

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | function_code | CHAR | 1 |  | 
    | argument(0..n) | | | defined in each request |

  * argument

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | size | INT | 4 |  | 
    | data | BYTES <br> STR <br> INT <br> DOUBLE | [size] <br> [size] <br> 4 <br> 8 | ARG_BYTES <br> ARG_STR <br> ARG_INT <br> ARG_DOUBLE |

#### 2.2.0 CONNECT_DB
  * function_code: 0

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | ARG_STR |  | 
    | dbuser | ARG_STR |  | 
    | dbpassword | ARG_STR |  | 
    | url | ARG_STR |  | 
    | driver_version | ARG_STR |  | 
    | session_id | ARG_BYTES (20) |  | 

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | server_version_major | SHORT | 2 |  | 
    | server_version_minor | SHORT | 2 |  | 
    | server_version_patch | SHORT | 2 |  | 
    | server_version_build | SHORT | 2 |  | 
    | cas_id | INT | 4 |  | 
    | cas_pid | INT | 4 |  | 
    | session_id_len | INT | 4 |  | 
    | session_id | CHAR | [session_id_len] |  | 
    | dbms | CHAR | 1 | 0x01 | 
    | support_holdable_cursor | CHAR | 1 | 0 or 1 | 
    | statement_pooling | CHAR | 1 | 0 or 1 | 
    | cci_default_autocommit | CHAR | 1 | 0 or 1 | 
    | server_start_time | INT | 4 |  | 

#### 2.2.1 END_TRAN
  * function_code: 1

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | transaction_type | ARG_BYTES (1) | 0x01: COMMIT <br> 0x02: ROLLBACK | 

  * result: none

#### 2.2.2 PREPARE
  * function_code: 2

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | sql_statement | ARG_STR |  | 
    | flag | ARG_BYTES | 0x01: HOLDABLE <br> 0x02: FROM_MIGRATOR | 
    | auto_commit_mode | ARG_BYTES | 0x00: OFF <br> 0x01: ON| 
    | deferred_close_handle_count | ARG_INT |  | 
    | deferred_close_handle (0..n) | ARG_INT * [count] |  | 

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | server_handle_id | INT | 4 |  | 
    | stmt_type  | CHAR | 1 |  | 
    | num_bind | INT | 4 |  | 
    | num_columns | INT | 4 |  | 
    | column_info (0.. n) |  |  |  | 
    | sharding_metadata | | | |

	* column_info

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | datatype | CHAR | 1 |  | 
      | scale | SHORT | 2 |  | 
      | precision | INT | 4 |  | 
      | col_label_length | INT | 4 |  | 
      | col_label | STR | [col_label_length] |  | 
      | col_name_length | INT | 4 |  | 
      | col_name | STR | [col_name_length] |  | 
      | table_name_length | INT | 4 |  | 
      | table_name | STR | [table_name_length] |  | 
      | is_not_null | CHAR | 1 | 0x00: NULL <br> 0x01: NOT_NULL | 
      | default_value_length | INT | 4 |  | 
      | default_value | STR | [default_value_length] |  | 
      | is_unique_key | CHAR | 1 |  | 
      | is_primary_key | CHAR | 1 |  | 

	* sharding_metadata

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | is_shard_table | CHAR | 1 |  | 
      | num_shard_values | INT | 4 |  | 
      | shard_value(0..n) | {INT + STR} arr | {Size(4 bytes) + value (size bytes)} * [num_shard_values] | 
      | num_shard_value_pos | INT | 4 |  | 
      | shard_value_pos | INT arr | 4 * num_shard_value_pos] |  | 

#### 2.2.3 EXECUTE
  * function_code: 3

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | server_handle_id | ARG_INT |  | 
    | flag | ARG_BYTES | Currently not used | 
    | max_col_size | ARG_INT |  | 
    | max_row | ARG_INT |  | 
    | auto_commit_mode | ARG_BYTES |  | 
    | appl_query_timeout | ARG_INT |  | 
    | groupid | ARG_INT |  | 
    | num_bind_values | ARG_INT |  | 
    | bind_value(0..n) |  |  | 

	* bind_value

      | Description | Type | Note | 
      | -------- | -------- | -------- | 
      | bind_type | ARG_BYTES | | 
      | value | ARG_XXX | |

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | execute_result | INT | 4 |  | 
    | cache_reusable | CHAR | 1 | Currently not used | 
    | statement_type | CHAR | 1 |  | 
    | tuple_count | INT | 4 |  | 
    | num_select_columns | INT | 4 |  | 
    | select_column_info(0..n) |  |  | Only if statement type is SELECT. | 

    * select_column_info

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | Type | CHAR | 1 |  | 
      | Scale | SHORT | 2 |  | 
      | Precision | INT | 4 |   | 

#### 2.2.4 GET_DB_PARAMETER
  * function_code: 4

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | parameter_type | ARG_INT | 0x01: MAX_STRING_LENGTH | 

  * result - MAX_STRING_LENGTH

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | max_str_len | INT | 4 |  | 

#### 2.2.5 CLOSE_REQ_HANDLE
  * function_code: 5

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | server_handle_id | ARG_INT |  | 
    | autocommit_mode | ARG_BYTES (1) | |

  * result: none

#### 2.2.6 FETCH
  * function_code: 6

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | server_handle_id | ARG_INT |  | 
    | cursor_position | ARG_INT |  | 
    | fetch_count | ARG_INT |  | 
    | result_set_index | ARG_INT | Always 0 | 

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | num_tuples | INT | 4 |  | 
    | tuple (0..n) |  |  |  | 
    | cursor_status | CHAR | 1 | 0: CURSOR_OPEN <br> 1: CURSOR_CLOSED | 

	* tuple

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | cursor_pos | INT | 4 |  | 
      | column_value (0..n) |  |  |  | 

	* column_value

	  | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | data_size | INT | 4 | -1 if data is NULL | 
      | data |  |  |  | 

#### 2.2.7 SCHEMA_INFO
  * function_code: 7

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | schema_info_type | ARG_INT | TABLE = 1 <br> QUERY_SPEC = 2 <br> COLUMN = 3 <br> INDEX = 4 <br> TABLE_PRIVILEGE = 5 <br> COLUMN_PRIVILEGE = 6 <br> PRIMARY_KEY = 7 | 
    | arg1 | ARG_STRING |  | 
    | arg2 | ARG_STRING |  | 
    | flag | ARG_BYTES | Currently not used | 

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | prepare_result |  |  | refer to 2.2.2 PREPARE result | 
    | tuple_count | INT | 4 |  | 

#### 2.2.8 GET_DB_VERSION
  * function_code: 8

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | autocommit_mode | ARG_BYTES (1) | |

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | size | INT | 4 |  | 
    | version_string | CHAR | [size] |  | 

#### 2.2.9 GET_CLASS_NUM_OBJS
  * function_code: 9

  * currently not used

#### 2.2.10 EXECUTE_BATCH
  * function_code: 10

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | server_handle_id | ARG_INT |  | 
    | appl_query_timeout | ARG_INT |  | 
    | autocommit_mode | ARG_BYTES |  | 
    | group_id | ARG_INT |  | 
    | mum_execution | ARG_INT |  | 
    | num_params | ARG_INT |  | 
    | num_values | ARG_INT |  | 
    | bind_value(0..n) |  | Refer to 2.2.3 EXECUTE argument - bind_value | 

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | num_query | INT | 4 |  | 
    | query_result(0..n) |  |  |  | 

    * query_result - SUCCESS

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | Result_flag | CHAR | 1 | EXEC_QUERY_SUCCESS | 
      | Result | INT | 4 |  | 

    * query_result - ERROR

      | Description | Type | Data length | Note | 
      | -------- | -------- | -------- | -------- | 
      | result_flag | CHAR | 1 | EXEC_QUERY_ERROR | 
      | error_indicator | INT | 4 |  | 
      | error_code | INT | 4 |  | 
      | error_msg_len | INT | 4 |  | 
      | error_msg | CHAR | [error_msg_len] |  | 

#### 2.2.11 GET_QUERY_PLAN
  * function_code: 11

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | sql_statement | ARG_STR | |

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | plan_str_len | INT | 4 |  | 
	| plan_str | CHAR | [plan_str_len] |  | 

#### 2.2.12 CON_CLOSE
  * function_code: 12

  * argument: none

  * result: none

#### 2.2.13 CHECK_CAS
  * function_code: 13

  * argument: none

  * result: none

#### 2.2.14 CURSOR_CLOSE
  * function_code: 14

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | server_handle_id | ARG_INT | |

  * result: none

#### 2.2.15 CHANGE_DBUSER
  * function_code: 15

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | user | ARG_STR | |
    | password | ARG_STR | |

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | server_start_time | INT | 4 |  | 

#### 2.2.16 UPDATE_GROUPID
  * function_code: 16

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | migrator_id | ARG_INT |  | 
    | groupid | ARG_INT |  | 
    | target | ARG_INT |  | 
    | on_off | ARG_INT |  | 

  * result: none

#### 2.2.17 INSERT_GID_REMOVED_INFO
  * function_code: 17

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | groupid | ARG_INT | |

  * result: none

#### 2.2.18 DELTE_GID_REMOVED_INFO
  * function_code: 18

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | groupid | ARG_INT | |

  * result: none

#### 2.2.19 DELETE_GID_SKEY_INFO
  * function_code: 19

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | groupid | ARG_INT | |

  * result: none

#### 2.2.20 BLOCK_GLOBAL_DML
  * function_code: 20

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | start_or_end | ARG_INT | |

  * result: none

#### 2.2.21 SERVER_MODE
  * function_code: 21

  * argument: none

  * result

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | server_state | ARG_INT |  |  | 
    | server_addr | ARG_INT |  |  | 

#### 2.2.22 SEND_REPL_DATA
  * function_code: 22

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | note | ARG_STR |  | 
    | autocommit_mode | ARG_INT |  | 
    | tran_type | ARG_INT |  | 
    | num_items | ARG_INT |  | 
    | obj_item(0..n) |  |  | 

  * result: none

#### 2.2.23 NOTIFY_HA_AGENT_STATE
  * function_code: 23

  * argument

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | host_ip | ARG_STR |  | 
    | state | ARG_INT |  | 
    | autocommit_mode | ARG_INT |  | 

  * result: none

