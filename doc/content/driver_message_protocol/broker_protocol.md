## Broker Request Message Protocol

### Broker Request Message Protocol
  
  * Broker request
  
	|Description |	Type | Data length | Note |
	|--------|--------|--------|--------|
	| client_magic_str | STR | 4 | "RYE\001" | 
	| client_version_major | SHORT | 2 |  | 
	| client_version_minor | SHORT | 2 |  | 
	| client_version_patch | SHORT | 2 |  | 
	| client_version_build | SHORT | 2 |  | 
	| client_type | CHAR | 1 | 0x01: JDBC <br> 0x02: CCI | 
	| op_code | CHAR | 1 |  | 
	| op_code_msg_size | SHORT | 2 |  | 
	| RESERVED | CHAR | 4 |  | 
	| op_code_msg | CHAR | [op_code_msg_size] | defined in each request | 

  * Broker response

    | Description | Type | Data length | Note | 
    |--------|--------|--------|--------|
    | msg_header_size | INT | 4 |  | 
    | msg_header |  | [msg_header_size] |  | 
    | msg |  | [msg_size_array in msg_header] | defined in each request | 

  * Broker response msg_header

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | server_version_major | SHORT | 2 |  | 
    | server_version_minor | SHORT | 2 |  | 
    | server_version_patch | SHORT | 2 |  | 
    | server_version_build | SHORT | 2 |  | 
    | result_code | INT | 4 |  | 
    | msg_size_array | INT * 5 | 4 * 5 |  | 

### Normal Broker Request Message

#### CAS_CONNECT
  * op_code: 1
  * op_code_msg

    | Description | Type | Data length | 
    | -------- | -------- | -------- | 
    | broker_name_len | INT | 4 |
    | broker_name | STRING | |

  * broker response msg: none
  
#### PING
  * op_code: 2
  * op_code_msg

    | Description | Type | Data length | 
    | -------- | -------- | -------- | 
    | broker_name_len | INT | 4 |
    | broker_name | STRING | |

  * broker response msg: none

#### CANCEL  
  * op_code: 2
  * op_code_msg

    | Description | Type | Data length | 
    | -------- | -------- | -------- | 
    | broker_name_len | INT | 4 |
    | broker_name | STRING | |
    | cas_id | INT | 4 | 
    | cas_pid | INT | 4 | 

  * broker response msg: none

### LOCAL_MGMT Request Message
  
  * op_code_msg format

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | num_args | INT | 4 |  | 
    | br_req_arg (0..n) |  |  | defined in each request  | 
    | last_br_req_arg | BR_ARG_INT |  | 0x12345678 | 

  * op_code_msg br_req_arg format

    | Description | Type | Data length | Note | 
    | -------- | -------- | -------- | -------- | 
    | arg_type | CHAR | 1 | 0x01: INT32 <br> 0x02: INT64 <br> 0x03: STRING <br> 0x04: STRING_ARRAY <br> 0x05: INT32_ARRAY |
	| arg_value | INT32 <br> INT64 <br> STRING <br> STRING_ARRAY <br> INT32_ARRAY | 4 <br>   8 <br> size(4)+ string value <br> array_size + STRING values <br> array_size + INT32 values | BR_ARG_INT <br> BR_ARG_BIGINT <br> BR_ARG_STR <br> BR_ARG_STR_ARR <br> BR_ARG_INT_ARR|

#### SYNC_SHARD_MGMT_INFO
  
  * op_code: 16
  * br_req_arg

    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | local_dbname | BR_ARG_STR |  | 
    | global_dbname | BR_ARG_STR |  | 
    | nodeid | BR_ARG_INT |  | 
    | port | BR_ARG_INT |  | 
    | nodeid_version | BR_ARG_BIGINT |  | 
    | groupid_version | BR_ARG_BIGINT |  | 

  * reponse msg

	| msg | Description | Type | Length | 
    | -------- | -------- | -------- | -------- | 
    | msg[0] | hostname | CHAR | - |
    | msg[1] | ha_state | INT | 4 | 

#### LAUNCH_PROCESS
  * op_code : 17
  * br_req_arg

	| Description | Type | Note | 
    | -------- | -------- | -------- | 
    | launch_proc_id | BR_ARG_INT | 1: migrator <br> 2: rye command | 
    | process_args | BR_ARG_STR_ARR |  | 
    | process_env | BR_ARG_STR_ARR |  | 

  * response msg
	
    | msg | Description | Type | Length | 
    | -------- | -------- | -------- | -------- | 
    | msg[0] | exit_status | INT | 4 | 
    | msg[1] | stdout_msg | CHAR | - | 
    | msg[2] | stderr_msg | CHAR | -  | 

#### GET_SHARD_MGMT_INFO
  * op_code : 18
  * br_req_arg: none
    
  * response msg
    
    | msg | Description | Type | Length |
    | -------- | -------- | -------- | -------- |
    | msg[0] | shard_mgmt_info | - | -  |

    * shard_mgmt_info

      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | num_shard_mgmt | INT | 4 | 
      | one_shard_mgmt_info(0..n) |  |  | 

    * one_shard_mgmt_info

      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | dbname_len | INT | 4 | 
      | dbname | CHAR | [dbname_len] | 
      | port | INT | 4 | 

#### SHARD_VERSION_INFO_COUNT
  * op_code: 19
  * br_req_arg: none
  * response msg

  | msg | Description | Type | Length | 
  | -------- | -------- | -------- | -------- | 
  | msg[0] | shard_version_info_count | INT | 4 | 

#### READ_RYE_FILE
  * op_code: 20
  * br_req_arg

  | Description | Type | Note | 
  | -------- | -------- | -------- | 
  | file_type | BR_ARG_INT | 1: rye-auto.conf <br> 2: rye_broker.acl | 
  
  * response msg
  
  | msg | Description | Type | Length | 
  | -------- | -------- | -------- | -------- | 
  | msg[0] | file_contents | CHAR |  | 

#### WRITE_RYE_CONF
  * op_code: 21
  * br_req_arg

  | Description | Type | Note | 
  | -------- | -------- | -------- | 
  | size | BR_ARG_INT |  | 
  | contents | BR_ARG_STR |  | 

  * response msg: none

#### UPDATE_CONF
  * op_code: 22
  * br_req_arg

  | Description | Type | Note | 
  | -------- | -------- | -------- | 
  | croc_name | BR_ARG_STR |  | 
  | sect_name | BR_ARG_STR |  | 
  | key | BR_ARG_STR |  | 
  | value | BR_ARG_STR |  | 
  
  * response msg: none

#### DELETE_CONF
  * op_code: 23
  * br_req_arg
  
  | Description | Type | Note | 
  | -------- | -------- | -------- | 
  | croc_name | BR_ARG_STR |  | 
  | sect_name | BR_ARG_STR |  | 
  | key | BR_ARG_STR |  | 

  * response msg: none

#### GET_CONF
  * op_code: 24
  * br_req_arg: same as DELETE_CONF
  * response msg
  
  | msg | Description | Type | Length | 
  | -------- | -------- | -------- | -------- | 
  | msg[0] | conf_value | CHAR |  | 

#### BROKER_ACL_RELOAD
  * op_code: 24
  * br_req_arg
  
  | Description | Type | Note | 
  | -------- | -------- | -------- | 
  | size | BR_ARG_INT |  | 
  | acl_contents | BR_ARG_STR |  | 

  * response msg: none

### SHARD_MGMT Request Message

#### GET_SHARD_INFO
  * op_code: 64
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | client_node_version | BR_ARG_BIGINT |  | 
    | client_groupid_version | BR_ARG_BIGINT |  | 
    | db_creation_time | BR_ARG_BIGINT |  | 

  * response msg:none
  
    | msg | Description | Type | Length | 
  	| -------- | -------- | -------- | -------- | 
  	| msg[0] | db_creation_time | INT64 | 8 | 
  	| msg[1] | node_info |  |  | 
  	| msg[2] | groupid_info |  |  | 
  	| msg[3] | node_state |  |  | 

    * node_info
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | node_info_version | INT64 | 8 | 
      | node_info_count | INT | 4 | 
      | one_node_info(0..n) |  |  | 

    * one_node_info
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | node_id | SHORT | 2 | 
      | local_dbname_len | INT | 4 | 
      | local_dbname | CHAR |  | 
      | ip_str_len | INT | 4 | 
      | ip_str | CHAR |  | 
      | port | INT | 4 | 

    * groupid_info
      - if client_groupid_version <= 0
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | groupid_version | INT64 | 8 | 
      | groupid_count | INT | 4 | 
      | nodeid_array | SHORT ARRAY | 2 * [groupid_count] | 

      - if client_groupid_version > 0
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | groupid_version | INT64 | 8 | 
      | groupid_count | INT | 4 | 
      | chaned_groupid_count | INT | 4 | 
      | changed_groupid_info(0..n) |  |  | 

    * changed_groupid_info
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | groupid | INT | 4 | 
      | nodeid | SHORT | 2 | 

    * node_state
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | node_count | INT | 4 | 
      | one_node_state(0..n) |  |  | 

    * one_node_state
    
      | Description | Type | Data length | 
      | -------- | -------- | -------- | 
      | ip_str_len | INT | 4 | 
      | ip_str | CHAR | [ip_str_len] | 
      | ha_state | CHAR | 1 | 

#### INIT_SHARD
  * op_code: 65
  * br_req_arg
	
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dba_password | BR_ARG_STR |  | 
    | global_dbname | BR_ARG_STR |  | 
    | groupid_count | BR_ARG_INT |  | 
    | num_init_nodes | BR_ARG_INT |  | 
    | init_nodes | BR_ARG_STR_ARR |  | 

  * response msg:none

#### ADD_NODE
  * op_code: 66
  * br_req_arg
    
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | dba_password | BR_ARG_STR |  | 
    | node_info | BR_ARG_STR |  | 

    * response msg:none

#### DROP_NODE
  * op_code: 67
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | dba_password | BR_ARG_STR |  | 
    | drop_all | BR_ARG_INT |  | 
    | node_info | BR_ARG_STR |  | 

  * response msg:none

#### MIGRATION_START
  * op_code: 68
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | mig_groupid | BR_ARG_INT |  | 
    | dest_nodeid | BR_ARG_INT |  | 
    | num_shard_keys | BR_ARG_INT |  | 
    | timeout_sec | BR_ARG_INT |  | 

  * response msg:none

#### MIGRATION_END
  * op_code: 69
  * br_req_arg: same as MIGRATION_START
  * response msg:none

#### DDL_START
  * op_code: 70
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | timeout_sec | BR_ARG_INT |  | 

  * response msg:none

#### DDL_END
  * op_code: 71
  * br_req_arg: same as DDL_START
  * response msg:none

#### REBALANCE_REQ
  * op_code: 72
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | rebalance_type | BR_ARG_INT |  | 
    | ignore_prev_fail | BR_ARG_INT |  | 
    | dba_password | BR_ARG_STR |  | 
    | src_nodes | BR_ARG_INT_ARR |  | 
    | dest_nodes | BR_ARG_INT_ARR |  | 

  * response msg:none

#### REBALANCE_JOB_COUNT
  * op_code: 73
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | job_type | BR_ARG_INT | 0: remain <br> 1: complete <br> 2: failed | 

  * response msg:none

#### GC_START
  * op_code: 74
  * br_req_arg
  
    | Description | Type | Note | 
    | -------- | -------- | -------- | 
    | dbname | BR_ARG_STR |  | 
    | timeout_sec | BR_ARG_INT |  | 

  * response msg:none

#### GC_END
  * op_code: 75
  * br_req_arg: same as GC_START
  * response msg:none
