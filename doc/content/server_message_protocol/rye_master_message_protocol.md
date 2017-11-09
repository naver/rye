## rye_master Message Protocol

  * packet structure : same as the PACKET structure of rye_server

### CONNECT

  * css_tcp_client_open_with_retry

  * css_send_magic

  * send connect request packet
    * COMMAND_TYPE packet
    * function_code: MASTER_CONN_TYPE_INFO
    * packet data

      | data | description | note |
      | -------- | -------- | -------- |
      | data0 | server_name (string) |  |

### rye_master info function call

  * request_for_int_value()

    * request data0: none
    * reply data0

      | description | type | note |
      | -------- | -------- | -------- |
      | int_value | int |  |

  * request_for_string_value()

    * request data0: none
    * reply data0

      | description | type | note |
      | -------- | -------- | -------- |
      | string_value | string |  |

#### MASTER_GET_START_TIME
  * request_for_string_value()

#### MASTER_GET_SERVER_COUNT
  * request_for_int_value()

#### MASTER_GET_REQUEST_COUNT
  * request_for_int_value()

#### MASTER_GET_SERVER_LIST
  * request_for_string_value()

#### MASTER_GET_HA_PING_HOST_INFO
  * request_for_string_value()

#### MASTER_GET_HA_NODE_LIST
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | verbose_yn | int |  |

  * reply data0

    | description | type | note |
    | -------- | -------- | -------- |
    | ha_node_list | string |  |

#### MASTER_GET_HA_PROCESS_LIST
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | verbose_yn | int |  |

    * reply data0

    | description | type | note |
    | -------- | -------- | -------- |
    | ha_process_list | string |  |

#### MASTER_GET_HA_ADMIN_INFO
  * request_for_string_value()

#### MASTER_IS_REGISTERED_HA_PROCS
  * request_for_string_value()
    * result string
      * HA_REQUEST_SUCCESS: "1\0"
      * HA_REQUEST_FAILURE: "0\0"

#### MASTER_GET_SERVER_STATE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | dbname | string |  |

    * reply data0

    | description | type | note |
    | -------- | -------- | -------- |
    | server_state | int |  |

#### MASTER_START_SHUTDOWN
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | minutes | int |  |

    * reply data0: none

#### MASTER_ACTIVATE_HEARTBEAT
  * request data0: none

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### MASTER_REGISTER_HA_PROCESS
```
typedef struct hbp_proc_register HBP_PROC_REGISTER;
struct hbp_proc_register
{
  int pid;
  HB_PROC_TYPE type;
  char exec_path[HB_MAX_SZ_PROC_EXEC_PATH];
  char args[HB_MAX_SZ_PROC_ARGS];
  char argv[HB_MAX_NUM_PROC_ARGV][HB_MAX_SZ_PROC_ARGV];
};
```

  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | hbp_regster | struct hbp_proc_register |  |

  * hb_create_master_reader()

#### MASTER_DEACT_STOP_ALL
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | deact_immediately | int |  |

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### MASTER_DEACT_CONFIRM_STOP_ALL
  * request data0: none

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | success_or_fail | int |  |

#### MASTER_DEACTIVATE_HEARTBEAT
  * request data0: none

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |
    | msg | string | |

#### MASTER_DEACT_CONFIRM_NO_SERVER
  * request data0: none

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | success_or_fail | int |  |

#### MASTER_RECONFIG_HEARTBEAT
  * request data0: none

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### MASTER_CHANGEMODE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | req_node_state | int |  |
    | force | int | |

    * reply data0: none

    | description | type | note |
    | -------- | -------- | -------- |
    | error | int |  |

#### MASTER_CHANGE_SERVER_STATE
  * request data0

    | description | type | note |
    | -------- | -------- | -------- |
    | server_state | int |  |

  * reply ??
