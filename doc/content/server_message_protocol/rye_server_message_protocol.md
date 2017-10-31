## rye_server Message Protocol

  * packet structure 

	|Description |	Data length |
    |--------| --------|
	| header | sizeof(NET_HEADER) |
	| data0 | buffer_size[0] in header |
	| data1 | buffer_size[1] in header |
	| data2 | buffer_size[2] in header |
	| data3 | buffer_size[3] in header |
	| data4 | buffer_size[4] in header |

```
typedef struct packet_header NET_HEADER;
struct packet_header
{
  char packet_type;
  char is_server_in_tran;
  char reset_on_commit;
  char is_client_ro_tran;
  short server_shard_nodeid;
  short function_code;
  int tran_index;
  int request_id;
  int num_buffers;
  int buffer_sizes[5];
};

enum css_packet_type
{
  COMMAND_TYPE = 1,
  DATA_TYPE = 2,
  ABORT_TYPE = 3,
  CLOSE_TYPE = 4,
  ERROR_TYPE = 5
};

```

### 1. CONNECT

  * css_tcp_client_open_with_retry

  * css_send_magic
    * DATA_TYPE packet

    * packet data

      | data | description | note |
      | -------- | -------- | -------- |
      | data0 | net_magic | { 0x00, 0x00, 0x00, 0x01, 0x20, 0x08, 0x11, 0x22 } |

  * send connect request packet
    * COMMAND_TYPE packet
    * function_code: MASTER_CONN_TYPE_TO_SERVER
    * packet data

      | data | description | note |
      | -------- | -------- | -------- |
      | data0 | server_name (string) |  |

  * receive packet
    * DATA_TYPE packet
    * packet data

      | data | description | note |
      | -------- | -------- | -------- |
      | data0 | reason + portid | int + int |

  * ping_server_with_handshake
    *  COMMAND_TYPE packet
    *  request data0

      | description | type | note |
      | -------- | -------- | -------- |
      | client_release | string |  |
      | client_capabilities | int | bit mask <br> NET_CAP_BACKWARD_COMPATIBLE, NET_CAP_FORWARD_COMPATIBLE, NET_CAP_INTERRUPT_ENABLED, NET_CAP_UPDATE_DISABLED, NET_CAP_HA_REPL_DELAY, NET_CAP_HA_REPLICA, NET_CAP_HA_IGNORE_REPL_DELAY |
      | bit_platform | int | __WORDSIZE |
      | client_type | int |  |
      | host_name | string |  |

    * reply data0

      | description | type | note |
      | -------- | -------- | -------- |
      | server_release | string |  |
      | server_capabilities | int |  |
      | server_bit_platform | int |  |
      | server_host_name | string |  |

