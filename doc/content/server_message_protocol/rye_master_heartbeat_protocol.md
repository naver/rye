## rye_master heartbeat Message Protocol

```
typedef struct hbp_header HBP_HEADER;
struct hbp_header
{
  unsigned char type;
  char reserved:7;
  char r:1;				// HBP_CLUSTER_HEARTBEAT
  unsigned short len;
  unsigned int seq;
  char group_id[HB_MAX_GROUP_ID_LEN];
  char orig_host_name[MAXHOSTNAMELEN];
  char dest_host_name[MAXHOSTNAMELEN];
};

char buffer[HB_BUFFER_SZ];		

```

  * msg

    | description | type | note |
    | -------- | -------- | -------- |
    | header | struct hbp_header |  |
    | node_state | int | |

  * msg is packed to buffer. total length of msg should be less than HB_BUFFER_SZ.

