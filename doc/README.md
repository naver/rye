## RYE: Native Sharding RDBMS

Scalability problems in relational databases are perceived as a major disadvantage. Sharding is one of the solutions to overcome the scalability problems, but the lack of functionality supported by DBMS makes it difficult to apply for the application. To solve it, we are developing RYE which is RDBMS supporting sharding functionality.

## [Quickstart](https://github.com/naver/rye#quickstart)

## [Manual](https://github.com/naver/rye/blob/master/doc/content/manual/readme.md)

## [Release notes](https://github.com/naver/rye/blob/master/doc/content/release_notes/release_notes.md)

## How-to Guides

### Creating and Managing RYE Databases

**Creating, starting and viewing database**

To create and start demodb database, with single shard node and HA configuration
* master HOST_IP=1.1.1.1
* slave HOST_IP=1.1.1.2
* SHARD_MGMT_PORT=40000

Execute service command at 1.1.1.1
```bash
rye service start
```

Execute service command at 1.1.1.2
```bash
rye service start
```

Execute shard init on a machine where RYE jdbc driver is installed
```bash
$ java rye.jdbc.admin.Shard init demodb 1.1.1.1,1.1.1.2 40000 --num-groups=10000
```

**Stopping, restarting database**

```bash
$ rye service stop
$ rye service start
$ rye service restart
```

**Viewing database operation status**

```bash
$ rye service status
$ rye broker status -s 1 -b
$ rye statdump -i 1 demodb1@localhost
```

**Viewing database space information**

```bash
$ rye spacedb --summarize --purpose demodb1@localhost
```

**Viewing, editing database settings**

```bash
$ cat $RYE_DATABASES/rye-auto.conf
{
  "server": {
    "common": {
      "ha_db_list": "demodb1",
      "ha_node_list": "bdogauunydzodngw@1.1.1.1:1.1.1.2",
      "ha_node_myself": "1.1.1.1"
    }
  },
  "broker": {
    "shard_mgmt": {
      "shard_mgmt_port": "40000",
      "shard_mgmt_metadb": "demodb1",
      "shard_mgmt_num_migrator": "10"
    }
  }
```

**Viewing log files**

```bash
$ cd $RYE_DATABASES/ryelog/
$ ls -t
rye_utility.log                      server
0685802b70eb_master.err              rye_client.err
rye_broker.log                       demodb1_createdb.err
demodb1@1.1.1.2_rye_repl.err  broker.err
broker
```

### Creating and Managing RYE Users

**Create and manage RYE users on a database**

Execute SQL
```SQL
CREATE USER test_user PASSWORD 'xxx';

CREATE USER ryeguest;
ALTER USER ryeguest PASSWORD 'firstpw';
```

### Connecting to databases

**Connecting RYE Client Using IP Addresses**

Connect to demodb database using jdbc connection url
(Can use a 3rd party SQL browser, such as the SQuirrel SQL Client)
* master HOST_IP=1.1.1.1
* SHARD_MGMT_PORT=40000

```
jdbc:rye://1.1.1.1:40000/demodb:dba:/rw?charset=utf-8&queryTimeout=6000
```

Insert sample data
```SQL
CREATE SHARD TABLE mail (
   	uid  	VARCHAR(20),
   	mid  	INT,
   	title 	VARCHAR(50),
   	PRIMARY KEY (uid, mid)
) SHARD BY uid;

INSERT INTO mail VALUES ('id1', 1, 'hello user1');
INSERT INTO mail VALUES ('id2', 2, 'hello user2');
INSERT INTO mail VALUES ('id3', 3, 'hello user3');
INSERT INTO mail VALUES ('id4', 4, 'hello user4');
INSERT INTO mail VALUES ('id5', 5, 'hello user5');
INSERT INTO mail VALUES ('id6', 6, 'hello user6');
INSERT INTO mail VALUES ('id7', 7, 'hello user7');
INSERT INTO mail VALUES ('id8', 8, 'hello user8');
INSERT INTO mail VALUES ('id9', 9, 'hello user9');

SELECT shard_nodeid(), count(*) FROM mail;

shard_nodeid()  count(*)              
--------------------------------------
1               9                     
```

### Managing RYE sharding

**Adding shard node**

To add shard node2 with HA configuration

* shard node2 master HOST_IP=1.1.1.3
* shard node2 slave HOST_IP=1.1.1.4

Execute service command at 1.1.1.3
```bash
rye service start
```

Execute service command at 1.1.1.4
```bash
rye service start
```

Execute shard nodeadd on a machine where RYE jdbc driver is installed
```bash
$ java rye.jdbc.admin.Shard nodeadd demodb 1.1.1.1 2:1.1.1.3,2:1.1.1.4
```

Execute SQL
```SQL
SELECT nodeid, host, version FROM shard_node ORDER BY nodeid, version;

  nodeid                host                  version
==================================================================
  1                     '1.1.1.1'      1
  1                     '1.1.1.2'      2
  2                     '1.1.1.3'      3
  2                     '1.1.1.4'      4

4 rows selected.
```

**Data rebalance**

Execute shard rebalance on a machine where RYE jdbc driver is installed
```bash
$ java rye.jdbc.admin.Shard rebalance demodb 1.1.1.1 1 2
```

Execute SQL
```SQL
SELECT shard_nodeid(), count(*) FROM mail;

shard_nodeid()  count(*)              
--------------------------------------
1               7                     
2               2 
```

## APIs & Reference

### Libraries and Samples

**Client Library for Java**

Information about getting started and downloading the RYE client library for Java.

## Concepts

## Tutorial

## Resources

### Resources

**FAQ**
Frequently asked questions about RYE.
