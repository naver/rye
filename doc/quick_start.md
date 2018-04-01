
#### Build on Linux
RYE builds successfully on 64-bit Linux (CentOS) environments. Other Linux distributions have not been tested.

**Compile**

To build RYE from the git-cloned source code,

```bash
$ ./autogen.sh
$ mkdir Release64
$ cd Release64
$ ../configure --prefix=$HOME/RYE
$ make
$ make install
```

#### Install

```bash
$ tar xvfz RYE.1.0.0.0617.tar.gz
$ mv RYE $HOME
$ export RYE=$HOME/RYE
$ export RYE_DATABASES=$HOME/db
$ export PATH=$RYE/bin:$PATH
$ export LD_LIBRARY_PATH=$RYE/lib:$LD_LIBRARY_PATH
$ export CLASSPATH=$RYE/jdbc/rye_jdbc.jar:$CLASS_PATH
$ rye --version
rye (Rye utilities)
Rye 1.0 (1.0.0.0617) (64bit release build for linux_gnu) (Oct 23 2017 12:53:00)
```

#### Run
To create and start demodb database, with single shard node
* HOST_IP=1.1.1.1
* SHARD_MGMT_PORT=40000

Execute service command at 1.1.1.1
```bash
$ rye service start
```

Execute shard init on a machine where RYE jdbc driver is installed
```bash
$ java rye.jdbc.admin.Shard init demodb 1.1.1.1 40000 --num-groups=10000
```

Execute service command at 1.1.1.1
```bash
$ rye service status
```

Connect to demodb database using jdbc connection url
(Can use a 3rd party SQL browser, such as the SQuirrel SQL Client)

```
jdbc:rye://1.1.1.1:40000/demodb:dba:/rw?charset=utf-8&queryTimeout=6000
```

Insert sample data

```SQL
CREATE SHARD TABLE mail (
   	uid  	VARCHAR,
   	mid  	INT,
   	title 	VARCHAR,
   	PRIMARY KEY (uid, mid)
) SHARD BY uid;

INSERT INTO mail VALUES ('id1', 1, 'hello user1');
INSERT INTO mail VALUES ('id2', 2, 'hello user2');
INSERT INTO mail VALUES ('id3', 3, 'hello user3');
INSERT INTO mail VALUES ('id4', 4, 'hello user4');
INSERT INTO mail VALUES ('id5', 5, 'hello user5');
```

Retrieve the data

```SQL
-- get shard node info
SELECT shard_nodeid(), count(*) FROM mail;
   shard_nodeid()   count(*)   
 ==============================
   1                5          

-- query with shardkey condition
SELECT title FROM mail WHERE uid = 'id1';
   title           
 ==================
   'hello user1'   

-- query with non-shardkey condition
SELECT title FROM mail WHERE mid < 3;
   title           
 ==================
   'hello user1'   
   'hello user2'
```

