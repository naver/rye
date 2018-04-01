
This section describes how to set up a database with docker in one machine.
The database consists of 2 shard nodes, and each node has one master database and one slave database.

### Download JDBC driver

Download rye jdbc driver from release page of rye github project. 
https://github.com/naver/rye/releases

set CLASSPATH
```
export CLASSPATH=<your_rye_jdbc_path>
```

### Run docker containers for node-1

make 2 docker containers for shard node-1.
One will be a master database and the other will be a slave database.

```
mkdir $HOME/DB1_1
sudo docker run -d -it --name ryedb1_1 -p 30000:30000 -p 30000:30000/udp \
                -v $HOME/DB1_1:/home/ryeadmin/DB ryedbms/rye:1.1

mkdir $HOME/DB1_2
sudo docker run -d -it --name ryedb1_2 -p 31000:30000 -p 31000:30000/udp \
                -v $HOME/DB1_2:/home/ryeadmin/DB ryedbms/rye:1.1
```

### Create databases and initialize shrard information

The following command creates 2 databases for shard node-1 and initializes shard information.
This example assumes that the machine's ip address is 1.1.1.1.

```
java rye.jdbc.admin.Shard init demodb 1.1.1.1:30000,1.1.1.1:31000 --num-groups=10000 -v
```

### Create table and data

Rye's JDBC connection url is:
```
jdbc:rye://<host_ip>:<host_port>/<dbname>:<dbuser>:<dbpassword>/rw?<property>
```

So, the connection url of this example is
```
jdbc:rye://1.1.1.1:30000/demodb:dba:/rw
```

You can use GUI Sql client like [squirrel-sql](http://squirrel-sql.sourceforge.net/)

create table and insert data
```
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
```
select mail.*, shard_nodeid() from mail
```

```
id1	1	hello user1	1
id2	2	hello user2	1
id3	3	hello user3	1
id4	4	hello user4	1
id5	5	hello user5	1
```

### Add node-2

make 2 more containers for node-2

```
mkdir $HOME/DB2_1
sudo docker run -d -it --name ryedb2_1 -p 32000:30000 -p 32000:30000/udp \
                -v $HOME/DB2_1:/home/ryeadmin/DB ryedbms/rye:1.1

mkdir $HOME/DB2_2
sudo docker run -d -it --name ryedb2_2 -p 33000:30000 -p 33000:30000/udp \
                -v $HOME/DB2_2:/home/ryeadmin/DB ryedbms/rye:1.1
```

The following command creates databases and makes the databases node-2.

```
java rye.jdbc.admin.Shard nodeAdd demodb 1.1.1.1:30000 2:1.1.1.1:32000,2:1.1.1.1:33000 -v

```

### Rebalance data

The following command rebalance shard data from node-1 to node-2.

```
java rye.jdbc.admin.Shard rebalance demodb 1.1.1.1:30000 1 2

```
This command operates just rebalance scheduling, and data migration will be executed sequentially.
You can check the shard nodes' information and rebalance status with the following command.

```
java rye.jdbc.admin.Shard info demodb 1.1.1.1:30000

```

You can see that some data's nodeid is changed by select query.
You don't need to change your connection url after node is addes.

```
select mail.*, shard_nodeid() from mail
```

```
id2	2	hello user2	1
id3	3	hello user3	1
id4	4	hello user4	1
id1	1	hello user1	2
id5	5	hello user5	2
```
