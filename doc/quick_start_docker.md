
This section describes how to set up a database with docker in one machine.
The database consists of 2 shard nodes, and each node has one master database and one slave database.

### run docker containers for node-1

make 2 docker containers for shard node 1.
The one will be a master database and the other will be a slave database.

```
mkdir $HOME/DB1_1
sudo docker run -d -it --name ryedb1_1 -p 30000:30000 -p 30000:30000/udp \
                -v $HOME/DB1_1:/home/ryeadmin/DB ryedbms/rye:1.1

mkdir $HOME/DB1_2
sudo docker run -d -it --name ryedb1_2 -p 31000:30000 -p 31000:30000/udp \
                -v $HOME/DB1_2:/home/ryeadmin/DB ryedbms/rye:1.1
```

### create databases and initialize shrard information

Following command creates 2 databases for shard node-1 and initialize shard information.

```
java rye.jdbc.admin.Shard init demodb 1.1.1.1:30000,1.1.1.1:31000 --num-groups=10000 -v
```


