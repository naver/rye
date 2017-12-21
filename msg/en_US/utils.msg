$
$ Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
$
$   This program is free software; you can redistribute it and/or modify 
$   it under the terms of the GNU General Public License as published by 
$   the Free Software Foundation; either version 2 of the License, or
$   (at your option) any later version.
$
$  This program is distributed in the hope that it will be useful, 
$  but WITHOUT ANY WARRANTY; without even the implied warranty of 
$  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
$  GNU General Public License for more details. 
$
$  You should have received a copy of the GNU General Public License 
$  along with this program; if not, write to the Free Software 
$  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
$

$
$ $Id$
$
$ utils.msg
$
$ The following set numbers have been assigned:
$
$       1       generic
$       2       backupdb
$       3       copydb
$       4       createdb
$       6       deletedb
$       9       master
$       10      optimizedb
$       11      restoredb
$       14      compactdb
$       15      commdb
$       16      pathdb
$       17      addvoldb
$       18      checkdb
$       19      spacedb
$       20      estimatedb_data
$       21      estimatedb_index
$       22      installdb
$       23      migdb
$       24      diagdb
$       25      lockdb
$       26      killtran
$       37      plandump
$       38      paramdump
$       39      changemode
$       43      statdump
$       44      applyinfo
$       45      acldb
$       46      tranlist
$       47      prefetchlogdb

$set 1 MSGCAT_UTIL_SET_GENERIC
1 Invalid length or character '%1$c' in database name '%2$s'\n
2 Couldn't open output file '%1$s'\n
3 Missing or invalid utility name.\n
4 Invalid utility name '%1$s'\n
5 The following list contains the recognized utility names:\n
6 Invalid character '%1$c' in volume name '%2$s'\n
9 \
%1$s (Rye utilities)\n\
%2$s\n\
Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.\n
10 \
rye administrator utility, version %1$s\n\
usage: %2$s <utility-name> [args]\n\
Type '%3$s <utility-name>' for help on a specific utility.\n\
\n\
Available admin utility-name:\n\
    acldb\n\
    addvoldb\n\
    applyinfo\n\
    backupdb\n\
    createdb\n\
    deletedb\n\
    diagdb\n\
    killtran\n\
    lockdb\n\
    paramdump\n\
    plandump\n\
    prefetchlogdb\n\
    restoredb\n\
    spacedb\n\
    statdump\n\
    tranlist\n\
\n\
%4$s is a tool for DBMS.\n
12 '%1$s' utility-name is invalid.\nType 'rye --help' to see usage.\n
13 '%1$s' command is invalid.\nType 'rye --help' to see usage.\n
14 Could not load one or more mandatory properties in configuration files.\n
15 @ %1$s %2$s: %3$s\n
16 @ %1$s %2$s\n
17 ++ %1$s '%2$s' is not running.\n
18 ++ %1$s is not running.\n
19 ++ %1$s '%2$s' is running.\n
20 ++ %1$s is running.\n
21 ++ %1$s %2$s: %3$s\n
22 One or more arguments are missing.\nType 'rye --help' to see usage.\n
23 rye utility, version %1$s\n\
usage: %2$s <utility-name> [args]\n\
Type '%3$s <utility-name>' for help on a specific utility.\n\
\n\
Available service's utilities:\n\
    service\n\
    broker\n\
    heartbeat\n\
\n\
Available administrator's utilities:\n\
    addvoldb\n\
    applyinfo\n\
    backupdb\n\
    createdb\n\
    deletedb\n\
    diagdb\n\
    killtran\n\
    lockdb\n\
    paramdump\n\
    plandump\n\
    restoredb\n\
    spacedb\n\
    statdump\n\
    tranlist\n\
\n\
%4$s is a tool for DBMS.\n
25 rye utility, version %1$s\n\
usage: %2$s service <command>\n\
\n\
Available command:\n\
    start\n\
    stop\n\
    restart\n\
    status\n
26 rye utility, version %1$s\n\
usage: %2$s broker <command> [args]\n\
\n\
Available command:\n\
    start\n\
    stop\n\
    restart\n\
    info\n\
    status      [-b] [-f] [-q] [-t] [-s <sec>] [-u] [<expr>]\n\
    on          broker-name\n\
    off         broker-name\n\
    reset       broker-name\n\
    acl         <status|reload> [broker-name]\n\
    test        [-S] [-L] [-d <database_name>] [-u <user_name>] [-p <user_password>] [-c <SQL_command>] [-o <output_file>] [-v] [<broker_name>...] \n
27 rye utility, version %1$s\n\
usage: %2$s heartbeat <command> [args]\n\
\n\
Available command:\n\
    start          [database-name]\n\
    stop           [-i] [database-name]\n\
    status         [-v]\n\
    reload\n
28 Invalid command.\n
31 '%1$s' argument is not needed.\n
32 A database-name is missing.\n
33 %1$s: Such option is deprecated.\n
34 Invalid Parameter. %1$s %2$s\n
35 Cannot allocate memory.\n
36 The server was not configured for HA.\n
37 The server was configured for HA.\n
38 The server configured for HA is not listed in ha_db_list.\n
39 The server configured for HA is not listed in ha_node_list.\n
40 Invalid command.\n
41 rye manager server is not installed.\n
42 Invalid argument.\n
43 File(%1$s) open error.\n

$set 2 MSGCAT_UTIL_SET_BACKUPDB
30 WARNING: -t option is ignored in stand-alone mode execution.\n
31 ERROR: Destination-path does not exist or is not a directory.\n
32 ERROR: Not found host.\n
60 \
backupdb: Backup a database.\n\
usage: %1$s backupdb [OPTION] database-name\n\
\n\
valid options:\n\
  -D, --destination-path=PATH  store backup volumes to directory PATH; default: current directory\n\
  -r, --remove-archive         delete unnecessary log-archives; use carefully and consult manual\n\
  -o, --output-file=FILE       print detailed backup messages to FILE\n\
  -t, --thread-count=COUNT     number of threads; default: 0(auto)\n\
  -z, --compress               compress backup volumes; default: uncompress\n\
  -f, --force-overwrite        overwrite backup file if exist; default: no\n\
  -m, --make-slave             backup for making a slave; default: no\n\
  -c, --connect-mode           connect mode (s: slave, m:master); default: sm\n\
      --sleep-msecs=N          sleep N millisecond per 1M read; default: 0\n
71 Incompatible arguments, \"%1$s\", and \"%2$s\"\n


$set 4 MSGCAT_UTIL_SET_CREATEDB
40 Group '%1$s' does not exist\n
41 User '%1$s' does not exist\n
42 Unknown command '%1$s'\n
43 Couldn't open output file '%1$s'\n
44 Couldn't removing existing database.\n
45 Creating database with %1$s size using locale %2$s. The total amount of disk space needed is %3$s.\n
46 Couldn't create database.\n
47 Problem with user file '%1$s'\n
48 The given '%1$s', %2$s is out of range. Choose a size between %3$s and %4$s.\n
49 The value of %1$s '%2$s' is invalid.\n
60 \
createdb: Create a database.\n\
usage: %1$s createdb [OPTION] database-name\n\
\n\
valid options:\n\
      --db-volume-size=SIZE           size of first information volume; default: 512M\n\
      --log-volume-size=SIZE          size of log archives; default: 256M\n\
  -r, --replace                       replace an existing database; default: don't replace\n\
      --rsql-initialization-file=FILE FILE contain SQLs for an initialization of the database; default: none\n\
  -o, --output-file=FILE              redirect output messages to FILE; default: none\n\
  -v, --verbose                       enable verbose status messages; default: disabled\n


$set 6 MSGCAT_UTIL_SET_DELETEDB
60 \
deletedb: Delete a database.\n\
usage: %1$s deletedb [OPTION] database-name\n\
\n\
valid options:\n\
  -o, --output-file=FILE       redirect output messages to FILE; default: none\n\
  -d, --delete-backup          delete backup volumes; default: don't delete\n

$set 9 MSGCAT_UTIL_SET_MASTER
11 A %1$s process is already running.\n
12 MASTER SERVER starting. \n
13 MASTER SERVER exiting.\n
14 Error finding local host name\n
15 Could not find parameter file\n
16 Could not initialize master process\n
17 Server %1$s going down in %2$d minutes
18 Server %1$s notified of shutdown.\nThis may take several minutes. Please wait.
19 Server %1$s not found.
20 Master going down in %1$d minutes\n
21 The heartbeat feature has been deactivated. Please wait until all shutdown processes of rye_servers are completed.\n


$set 11 MSGCAT_UTIL_SET_RESTOREDB
19 Invalid date format, valid format is yyyy-mm-dd:hh:mm:ss or 'backuptime'\n
20 Restoredb cancelled or an error occurred.\n
21 Missing backup-file-path argument.\n
60 \
restoredb: Restore a database.\n\
usage: %1$s restoredb [OPTION] database-name\n\
\n\
valid options:\n\
  -d, --up-to-date=DATE             restore the database up to its condition at given DATE. DATE is given as follows: yyyy-mm-dd:hh:mm:ss or 'backuptime' keyword\n\
      --list                        display a list of backup volumes without restoring\n\
  -B, --backup-file-path=PATH       PATH is a file of backup volumes to be restored\n\
  -p, --partial-recovery            perform partial recovery if any log archive is absent\n\
  -o, --output-file=FILE            redirect output messages to FILE; default: none\n\
  -m, --make-slave                  restore for making a slave; default: no\n\



$set 15 MSGCAT_UTIL_SET_COMMDB
21 The Rye master running on host %1$s \nwas started at %2$s
22 has serviced %1$d requests. %2$d server(s) running.\n
23 SHUTTING DOWN in %1$s minutes\n
24 %1$s
25 Name of server to shutdown :
26 How many minutes until shutdown? :
27 \
rye_commdb: This utility is deprecated. Use 'rye' command instead.\n\
usage: rye_commdb [OPTION]\n\
\n\
valid options:\n\
  -P, --server-list                print server processes\n\
  -O, --all-list                   print all processes\n\
  -S, --shutdown-server=NAME       shutdown NAME database server\n\
  -A, --shutdown-all               shutdown all processes\n\
  -c, --server-mode=NAME           print NAME database server's HA mode\n\
  -N, --node-list                  list nodes managed by Rye heartbeat\n\
  -L, --process-list               list local process managed by Rye heartbeat\n\
  -p, --ping-host                  list ping hosts used for Rye heartbeat\n\
  -D, --dereg-process=PID          exit the target process and remove it from managed process entries\n\
  -F, --reconfig-node-list         apply Rye heartbeat configuration recently changed\n\
  -V, --verbose                    enable verbose status messages; default: disabled\n
28 Sorry, unknown command: %1$s\n
29 Could not get host name for local host - aborting\n
30 Could not find parameter file\n
31 Could not connect to master server on %1$s\n
32 How many minutes until shutdown? :
33 Master will shutdown in %1$d minutes\n
34 Error receiving data from client
35 the database server %1$s is not currently running
36 can't exec shell (%1$s) for %2$s, %3$s
37 process fork %1$s, returned %2$d, %3$s
38 can't execute %1$s
39 -i(--immediately) option must be used with --deact-stop-all option.\n


$set 17 MSGCAT_UTIL_SET_ADDVOLDB
20 Wrong value %1$d for npages\n
21 Wrong value '%1$s' for volume storage purpose.\n
22 WARNING: --max-writesize-in-sec option is ignored in stand-alone mode execution.\n
60 \
addvoldb: Add a volume-file for the database.\n\
usage: %1$s addvoldb [OPTION] database-name\n\
\n\
valid options:\n\
      --db-volume-size=SIZE        size of additional volume; default: db_volume_size in rye-auto.conf\n\
      --max-writesize-in-sec=SIZE  the amount of volume written per second; (ex. 512K, 1M, 10M); default: not used; minimum: 160K\n\
  -p, --purpose=PURPOSE            PURPOSE for adding volume file; allowed:\n\
                                           DATA - only for data\n\
                                           INDEX - only for indices\n\
                                           TEMP - only for temporary\n\
                                           GENERIC - for all purposes\n\
  -S, --SA-mode                    stand-alone mode execution\n\
  -C, --CS-mode                    client-server mode execution\n


$set 19 MSGCAT_UTIL_SET_SPACEDB
15 Space description for database '%1$s' with pagesize %2$s. (log pagesize: %3$s)\n\n
16 %1$5d %2$9s  %3$s %4$s  %5$s\n
17 Couldn't open output file '%1$s'\n
18 Space description for temporary volumes for database '%1$s' with pagesize %2$s.\n\n
19 Volid  Purpose   total_pages  free_pages  Vol Name\n\n
20 Volid  Purpose    total_size   free_size  Vol Name\n\n
21 -------------------------------------------------------------------------------\n
30 Summarized space description for database '%1$s' with pagesize %2$s. (log pagesize: %3$s)\n\n
31 Purpose    total_pages  used_pages  free_pages  volume_count\n
32 Purpose     total_size   used_size   free_size  volume_count\n
33 %1$10s %2$s %3$s %4$s %5$10d\n
34 -------------------------------------------------------------\n
40 Volid  Purpose   total_pages  free_pages  data_pages index_pages  temp_pages  Vol Name\n\n
41 Volid  Purpose    total_size   free_size   data_size  index_size   temp_size  Vol Name\n\n
42 %1$5d %2$9s  %3$s %4$s %5$s %6$s %7$s  %8$s\n
43 ----------------------------------------------------------------------------------------------------\n
50 Purpose    total_pages  used_pages  free_pages  data_pages index_pages  temp_pages  volume_count\n
51 Purpose     total_size   used_size   free_size   data_size  index_size   temp_size  volume_count\n
52 %1$10s %2$s %3$s %4$s %5$s %6$s %7$s %8$10d\n
53 -------------------------------------------------------------------------------------------------\n
60 \
spacedb: Display usage/free space of the database.\n\
usage: %1$s spacedb [OPTION] database-name\n\
\n\
valid options:\n\
  -o, --output-file=FILE       redirect output messages to FILE; default: none\n\
  -S, --SA-mode                stand-alone mode execution\n\
  -C, --CS-mode                client-server mode execution\n\
      --size-unit={PAGE|M|G|T|H}\n\
                               set the display unit (page, mega, giga, tera, human readable); default: H\n\
  -s, --summarize              display only a summary for each volume purpose\n\
  -p, --purpose                display the used size of each purpose(data/index/temp) of each volume\n





$set 23 MSGCAT_UTIL_SET_MIGDB
1 Temporary table OID\n
2 Can't print elo values\n
3 Error accessing long object for migration\n
4 Error opening output file for long object migration \n
5 Read error\n
6 Write error\n
7 Error opening ELO input file "%1$s"\n
8 TABLES:
9 hash filename
10 Name:                         %1$s\n
11 Size:                         %1$d\n
12 Page size:                    %1$d\n
13 Data size:                    %1$d\n
14 Entry size:                   %1$d\n
15 Entries per page:             %1$d\n
16 Cached pages:                 %1$d\n
17 Number of entries:            %1$d\n
18 Number of collisions:         %1$d\n
19 Hash filename:                %1$s\n
20 Next overflow entry:          %1$d\n
21 Key type:                     %1$s\n
22 Address of page headers:      %1$x\n
23 Address of last page header:  %1$x\n
24 Address of free page header:  %1$x\n
25 Address of page bitmap:       %1$x\n
26 Page bitmap size:             %1$d\n


$set 24 MSGCAT_UTIL_SET_DIAGDB
15 Couldn't open output file '%1$s'\n
60 \
diagdb: Dump a database.\n\
usage: %1$s diagdb [OPTION] database-name\n\
\n\
valid options:\n\
  -d, --dump-type=TYPE         TYPE of dump\n\
                               -1 dump all\n\
                                1 dump file tables\n\
                                2 dump file capacities\n\
                                3 dump heap capacities\n\
                                4 dump index capacities\n\
                                5 dump tablenames\n\
                                6 dump disk bitmaps\n\
                                7 dump catalog\n\
                                8 dump log\n\
                                9 dump heap\n\
      --start_pageid=ID        start log page id for log dump;default: 0\n\
      --num_pages=NUMBER       number of dump page for log dump; default: -1(all)\n\
  -o, --output-file=FILE       redirect output messages to FILE; default: none\n


$set 25 MSGCAT_UTIL_SET_LOCKDB
15 Couldn't open output file '%1$s'\n
59 lockdb cannot run as standalone mode.\n
60 \
lockdb: Display lock-status of the database.\n\
usage: %1$s lockdb [OPTION] database-name\n\
\n\
valid options:\n\
  -o, --output-file=FILE       redirect output messages to FILE; default: none\n


$set 26 MSGCAT_UTIL_SET_KILLTRAN
20 Only one argument must be given among: tran-index, user, host, progname\n
21 Enter DBA password:
26 There is not a transaction with the specified argument values.\n
27 Ready to kill the following transactions:\n\n
28 Do you wish to proceed ? (Y/N)
29 Killing transaction associated with transaction index %1$d\n
30 Killing of the following transactions has failed\n\n
31 The transaction does not finish yet, but it may be doing the rollback work.\n Please, wait a second and try it later if needed.\n
32 Invalid tranindex(%1$s).\n
59 killtran cannot run as standalone mode.\n
60 \
killtran: Manage transactions of the database.\n\
usage: %1$s killtran [OPTION] database-name\n\
\n\
valid options:\n\
  -i, --kill-transaction-index=INDEX kill transaction with transaction INDEX\n\
      --kill-user-name=ID            kill all transactions with user ID\n\
      --kill-host-name=HOST          kill all transactions with client HOST\n\
      --kill-program-name=NAME       kill all transactions with client program NAME\n\
      --kill-sql-id=SQL_ID           kill all transactions with SQL_ID\n\
  -p, --dba-password=PASS            password of the DBA user; will prompt if don't specify\n\
  -d, --display-information          display information about active transactions\n\
  -q, --query-exec-info              display information about query execution information of active transactions\n\
  -f, --force                        kill the transaction without a prompt for verification\n


$set 31 MSGCAT_UTIL_SET_MIGRATE
1 database-name
2 required argument, no default
3 compatibility-check
4 compatibility check only
5 backup-destination
6 a directory path or device for the destination of the backup volumes
20 database '%1$s' does not exist.
21 '%1$s' can't be migrated to %2$s, because its DB version is too low.\n
22 '%1$s' don't need to be migrated.\n
23 '%1$s' needs to be migrated.\n
24 Backup is recommended after migration. Do you want to back up your database now? (y/n):
25 We recommend to back up your new database with backupdb command after migration.
26 \n*** WARNING: Backup your database before you start migrating to %1$s ***\n\nTo recover errors which might be occurred during migration, we strongly recommend to back up your database.\nHave you already backed up your database? (y/n):
27 \n1. Run backupdb \n2. Enter the new backup path for the new database.(current path: %1$s)\n3. Exit \n
28 \nThe new backup path for the database:
29 \nMigration and backup have been successfully completed.
30 Backup failed. Please, retry to back up your database.
31 \n\n*** WARNING : Once the migration is completed, you cannot restore the database with the former backup volumes.\nNotice that the previous version of backup files can be restored only on the corresponding version of restoredb.\nPlease, check the former backup volumes is securely kept.\n

$set 34 MSGCAT_UTIL_SET_LOADJAVA
1 database-name
2 required argument, no default
3 java-class-file
4 required argument, no default
5 force-overwrite
6 overwrite class file if exist, default no

$set 37 MSGCAT_UTIL_SET_PLANDUMP
15 Cannot open output file '%1$s'\n
59 plandump cannot run as standalone mode.\n
60 \
plandump: Display or control cached query plans.\n\
usage: %1$s plandump [OPTION] database-name\n\
\n\
valid options:\n\
  -d, --drop                   drop all plans in the server's cache\n\
  -o, --output-file=FILE       redirect output messages to FILE; default: none\n

$set 38 MSGCAT_UTIL_SET_DUMPPARAM
11 Cannot open output file '%1$s'\n
21 System parameters to be used by the client program.\n
22 System parameters loaded by the server '%1$s'\n
23 System parameters to be used in standalone mode.\n
24 System parameters set at persist conf file.\n
60 \
paramdump: Dump system parameter settings.\n\
usage: %1$s paramdump [OPTION] database-name\n\
\n\
valid options:\n\
  -o, --output-file=FILE       redirect output message to FILE; default: none\n\
  -b, --both                   dump both client and server parameters\n\
  -S, --SA-mode                stand-alone mode execution\n\
  -C, --CS-mode                client-server mode execution\n


$set 40 MSGCAT_UTIL_SET_COPYLOGDB
11 Wrong value \"%1$s\" for the copylogdb's running mode.\n
21 Enter DBA password:
22 The server was not configured for HA.\n
58 HA feature is not supported in this platform.\n
59 copylogdb cannot run as standalone mode.\n

$set 41 MSGCAT_UTIL_SET_APPLYLOGDB
21 Enter DBA password:
22 The server was not configured for HA.\n
58 HA feature is not supported in this platform.\n
59 applylogdb cannot run as standalone mode.\n

$set 43 MSGCAT_UTIL_SET_STATDUMP
11 Cannot open output file '%1$s'\n
12 Cannot open rye_server shared memory.\n
59 statdump cannot run as standalone mode.\n
60 \
statdump: Dump global statistics.\n\
usage: %1$s statdump [OPTION] database-name\n\
\n\
valid options:\n\
  -o, --output-file=FILE       redirect output message to FILE; default: none\n\
  -c, --cumulative             print cumulative stats\n\
  -i, --interval=S             print stats every S secs\n\
  -s, --substr=STRING          print stat item only that match the substring\n
  -t, --output-type=STRING     print format(csv...); default: none\n

$set 44 MSGCAT_UTIL_SET_APPLYINFO
21 Enter DBA password:
22 The server was not configured for HA.\n
58 HA feature is not supported in this platform.\n
59 applyinfo cannot run as standalone mode.\n
60 \
applyinfo: display Rye HA Apply information.\n\
usage: %1$s applyinfo [OPTION] database-name\n\
\n\
valid options:\n\
  -r, --remote-host-name      remote host name; display remote node's active log information \n\
  -a, --applied-info 	      display applied information \n\
  -L, --copied-log-path=PATH  path of copied log volumes; display copied log information \n\
  -p, --page=ID               page id; default : 0\n\
  -v, --verbose               enable verbose status messages; default : disable\n\
  -i, --interval=S            print information every S secs\n

$set 45 MSGCAT_UTIL_SET_ACLDB
59 acldb cannot run as standalone mode.\n
60 \
acldb: Display or reload access control setting of the database.\n\
usage: %1$s acldb [OPTION] database-name\n\
\n\
valid options:\n\
  -r, --reload       reload ip control file\n

$set 46 MSGCAT_UTIL_SET_TRANLIST
20 Enter %1$s password: 
22 Tran index          User name      Host name      Process id      Program name\n
23 -------------------------------------------------------------------------------\n
24 %1$4d%2$-10s %3$14s %4$14s %5$15d %6$17s\n
25 There are no transactions\n
26 tranlist can only be performed by the DBA or a DBA group member.\n
27 sort-key column number(%1$d) is invalid.\n
32 Tran index         Process id    Program name              Query time    Tran time              Wait for lock holder      SQL_ID       SQL Text\n
33 %1$4d%2$-10s %3$14d    %4$-25s %5$10.2f %6$12.2f    %7$28s     %8$13s   %9$s\n
34 --------------------------------------------------------------------------------------------------------------------------------------------------\n
42 Tran index          User name      Host name      Process id    Program name              Query time    Tran time              Wait for lock holder      SQL_ID       SQL Text\n
43 %1$4d%2$-10s %3$14s %4$14s %5$14d    %6$-25s %7$10.2f %8$12.2f    %9$28s     %10$13s   %11$s\n
44 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n
45 Tran index : %1$d
46 SQL_ID: %1$s\n
59 tranlist cannot run as standalone mode.\n
60 \
tranlist: Dump active transaction information.\n\
usage: %1$s tranlist [OPTION] database-name\n\
\n\
valid options:\n\
  -u, --user=ID                user ID for database access; default: DBA user\n\
  -p, --password=PASS          user password; default: none\n\
  -s, --summary                print summarized information of transaction\n\
      --sort-key=COLUMN_NUMBER column number to be sorted\n\
      --reverse                reverse the sort result\n

$set 47 MSGCAT_UTIL_SET_PREFETCHLOGDB
22 The server was not configured for HA.\n
23 Prefetch feature is disabled.\n
58 HA feature is not supported in this platform.\n
59 prefetchlogdb cannot run as standalone mode.\n
