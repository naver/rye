Rye 프로세스 제어
====================

rye 유틸리티를 통해서 Rye 프로세스들을 제어할 수 있다.

Rye 서비스 제어
------------------

Rye 설정 파일에 등록된 서비스를 제어하기 위한 **rye** 유틸리티 구문은 다음과 같다. *command*에 올 수 있는 명령어는 서비스 구동을 위한 **start**, 종료를 위한 **stop**, 재시작을 위한 **restart**, 상태 확인을 위한 **status** 중 하나이며, 추가로 입력해야 할 옵션이나 인수는 없다.

    rye service <command>
    <command>: {start|stop|restart|status}

데이터베이스 서버 제어
----------------------

데이터베이스 서버 프로세스를 제어하기 위한 **rye** 유틸리티 구문은 다음과 같다. &lt;command&gt;에 올 수 있는 명령어는 서버 프로세스 구동을 위한 **start**, 종료를 위한 **stop**, 재시작을 위한 **restart**, 상태 확인을 위한 **status**가 있으며, **status**를 제외한 명령어에는 데이터베이스 이름이 인수로 지정되어야 한다.

    rye server <command> [database_name]
    <command>: {start|stop|restart|status}

브로커 제어
-----------

Rye 브로커 프로세스를 제어하기 위한 **rye** 유틸리티 구문은 다음과 같다. &lt;command&gt;로 올 수 있는 명령어는 브로커 프로세스 구동을 위한 **start** , 종료를 위한 **stop**, 재시작을 위한 **restart**, 상태 확인을 위한 **status**, 브로커 접속 제한을 위한 **acl**, 명시한 브로커만 사용 가능하게 하거나 불가능하게 하는 **on/off**, 브로커 접속을 리셋하기 위한 **reset**, 설정 정보 출력을 위한 **info**, SHARD key를 가지고 SHARD ID(SHARD 데이터베이스 ID)를 얻는 **getid**가 있다.

또한, SHARD 기능은 브로커가 구동되고 rye\_broker.conf의 SHARD라는 브로커 파라미터 값이 ON일 때만 사용할 수 있다.

    rye broker <command> 
    <command>: start
               |stop
               |restart
               |status [options] [broker_name_expr]
               |acl {status|reload} broker_name
               |on <broker_name> |off <broker_name>
               |reset broker_name 
               |info
               |getid -b <broker_name> [-f] shard_key

Rye HA 제어
--------------

Rye HA 기능을 사용하기 위한 **rye heartbeat** 유틸리티 구문은 다음과 같다.

    rye heartbeat <command>
    <command>: {start|stop|copylogdb|applylogdb|reload|status}

-   start: HA 관련 프로세스 구동
-   stop: HA 관련 프로세스 종료
-   copylogdb: copylogdb 프로세스를 시작 또는 정지
-   applylogdb: applylogdb 프로세스를 시작 또는 정지
-   reload: HA 구성정보를 다시 읽어서 새로운 구성에 맞게 실행
-   status: HA 상태 정보를 확인

자세한 내용은 rye-heartbeat를 참고한다.

Rye 서비스
=============

서비스 등록
-----------

사용자는 임의로 데이터베이스 서버, Rye 브로커, Rye 매니저, Rye HA를 데이터베이스 환경 설정 파일(rye.conf)에 Rye 서비스로 등록할 수 있다. 이를 위해 rye.conf의 service 파라미터 값으로 각각 server, broker, manager, heartbeat를 입력하면 되며, 이들을 쉼표(,)로 구분하여 여러 개를 같이 등록할 수 있다.

사용자가 별도로 서비스를 등록하지 않으면, 기본적으로 마스터 프로세스(cub\_master)만 등록된다. Rye 서비스에 등록되어 있으면 **rye service** 유틸리티를 사용해서 한 번에 관련된 프로세스들을 모두 구동, 정지하거나 상태를 알아볼 수 있어 편리하다.

Rye HA를 설정하는 방법은 rye-service-util을 참고한다.

다음은 데이터베이스 환경 설정 파일에서 데이터베이스 서버와 브로커를 서비스로 등록하고, Rye 서비스 구동과 함께 *demodb*와 *testdb*라는 데이터베이스를 자동으로 시작하도록 설정한 예이다.

    # rye.conf
    ...

    [service]

    # The list of processes to be started automatically by 'rye service start' command
    # Any combinations are available with server, broker, manager and heartbeat.
    service=server,broker

    # The list of database servers in all by 'rye service start' command.
    # This property is effective only when the above 'service' property contains 'server' keyword.
    server=demodb,testdb

서비스 구동
-----------

Linux 환경에서는 Rye 설치 후 Rye 서비스 구동을 위해 아래와 같이 입력한다. 데이터베이스 환경 설정 파일에서 서비스를 등록하지 않으면 기본적으로 마스터 프로세스(cub\_master)만 구동된다.

Windows 환경에서는 시스템 권한을 가진 사용자로 로그인한 경우에만 아래의 명령이 정상 수행된다. 관리자 또는 일반 사용자는 Rye 매니저 설치 후 작업 표시줄에 생성되는 Rye 서비스 트레이 아이콘을 클릭하여 Rye Server를 구동 또는 정지할 수 있다.

    % rye service start

    @ rye master start
    ++ rye master start: success

이미 마스터 프로세스가 구동 중이라면 다음과 같은 메시지가 표시된다.

    % rye service start

    @ rye master start
    ++ rye master is running.

마스터 프로세스의 구동에 실패한 경우라면 다음과 같은 메시지가 표시된다. 다음은 데이터베이스 환경 설정 파일(rye.conf)에 설정된 **rye\_port\_id** 파라미터 값이 충돌하여 구동에 실패한 예이다. 이런 경우에는 해당 포트를 변경하여 충돌 문제를 해결할 수 있다. 해당 포트를 점유하고 있는 프로세스가 없는데도 구동에 실패한다면 /tmp/Rye1523 파일을 삭제한 후 재시작한다. :

    % rye service start

    @ rye master start
    cub_master: '/tmp/Rye1523' file for UNIX domain socket exist.... Operation not permitted
    ++ rye master start: fail

control-rye-services 에 설명된 대로 서비스를 등록한 후, 서비스를 구동하기 위해 다음과 같이 입력한다. 마스터 프로세스, 데이터베이스 서버 프로세스, 브로커 및 등록된 *demodb*, *testdb*가 한 번에 구동됨을 확인할 수 있다.

    % rye service start

    @ rye master start
    ++ rye master start: success
    @ rye server start: demodb

    This may take a long time depending on the amount of recovery works to do.
    Rye 1.0

    ++ rye server start: success
    @ rye server start: testdb

    This may take a long time depending on the amount of recovery works to do.
    Rye 1.0

    ++ rye server start: success
    @ rye broker start
    ++ rye broker start: success

서비스 종료
-----------

Rye 서비스를 종료하려면 다음과 같이 입력한다. 사용자에 의해 등록된 서비스가 없는 경우, 마스터 프로세스만 종료된다.

    % rye service stop
    @ rye master stop
    ++ rye master stop: success

등록된 Rye 서비스를 종료하려면 다음과 같이 입력한다. *demodb*, *testdb*는 물론, 서버 프로세스, 브로커 프로세스, 마스터 프로세스가 모두 종료됨을 확인할 수 있다.

    % rye service stop
    @ rye server stop: demodb

    Server demodb notified of shutdown.
    This may take several minutes. Please wait.
    ++ rye server stop: success
    @ rye server stop: testdb
    Server testdb notified of shutdown.
    This may take several minutes. Please wait.
    ++ rye server stop: success
    @ rye broker stop
    ++ rye broker stop: success
    @ rye master stop
    ++ rye master stop: success

서비스 재구동
-------------

Rye 서비스를 재구동하려면 다음과 같이 입력한다. 사용자에 의해 등록된 서비스가 없는 경우, 마스터 프로세스만 종료 후 재구동된다.

    % rye service restart

    @ rye master stop
    ++ rye master stop: success
    @ rye master start
    ++ rye master start: success

등록된 Rye 서비스를 다음과 같이 입력한다. *demodb*, *testdb*는 물론, 서버 프로세스, 브로커 프로세스, 마스터 프로세스가 모두 종료된 후 재구동되는 것을 확인할 수 있다.

    % rye service restart

    @ rye server stop: demodb
    Server demodb notified of shutdown.
    This may take several minutes. Please wait.
    ++ rye server stop: success
    @ rye server stop: testdb
    Server testdb notified of shutdown.
    This may take several minutes. Please wait.
    ++ rye server stop: success
    @ rye broker stop
    ++ rye broker stop: success
    @ rye master stop
    ++ rye master stop: success
    @ rye master start
    ++ rye master start: success
    @ rye server start: demodb

    This may take a long time depending on the amount of recovery works to do.

    Rye 9.3

    ++ rye server start: success
    @ rye server start: testdb

    This may take a long time depending on the amount of recovery works to do.

    Rye 9.3

    ++ rye server start: success
    @ rye broker start
    ++ rye broker start: success

서비스 상태 관리
----------------

등록된 마스터 프로세스, 데이터베이스 서버의 상태를 확인하기 위하여 다음과 같이 입력한다.

    % rye service status

    @ rye master status
    ++ rye master is running.
    @ rye server status

    Server testdb (rel 1.0, pid 31059)
    Server demodb (rel 1.0, pid 30950)

    @ rye broker status
    % query_editor
    ----------------------------------------
    ID   PID   QPS   LQS PSIZE STATUS
    ----------------------------------------
     1 15465     0     0 48032 IDLE
     2 15466     0     0 48036 IDLE
     3 15467     0     0 48036 IDLE
     4 15468     0     0 48036 IDLE
     5 15469     0     0 48032 IDLE

    % broker1 OFF

    @ rye manager server status
    ++ rye manager server is not running.

만약, 마스터 프로세스가 중지된 상태라면, 다음과 같은 메시지가 출력된다.

    % rye service status
    @ rye master status
    ++ rye master is not running.

rye 유틸리티 로깅
--------------------

Rye는 rye 유틸리티의 수행 결과에 대한 로깅 기능을 제공한다.

**로깅 내용**

$Rye/log/rye\_utility.log 파일에 다음의 내용들이 로깅된다.

-   rye 유틸리티를 통해 수행된 모든 명령: usage, version, parsing 에러는 제외
-   rye 유틸리티 명령들의 수행 결과: 성공/실패
-   실패 시 오류메시지

**로그 파일 크기**

rye\_utility.log 파일의 크기는 rye.conf의 error\_log\_size 파라미터에 설정한 값만큼 커지고, 해당 크기만큼 커지면 rye\_utility.log.bak 파일로 백업된다.

**로그 포맷**

    시간 (rye PID) 내용

출력되는 로그 파일의 예는 다음과 같다.

    13-11-19 15:27:19.426 (17724) rye manager stop
    13-11-19 15:27:19.430 (17724) FAILURE: ++ rye manager server is not running.
    13-11-19 15:27:19.434 (17726) rye service start
    13-11-19 15:27:19.439 (17726) FAILURE: ++ rye master is running.
    13-11-19 15:27:22.931 (17726) SUCCESS
    13-11-19 15:27:22.936 (17756) rye service restart
    13-11-19 15:27:31.667 (17756) SUCCESS
    13-11-19 15:27:31.671 (17868) rye service stop
    13-11-19 15:27:34.909 (17868) SUCCESS

단, Windows 환경에서는 일부 rye 명령이 서비스 프로세스를 통해 다시 실행되는 구조이므로 Linux와 달리 중첩된 정보가 출력될 수 있다.

    13-11-13 17:17:47.638 ( 3820) rye service stop
    13-11-13 17:17:47.704 ( 7848) d:\Rye\bin\rye.exe service stop --for-windows-service
    13-11-13 17:17:56.027 ( 7848) SUCCESS
    13-11-13 17:17:57.136 ( 3820) SUCCESS

또한 Windows 환경에서는 서비스 프로세스를 통해 수행되는 프로세스는 오류 메시지를 출력하지 못하므로, 서비스 구동과 관련된 오류메시지는 반드시 rye\_utility.log를 통해 확인해야 한다.

데이터베이스 서버
=================

데이터베이스 서버 구동
----------------------

*demodb* 서버를 구동하기 위하여 다음과 같이 입력한다.

    % rye server start demodb

    @ rye server start: demodb

    This may take a long time depending on the amount of recovery works to do.

    Rye 1.0

    ++ rye server start: success

마스터 프로세스가 중지된 상태에서 *demodb* 서버를 시작하면 다음과 같이 자동으로 마스터 프로세스를 구동한 후 지정된 데이터베이스 서버를 구동한다.

    % rye server start demodb

    @ rye master start
    ++ rye master start: success
    @ rye server start: demodb

    This may take a long time depending on the amount of recovery works to do.

    Rye 1.0

    ++ rye server start: success

이미 *demodb* 서버가 구동 중인 상태라면 다음과 같은 메시지가 출력된다.

    % rye server start demodb

    @ rye server start: demodb
    ++ rye server 'demodb' is running.

**rye server start** 명령은 HA 모드의 설정과는 상관없이 특정 데이터베이스의 cub\_server 프로세스만 구동한다. HA 환경에서 데이터베이스를 구동하려면 **rye heartbeat start**를 사용해야 한다.

데이터베이스 서버 종료
----------------------

*demodb* 서버 구동을 종료하기 위하여 다음과 같이 입력한다.

    % rye server stop demodb

    @ rye server stop: demodb
    Server demodb notified of shutdown.
    This may take several minutes. Please wait.
    ++ rye server stop: success

이미 *demodb* 서버가 종료된 상태라면, 다음과 같은 메시지가 출력된다.

    % rye server stop demodb

    @ rye server stop: demodb
    ++ rye server 'demodb' is not running.

**rye server stop** 명령은 HA 모드의 설정과는 상관없이 특정 데이터베이스의 cub\_server 프로세스만 종료하며, 데이터베이스 서버가 재시작되거나 failover가 일어나지 않으므로 주의해야 한다. HA 환경에서 데이터베이스를 중지하려면 **rye heartbeat stop** 를 사용해야 한다.

데이터베이스 서버 재구동
------------------------

*demodb* 서버를 재구동하기 위하여 다음과 같이 입력한다. 이미 구동 중인 *demodb* 서버를 중지시킨 후 재구동하는 것을 알 수 있다.

    % rye server restart demodb

    @ rye server stop: demodb
    Server demodb notified of shutdown.
    This may take several minutes. Please wait.
    ++ rye server stop: success
    @ rye server start: demodb

    This may take a long time depending on the amount of recovery works to do.

    Rye 1.0

    ++ rye server start: success

데이터베이스 상태 확인
----------------------

데이터베이스 서버의 상태를 확인하기 위하여 다음과 같이 입력한다. 구동 중인 모든 데이터베이스 서버의 이름이 표시된다.

    % rye server status

    @ rye server status
    Server testdb (rel 1.0, pid 24465)
    Server demodb (rel 1.0, pid 24342)

마스터 프로세스가 중지된 상태라면, 다음과 같은 메시지가 출력된다.

    % rye server status

    @ rye server status
    ++ rye master is not running.

데이터베이스 서버 접속 제한
---------------------------

데이터베이스 서버에 접속하는 브로커 및 RSQL 인터프리터를 제한하려면 **rye.conf**의 **access\_ip\_control** 파라미터 값을 yes로 설정하고, **access\_ip\_control\_file** 파라미터 값에 접속을 허용하는 IP 목록을 작성한 파일 경로를 입력한다. 파일 경로는 절대 경로로 입력하며, 상대 경로로 입력하면 Linux에서는 **$Rye/conf** 이하, Windows에서는 **%Rye%\\conf** 이하의 위치에서 파일을 찾는다.

**rye.conf** 파일에는 다음과 같이 설정한다.

    # rye.conf
    access_ip_control=yes
    access_ip_control_file="/home1/rye1/Rye/db.access"

**access\_ip\_control\_file** 파일의 작성 형식은 다음과 같다.

    [@<db_name>]
    <ip_addr>
    ...

-   &lt;db\_name&gt;: 접근을 허용할 데이터베이스 이름.
-   &lt;ip\_addr&gt;: 접근을 허용할 IP 주소. 뒷자리를 \*로 입력하면 뒷자리의 모든 IP를 허용한다. 하나의 데이터베이스 이름 다음 줄에 여러 줄의 &lt;ip\_addr&gt;을 추가할 수 있다.

여러 개의 데이터베이스에 대해 설정하기 위해 \[@&lt;db\_name&gt;\]과 &lt;ip\_addr&gt;을 추가로 지정할 수 있다.

**access\_ip\_control**이 yes인 상태에서 **access\_ip\_control\_file**이 설정되지 않으면, 서버는 모든 IP를 차단하고 localhost만 접속을 허용한다. 서버 구동 시 잘못된 형식으로 인해 **access\_ip\_control\_file** 분석에 실패하면 서버는 구동되지 않는다.

다음은 **access\_ip\_control\_file**의 한 예이다.

    [@dbname1]
    10.10.10.10
    10.156.*

    [@dbname2]
    *

    [@dbname3]
    192.168.1.15

위의 예에서 *dbname1* 데이터베이스는 10.10.10.10이거나 10.156으로 시작하는 IP의 접속을 허용한다. *dbname2* 데이터베이스는 모든 IP의 접속을 허용한다. *dbname3* 데이터베이스는 192.168.1.15인 IP의 접속을 허용한다.

이미 구동되어 있는 데이터베이스에 대해서는 다음 명령어를 통해 설정 파일을 다시 적용하거나, 현재 적용된 상태를 확인할 수 있다.

**access\_ip\_control\_file**의 내용을 변경하고 이를 서버에 적용하려면 다음 명령어를 사용한다.

    rye server acl reload <database_name>

현재 구동 중인 서버의 IP 설정 내용을 출력하려면 다음 명령어를 사용한다.

    rye server acl status <database_name>

데이터베이스 서버 로그
----------------------

### 에러 로그

허용되지 않는 IP에서 접근하면 서버 에러 로그 파일에 다음과 같은 서버 에러 로그가 남는다.

    Time: 10/29/10 17:32:42.360 - ERROR *** ERROR CODE = -1022, Tran = 0, CLIENT = (unknown):(unknown)(-1), EID = 2
    Address(10.24.18.66) is not authorized.

데이터베이스 서버의 에러 로그는 $Rye/log/server 디렉터리에 생성되며, 파일 이름은 &lt;db\_name&gt;\_&lt;yyyymmdd&gt;\_&lt;hhmi&gt;.err 형식으로 저장된다. 확장자는 .err이다.

    demodb_20130618_1655.err

브로커에서의 접속 제한을 위해서는 limiting-broker-access 을 참고한다.

### 이벤트 로그

질의 성능에 영향을 주는 이벤트가 발생하면 해당 이벤트를 이벤트 로그에 기록한다.

이벤트 로그에 저장되는 이벤트는 SLOW\_QUERY, MANY\_IOREADS, LOCK\_TIMEOUT, DEADLOCK, 그리고 TEMP\_VOLUME\_EXPAND가 있다.

해당 로그 파일은 $Rye/log/server 디렉터리에 생성되며, 파일 이름은 &lt;db\_name&gt;\_&lt;yyyymmdd&gt;\_&lt;hhmi&gt;.event 형식으로 저장된다. 확장자는 .event이다.

    demodb_20130618_1655.event

**SLOW\_QUERY**

슬로우 쿼리(slow query)가 발생했을 때 기록한다. rye.conf의 **sql\_trace\_slow** 파라미터 값이 설정되면 동작한다. 다음은 출력 예이다.

    06/12/13 16:41:05.558 - SLOW_QUERY
      client: PUBLIC@testhost|rsql(13173)
      sql: update [y] [y] set [y].[a]= ?:1  where [y].[a]= ?:0  using index [y].[pk_y_a](+)
      bind: 5
      bind: 200
      time: 1015
      buffer: fetch=48, ioread=2, iowrite=0
      wait: cs=1, lock=1010, latch=0

-   client: &lt;DB 사용자&gt;@&lt;응용 클라이언트 호스트 명&gt;|&lt;프로그램 이름&gt;(&lt;프로세스 ID&gt;)
-   sql: 슬로우 쿼리
-   bind: 바인딩되는 값. sql 항목에 나타난 ?:&lt;num&gt;에서 &lt;num&gt;의 순서대로 출력된다. ?:0의 값이 5이고, ?:1의 값이 200이다.
-   time: 수행 시간 (ms)
-   buffer: buffer 수행 통계
    -   fetch: 페치 페이지 개수
    -   ioread: I/O 읽기 페이지 개수
    -   iowrite: I/O 쓰기 페이지 개수
-   wait: 대기 시간
    -   cs: 크리티컬 섹션에서 대기한 시간(ms)
    -   lock: 잠금을 획득하려고 대기한 시간(ms)
    -   latch: 래치를 획득하려고 대기한 시간(ms)

위의 예에서 질의 수행 시간이 1015ms가 소요되었는데 lock wait 시간이 1010ms 소요되어, 질의 수행 시간의 대부분이 잠금 대기 시간이었음을 알 수 있다.

**MANY\_IOREADS**

I/O 읽기를 많이 발생시킨 질의를 기록한다. rye.conf의 **sql\_trace\_ioread\_pages** 파라미터 설정 값 이상 I/O 읽기가 발생하면 로그를 기록한다. 다음은 출력 예이다.

    06/12/13 17:07:29.457 - MANY_IOREADS
      client: PUBLIC@testhost|rsql(12852)
      sql: update [x] [x] set [x].[a]= ?:1  where ([x].[a]> ?:0 ) using index [x].[idx](+)
      bind: 8
      bind: 100
      time: 528
      ioreads: 15648 

-   client: &lt;DB 사용자&gt;@&lt;응용 클라이언트 호스트 명&gt;|&lt;프로세스 이름&gt;(&lt;프로세스 ID&gt;)
-   sql: 많은 I/O 읽기를 유발한 SQL
-   bind: 바인딩되는 값. sql 항목에 나타난 ?:&lt;num&gt;에서 &lt;num&gt;의 순서대로 출력된다. ?:0의 값이 8이고, ?:1의 값이 100이다.
-   time: 수행 시간 (ms)
-   ioreads: I/O 읽기 페이지 개수

**LOCK\_TIMEOUT**

잠금 타임아웃(lock timeout)이 발생하면 waiter와 blocker의 질의문을 기록한다. 다음은 출력 예이다.

    06/13/13 20:56:18.650 - LOCK_TIMEOUT
    waiter:
      client: public@testhost|rsql(21529)
      lock:   NX_LOCK (oid=-532|540|16386, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=400 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 1

    blocker:
      client: public@testhost|rsql(21541)
      lock:   NX_LOCK (oid=-532|540|16386, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=100 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 1

-   waiter: 잠금(lock)을 획득하려고 대기하는 클라이언트
    -   lock: 잠금 종류, 테이블 및 인덱스 이름
    -   sql: 잠금을 획득하려고 대기하는 SQL
    -   bind: 바인딩된 값
-   blocker: 잠금(lock)을 소유하고 있는 클라이언트
    -   lock: 잠금 종류, 테이블 및 인덱스 이름
    -   sql: 잠금을 획득 중인 SQL
    -   bind: 바인딩된 값

위에서 잠금 타임아웃을 유발한 blocker와 잠금을 대기한 waiter를 알 수 있다.

**DEADLOCK**

교착 상태(deadlock)가 발생했을 때, cycle에 속해있는 트랜잭션의 잠금(lock) 정보들을 기록한다. 다음은 출력 예이다.

    06/13/13 20:56:17.638 - DEADLOCK
    client: public@testhost|rsql(21541)
    hold:
      lock:   NX_LOCK (oid=-532|540|16385, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=100 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 1

      lock:   NX_LOCK (oid=-532|540|16386, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=100 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 1

      lock:    X_LOCK (oid=0|540|1, table=y)
      sql: update [y] [y] set [a]=100 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 1

    wait:
      lock:   NX_LOCK (oid=-532|540|16390, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=300 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 5

    client: public@testhost|rsql(21529)
    hold:
      lock:   NX_LOCK (oid=-532|540|16389, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=200 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 5

      lock:   NX_LOCK (oid=-532|540|16390, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=200 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 5

      lock:    X_LOCK (oid=0|540|5, table=y)
      sql: update [y] [y] set [a]=200 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 5

    wait:
      lock:   NX_LOCK (oid=-532|540|16386, table=y, index=pk_y_a)
      sql: update [y] [y] set [a]=400 where ([y].[a]= ?:0 ) using index [y].[pk_y_a](+)
      bind: 1

-   client: &lt;DB 사용자&gt;@&lt;응용 클라이언트 호스트 명&gt;|&lt;프로세스 이름&gt;(&lt;프로세스 ID&gt;)
    -   hold: 잠금을 소유하고 있는 객체
        -   lock: 잠금 종류, 테이블 및 인덱스 이름
        -   sql: 잠금을 소유하고 있는 SQL
        -   bind: 바인딩된 값
    -   wait: 잠금을 대기하고 있는 객체
        -   lock: 잠금 종류, 테이블 및 인덱스 이름
        -   sql: 잠금을 대기하고 있는 SQL
        -   bind: 바인딩된 값

위에서 교착 상태를 유발한 응용 클라이언트들과 SQL을 확인할 수 있다.

잠금(lock)에 대한 자세한 설명은 lockdb과 lock-protocol을 참고한다.

**TEMP\_VOLUME\_EXPAND**

일시적 임시 볼륨(temporary temp volume)이 확장되면 해당 시각을 기록한다. 이를 통해 일시적 임시 볼륨 확장을 유발한 트랜잭션을 확인할 수 있다.

    06/15/13 18:55:43.458 - TEMP_VOLUME_EXPAND
      client: public@testhost|rsql(17540)
      sql: select [x].[a], [x].[b] from [x] [x] where (([x].[a]< ?:0 )) group by [x].[b] order by 1
      bind: 1000
      time: 44
      pages: 24399

-   client: &lt;DB 사용자&gt;@&lt;응용 클라이언트 호스트 명&gt;|&lt;프로그램 이름&gt;(&lt;프로세스 ID&gt;)
-   sql: 일시적 임시 볼륨이 필요한 SQL. INSERT ... SELECT를 제외한 모든 INSERT 문, DDL 문 등은 DB 서버에 SQL이 전달되지 않기 때문에 EMPTY로 표시된다. SELECT, UPDATE, DELETE 문은 SQL이 표시된다.
-   bind: 바인딩된 값
-   time: 일시적 임시 볼륨을 생성하는데 소요된 시간(ms).
-   pages: 일시적 임시 볼륨 생성에 필요한 페이지 개수

데이터베이스 서버 에러
----------------------

데이터베이스 서버 프로세스는 에러 발생 시 서버 에러 코드를 사용한다. 서버 에러는 서버 프로세스를 사용하는 모든 작업에서 발생할 수 있다. 예를 들어 질의를 처리하는 프로그램 또는 **rye** 유틸리티 사용 중에도 발생할 수 있다.

**데이터베이스 서버 에러 코드의 확인**

-   **Rye/include/dbi.h** 파일의 **\#define ER\_**로 시작하는 정의문은 모두 서버 에러 코드를 나타낸다.
-   **Rye/msg/en\_US** (한글은 ko\_KR.eucKR 혹은 ko\_KR.utf8) **/rye.msg** 파일의 "$set 5 MSGCAT\_SET\_ERROR" 이하 메시지 그룹은 모두 서버 에러 메시지를 나타낸다.

CCI 드라이버를 사용하여 C로 프로그램을 작성할 때는 에러 코드 번호를 직접 사용하는 것보다는 에러 코드 이름을 사용할 것을 권장한다. 예를 들어, 고유 키 위반 시 에러 코드 번호는 -670 혹은 -886이지만 이 번호보다는 **ER\_BTREE\_UNIQUE\_FAILED** 혹은 **ER\_UNIQUE\_VIOLATION\_WITHKEY**을 사용하는 것이 프로그램 가독성을 높이기 때문이다.

하지만 JDBC 드라이버를 사용하여 JAVA로 프로그램을 작성할 때는 dbi.h 파일을 포함할 수 없으므로 에러 코드 번호를 직접 사용하도록 한다. JDBC의 경우 SQLException 클래스의 getErrorCode() 메서드를 통해 에러 번호를 얻을 수 있다.

    $ vi $Rye/include/dbi.h

    #define NO_ERROR                                       0
    #define ER_FAILED                                     -1
    #define ER_GENERIC_ERROR                              -1
    #define ER_OUT_OF_VIRTUAL_MEMORY                      -2
    #define ER_INVALID_ENV                                -3
    #define ER_INTERRUPTED                                -4
    ...
    #define ER_LK_OBJECT_TIMEOUT_SIMPLE_MSG              -73
    #define ER_LK_OBJECT_TIMEOUT_CLASS_MSG               -74
    #define ER_LK_OBJECT_TIMEOUT_CLASSOF_MSG             -75
    #define ER_LK_PAGE_TIMEOUT                           -76
    ...
    #define ER_PT_SYNTAX                                -493
    ...
    #define ER_BTREE_UNIQUE_FAILED                      -670
    ...
    #define ER_UNIQUE_VIOLATION_WITHKEY                 -886
    ...
    #define ER_LK_OBJECT_DL_TIMEOUT_SIMPLE_MSG          -966
    #define ER_LK_OBJECT_DL_TIMEOUT_CLASS_MSG           -967
    #define ER_LK_OBJECT_DL_TIMEOUT_CLASSOF_MSG         -968

    ...

몇 가지 서버 에러 코드 이름 및 에러 코드 번호, 에러 메시지를 살펴보면 다음과 같다.

<table>
<colgroup>
<col width="17%" />
<col width="11%" />
<col width="71%" />
</colgroup>
<thead>
<tr class="header">
<th>에러 코드 이름</th>
<th>에러 번호</th>
<th>에러 메시지</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>ER_LK_OBJECT_TIMEOUT_SIMPLE_MSG</td>
<td>-73</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? lock on object ?|?|?. You are waiting for user(s) ? to finish.</td>
</tr>
<tr class="even">
<td>ER_LK_OBJECT_TIMEOUT_CLASS_MSG</td>
<td>-74</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? lock on class ?. You are waiting for user(s) ? to finish.</td>
</tr>
<tr class="odd">
<td>ER_LK_OBJECT_TIMEOUT_CLASSOF_MSG</td>
<td>-75</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? lock on instance ?|?|? of class ?. You are waiting for user(s) ? to finish.</td>
</tr>
<tr class="even">
<td>ER_LK_PAGE_TIMEOUT</td>
<td>-76</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? on page ?|?. You are waiting for user(s) ? to release the page lock.</td>
</tr>
<tr class="odd">
<td>ER_PT_SYNTAX</td>
<td>-493</td>
<td>Syntax: ?</td>
</tr>
<tr class="even">
<td>ER_BTREE_UNIQUE_FAILED</td>
<td>-670</td>
<td>Operation would have caused one or more unique constraint violations.</td>
</tr>
<tr class="odd">
<td>ER_UNIQUE_VIOLATION_WITHKEY</td>
<td>-886</td>
<td>&quot;?&quot; caused unique constraint violation.</td>
</tr>
<tr class="even">
<td>ER_LK_OBJECT_DL_TIMEOUT_SIMPLE_MSG</td>
<td>-966</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? lock on object ?|?|? because of deadlock. You are waiting for user(s) ? to finish.</td>
</tr>
<tr class="odd">
<td>ER_LK_OBJECT_DL_TIMEOUT_CLASS_MSG</td>
<td>-967</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? lock on class ? because of deadlock. You are waiting for user(s) ? to finish.</td>
</tr>
<tr class="even">
<td>ER_LK_OBJECT_DL_TIMEOUT_CLASSOF_MSG</td>
<td>-968</td>
<td>Your transaction (index ?, ?@?|?) timed out waiting on ? lock on instance ?|?|? of class ? because of deadlock. You are waiting for user(s) ? to</td>
</tr>
</tbody>
</table>

브로커
======

브로커 구동
-----------

브로커를 구동하기 위하여 다음과 같이 입력한다. rye\_broker.conf 의 브로커 파라미터인 **SHARD**가 ON으로 설정된 경우 SHARD 기능이 활성화된다.

    $ rye broker start
    @ rye broker start
    ++ rye broker start: success

이미 브로커가 구동 중이라면 다음과 같은 메시지가 출력된다.

    rye broker start
    @ rye broker start
    ++ rye broker is running.

Linux 시스템에서 샤드 구동 시 필요한 파일 디스크립터(file descriptor, fd) 개수는 rye\_broker.conf에서 설정하는 SHARD\_MAX\_CLIENTS보다 적당히 많은 정도이므로, "ulimit -n"으로 fd의 개수를 제약할 때 SHARD\_MAX\_CLIENTS보다 적당히 크게 설정해야 한다. Linux 시스템의 fd 개수 제약이 SHARD에서 필요한 fd 개수보다 작게 설정된 경우, SHARD 구동에 실패하면서 출력되는 오류 메시지에는 SHARD에서 필요한 fd 개수가 표시된다.

브로커 종료
-----------

브로커를 종료하기 위하여 다음과 같이 입력한다. rye\_broker.conf 의 브로커 파라미터인 SHARD가 ON으로 설정된 경우 SHARD 기능이 정지된다.

    $ rye broker stop
    @ rye broker stop
    ++ rye broker stop: success

이미 브로커가 종료되었다면 다음과 같은 메시지가 출력된다.

    $ rye broker stop
    @ rye broker stop
    ++ rye broker is not running.

브로커 재시작
-------------

전체 브로커를 재시작하기 위하여 다음과 같이 입력한다.

    $ rye broker restart

브로커 상태 확인
----------------

**rye broker status**는 여러 옵션을 제공하여, 각 브로커의 처리 완료된 작업 수, 처리 대기중인 작업 수를 포함한 브로커 상태 정보를 확인할 수 있도록 한다. **rye\_broker.conf**의 **SHARD** 브로커 파라미터가 ON으로 설정된 경우 **-c** 옵션과 **-m** 옵션을 사용하여 SHARD에 접속한 클라이언트 또는 SHARD 상태를 확인할 수 있다. 또한 **-S** 옵션 또는 **-P** 옵션을 사용하여 shard DB 또는 proxy 별로 구분하여 정보를 출력할 수 있다.

    rye broker status [options] [expr]

-   *expr*: 브로커 이름의 일부 또는 "SERVICE=ON|OFF"

*expr*이 명시되면 이름이 *expr*을 포함하는 브로커에 대한 상태 모니터링을 수행하고, 생략되면 Rye 브로커 환경 설정 파일( **rye\_broker.conf** )에 등록된 전체 브로커에 대해 상태 모니터링을 수행한다.

*expr*에 "SERVICE=ON"이 명시되면 구동 중인 브로커의 상태만 출력하며, "SERVICE=OFF"가 명시되면 멈춰있는 브로커의 이름만 출력한다.

**rye broker status**에서 사용하는 \[options\]는 다음과 같다. 이들 중 -b, -q, -c, -m, -S, -P, -f는 출력할 정보를 정의하는 모니터링 옵션이고, -s, -l, -t는 출력을 제어하는 옵션이다. 또한, -c, -m, -S, -P는 주로 SHARD 기능을 사용할 때 적용하는 옵션이다. 이 모든 옵션들은 서로 조합하여 사용하는 것이 가능하다.

옵션 및 인수를 입력하지 않으면 전체 브로커 상태 정보를 출력한다.

    $ rye broker status
    @ rye broker status
    % query_editor
    ----------------------------------------
    ID   PID   QPS   LQS PSIZE STATUS
    ----------------------------------------
     1 28434     0     0 50144 IDLE
     2 28435     0     0 50144 IDLE
     3 28436     0     0 50144 IDLE
     4 28437     0     0 50140 IDLE
     5 28438     0     0 50144 IDLE

    % broker1 OFF

-   % query\_editor: 브로커의 이름
-   ID: 브로커 내에서 순차적으로 부여한 CAS의 일련 번호
-   PID: 브로커 내 CAS 프로세스의 ID
-   QPS: 초당 처리된 질의의 수
-   LQS: 초당 처리되는 장기 실행 질의의 수
-   PSIZE: CAS 프로세스 크기
-   STATUS: CAS의 현재 상태로서, BUSY/IDLE/CLIENT\_WAIT/CLOSE\_WAIT가 있다.
-   % broker1 OFF: broker1의 SERVICE 파라미터가 OFF이다. 따라서, broker1은 구동되지 않는다.

다음은 **-b** 옵션을 사용하여 브로커에 관해 5초 간격으로 상세한 상태 정보를 출력한다. 화면이 5초 간격마다 새로운 상태 정보로 갱신되며, 상태 정보 화면을 벗어나려면 &lt;Q&gt;를 누른다.

    $ rye broker status -b -s 5
    @ rye broker status

     NAME                    PID  PORT   AS   JQ    TPS    QPS   SELECT   INSERT   UPDATE   DELETE   OTHERS     LONG-T     LONG-Q   ERR-Q  UNIQUE-ERR-Q  #CONNECT  #REJECT
    =======================================================================================================================================================================
    * query_editor         13200 30000    5    0      0      0        0        0        0        0        0     0/60.0     0/60.0       0             0         0        0
    * broker1              13269 33000    5    0     70     60       10       20       10       10       10     0/60.0     0/60.0      30            10       213        1

-   NAME: 브로커 이름
-   PID: 브로커의 프로세스 ID
-   PORT: 브로커의 포트 번호
-   AS: CAS 개수
-   JQ: 작업 큐에서 대기 중인 작업 개수
-   TPS: 초당 처리된 트랜잭션의 수(옵션이 "-b -s &lt;sec&gt;"일 때만 해당 구간 계산)
-   QPS: 초당 처리된 질의의 수(옵션이 "-b -s &lt;sec&gt;"일 때만 해당 구간 계산)
-   SELECT: 브로커 시작 이후 SELECT 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 SELECT 개수로 매번 갱신됨.
-   INSERT: 브로커 시작 이후 INSERT 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 INSERT 개수로 매번 갱신됨.
-   UPDATE: 브로커 시작 이후 UPDATE 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 UPDATE 개수로 매번 갱신됨.
-   DELETE: 브로커 시작 이후 DELETE 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 DELETE 개수로 매번 갱신됨.
-   OTHERS: 브로커 시작 이후 SELECT, INSERT, UPDATE, DELETE를 제외한 CREATE, DROP 등의 질의 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 질의 개수로 매번 갱신됨.
-   LONG-T: LONG\_TRANSACTION\_TIME 시간을 초과한 트랜잭션 개수 / LONG\_TRANSACTION\_TIME 파라미터의 값. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 트랜잭션 개수로 매번 갱신됨.
-   LONG-Q: LONG\_QUERY\_TIME 시간을 초과한 질의의 개수 / LONG\_QUERY\_TIME 파라미터의 값. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 질의 개수로 매번 갱신됨.
-   ERR-Q: 에러가 발생한 질의의 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 에러 개수로 매번 갱신됨. rye\_broker.conf의 SHARD 파라미터가 ON으로 설정된 경우, proxy에서 에러가 발생하는 경우에도 ERR-Q의 값이 증가한다.
-   UNIQUE-ERR-Q: 고유 키 에러가 발생한 질의의 개수. 옵션이 "-b -s &lt;sec&gt;"인 경우 -s 옵션으로 지정한 초 동안의 고유 키 에러 개수로 매번 갱신됨.
-   \#CONNECT: 브로커 시작 후 응용 클라이언트가 CAS에 접속한 회수
-   \#REJECT: 브로커 시작 후 ACL에 포함되지 않은 IP로부터 접속하는 응용 클라이언트가 CAS에 접속하는 것을 거부당한 회수. ACL 설정과 관련하여 limiting-broker-access를 참고한다.

다음은 **-q** 옵션을 이용하여, broker1을 포함하는 이름을 가진 브로커의 상태 정보를 확인하고 해당 브로커의 작업 큐에 대기 중인 작업 상태를 확인한다. 인자로 broker1을 입력하지 않으면 모든 브로커에 대하여 작업 큐에 대기 중인 작업 리스트가 출력된다.

    % rye broker status -q broker1
    @ rye broker status
    % broker1
    ----------------------------------------
    ID   PID   QPS   LQS PSIZE STATUS
    ----------------------------------------
     1 28444     0     0 50144 IDLE
     2 28445     0     0 50140 IDLE
     3 28446     0     0 50144 IDLE
     4 28447     0     0 50144 IDLE
     5 28448     0     0 50144 IDLE

다음은 **-s** 옵션을 이용하여 broker1을 포함하는 이름을 가진 브로커의 상태를 주기적으로 모니터링한다. 인자로 broker1을 입력하지 않으면 모든 브로커에 대하여 상태 모니터링이 주기적으로 수행된다. 또한, q를 입력하면 모니터링 화면에서 명령 프롬프트로 복귀한다.

    % rye broker status -s 5 broker1
    % broker1
    ----------------------------------------
    ID   PID   QPS   LQS PSIZE STATUS
    ----------------------------------------
     1 28444     0     0 50144 IDLE
     2 28445     0     0 50140 IDLE
     3 28446     0     0 50144 IDLE
     4 28447     0     0 50144 IDLE
     5 28448     0     0 50144 IDLE

**-t** 옵션을 이용하여 TPS와 QPS 정보를 파일로 출력한다. 파일로 출력하는 것을 중단하려면 &lt;Ctrl+C&gt;를 눌러서 프로그램을 정지시킨다.

    % rye broker status -b -t -s 1 > log_file

다음은 **-f** 옵션을 이용하여 브로커가 연결한 서버/데이터베이스 정보와 응용 클라이언트의 최근 접속 시각, CAS에 접속하는 클라이언트의 IP 주소와 드라이버의 버전 등을 출력한다.

    $ rye broker status -f broker1
    @ rye broker status
    % broker1 
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ID   PID   QPS   LQS PSIZE STATUS         LAST ACCESS TIME      DB       HOST   LAST CONNECT TIME       CLIENT IP   CLIENT VERSION    SQL_LOG_MODE   TRANSACTION STIME  #CONNECT  #RESTART
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     1 26946     0     0 51168 IDLE         2011/11/16 16:23:42  demodb  localhost 2011/11/16 16:23:40      10.0.1.101     9.2.0.0062              NONE 2011/11/16 16:23:42         0         0
     2 26947     0     0 51172 IDLE         2011/11/16 16:23:34      -          -                   -          0.0.0.0                                -                   -         0         0
     3 26948     0     0 51172 IDLE         2011/11/16 16:23:34      -          -                   -          0.0.0.0                                -                   -         0         0
     4 26949     0     0 51172 IDLE         2011/11/16 16:23:34      -          -                   -          0.0.0.0                                -                   -         0         0
     5 26950     0     0 51172 IDLE         2011/11/16 16:23:34      -          -                   -          0.0.0.0                                -                   -         0         0

각 칼럼에 대한 설명은 다음과 같다.

-   LAST ACCESS TIME: CAS가 구동한 시각 또는 응용 클라이언트의 CAS에 최근 접속한 시각
-   DB: CAS의 최근 접속 데이터베이스 이름
-   HOST: CAS의 최근 접속 호스트 이름
-   LAST CONNECT TIME: CAS의 DB 서버 최근 접속 시각
-   CLIENT IP: 현재 CAS에 접속 중인 응용 클라이언트의 IP 주소. 현재 접속 중인 응용 클라이언트가 없으면 0.0.0.0으로 출력
-   CLIENT VERSION: 현재 CAS에 접속 중인 응용 클라이언트의 드라이버 버전
-   SQL\_LOG\_MODE: CAS의 SQL 로그 기록 모드. 브로커에 설정된 모드와 동일한 경우 "-"으로 출력
-   TRANSACTION STIME: 트랜잭션 시작 시간
-   \#CONNECT: 브로커 시작 후 응용 클라이언트가 CAS에 접속한 회수
-   \#RESTART: 브로커 시작 후 CAS의 재구동 회수

**-b** 옵션에 **-f** 옵션을 추가하여 AS(T W B Ns-W Ns-B), CANCELED 정보를 추가로 출력한다.

    // 브로커 상태 정보 실행 시 -f 옵션 추가. -l 옵션으로 N초 동안의 Ns-W, Ns-B를 출력하도록 초를 설정
    % rye broker status -b -f -l 2
    @ rye broker status
    NAME          PID    PSIZE PORT  AS(T W B 2s-W 2s-B) JQ TPS QPS LONG-T LONG-Q  ERR-Q UNIQUE-ERR-Q CANCELED ACCESS_MODE SQL_LOG  #CONNECT #REJECT
    ================================================================================================================================================
    query_editor 16784 56700 30000      5 0 0     0   0   0  16  29 0/60.0 0/60.0      1            1        0          RW     ALL         4       1

추가된 칼럼에 대한 설명은 다음과 같다.

-   AS(T): 실행 중인 CAS의 전체 개수
-   AS(W): 현재 클라이언트 대기(Waiting) 상태인 CAS의 개수
-   AS(B): 현재 클라이언트 수행(Busy) 상태인 CAS의 개수
-   AS(Ns-W): N초 동안 클라이언트 대기(Waiting) 상태였던 CAS의 개수
-   AS(Ns-B): N초 동안 클라이언트 수행(Busy) 상태였던 CAS의 개수
-   CANCELED: 브로커 시작 이후 사용자 인터럽트로 인해 취소된 질의의 개수 (**-l** *N* 옵션과 함께 사용하면 *N*초 동안 누적된 개수)


브로커 서버 접속 제한
---------------------

브로커에 접속하는 응용 클라이언트를 제한하려면 **rye\_broker.conf**의 **ACCESS\_CONTROL** 파라미터 값을 ON으로 설정하고, **ACCESS\_CONTROL\_FILE** 파라미터 값에 접속을 허용하는 사용자와 데이터베이스 및 IP 목록을 작성한 파일 이름을 입력한다. **ACCESS\_CONTROL** 브로커 파라미터의 기본값은 **OFF**이다. **ACCESS\_CONTROL**, **ACCESS\_CONTROL\_FILE** 파라미터는 공통 적용 파라미터가 위치하는 \[broker\] 아래에 작성해야 한다.

**ACCESS\_CONTROL\_FILE**의 형식은 다음과 같다.

    [%<broker_name>]
    <db_name>:<db_user>:<ip_list_file>
    ...

-   &lt;broker\_name&gt;: 브로커 이름. **rye\_broker.conf**에서 지정한 브로커 이름 중 하나이다.
-   &lt;db\_name&gt;: 데이터베이스 이름. \*로 지정하면 모든 데이터베이스를 허용한다.
-   &lt;db\_user&gt;: 데이터베이스 사용자 ID. \*로 지정하면 모든 데이터베이스 사용자 ID를 허용한다.
-   &lt;ip\_list\_file&gt;: 접속 가능한 IP 목록을 저장한 파일의 이름. ip\_list\_file1, ip\_list\_file2, ...와 같이 파일 여러 개를 쉼표(,)로 구분하여 지정할 수 있다.

브로커별로 \[%&lt;*broker\_name*&gt;\]과 &lt;*db\_name*&gt;:*&lt;db\_user*&gt;:&lt;*ip\_list\_file*&gt;을 추가 지정할 수 있으며, 같은 &lt;*db\_name*&gt;, 같은 &lt;*db\_user*&gt;에 대해 별도의 라인으로 추가 지정할 수 있다. IP 목록은 하나의 브로커 내에서 &lt;*db\_name*&gt;:&lt;*db\_user*&gt; 별로 최대 256 라인까지 작성될 수 있다.

ip\_list\_file의 작성 형식은 다음과 같다.

    <ip_addr>
    ...

-   &lt;ip\_addr&gt;: 접근을 허용할 IP 명. 뒷자리를 \*로 입력하면 뒷자리의 모든 IP를 허용한다.

**ACCESS\_CONTROL** 값이 ON인 상태에서 **ACCESS\_CONTROL\_FILE**이 지정되지 않으면 브로커는 localhost에서의 접속 요청만을 허용한다.

브로커 구동 시 **ACCESS\_CONTROL\_FILE** 및 ip\_list\_file 분석에 실패하는 경우 브로커는 구동되지 않는다.

    # rye_broker.conf
    [broker]
    MASTER_SHM_ID           =30001
    ADMIN_LOG_FILE          =log/broker/rye_broker.log
    ACCESS_CONTROL   =ON
    ACCESS_CONTROL_FILE     =/home1/rye/access_file.txt
    [%QUERY_EDITOR]
    SERVICE                 =ON
    BROKER_PORT             =30000
    ......

다음은 **ACCESS\_CONTROL\_FILE**의 한 예이다. 파일 내에서 사용하는 \*은 모든 것을 나타내며, 데이터베이스 이름, 데이터베이스 사용자 ID, 접속을 허용하는 IP 리스트 파일 내의 IP에 대해 지정할 때 사용할 수 있다.

    [%QUERY_EDITOR]
    dbname1:dbuser1:READIP.txt
    dbname1:dbuser2:WRITEIP1.txt,WRITEIP2.txt
    *:dba:READIP.txt
    *:dba:WRITEIP1.txt
    *:dba:WRITEIP2.txt

    [%BROKER2]
    dbname:dbuser:iplist2.txt

    [%BROKER3]
    dbname:dbuser:iplist2.txt

    [%BROKER4]
    dbname:dbuser:iplist2.txt

위의 예에서 지정한 브로커는 QUERY\_EDITOR, BROKER2, BROKER3, BROKER4이다.

QUERY\_EDITOR 브로커는 다음과 같은 응용의 접속 요청만을 허용한다.

-   *dbname1*에 *dbuser1*으로 로그인하는 사용자가 READIP.txt에 등록된 IP에서 접속
-   *dbname1*에 *dbuser2*로 로그인하는 사용자가 WRITEIP1.txt나 WRITEIP2.txt에 등록된 IP에서 접속
-   모든 데이터베이스에 **DBA**로 로그인하는 사용자가 READIP.txt나 WRITEIP1.txt 또는 WRITEIP2.txt에 등록된 IP에서 접속

다음은 ip\_list\_file에서 허용하는 IP를 설정하는 예이다.

    192.168.1.25
    192.168.*
    10.*
    *

위의 예에서 지정한 IP를 보면 다음과 같다.

-   첫 번째 줄의 설정은 192.168.1.25을 허용한다.
-   두 번째 줄의 설정은 192.168 로 시작하는 모든 IP를 허용한다.
-   세 번째 줄의 설정은 10으로 시작하는 모든 IP를 허용한다.
-   네 번째 줄의 설정은 모든 IP를 허용한다.

이미 구동되어 있는 브로커에 대해서는 다음 명령어를 통해 설정 파일을 다시 적용하거나 현재 적용 상태를 확인할 수 있다.

브로커에서 허용하는 데이터베이스, 데이터베이스 사용자 ID, IP를 설정한 후 변경된 내용을 서버에 적용하려면 다음 명령어를 사용한다. :

    rye broker acl reload [<BR_NAME>]

-   &lt;BR\_NAME&gt;: 브로커 이름. 이 값을 지정하면 특정 브로커만 변경 내용을 적용할 수 있으며, 생략하면 전체 브로커에 변경 내용을 적용한다.

현재 구동 중인 브로커에서 허용하는 데이터베이스, 데이터베이스 사용자 ID, IP 목록, 최종 접속 시간을 화면에 출력하려면 다음 명령어를 사용한다.

    rye broker acl status [<BR_NAME>]

-   &lt;BR\_NAME&gt;: 브로커 이름. 이 값을 지정하면 특정 브로커의 설정을 출력할 수 있으며, 생략하면 전체 브로커의 설정을 출력한다.

다음은 출력 화면의 예이다.

    $ rye broker acl status 
    ACCESS_CONTROL=ON 
    ACCESS_CONTROL_FILE=access_file.txt 

    [%broker1] 
    demodb:dba:iplist1.txt 
           CLIENT IP LAST ACCESS TIME 
    ========================================== 
        10.20.129.11 
      10.113.153.144 2013-11-07 15:19:14 
      10.113.153.145 
      10.113.153.146 
             10.64.* 2013-11-07 15:20:50 

    testdb:dba:iplist2.txt 
           CLIENT IP LAST ACCESS TIME 
    ========================================== 
                   * 2013-11-08 10:10:12 

**브로커 로그**

> 허용되지 않는 IP에서 접근하면 다음과 같은 로그가 남는다.
>
> -   ACCESS\_LOG
>
> <!-- -->
>
>     1 192.10.10.10 - - 1288340944.198 1288340944.198 2010/10/29 17:29:04 ~ 2010/10/29 17:29:04 14942 - -1 db1 dba : rejected
>
> -   SQL LOG
>
> <!-- -->
>
>     10/29 10:28:57.591 (0) CLIENT IP 192.10.10.10 10/29 10:28:57.592 (0) connect db db1 user dba url jdbc:rye:192.10.10.10:30000:db1::: - rejected

데이터베이스 서버에서의 접속 제한을 위해서는 limiting-server-access 을 참고한다.

특정 브로커 관리
----------------

*broker1*만 구동하기 위하여 다음과 같이 입력한다. 단, *broker1*은 이미 공유 메모리에 설정된 브로커이다.

    % rye broker on broker1

만약, *broker1*이 공유 메모리에 설정되지 않은 상태라면 다음과 같은 메시지가 출력된다.

    % rye broker on broker1
    Cannot open shared memory

*broker1*만 종료하기 위하여 다음과 같이 입력한다. 이때, *broker1*의 서비스 풀을 함께 제거할 수 있다.

    % rye broker off broker1

브로커 리셋 기능은 HA에서 failover 등으로 브로커 응용 서버(CAS)가 원하지 않는 데이터베이스 서버에 연결되었을 때, 기존 연결을 끊고 새로 연결할 수 있도록 한다. 예를 들어 Read Only 브로커가 액티브 서버와 연결된 후에는 스탠바이 서버가 연결이 가능한 상태가 되더라도 자동으로 스탠바이 서버와 재연결하지 않으며, **rye broker reset** 명령을 통해서만 기존 연결을 끊고 새로 스탠바이 서버와 연결할 수 있다.

*broker1*을 리셋하려면 다음과 같이 입력한다.

    % rye broker reset broker1

브로커 파라미터의 동적 변경
---------------------------

브로커 구동과 관련된 파라미터는 브로커 환경 설정 파일( **rye\_broker.conf** )에서 설정할 수 있다. 그 밖에, **broker\_changer** 유틸리티를 이용하여 구동 중에만 한시적으로 일부 브로커 파라미터를 동적으로 변경할 수 있다. 브로커 파라미터 설정 및 동적으로 변경 가능한 브로커 파라미터 등 기타 자세한 내용은 broker-configuration을 참조한다.

브로커 구동 중에 브로커 파라미터를 변경하기 위한 **broker\_changer** 유틸리티의 구문은 다음과 같다. *broker\_name*에는 구동 중인 브로커 이름을 입력하면 되고 *parameter*는 동적으로 변경할 수 있는 브로커 파라미터에 한정된다. 변경하고자 하는 파라미터에 따라 *value*가 지정되어야 한다. 브로커 응용 서버 식별자( *cas\_id* )를 지정하여 특정 브로커 응용 서버(CAS)에만 변경을 적용할 수도 있다.

Rye SHARD 기능이 활성화된 경우(rye\_broker.conf에서 SHARD=ON) 응용 서버 식별자(cas\_id)를 지정하여 특정 응용 서버(CAS)에만 변경을 적용할 수 없다.

*cas\_id*는 **rye broker status** 명령에서 출력되는 ID이다.

    broker_changer    broker_name [cas_id] parameters value

구동 중인 브로커에서 SQL 로그가 기록되도록 **SQL\_LOG** 파라미터를 ON으로 설정하기 위하여 다음과 같이 입력한다. 이와 같은 파라미터의 동적 변경은 브로커 구동 중일 때만 한시적으로 효력이 있다.

    % broker_changer query_editor sql_log on
    OK

HA 환경에서 브로커의 **ACCESS\_MODE**를 Read Only로 변경하고 해당 브로커를 자동으로 reset하기 위하여 다음과 같이 입력한다.

    % broker_changer broker_m access_mode ro
    OK

Windows Vista 이상 버전에서 rye 유틸리티를 사용하여 서비스를 제어하려면 명령 프롬프트 창을 관리자 권한으로 구동한 후 사용하는 것을 권장한다. 자세한 내용은 Rye 유틸리티 &lt;utility-on-windows&gt; 를 참고한다.

브로커 설정 정보 확인
---------------------

**rye broker info**는 현재 "실행 중"인 브로커 파라미터의 설정 정보(rye\_broker.conf)를 출력한다. **broker\_changer** 명령에 의해 브로커 파라미터의 설정 정보가 동적으로 변경될 수 있는데, **rye broker info** 명령으로 동작 중인 브로커의 설정 정보를 확인할 수 있다.

    % rye broker info

참고로 현재 "실행 중"인 시스템 파라미터의 설정 정보(rye.conf)를 확인하려면 **rye paramdump** *database\_name* 명령을 사용한다. **SET SYSTEM PARAMETERS** 구문에 의해 시스템 파라미터의 설정 정보가 동적으로 변경될 수 있는데, **rye broker info** 명령으로 동작 중인 시스템의 설정 정보를 확인할 수 있다.

브로커와 DB 간 연결 테스트
--------------------------

**rye broker test**는 지정한 브로커와 접속하는 DB에 사용자가 정의한 질의문을 수행해 보는 명령이다. 샤드 기능이 활성화되면 모든 SHARD DB에 질의를 수행해 본다. 질의 수행 후 트랜잭션은 롤백된다. 이 명령어를 통해 지정한 브로커에 접속하는 모든 SHARD DB에 질의를 수행하면 각 SHARD DB에 대한 질의 성공 여부를 확인할 수 있고, SHARD HASH 기능을 설정한 경우 입력한 질의가 어떤 SHARD DB에서 수행되었는지 확인할 수 있다.

    rye broker test <broker_name> [-D <db_name>] [-u <db_user>] [-p <db_password>] {-c <query> | -i <input_file>} [-o <output_file>] [-s] [-v] 

-   db\_name: DB 이름
-   db\_user: DB 사용자 계정
-   db\_password: DB 사용자 암호
-   query: 질의문
-   input\_file: 입력할 질의문을 저장한 파일
-   output\_file: 결과를 저장할 파일

**rye broker test**에서 사용하는 옵션은 다음과 같다.

다음은 위의 옵션들을 사용한 예이다.

-   DB에 질의

    DB 접속이 잘 되는지 확인한다.

        $ rye broker test shard1 -D shard -u shard -p shard123 -c "select 1 from db_root where charset = 3" 

        @ rye broker test 
        @ [OK] CONNECT broker1 DB [demodb] USER [shard] 

        @ SHARD OFF 

        RESULT ROW COUNT EXECUTION TIME QUERY 
        ======================================================== 
        OK 1 0.011341 sec select 1,'a' from db_root where charset = 3 
        @ [OK] QUERY TEST 

-   사용자 권한 확인

    **브로커에 접속하는 DB 중 하나에 UPDATE 권한이 없는 경우**

    UPDATE 권한이 없으면 RESULT가 FAIL로 표시된다.

        $ vi dml.txt 

        #query 
        select a from foo 
        insert into foo(b) values(3) 
        update foo set c = 2 where b = 3 
        delete foo where b = 3 

        $ rye broker test broker1 -D demodb -u shard -p shard123 -i dml.txt -v 

        @ rye broker test 
        @ [OK] CONNECT broker1 DB [demodb] USER [shard] 

        @ SHARD OFF 

        RESULT ROW COUNT EXECUTION TIME QUERY 
        ======================================================== 
        OK 1 0.001612 sec select a from foo 
        <Result of SELECT Command> 
          a 
        ------------ 
          1 

        OK 1 0.001215 sec insert into foo(b) values(3) 
        FAIL(-494) -1 0.001291 sec update foo set c = 2 where b = 3 
        <Error> 
        ERROR CODE : -494 
        Semantic: UPDATE is not authorized on foo. update foo foo set foo.c=2 where foo.b=3[CAS INFO - 127.0.0.1:52001, 1, 18139]. 

        OK 0 0.001534 sec delete foo where b = 3 
        @ [FAIL] QUERY TEST 

-   -v 옵션 사용 여부

    **-v 옵션을 사용할 때**

    SELECT 질의가 성공하면 결과셋을 출력하며, 실패 시 에러 메시지를 출력한다.

        $ rye broker test broker1 -D demodb -u shard -p shard123 -i dml.txt -v 
        @ rye broker test 
        @ [OK] CONNECT broker1 DB [demodb] USER [shard] 

        @ SHARD OFF 

        RESULT ROW COUNT EXECUTION TIME QUERY 
        OK 1 0.001311 sec select a from foo 
        <Result of SELECT Command> 
          a 
        ------------ 
          1 

        OK 1 0.001083 sec insert into foo(b) values(3) 
        FAIL(-494) -1 0.001166 sec update foo set c = 2 where b = 3 
        <Error> 
        ERROR CODE : -494 
        Semantic: UPDATE is not authorized on foo. update foo foo set foo.c=2 where foo.b=3[CAS INFO - 127.0.0.1:52001, 1, 18139]. 

        OK 0 0.001399 sec delete foo where b = 3 
        @ [FAIL] QUERY TEST 

    **-v 옵션을 사용하지 않을 때**

    질의 성공, 실패 여부만 출력한다.

        $ rye broker test broker1 -D demodb -u shard -p shard123 -i dml.txt 

        @ rye broker test 
        @ [OK] CONNECT broker1 DB [demodb] USER [shard] 

        @ SHARD OFF 

        RESULT ROW COUNT EXECUTION TIME QUERY 
        OK 1 0.001485 sec select a from foo 
        OK 1 0.001123 sec insert into foo(b) values(3) 
        FAIL(-494) -1 0.001180 sec update foo set c = 2 where b = 3 
        OK 0 0.001393 sec delete foo where b = 3 
        @ [FAIL] QUERY TEST 

브로커 로그
-----------

브로커 구동과 관련된 로그에는 접속 로그, 에러 로그, SQL 로그가 있다. 각각의 로그는 설치 디렉터리의 log 디렉터리에서 확인할 수 있으며, 저장 디렉터리의 변경은 브로커 환경 설정 파일( **rye\_broker.conf** )의 **LOG\_DIR** 파라미터와 **ERROR\_LOG\_DIR** 파라미터를 통해 설정할 수 있다.

SHARD = ON 인 경우, Rye proxy의 로그 디렉터리는 **SHARD\_PROXY\_LOG\_DIR** 파라미터를 통해 설정할 수 있다.

### 접속 로그 확인

접속 로그 파일은 응용 클라이언트 접속에 관한 정보를 기록하며, **$Rye/log/broker/**&lt;broker\_name&gt;**.access**에 저장된다. 또한, 브로커 환경 설정 파일에서 **LOG\_BACKUP** 파라미터가 ON으로 설정된 경우, 브로커의 구동이 정상적으로 종료되면 접속 로그 파일에 종료된 날짜와 시간 정보가 추가되어 로그 파일이 저장된다. 예를 들어, broker1이 2008년 6월 17일 오후 12시 27분에 정상 종료되었다면, broker1.access.20080617.1227 이라는 접속 로그 파일이 생성된다.

다음은 log 디렉터리에 생성된 접속 로그 파일의 예제와 설명이다.

    1 192.168.1.203 - - 972523031.298 972523032.058 2008/06/17 12:27:46~2008/06/17 12:27:47 7118 - -1
    2 192.168.1.203 - - 972523052.778 972523052.815 2008/06/17 12:27:47~2008/06/17 12:27:47 7119 ERR 1025
    1 192.168.1.203 - - 972523052.778 972523052.815 2008/06/17 12:27:49~2008/06/17 12:27:49 7118 - -1

-   1: 브로커의 응용서버에 부여된 ID
-   192.168.1.203: 응용 클라이언트의 IP 주소
-   972523031.298: 클라이언트의 요청 처리를 시작한 시각의 UNIX 타임스탬프 값
-   2008/06/17 12:27:46: 클라이언트 요청을 처리 시작한 시각
-   972523032.058: 클라이언트의 요청 처리를 완료한 시각의 UNIX 타임스탬프 값
-   2008/06/17 12:27:47: 클라이언트의 요청을 처리 완료한 시각
-   7118: 응용서버의 프로세스 ID
-   -1: 요청 처리 중 발생한 에러가 없음
-   ERR 1025: 요청 처리 중 발생한 에러가 있고, 에러 정보는 에러 로그 파일의 offset=1025에 존재함

### 에러 로그 확인

에러 로그 파일은 응용 클라이언트의 요청을 처리하는 도중에 발생한 에러에 관한 정보를 기록하며, **$Rye/log/broker/error\_log**&lt;broker\_name&gt;\_&lt;app\_server\_num&gt;**.err**에 저장된다. 에러 코드 및 에러 메시지는 cas-error를 참고한다.

다음은 에러 로그의 예제와 설명이다.

    Time: 02/04/09 13:45:17.687 - SYNTAX ERROR *** ERROR CODE = -493, Tran = 1, EID = 38
    Syntax: Unknown class "unknown_tbl". select * from unknown_tbl

-   Time: 02/04/09 13:45:17.687: 에러 발생 시각
-   - SYNTAX ERROR: 에러의 종류(SYNTAX ERROR, ERROR 등)
-   \*\*\* ERROR CODE = -493: 에러 코드
-   Tran = 1: 트랜잭션 ID. -1은 트랜잭션 ID를 할당 받지 못한 경우임.
-   EID = 38: 에러 ID. SQL 문 처리 중 에러가 발생한 경우, 서버나 클라이언트 에러 로그와 관련이 있는 SQL 로그를 찾을 때 사용함.
-   Syntax ...: 에러 메시지

### SQL 로그 관리

SQL 로그 파일은 응용 클라이언트가 요청하는 SQL을 기록하며, *&lt;broker\_name&gt;\_&lt;app\_server\_num&gt;*.sql.log라는 이름으로 저장된다. SQL 로그는 **SQL\_LOG** 파라미터 값이 ON인 경우에 설치 디렉터리의 log/broker/sql\_log 디렉터리에 생성된다. 이 때, 생성되는 SQL 로그 파일의 크기는 **SQL\_LOG\_MAX\_SIZE** 파라미터의 설정값을 초과할 수 없으므로 주의한다. Rye는 SQL 로그를 관리하기 위한 유틸리티로서 **broker\_log\_top**, **rye\_replay**를 제공하며, 이 유틸리티는 SQL 로그가 존재하는 디렉터리에서 실행해야 한다.

다음은 SQL 로그 파일의 예제와 설명이다.

    13-06-11 15:07:39.282 (0) STATE idle
    13-06-11 15:07:44.832 (0) CLIENT IP 192.168.10.100
    13-06-11 15:07:44.835 (0) CLIENT VERSION 1.0.0.0062
    13-06-11 15:07:44.835 (0) session id for connection 0
    13-06-11 15:07:44.836 (0) connect db demodb user dba url jdbc:rye:192.168.10.200:30000:demodb:dba:********: session id 12
    13-06-11 15:07:44.836 (0) DEFAULT isolation_level 3, lock_timeout -1
    13-06-11 15:07:44.840 (0) end_tran COMMIT
    13-06-11 15:07:44.841 (0) end_tran 0 time 0.000
    13-06-11 15:07:44.841 (0) *** elapsed time 0.004

    13-06-11 15:07:44.844 (0) check_cas 0
    13-06-11 15:07:44.848 (0) set_db_parameter lock_timeout 1000
    13-06-11 15:09:36.299 (0) check_cas 0
    13-06-11 15:09:36.303 (0) get_db_parameter isolation_level 3
    13-06-11 15:09:36.375 (1) prepare 0 CREATE TABLE unique_tbl (a INT PRIMARY key);
    13-06-11 15:09:36.376 (1) prepare srv_h_id 1
    13-06-11 15:09:36.419 (1) set query timeout to 0 (no limit)
    13-06-11 15:09:36.419 (1) execute srv_h_id 1 CREATE TABLE unique_tbl (a INT PRIMARY key);
    13-06-11 15:09:38.247 (1) execute 0 tuple 0 time 1.827
    13-06-11 15:09:38.247 (0) auto_commit
    13-06-11 15:09:38.344 (0) auto_commit 0
    13-06-11 15:09:38.344 (0) *** elapsed time 1.968

    13-06-11 15:09:54.481 (0) get_db_parameter isolation_level 3
    13-06-11 15:09:54.484 (0) close_req_handle srv_h_id 1
    13-06-11 15:09:54.484 (2) prepare 0 INSERT INTO unique_tbl VALUES (1);
    13-06-11 15:09:54.485 (2) prepare srv_h_id 1
    13-06-11 15:09:54.488 (2) set query timeout to 0 (no limit)
    13-06-11 15:09:54.488 (2) execute srv_h_id 1 INSERT INTO unique_tbl VALUES (1);
    13-06-11 15:09:54.488 (2) execute 0 tuple 1 time 0.001
    13-06-11 15:09:54.488 (0) auto_commit
    13-06-11 15:09:54.505 (0) auto_commit 0
    13-06-11 15:09:54.505 (0) *** elapsed time 0.021

    ...

    13-06-11 15:19:04.593 (0) get_db_parameter isolation_level 3
    13-06-11 15:19:04.597 (0) close_req_handle srv_h_id 2
    13-06-11 15:19:04.597 (7) prepare 0 SELECT * FROM unique_tbl  WHERE ROWNUM BETWEEN 1 AND 5000;
    13-06-11 15:19:04.598 (7) prepare srv_h_id 2 (PC)
    13-06-11 15:19:04.602 (7) set query timeout to 0 (no limit)
    13-06-11 15:19:04.602 (7) execute srv_h_id 2 SELECT * FROM unique_tbl  WHERE ROWNUM BETWEEN 1 AND 5000;
    13-06-11 15:19:04.602 (7) execute 0 tuple 1 time 0.001
    13-06-11 15:19:04.607 (0) end_tran COMMIT
    13-06-11 15:19:04.607 (0) end_tran 0 time 0.000
    13-06-11 15:19:04.607 (0) *** elapsed time 0.009

-   13-06-11 15:07:39.282: 응용 클라이언트의 요청 시각
-   (1): SQL 문 그룹의 시퀀스 번호이며, prepared statement pooling을 사용하는 경우, 파일 내에서 SQL 문마다 고유(unique)하게 부여된다.
-   CLIENT IP: 응용 클라이언트의 IP
-   CLIENT VERSION: 응용 클라이언트 드라이버의 버전
-   prepare 0: prepared statement인지 여부
-   prepare srv\_h\_id 1: 해당 SQL 문을 srv\_h\_id 1로 prepare한다.
-   (PC): 플랜 캐시에 저장되어 있는 내용을 사용하는 경우에 출력된다.
-   Execute 0 tuple 1 time 0.001: 1개의 row가 실행되고, 소요 시간은 0.001초
-   auto\_commit/auto\_rollback: 자동으로 커밋되거나, 롤백되는 것을 의미한다. 두 번째 auto\_commit/auto\_rollback은 에러 코드이며, 0은 에러가 없이 트랜잭션이 완료되었음을 뜻한다.

#### broker\_log\_top

**broker\_log\_top** 유틸리티는 특정 기간 동안 생성된 SQL 로그를 분석하여 실행 시간이 긴 순서대로 각 SQL 문과 실행 시간에 관한 정보를 파일에 출력하며, 분석된 결과는 각각 log.top.q 및 log.top.res에 저장된다.

**broker\_log\_top** 유틸리티는 실행 시간이 긴 질의를 분석할 때 유용하며, 구문은 다음과 같다.

    broker_log_top [options] sql_log_file_list

-   *sql\_log\_file\_list*: 분석할 로그 파일 이름

**broker\_log\_top** 에서 사용하는 \[options\]는 다음과 같다.

옵션을 모두 생략하면, 명시한 로그의 모든 SQL에 대해 SQL 문 단위로 결과를 출력한다.

다음은 밀리초까지 검색 범위를 설정하는 예제이다.

    broker_log_top -F "01/19 15:00:25.000" -T "01/19 15:15:25.180" log1.log

다음 예에서 시간 형식이 생략된 부분은 기본값 0으로 정해진다. 즉, -F "01/19 00:00:00.000" -T "01/20 00:00:00.000"을 입력한 것과 같다.

    broker_log_top -F "01/19" -T "01/20" log1.log

다음 예는 11월 11일부터 11월 12일까지 생성된 SQL 로그에 대해 실행 시간이 긴 SQL문을 확인하기 위하여 **broker\_log\_top** 유틸리티를 실행한 화면이다. 기간을 지정할 때, 월과 일은 빗금(/)으로 구분한다. Windows에서는 "\*.sql.log" 를 인식하지 않으므로 SQL 로그 파일들을 공백(space)으로 구분해서 나열해야 한다.

    --Linux에서 broker_log_top 실행
    % broker_log_top -F "11/11" -T "11/12" -t *.sql.log

    query_editor_1.sql.log
    query_editor_2.sql.log
    query_editor_3.sql.log
    query_editor_4.sql.log
    query_editor_5.sql.log

    --Windows에서 broker_log_top 실행
    % broker_log_top -F "11/11" -T "11/12" -t query_editor_1.sql.log query_editor_2.sql.log query_editor_3.sql.log query_editor_4.sql.log query_editor_5.sql.log

위 예제를 실행하면 SQL 로그 분석 결과가 저장되는 **log.top.q** 및 **log.top.res** 파일이 동일한 디렉터리에 생성된다. **log.top.q** 에서 각 SQL 문 및 SQL 로그 상의 라인 번호를 확인할 수 있고, **log.top.res** 에서 각 SQL 문에 대한 최소 실행 시간, 최대 실행 시간, 평균 실행 시간, 쿼리 실행 수를 확인할 수 있다.

    --log.top.q 파일의 내용
    [Q1]-------------------------------------------
    broker1_6.sql.log:137734
    11/11 18:17:59.396 (27754) execute_all srv_h_id 34 select a.int_col, b.var_col from dml_v_view_6 a, dml_v_view_6 b, dml_v_view_6 c , dml_v_view_6 d, dml_v_view_6 e where a.int_col=b.int_col and b.int_col=c.int_col and c.int_col=d.int_col and d.int_col=e.int_col order by 1,2;
    11/11 18:18:58.378 (27754) execute_all 0 tuple 497664 time 58.982
    .
    .
    [Q4]-------------------------------------------
    broker1_100.sql.log:142068
    11/11 18:12:38.387 (27268) execute_all srv_h_id 798 drop table list_test;
    11/11 18:13:08.856 (27268) execute_all 0 tuple 0 time 30.469

    --log.top.res 파일의 내용

                  max       min        avg   cnt(err)
    -----------------------------------------------------
    [Q1]        58.982    30.371    44.676    2 (0)
    [Q2]        49.556    24.023    32.688    6 (0)
    [Q3]        35.548    25.650    30.599    2 (0)
    [Q4]        30.469     0.001     0.103 1050 (0)

#### rye\_replay

**rye\_replay** 유틸리티는 브로커의 SQL 로그를 재생하여, 기존의 수행 시간과 재생할 때의 수행 시간 차이를 비교하여 차이가 큰 것부터(기존보다 느린 것부터) 순서대로 정렬한 결과를 출력한다.

이 유틸리티는 SQL 로그에 기록된 질의들을 재생하되, 데이터의 변경이 발생하는 질의는 실행하지 않는다. 별도의 옵션을 주지 않으면 SELECT 문만 수행되며, -r 옵션을 부여하면 UPDATE, DELETE 문을 SELECT 문으로 변환하여 수행한다.

이 유틸리티는 서로 다른 두 장비 간 성능을 비교할 때 사용할 수 있는데, 예를 들어 하드웨어 스펙이 동일한 마스터와 슬레이브 사이에서도 같은 질의에 대해 성능 차이가 존재할 수 있다.

    rye_replay -I <broker_host> -P <broker_port> -d <db_name> [options] <sql_log_file> <output_file> 

-   *broker\_host*: Rye 브로커의 IP 주소 또는 호스트 이름
-   *broker\_port*: Rye 브로커의 포트 번호
-   *db\_name*: 질의를 실행할 데이터베이스
-   *sql\_log\_file*: Rye 브로커의 SQL 로그 파일($Rye/log/broker/sql\_log/\*.log, \*.log.bak)
-   *output\_file*: 수행 결과를 저장할 파일 이름

**rye\_replay** 에서 사용하는 \[options\]는 다음과 같다.

    $ rye_replay -I testhost -P 33000 -d testdb -u dba -r testdb_1_11_1.sql.log.bak output.txt 

위의 명령을 실행하면 실행 결과의 요약 정보가 화면에 출력된다.

    ------------------- Result Summary -------------------------- 
    * Total queries : 153103 
    * Skipped queries (see skip.sql) : 5127 
    * Error queries (see replay.err) : 30 
    * Slow queries (time diff > 0.000 secs) : 89987 
    * Max execution time diff : 0.016 
    * Avg execution time diff : -0.001 

    rye_replay run time : 245.308417 sec 

-   Total queries: 날짜 및 시간이 지정된 범위 안의 전체 질의 개수. DDL, DML을 포함
-   Skipped queries: **-r** 옵션이 사용되었을 때 UPDATE/DELETE 문을 SELECT 문으로 변환할 수 없는 질의 개수. 이 질의는 skip.sql 파일에 기록됨
-   Slow queries: **-D** 옵션의 설정 값보다 수행 시간의 차이가 더 큰(재생된 실행 시간이 기존 실행 시간에 설정한 값을 더한 것보다 느린) 질의 개수. **-D** 옵션을 생략하면 0.01초를 기본으로 설정함.
-   Max execution time diff: 수행 시간의 차이 중 가장 큰 값(단위: 초)
-   Avg execution time diff: 수행 시간의 차이의 평균 값(단위: 초)
-   rye\_replay run time: 유틸리티의 수행 시간

"Skipped queries"는 내부 요인에 의해 UPDATE/DELETE 문에서 SELECT 문으로 질의 변환이 불가능한 경우로, skip.sql 파일에 기록된 질의문의 성능에 대해서는 별도로 확인해볼 필요가 있다.

또한, 변환된 질의문의 수행 시간은 데이터 변경 시간이 빠진 것임을 감안해야 한다.

*output.txt* 파일에는 SQL 로그의 수행 시간보다 재생된 SQL 수행 시간이 더 느린 SQL부터 정렬되어 기록된다. 즉, {(재생된 SQL 수행 시간) - {(SQL 로그의 수행 시간) + (**-D** 옵션 설정 시간)}}이 내림차순으로 정렬되어 기록된다. "-r" 옵션이 사용되었으므로 UPDATE/DELETE 문은 SELECT 문으로 재작성되어 실행된다.

    EXEC TIME (REPLAY / SQL_LOG / DIFF): 0.003 / 0.001 / 0.002 
    SQL: UPDATE NDV_QUOTA_INFO SET last_mod_date = now() , used_quota = ( SELECT IFNULL(sum(file_size),0) FROM NDV_RECYCLED_FILE_INFO WHERE user_id = ? ) + ( SELECT IFNULL(sum(file_size),0) FROM NDV_FILE_INFO WHERE user_id = ? ) WHERE user_id = ? /+shard_val(6900403)/ /* SQL : NDVMUpdResetUsedQuota */ 
    REWRITE SQL: select NDV_QUOTA_INFO, class NDV_QUOTA_INFO, cast( SYS_DATETIME as datetime), cast((select ifnull(sum(NDV_RECYCLED_FILE_INFO.file_size), 0) from NDV_RECYCLED_FILE_INFO NDV_RECYCLED_FILE_INFO where (NDV_RECYCLED_FILE_INFO.user_id= ?:0 ))+(select ifnull(sum(NDV_FILE_INFO.file_size), 0) from NDV_FILE_INFO NDV_FILE_INFO where (NDV_FILE_INFO.user_id= ?:1 )) as bigint) from NDV_QUOTA_INFO NDV_QUOTA_INFO where (NDV_QUOTA_INFO.user_id= ?:2 ) 
    BIND 1: 'babaemo' 
    BIND 2: 'babaemo' 
    BIND 3: 'babaemo' 

-   EXEC TIME: (재생 시간 / SQL 로그에서의 수행 시간 / 두 수행 시간의 차이)
-   SQL: 브로커의 SQL 로그에 존재하는 원본 SQL
-   REWRITE SQL: **-r** 옵션이 지정되어 UPDATE 또는 DELETE 문에서 변환된 SELECT 문

CAS 에러
--------

CAS 에러는 브로커 응용 서버(CAS) 프로세스에서 발생하는 에러로, 드라이버를 이용하여 CAS에 접속하는 모든 응용 프로그램에서 발생할 수 있다.

다음은 CAS에서 발생하는 에러 코드를 정리한 표이다. 같은 에러 번호에 대해 CCI와 JDBC의 에러 메시지가 서로 다르게 나타날 수 있다. 에러 메시지가 하나만 있으면 같은 것이며, 두 개가 있는 경우 앞에 있는 것이 CCI 에러 메시지, 뒤에 있는 것이 JDBC 에러 메시지이다.

<table>
<colgroup>
<col width="21%" />
<col width="29%" />
<col width="49%" />
</colgroup>
<thead>
<tr class="header">
<th>에러 코드명(에러 번호)</th>
<th>에러 메시지 (CCI / JDBC)</th>
<th>비고</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>CAS_ER_INTERNAL(-10001)</td>
<td></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_NO_MORE_MEMORY(-10002)</td>
<td><blockquote>
<p>Memory allocation error</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_COMMUNICATION(-10003)</td>
<td><blockquote>
<p>Cannot receive data from client / Communication error</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_ARGS(-10004)</td>
<td><blockquote>
<p>Invalid argument</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_TRAN_TYPE(-10005)</td>
<td><blockquote>
<p>Invalid transaction type argument / Unknown transaction type</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_SRV_HANDLE(-10006)</td>
<td><blockquote>
<p>Server handle not found / Internal server error</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_NUM_BIND(-10007)</td>
<td><blockquote>
<p>Invalid parameter binding value argument / Parameter binding error</p>
</blockquote></td>
<td>바인딩할 데이터 수가 전송할 데이터 수와 일치하지 않음.</td>
</tr>
<tr class="even">
<td>CAS_ER_UNKNOWN_U_TYPE(-10008)</td>
<td><blockquote>
<p>Invalid T_CCI_U_TYPE value / Parameter binding error</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_DB_VALUE(-10009)</td>
<td><blockquote>
<p>Cannot make DB_VALUE</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_TYPE_CONVERSION(-10010)</td>
<td><blockquote>
<p>Type conversion error</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_PARAM_NAME(-10011)</td>
<td><blockquote>
<p>Invalid T_CCI_DB_PARAM value / Invalid database parameter name</p>
</blockquote></td>
<td>시스템 파라미터 이름이 유효하지 않음</td>
</tr>
<tr class="even">
<td>CAS_ER_NO_MORE_DATA(-10012)</td>
<td><blockquote>
<p>Invalid cursor position / No more data</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_OBJECT(-10013)</td>
<td><blockquote>
<p>Invalid oid / Object is not valid</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_OPEN_FILE(-10014)</td>
<td><blockquote>
<p>Cannot open file / File open error</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_SCHEMA_TYPE(-10015)</td>
<td><blockquote>
<p>Invalid T_CCI_SCH_TYPE value / Invalid schema type</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_VERSION(-10016)</td>
<td><blockquote>
<p>Version mismatch</p>
</blockquote></td>
<td>DB 서버 버전과 클라이언트(CAS) 버전이 호환되지 않음.</td>
</tr>
<tr class="odd">
<td>CAS_ER_FREE_SERVER(-10017)</td>
<td><blockquote>
<p>Cannot process the request. Try again later</p>
</blockquote></td>
<td>응용 프로그램의 연결요청을 처리할CAS를 할당할 수 없음.</td>
</tr>
<tr class="even">
<td>CAS_ER_NOT_AUTHORIZED_CLIENT(-10018)</td>
<td><blockquote>
<p>Authorization error</p>
</blockquote></td>
<td>접근을 불허함.</td>
</tr>
<tr class="odd">
<td>CAS_ER_QUERY_CANCEL(-10019)</td>
<td><blockquote>
<p>Cannot cancel the query</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_NOT_COLLECTION(-10020)</td>
<td><blockquote>
<p>The attribute domain must be the set type</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_COLLECTION_DOMAIN(-10021)</td>
<td><blockquote>
<p>Heterogeneous set is not supported / The domain of a set must be the same data type</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_NO_MORE_RESULT_SET(-10022)</td>
<td><blockquote>
<p>No More Result</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_INVALID_CALL_STMT(-10023)</td>
<td><blockquote>
<p>Illegal CALL statement</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_STMT_POOLING(-10024)</td>
<td><blockquote>
<p>Invalid plan</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_DBSERVER_DISCONNECTED(-10025)</td>
<td><blockquote>
<p>Cannot communicate with DB Server</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED(-10026)</td>
<td><blockquote>
<p>Cannot prepare more than MAX_PREPARED_STMT_COUNT statements</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_HOLDABLE_NOT_ALLOWED(-10027)</td>
<td><blockquote>
<p>Holdable results may not be updatable or sensitive</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_HOLDABLE_NOT_ALLOWED_KEEP_CON_OFF(-10028)</td>
<td><blockquote>
<p>Holdable results are not allowed while KEEP_CONNECTION is off</p>
</blockquote></td>
<td></td>
</tr>
<tr class="odd">
<td>CAS_ER_NOT_IMPLEMENTED(-10100)</td>
<td><blockquote>
<p>None / Attempt to use a not supported service</p>
</blockquote></td>
<td></td>
</tr>
<tr class="even">
<td>CAS_ER_IS(-10200)</td>
<td><blockquote>
<p>None / Authentication failure</p>
</blockquote></td>
<td></td>
</tr>
</tbody>
</table>
