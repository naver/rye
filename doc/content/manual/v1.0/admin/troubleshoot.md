트러블슈팅
==========

SQL 로그 확인
-------------

### CAS의 SQL 로그

특정 오류가 발생했을 때 보통 브로커 응용 서버(CAS)의 SQL 로그를 확인한다.

각 CAS 당 하나의 SQL 로그 파일이 생성되는데, CAS 프로세스가 많은 환경에서는 SQL 로그도 많아지므로 오류가 발생한 SQL 로그 파일을 찾기가 어렵다. 하지만, SQL 로그 파일 이름은 뒷부분에 CAS ID를 포함하고 있기 때문에 오류 발생 질의를 수행한 CAS의 ID를 알면 SQL 로그 파일을 찾기 쉽다.

CAS의 SQL 로그 파일은 &lt;broker\_name&gt;\_&lt;app\_server\_num&gt;.sql.log으로 저장되는데(broker-logs 참고), &lt;app\_server\_num&gt;이 CAS ID이다.

### CAS 정보 출력 함수

cci\_get\_cas\_info 함수 또는 JDBC의 rye.jdbc.driver.RyeConnection 클래스의 toString() 메서드 &lt;jdbc-con-tostring&gt;)는 질의를 수행할 때 해당 질의가 수행된 브로커의 호스트와 CAS ID를 포함한 정보를 출력하여, 이를 통해 해당 CAS의 SQL 로그 파일을 쉽게 찾을 수 있다.

    <host>:<port>,<cas id>,<cas process id> 
    예) 127.0.0.1:33000,1,12916 

### 응용 프로그램 로그

응용 프로그램에서 로그를 출력하도록 연결 URL을 설정하면 특정 질의에서 오류가 발생했을 때 해당 오류가 발생한 CAS ID를 확인할 수 있다. 에러가 발생했을 때 작성되는 응용 프로그램 로그의 예는 다음과 같다.

**JDBC 응용 프로그램 로그**

    Syntax: syntax error, unexpected IdName [CAS INFO - localhost:33000,1,30560],[SESSION-16],[URL-jdbc:rye:localhost:33000:demodb::********:?logFile=driver_1.log&logSlowQueries=true&slowQueryThresholdMillis=5]. 

**CCI 응용 프로그램 로그**

    Syntax: syntax error, unexpected IdName [CAS INFO - 127.0.0.1:33000, 1, 30560]. 

### 슬로우 쿼리

슬로우 쿼리 발생 시, 응용 프로그램 로그와 CAS의 SQL 로그를 통해 슬로우 쿼리의 원인을 파악해야 한다.

Rye는 응용 프로그램-브로커-DB 서버의 3 계층 구조로 되어 있기 때문에, 슬로우 쿼리(slow query) 발생 시 원인이 응용 프로그램-브로커 구간에 있는지 브로커-DB 서버 구간에 있는지 파악하기 위해 응용 프로그램 로그 또는 브로커 응용 서버(CAS)의 SQL 로그를 확인해야 한다.

응용 프로그램 로그에는 슬로우 쿼리가 출력되었는데 CAS의 SQL 로그에는 해당 질의가 슬로우 쿼리로 출력되지 않았다면, 응용 프로그램-브로커 사이에서 속도가 저하된 원인이 존재할 것이다. 몇가지 예는 다음과 같다.

-   응용 프로그램-브로커 사이에서 네트워크 통신 속도가 저하되었는지 확인해 본다.
-   브로커 로그($Rye/log/broker 디렉터리 이하에 존재)에 기록되는 정보를 통해 CAS가 재시작된 경우가 있는지 확인한다. CAS 개수가 부족한 것으로 파악되면 CAS 개수를 늘리는데, 이를 위해 rye\_broker.conf의 MAX\_NUM\_APPL\_SERVER &lt;max-num-appl-server&gt;값을 적절히 늘려야 한다. 이와 함께 rye.conf의 max\_clients &lt;max\_clients&gt; 값도 늘리는 것을 고려해야 한다.

응용 프로그램 로그와 CAS의 SQL 로그에 둘 다 슬로우 쿼리로 출력되고 둘 사이에 해당 질의의 수행 시간 차이가 거의 없다면, 브로커-DB 서버 사이에서 속도가 저하된 원인이 존재할 것이다. 한 예로, DB 서버에서 질의를 처리하는데 시간이 걸렸을 것이다.

슬로우 쿼리 발생 시 각 응용 프로그램 로그의 예는 다음과 같다.

**JDBC 응용 프로그램 로그**

    2013-05-09 16:25:08.831|INFO|SLOW QUERY 
    [CAS INFO] 
    localhost:33000, 1, 12916 
    [TIME] 
    START: 2013-05-09 16:25:08.775, ELAPSED: 52 
    [SQL] 
    SELECT * from db_class a, db_class b 

**CCI 응용 프로그램 로그**

    2013-05-10 18:11:23.023 [TID:14346] [DEBUG][CONHANDLE - 0002][CAS INFO - 127.0.0.1:33000, 1, 12916] [SLOW QUERY - ELAPSED : 45] [SQL - select * from db_class a, db_class b] 

응용 프로그램과 브로커의 슬로우 쿼리 정보는 각각 다른 파일로 다음과 같은 경우에 저장된다.

-   응용 프로그램의 슬로우 쿼리 정보는 연결 URL의 **logSlowQueries** 속성을 **yes**로 설정하고 **slowQueryThresholdMillis** 값을 설정하면 logFile 속성으로 지정한 응용 프로그램 로그 파일에 저장된다(cci\_connect\_with\_url, jdbc-connection-conf 참고).
-   브로커의 슬로우 쿼리 정보는 broker-configuration의 SLOW\_LOG 값을 ON으로 설정하고 **LONG\_QUERY\_TIME** 값을 설정하면 $Rye/log/broker/sql\_log 디렉터리에 저장된다.

서버 에러 로그
--------------

rye.conf의 error\_log\_level 파라미터의 설정에 따라 서버 에러 로그에서 다양한 정보를 얻을 수 있다. error\_log\_level 파라미터의 기본값은 ERROR이다. NOTIFICATION 메시지를 출력하려면 rye.conf의 error\_log\_level 파라미터의 값을 NOTIFICATION으로 지정해야 한다. 관련 파라미터 설정 방법은 error-parameters를 참고한다.

### 인덱스와 데이터 사이의 불일치 감지

인덱스와 데이터 사이의 불일치가 감지되는 경우 트랜잭션의 격리 수준(isolation level)에 따라 에러일 수도 있고, 에러인지 불확실할 수도 있다.

rye.conf의 isolation\_level 파라미터가 1 또는 3으로서, UNCOMMITTED INSTANCE를 허용하는 경우 인덱스와 데이터가 순간적으로 불일치할 수 있다. 따라서 이러한 경우를 서버 에러 로그에 출력하려면 rye.conf의 error\_log\_level 파라미터의 값이 NOTIFICATION이어야 한다. 출력되는 메시지는 다음과 같다.

    ----  database server error log
    Time: 03/15/11 15:20:31.804 - NOTIFICATION *** CODE = -545, Tran = 1, CLIENT = cdbs034.cub:rsql(3926), EID = 3
    Internal error: INDEX u_foo_i ON CLASS foo (CLASS_OID: 0|550|8). Key and OID: 0|600|16 entry on B+tree: 0|209|590 is incorrect. The object does not exist.

rye.conf의 isolation\_level 파라미터가 2 또는 4 이상의 값으로서, COMMITTED INSTANCE만 허용하는 경우 인덱스와 데이터가 불일치하면 안 된다. 따라서 이러한 경우를 서버 에러 로그에 출력하려면 rye.conf의 error\_log\_level 파라미터의 값이 ERROR여야 한다. 출력되는 메시지는 다음과 같다.

    ----  database server error log
    Time: 03/15/11 15:14:35.907 - ERROR *** ERROR CODE = -545, Tran = 1, CLIENT = cdbs034.cub:rsql(3776), EID = 1
    Internal error: INDEX u_foo_i ON CLASS foo (CLASS_OID: 0|550|8). Key and OID: 0|600|2 entry on B+tree: 0|209|590 is incorrect. The object does not exist.

    ---- client error log
    ERROR: Internal error: INDEX u_foo_i ON CLASS foo (CLASS_OID: 0|550|8). Key and OID: 0|600|2 entry on B+tree: 0|209|590 is incorrect. The object does not exist.

### 오버플로우 키 또는 오버플로우 페이지 감지

오버플로우 키나 오버플로우 페이지가 발생하면 서버 에러 로그 파일에 NOTIFICATION 메시지를 출력한다. 사용자는 이 메시지를 통해 오버플로우 키 또는 오버플로우 페이지로 인해 DB 성능이 느려졌음을 감지할 수 있다. 가능하다면 오버플로우 키나 오버플로우 페이지가 발생하지 않도록 하는 것이 좋다. 즉, 크기가 큰 칼럼에 인덱스를 사용하지 않는 것이 좋으며, 레코드의 크기를 너무 크게 잡지 않는 것이 좋다.

    Time: 06/14/13 19:23:40.485 - NOTIFICATION *** file ../../src/storage/btree.c, line 10617 CODE = -1125 Tran = 1, CLIENT = testhost:rsql(24670), EID = 6 
    Created the overflow key file. INDEX idx(B+tree: 0|131|540) ON CLASS hoo(CLASS_OID: 0|522|2). key: 'z ..... '(OID: 0|530|1). 
    ........... 

    Time: 06/14/13 19:23:41.614 - NOTIFICATION *** file ../../src/storage/btree.c, line 8785 CODE = -1126 Tran = 1, CLIENT = testhost:rsql(24670), EID = 9 
    Created a new overflow page. INDEX i_foo(B+tree: 0|149|580) ON CLASS foo(CLASS_OID: 0|522|3). key: 1(OID: 0|572|578). 
    ........... 

    Time: 06/14/13 19:23:48.636 - NOTIFICATION *** file ../../src/storage/btree.c, line 5562 CODE = -1127 Tran = 1, CLIENT = testhost:rsql(24670), EID = 42 
    Deleted an empty overflow page. INDEX i_foo(B+tree: 0|149|580) ON CLASS foo(CLASS_OID: 0|522|3). key: 1(OID: 0|572|192).

### 로그 회복 시간 감지

DB 서버 시작이나 백업 볼륨 복구 시 서버 에러 로그 또는 restoredb 에러 로그 파일에 로그 회복(log recovery) 시작 시간과 종료 시간에 대한 NOTIFICATION 메시지를 출력하여, 해당 작업의 소요 시간을 확인할 수 있다. 해당 메시지에는 적용(redo)해야할 로그의 개수와 로그 페이지 개수가 함께 기록된다.

    Time: 06/14/13 21:29:04.059 - NOTIFICATION *** file ../../src/transaction/log_recovery.c, line 748 CODE = -1128 Tran = -1, EID = 1 
    Log recovery is started. The number of log records to be applied: 96916. Log page: 343 ~ 5104. 
    ..... 
    Time: 06/14/13 21:29:05.170 - NOTIFICATION *** file ../../src/transaction/log_recovery.c, line 843 CODE = -1129 Tran = -1, EID = 4 
    Log recovery is finished.

### 교착 상태 감지

rye.conf의 error\_log\_level 시스템 파라미터의 값이 NOTIFICATION일 때 교착 상태(deadlock)가 발생하면 서버 에러 로그 파일에 잠금 관련 정보를 기록한다.

다음의 에러 로그 파일 정보에서 (1)은 교착상태를 유발한 테이블 이름을, (2)는 인덱스 이름을 나타낸다.

    demodb_20111102_1811.err

          ...

         OID = -532| 520| 1
    (1) Object type: Index key of class ( 0| 417| 7) = tbl.
         BTID = 0| 123| 530
    (2) Index Name : i_tbl_col1
         Total mode of holders = NS_LOCK, Total mode of waiters = NULL_LOCK.
         Num holders= 1, Num blocked-holders= 0, Num waiters= 0
         LOCK HOLDERS:
        Tran_index = 2, Granted_mode = NS_LOCK, Count = 1
        ...

HA 상태 변경 감지
-----------------

HA 상태 변경은 cub\_master 프로세스의 로그 파일에서 확인할 수 있다. 로그 파일은 $Rye/log 디렉터리에 &lt;host\_name&gt;.cub\_master.err 이름으로 저장된다.

### HA split-brain 감지

HA 환경에서 복제 구성된 두 개 이상의 장비 모두 마스터 역할을 맡게 되는 비정상적인 상황이 발생하는 것을 split-brain이라고 한다.

split-brain 상태를 해소하기 위해 스스로 종료하는 마스터 노드의 cub\_master 로그 파일은 다음과 같이 노드 정보를 포함한다.

    Time: 05/31/13 17:38:29.138 - ERROR *** file ../../src/executables/master_heartbeat.c, line 714 ERROR CODE = -988 Tran = -1, EID = 19 
    Node event: More than one master detected and local processes and cub_master will be terminated. 

    Time: 05/31/13 17:38:32.337 - ERROR *** file ../../src/executables/master_heartbeat.c, line 4493 ERROR CODE = -988 Tran = -1, EID = 20 
    Node event:HA Node Information 
    ================================================================================ 
     * group_id : hagrp host_name : testhost02 state : unknown 
    -------------------------------------------------------------------------------- 
    name priority state score missed heartbeat 
    -------------------------------------------------------------------------------- 
    testhost03 3 slave 3 0 
    testhost02 2 master 2 0 
    testhost01 1 master -32767 0 
    ================================================================================ 

위의 예는 testhost02 서버가 split-brain을 감지하고 스스로 종료될 때 cub\_master 로그에 출력하는 정보이다.

### Fail-over, Fail-back 감지

Fail-over 혹은 Fail-back이 발생하면 노드는 역할을 변경하게 된다.

fail-over 후 마스터로 변경되는 노드 혹은 fail-back 후 슬레이브로 변경되는 노드의 cub\_master 로그 파일은 다음과 같이 노드 정보를 포함한다.

    Time: 06/04/13 15:23:28.056 - ERROR *** file ../../src/executables/master_heartbeat.c, line 957 ERROR CODE = -988 Tran = -1, EID = 25 
    Node event: Failover completed. 

    Time: 06/04/13 15:23:28.056 - ERROR *** file ../../src/executables/master_heartbeat.c, line 4484 ERROR CODE = -988 Tran = -1, EID = 26 
    Node event: HA Node Information 
    ================================================================================ 
     * group_id : hagrp host_name : testhost02 state : master 
    -------------------------------------------------------------------------------- 
    name priority state score missed heartbeat 
    -------------------------------------------------------------------------------- 
    testhost03 3 slave 3 0 
    testhost02 2 to-be-master -4094 0 
    testhost01 1 unknown 32767 0 
    ================================================================================ 

위의 예는 fail-over로 인해 testhost02 서버가 슬레이브에서 마스터로 역할을 변경하는 도중 cub\_master 로그에 출력하는 정보이다.

HA 구동 실패
------------

사용자의 개입 없이 복제되는 DB 볼륨의 복구가 불가능한 경우의 예는 다음과 같다.

-   copylogdb에서 복사하려는 로그가 원본 노드에서 삭제된 경우
-   active 서버에서 반영해야 하는 보관 로그(archive log)가 이미 삭제된 경우
-   서버의 복구에 실패한 경우

이와 같이 복제 볼륨의 자동 복구가 불가능한 경우 "rye heartbeat start" 명령 수행에 실패하는데, 각각의 경우에 맞게 조치한다.

### 대표적인 복구 불가능 장애

사용자의 개입 없이 자동으로 복제되는 DB 볼륨의 복구가 불가능한 경우 중 서버 프로세스가 원인인 경우는 워낙 다양하므로 설명을 생략하며, copylogdb 또는 applylogdb 프로세스가 원인인 경우 에러 메시지는 다음과 같다.

-   copylogdb가 원인인 경우

    <table>
    <colgroup>
    <col width="38%" />
    <col width="61%" />
    </colgroup>
    <thead>
    <tr class="header">
    <th>원인</th>
    <th>에러 메시지</th>
    </tr>
    </thead>
    <tbody>
    <tr class="odd">
    <td>아직 복사되지 않은 로그가 대상 노드에서 이미 삭제됨</td>
    <td>log writer: failed to get log page(s) starting from page id 80.</td>
    </tr>
    <tr class="even">
    <td>이전 복사되던 DB와 다른 DB의 로그로 판단됨</td>
    <td>Log &quot;/home1/rye/DB/tdb01_cdbs037.cub/tdb01_lgat&quot; does not belong to the given database.</td>
    </tr>
    </tbody>
    </table>

-   applylogdb가 원인인 경우

    <table>
    <colgroup>
    <col width="38%" />
    <col width="61%" />
    </colgroup>
    <thead>
    <tr class="header">
    <th>원인</th>
    <th>에러 메시지</th>
    </tr>
    </thead>
    <tbody>
    <tr class="odd">
    <td>복제 반영할 로그가 포함된 archive 로그가 이미 삭제됨</td>
    <td><p>Internal error: unable to find log page 81 in log archives.</p>
    <p>Internal error: logical log page 81 may be corrupted.</p></td>
    </tr>
    <tr class="even">
    <td>db_ha_apply_info 카탈로그와 현재 복제 로그의 DB 생성 시간이 다름. 즉, 이전 반영하던 복제 로그가 아님</td>
    <td>HA generic: Failed to initialize db_ha_apply_info.</td>
    </tr>
    <tr class="odd">
    <td>데이터베이스 로캘이 다름</td>
    <td>Locale initialization: Active log file(/home1/rye/DB/tdb01_cdbs037.cub/tdb01_lgat) charset is not valid (iso88591), expecting utf8.</td>
    </tr>
    </tbody>
    </table>

### HA 구동 실패 시 대처 방법

| 상황 대처                                                                    | 방법                           |
|------------------------------------------------------------------------------|--------------------------------|
| 실패 원인이 된 원본 노드가 마스터 상태인 경우 복제 재구성                    |                                |
| 실패 원인이 된 원본 노드가 슬레이브 상태인 경우 복제 로그 및 db\_ha\_apply\_ | info 카탈로그 초기화 후 재시작 |


