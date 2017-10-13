데이터베이스 백업
=================

데이터베이스 백업은 Rye 데이터베이스 볼륨, 제어 파일, 로그 파일을 저장하는 작업으로 **rye backupdb** 유틸리티 또는 Rye 매니저를 이용하여 수행된다. **DBA** 는 저장 매체의 오류 또는 파일 오류 등의 장애에 대비하여 데이터베이스를 정상적으로 복구할 수 있도록 주기적으로 데이터베이스를 백업해야 한다. 이 때 복구 환경은 백업 환경과 동일한 운영체제 및 동일한 버전의 Rye가 설치된 환경이어야 한다. 이러한 이유로 데이터베이스를 새로운 버전으로 마이그레이션한 후에는 즉시 새로운 버전의 환경에서 백업을 수행해야 한다.

**rye backupdb** 유틸리티는 모든 데이터베이스 페이지들, 제어 파일들, 데이터베이스를 백업 시와 일치된 상태로 복구하기 위해 필요한 로그 레코드들을 복사한다. :

    rye backupdb [options] database_name[@hostname]

-   @*hostname*: 독립(Standalone) 모드 DB를 백업하는 경우 생략되며, HA 환경에서 백업하는 경우 데이터베이스 이름 뒤에 "@*hostname*"을 지정한다. *hostname*은 $Rye\_DATABASES/databases.txt에 지정된 이름이다. 로컬 서버를 설정하려면 "@localhost"를 지정하면 된다.

다음은 **rye backupdb** 유틸리티와 결합할 수 있는 옵션이다. 대소문자가 구분됨을 주의한다.

**-D, --destination-path**=PATH

지정된 디렉터리에 백업 파일이 저장되도록 하며, 현재 존재하는 디렉터리가 지정되어야 한다. 그렇지 않으면 지정한 이름의 백업 파일이 생성된다. **-D** 옵션이 지정되지 않으면 백업 파일은 해당 데이터베이스의 위치 정보를 저장하는 파일인 **databases.txt** 에 명시된 로그 디렉터리에 생성된다.
    
    rye backupdb -D /home/rye/backup demodb

**-D** 옵션을 이용하여 현재 디렉터리에 백업 파일이 저장되도록 한다. **-D** 옵션의 인수로 "."을 입력하면 현재 디렉터리가 지정된다.

    rye backupdb -D . demodb

**-r, --remove-archive**

활성 로그(active log)가 꽉 차면 활성 로그를 새로운 보관 로그 파일에 기록한다. 이때 백업을 수행하여 백업 볼륨이 생성되면, 백업 시점 이전의 보관 로그는 추후 복구 작업에 필요 없다. **-r** 옵션은 백업을 수행한 후에, 추후 복구 작업에 더 이상 사용되지 않을 보관 로그 파일을 제거하는 옵션이다. **-r** 옵션은 백업 시점 이전의 불필요한 보관 로그만 제거하므로 복구 작업에는 영향을 끼치지 않지만, 관리자가 백업 시점 이후의 보관 로그까지 제거하는 경우 전체 복구가 불가능할 수도 있다. 따라서 보관 로그를 제거할 때에는 추후 복구 작업에 필요한 것인지 반드시 검토해야 한다.

**-r** 옵션을 사용하여 증분 백업(백업 수준 1 또는 2)을 수행하는 경우, 추후 데이터베이스의 정상 복구가 불가능할 수도 있으므로 **-r** 옵션은 전체 백업 수행 시에만 사용하는 것을 권장한다.

    rye backupdb -r demodb

**-o, --output-file**=FILE

대상 데이터베이스의 백업에 관한 진행 정보를 info_backup이라는 파일에 기록한다.

    rye backupdb -o info_backup demodb

다음은 info_backup 파일 내용의 예시로서, 스레드 개수, 압축 방법, 백업 시작 시간, 영구 볼륨의 개수, 백업 진행 정보, 백업 완료 시간 등의 정보를 확인할 수 있다. ::

    [ Database(demodb) Full Backup start ]
    - num-threads: 1
    - compression method: NONE
    - backup start time: Mon Jul 21 16:51:51 2008
    - number of permanent volumes: 1
    - backup progress status
    -----------------------------------------------------------------------------
     volume name                  | # of pages | backup progress status    | done
    -----------------------------------------------------------------------------
     demodb_vinf                  |          1 | ######################### | done
     demodb                       |      25000 | ######################### | done
     demodb_lginf                 |          1 | ######################### | done
     demodb_lgat                  |      25000 | ######################### | done
    -----------------------------------------------------------------------------
    # backup end time: Mon Jul 21 16:51:53 2008
    [Database(demodb) Full Backup end]

**-S, --SA-mode**

독립 모드, 즉 오프라인으로 백업을 수행한다. **-S** 옵션이 생략되면 클라이언트/서버 모드에서 백업이 수행된다. 

    rye backupdb -S demodb

**-C, --CS-mode**

클라이언트/서버 모드에서 백업을 수행하며, demodb를 온라인 백업한다. **-C** 옵션이 생략되면 클라이언트/서버 모드에서 백업이 수행된다. 

    rye backupdb -C demodb

**--no-check**

대상 데이터베이스의 일관성을 체크하지 않고 백업을 수행한다.

    rye backupdb --no-check demodb

**-t, --thread-count**=COUNT

관리자가 임의로 스레드의 개수를 지정함으로써 병렬 백업을 수행한다. **-t** 옵션의 인수를 지정하지 않더라도 시스템의 CPU 개수만큼 스레드를 자동 부여하여 병렬 백업을 수행한다. 

    rye backupdb -t 4 demodb

**-z, --compress**

대상 데이터베이스를 압축하여 백업 파일에 저장한다. **-z** 옵션을 사용하면, 백업 파일의 크기 및 백업 시간을 단축시킬 수 있다.

    rye backupdb -z demodb

**-e, --except-active-log**

대상 데이터베이스의 활성 로그(active log)를 포함하지 않고 백업을 수행한다. **-e** 옵션을 이용하면 활성 로그를 생성하지 않고 백업이 이루어지므로 백업 시간을 단축시킬 수 있으나, 백업 시점 이후 최근 시점까지의 데이터를 복구할 수 없으므로 상당한 주의를 요한다. 

    rye backupdb -e demodb

**--sleep-msecs**=NUMBER

대상 데이터베이스를 백업하는 도중 쉬는 시간을 설정한다. 단위는 밀리초이며, 기본값은 **0** 이다. 1MB의 파일을 읽을 때마다 설정한 시간만큼 쉰다. 백업 작업이 과도한 디스크 I/O를 유발하기 때문에, 운영 중인 서비스에 백업 작업으로 인한 영향을 줄이고자 할 때 이 옵션이 사용된다. 

    rye backupdb --sleep-msecs=5 demodb


백업 정책 및 방식
-----------------

백업을 진행할 때 고려해야 할 사항은 다음과 같다.

-   **백업할 대상 데이터 선별**
    -   보존 가치가 있는 유효한 데이터인지 판단한다.
    -   데이터베이스 전체를 백업할 것인지, 일부만 백업할 것인지 결정한다.
    -   데이터베이스와 함께 백업해야 할 다른 파일이 있는지 확인한다.
-   **백업 방식 결정**
    -   온라인 백업 방식을 결정한다. 부가적으로 압축 백업, 병렬 백업 모드 사용 여부를 결정한다.
    -   사용 가능한 백업 도구 및 백업 장비를 준비한다.
-   **백업 시기 판단**
    -   데이터베이스 사용이 가장 적은 시간을 파악한다.
    -   보관 로그의 양을 파악한다.
    -   백업할 데이터베이스를 이용하는 클라이언트 수를 파악한다.

**온라인 백업**

온라인 백업(또는 핫 백업)은 운영 중인 데이터베이스에 대해 백업을 수행하는 방식으로, 특정 시점의 데이터베이스 이미지의 스냅샷을 제공한다. 운영 중인 데이터베이스를 대상으로 백업을 수행하기 때문에 커밋되지 않은 데이터가 저장될 우려가 있고, 다른 데이터베이스 운영에도 영향을 줄 수 있다.

온라인 백업을 하려면 **rye backupdb -C** 명령어를 사용한다.

**오프라인 백업**

오프라인 백업(또는 콜드 백업)은 정지 상태인 데이터베이스에 대해 백업을 수행하는 방식으로 특정 시점의 데이터베이스 이미지의 스냅샷을 제공한다.

오프라인 백업을 하려면 **rye backupdb -S** 명령어를 사용한다.

**압축 백업 모드**

압축 백업(compress backup)은 데이터베이스를 압축하여 백업을 수행하기 때문에 백업 볼륨의 크기가 줄어들어 디스크 I/O 비용을 감소시킬 수 있고, 디스크 공간을 절약할 수 있다.

압축 백업을 하려면 **rye backupdb -z** | **--compress** 명령어를 사용한다.

**병렬 백업 모드**

병렬 백업 또는 다중 백업(multi-thread backup)은 지정된 스레드 개수만큼 동시 백업을 수행하기 때문에 백업 시간을 크게 단축시켜 준다. 기본적으로 시스템의 CPU 수만큼 스레드를 부여하게 된다.

병렬 백업을 하려면 **rye backupdb -t** | **--thread-count** 명령어를 사용한다.

백업 파일 관리
--------------

백업 대상 데이터베이스의 크기에 따라 하나 이상의 백업 파일이 연속적으로 생성될 수 있으며, 각각의 백업 파일의 확장자에는 생성 순서에 따라 000, 001~0xx와 같은 유닛 번호가 순차적으로 부여된다.

**백업 작업 중 디스크 용량 관리**

백업 작업 도중, 백업 파일이 저장되는 디스크 용량에 여유가 없는 경우 백업 작업을 진행할 수 없다는 안내 메시지가 화면에 나타난다. 안내 메시지에는 백업 대상이 되는 데이터베이스의 이름과 경로명, 백업 파일명, 백업 파일의 유닛 번호, 백업 수준이 표시된다. 백업 작업을 계속 진행하려는 관리자는 다음과 같이 옵션을 선택할 수 있다.

-   옵션 0: 백업 작업을 더 이상 진행하지 않을 경우, 0을 입력한다.
-   옵션 1: 백업 작업을 진행하기 위해 관리자는 현재 장치에 새로운 디스크를 삽입한 후 1을 입력한다.
-   옵션 2: 백업 작업을 진행하기 위해 관리자는 장치를 변경하거나 백업 파일이 저장되는 디렉터리 경로를 변경한 후 2를 입력한다.

<!-- -->

    ******************************************************************
    Backup destination is full, a new destination is required to continue:
    Database Name: /local1/testing/demodb
         Volume Name: /dev/rst1
            Unit Num: 1
        Backup Level: 0 (FULL LEVEL)
    Enter one of the following options:
    Type
       -  0 to quit.
       -  1 to continue after the volume is mounted/loaded. (retry)
       -  2 to continue after changing the volume's directory or device.
    ******************************************************************

보관 로그 관리
--------------

운영체제의 파일 삭제 명령(rm, del)을 사용하여 보관 로그(archive log)를 임의로 삭제해서는 안 되며, 시스템의 설정, **rye backupdb** 유틸리티 또는 서버 프로세스에 의해 보관 로그가 삭제되어야 한다. 보관 로그가 삭제될 수 있는 경우는 다음의 3가지이다.

-   HA 환경이 아닌 경우(ha\_mode=off)

    **force\_remove\_log\_archives**를 yes(기본값)로 설정하면, 최대 **log\_max\_archives** 개수만큼만 보관 로그가 유지되고 나머지는 자동으로 삭제된다. 단, 가장 오래된 보관 로그 파일에 액티브한 트랜잭션이 있다면 이 트랜잭션이 종료될 때까지 해당 로그 파일이 삭제되지 않는다.

-   HA 환경인 경우(ha\_mode=on)

    **force\_remove\_log\_archives**를 no로 설정하고, **log\_max\_archives** 개수를 지정하면 복제 반영 후 자동으로 삭제된다.

    ha\_mode=on일 때 **force\_remove\_log\_archives**를 yes로 설정하면 복제 반영이 안 된 보관 로그가 삭제될 수 있으므로, 이를 권장하지는 않는다. 다만, 복제 재구축을 감수하더라도 마스터 노드의 디스크 여유 공간을 확보하는 것이 우선된다면 **force\_remove\_log\_archives**를 yes로 설정하고, **log\_max\_archives**를 적당한 값으로 설정한다.

-   **rye backupdb -r** 옵션을 사용하여 명령을 실행하면 삭제된다. 하지만 HA 환경에서는 -r 옵션을 사용하면 안 된다.

즉, 데이터베이스 운영 중에 보관 로그 볼륨을 가급적 남기고 싶지 않다면 **rye.conf**의 **log\_max\_archives** 값을 작은 값 또는 0으로 설정하되, **force\_remove\_log\_archives**의 값은 HA 환경이면 가급적 no로 설정한다.

데이터베이스 복구
=================

데이터베이스 복구는 동일 버전의 Rye 환경에서 수행된 백업 작업에 의해 생성된 백업 파일, 활성 로그 및 보관 로그를 이용하여 특정 시점의 데이터베이스로 복구하는 작업이다. 데이터베이스 복구를 진행하려면 **rye restoredb** 유틸리티 또는 Rye 매니저를 사용한다.

**rye restoredb** 유틸리티는 백업이 수행된 이후에 모든 보관 및 활동 로그들에 기록된 정보들을 이용하여 데이터베이스 백업으로부터 데이터베이스를 복구한다. 

    rye restoredb [options] database_name

어떠한 옵션도 지정되지 않은 경우 기본적으로 마지막 커밋 시점까지 데이터베이스가 복구된다. 만약, 마지막 커밋 시점까지 복구하기 위해 필요한 활성 로그/보관 로그 파일이 없다면 마지막 백업 시점까지만 부분 복구된다. 

    rye restoredb demodb

다음은 **rye restoredb** 유틸리티와 결합할 수 있는 옵션을 정리한 표이다. 대소문자가 구분됨을 주의한다.

**-d, --up-to-date**=DATE

**-d** 옵션으로 지정된 날짜-시간까지 데이터베이스를 복구한다. 사용자는 dd-mm-yyyy:hh:mi:ss(예: 14-10-2008:14:10:00)의 형식으로 복구 시점을 직접 지정할 수 있다. 만약 지정한 복구 시점까지 복구하기 위해 필요한 활성 로그/보관 로그 파일이 없다면 마지막 백업 시점까지만 부분 복구된다.

    rye restoredb -d 14-10-2008:14:10:00 demodb

**backuptime** 이라는 키워드를 복구 시점으로 지정하면 데이터베이스를 마지막 백업이 수행된 시점까지 복구한다. 

    rye restoredb -d backuptime demodb

**--list**

대상 데이터베이스의 백업 파일에 관한 정보를 화면에 출력하며 복구는 수행하지 않는다.
    
    rye restoredb --list demodb

다음은 **--list** 옵션에 의해 출력되는 백업 정보의 예로서, 복구 작업을 수행하기 이전에 대상 데이터베이스의 백업 파일이 최초 저장된 경로와 백업 수준을 검증할 수 있다. 

    *** BACKUP HEADER INFORMATION ***
    Database Name: /local1/testing/demodb
     DB Creation Time: Mon Oct 1 17:27:40 2008
             Pagesize: 4096
    Backup Level: 0
            Start_lsa: 513|3688
             Last_lsa: 513|3688
    Backup Time: Mon Oct 1 17:32:50 2008
    Backup Unit Num: 0
    Release: 1.0.0
         Disk Version: 1
    Backup Pagesize: 4096
    Zip Method: 0 (NONE)
            Zip Level: 0 (NONE)
    (start_lsa was -1|-1)
    Database Volume name: /local1/testing/demodb_vinf
         Volume Identifier: -5, Size: 308 bytes (1 pages)
    Database Volume name: /local1/testing/demodb
         Volume Identifier: 0, Size: 2048000 bytes (500 pages)
    Database Volume name: /local1/testing/demodb_lginf
         Volume Identifier: -4, Size: 165 bytes (1 pages)
    Database Volume name: /local1/testing/demodb_bkvinf
         Volume Identifier: -3, Size: 132 bytes (1 pages)
     
**-B, --backup-file-path**=PATH

백업 파일이 위치하는 디렉터리를 지정할 수 있다. 만약, 이 옵션이 지정되지 않으면 시스템은 데이터베이스 위치 정보 파일인 **databases.txt** 에 지정된 **log-path** 디렉터리에서 대상 데이터베이스를 백업했을 때 생성된 백업 정보 파일(*dbname* **_bkvinf**)을 검색하고, 백업 정보 파일에 지정된 디렉터리 경로에서 백업 파일을 찾는다. 그러나, 백업 정보 파일이 손상되거나 백업 파일의 위치 정보가 삭제된 경우라면 시스템이 백업 파일을 찾을 수 없으므로, 관리자가 **-B** 옵션을 이용하여 백업 파일이 위치하는 디렉터리 경로를 직접 지정해야 한다. 

    rye restoredb -B /home/rye/backup demodb

데이터베이스의 백업 파일이 현재 디렉터리에 있는 경우, 관리자는 **-B** 옵션을 이용하여 백업 파일이 위치하는 디렉터리를 지정할 수 있다. 

    rye restoredb -B . demodb

**-o, --output-file**=FILE

대상 데이터베이스의 복구에 관한 진행 정보를 info_restore라는 파일에 기록하는 명령이다.

    rye restoredb -o info_restore demodb

데이터베이스 서버 시작이나 백업 볼륨 복구 시 서버 에러 로그 또는 restoredb 에러 로그 파일에 로그 회복(log recovery) 시작 시간과 종료 시간에 대한 NOTIFICATION 메시지를 출력하여, 해당 작업의 소요 시간을 확인할 수 있다. 해당 메시지에는 적용(redo)해야할 로그의 개수와 로그 페이지 개수가 함께 기록된다.

NOTIFICATION 메시지를 출력하려면 rye.conf의 error\_log\_level 파라미터의 값을 NOTIFICATION 으로 지정해야 한다.

    Time: 06/14/13 21:29:04.059 - NOTIFICATION *** file ../../src/transaction/log_recovery.c, line 748 CODE = -1128 Tran = -1, EID = 1 
    Log recovery is started. The number of log records to be applied: 96916. Log page: 343 ~ 5104. 
    ..... 
    Time: 06/14/13 21:29:05.170 - NOTIFICATION *** file ../../src/transaction/log_recovery.c, line 843 CODE = -1129 Tran = -1, EID = 4 
    Log recovery is finished.

복구 정책과 절차
----------------

데이터베이스를 복구할 때 고려해야 할 사항은 다음과 같다.

-   **백업 파일 준비**
    -   백업 파일 및 로그 파일이 저장된 디렉터리를 파악한다.
    -   증분 백업으로 대상 데이터베이스가 백업된 경우, 각 백업 수준에 따른 백업 파일이 존재하는지를 파악한다.
    -   백업이 수행된 Rye 데이터베이스의 버전과 복구가 이루어질 Rye 데이터베이스 버전이 동일한지를 파악한다.
-   **복구 방식 결정**
    -   부분 복구인지 전체 복구인지를 결정한다.
    -   사용 가능한 복구 도구 및 복구 장비를 준비한다.
-   **복구 시점 판단**
    -   데이터베이스 서버가 종료된 시점을 파악한다.
    -   장애 발생 전에 이루어진 마지막 백업 시점을 파악한다.
    -   장애 발생 전에 이루어진 마지막 커밋 시점을 파악한다.

**데이터베이스 복구 절차**

다음은 백업 및 복구 작업의 절차를 시간별로 예시한 것이다.

1.  2008/8/14 04:30분에 운영이 중단된 *demodb* 를 전체 백업을 수행한다.
2.  2008/8/14 15:30분에 시스템 장애가 발생하였고, 관리자는 *demodb* 의 복구 작업을 준비한다. 장애 발생 이전의 마지막 커밋 시점이 15:25분이므로 이를 복구 시점으로 지정한다.
3.  관리자는 1.에서 생성된 전체 백업 파일, 활성 로그 및 보관 로그를 준비하여 마지막 커밋 시점인 15:25 시점까지 *demodb* 를 복구한다.

<table>
<colgroup>
<col width="13%" />
<col width="35%" />
<col width="51%" />
</colgroup>
<thead>
<tr class="header">
<th>Time</th>
<th>Command</th>
<th>설명</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>2008/8/14 04:25</td>
<td>rye server stop demodb</td>
<td><em>demodb</em> 운영을 중단한다.</td>
</tr>
<tr class="even">
<td>2008/8/14 04:30</td>
<td>rye backupdb -S -D /home/backup -l 0 demodb</td>
<td>오프라인에서 <em>demodb</em> 를 전체 백업하여 지정된 디렉터리에 백업 파일을 생성한다.</td>
</tr>
<tr class="odd">
<td>2008/8/14 05:00</td>
<td>rye server start demodb</td>
<td><em>demodb</em> 운영을 시작한다.</td>
</tr>
<tr class="even">
<td>2008/8/14 15:30</td>
<td></td>
<td>시스템 장애가 발생한 시각이다.</td>
</tr>
<tr class="odd">
<td>2008/8/14 15:40</td>
<td>rye restoredb -d 08/14/2008:15:25:00 demodb</td>
<td>전체 백업 파일, 활성 로그 및 보관 로그를 기반으로 <em>demodb</em> 를 복구한다. 전체 백업 파일, 활성 로그 및 보관 로그에 의해 15:25 시점까지 복구된다.</td>
</tr>
</tbody>
</table>

다른 서버로의 데이터베이스 복구
-------------------------------

다음은 *A* 서버에서 *demodb* 를 백업하고, 백업된 파일을 기반으로 *B* 서버에서 *demodb* 를 복구하는 방법이다.

**백업 환경과 복구 환경**

*A* 서버의 /home/rye/db/demodb 디렉터리에서 *demodb* 를 백업하고, *B* 서버의 /home/rye/data/demodb 디렉터리에 *demodb* 를 복구하는 것으로 가정한다.

![image](images/image12.png)

1.  A 서버에서 백업

    *A* 서버에서 *demodb* 를 백업한다. 이전에 백업을 수행하였다면 이후 변경된 부분만 증분 백업을 수행할 수 있다. 백업 파일이 생성되는 디렉터리는 **-D** 옵션에 의해 지정하지 않으면, 기본적으로 로그 볼륨이 저장되는 위치에 생성된다. 다음은 권장되는 옵션을 사용한 백업 명령이며, 옵션에 관한 보다 자세한 내용은 db-backup 을 참조한다. :

        rye backupdb -z demodb

2.  B 서버에서 데이터베이스 위치 정보 파일 편집

    동일한 서버에서 백업 및 복구 작업이 이루어지는 일반적인 시나리오와는 달리, 타 서버 환경에서 백업 파일을 복구하는 시나리오에서는 *B* 서버의 데이터베이스 위치 정보 파일(**databases.txt**)에서 데이터베이스를 복구할 위치 정보를 추가해야 한다. 위 그림에서는 *B* 서버(호스트명은 pmlinux)의 /home/rye/data/demodb 디렉터리에 *demodb* 를 복구하는 것을 가정하였으므로, 이에 따라 데이터베이스 위치 정보 파일을 편집하고, 해당 디렉터리를 *B* 서버에서 생성한다.

    데이터베이스 위치 정보는 한 라인으로 작성하고, 각 항목은 공백으로 구분한다. 한 라인은 \[데이터베이스명\] \[데이터볼륨경로\] \[호스트명\] \[로그볼륨경로\]의 형식으로 작성한다. 따라서 다음과 같이 *demodb* 의 위치 정보를 작성한다. :

        demodb /home/rye/data/demodb pmlinux /home/rye/data/demodb

3.  B 서버로 백업 파일 전송

    복구를 위해서는 대상 데이터베이스의 백업 파일이 필수적으로 준비되어야 한다. 따라서, *A* 서버에서 생성된 백업 파일(예: demodb\_bk0v000)을 *B* 서버에 전송한다. 즉, *B* 서버의 임의 디렉터리(예: /home/rye/temp)에는 백업 파일이 위치해야 한다.

    백업 이후에 현재 시점까지 모두 복구하려면 백업 이후의 로그, 즉 활성 로그(예: demodb\_lgat)와 보관 로그(예: demodb\_lgar000)까지 모두 추가적으로 복사해야 된다. 활성 로그와 보관 로그는 복구될 데이터베이스의 로그 디렉터리, 즉 $Rye/databases/databases.txt 파일에서 명시한 로그 파일의 디렉터리(예: $Rye/databases/demodb/log)에 위치해야 한다. 하지만 백업 파일의 위치는 -D 옵션으로 명시할 수 있기 때문에 다른 위치에 있어도 된다.

    또한, 백업 이후 추가된 로그를 반영하려면 보관 로그 파일이 삭제되기 전에 복사해야 하는데, 보관 로그의 삭제 관련 시스템 파라미터인 log\_max\_archives의 기본값이 0으로 설정되어 있으므로 백업 이후 보관 로그 파일이 삭제될 수 있다. 이러한 상황을 방지하기 위해, log\_max\_archives의 값을 적당히 크게 설정하여야 한다. log\_max\_archives &lt;log\_max\_archives&gt;를 참고한다.

4.  B 서버에서 복구

    *B* 서버로 전송한 백업 파일이 있는 디렉터리에서 **rye restoredb** 유틸리티를 호출하여 데이터베이스 복구 작업을 수행한다. **-u** 옵션에 의해 **databases.txt** 에 지정된 디렉터리 경로에 *demodb* 가 복구된다.

        rye restoredb -u demodb

    다른 위치에서 **rye restoredb** 유틸리티를 호출하려면, 다음과 같이 **-B** 옵션을 이용하여 백업 파일이 위치하는 디렉터리 경로를 지정해야 한다.

        rye restoredb -u -B /home/rye/temp demodb


