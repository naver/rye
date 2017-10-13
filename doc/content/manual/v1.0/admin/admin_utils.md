rye 유틸리티
===============

rye 유틸리티의 사용법(구문)은 다음과 같다. :

    rye utility_name
    utility_name :
        createdb [option] <database_name> --- 데이터베이스 생성
        deletedb [option] <database_name>   --- 데이터베이스 삭제
        backupdb [option] <database-name>  --- 데이터베이스 백업
        restoredb [option] <database-name>  --- 데이터베이스 복구
        addvoldb [option] <database-name>  --- 데이터베이스 볼륨 파일 추가
        spacedb [option] <database-name>  --- 데이터베이스 공간 정보 출력
        lockdb [option] <database-name>  --- 데이터베이스의 lock 정보 출력
        tranlist [option] <database-name>  --- 트랜잭션 확인
        killtran [option] <database-name>  --- 트랜잭션 제거
        statdump [option] <database-name>  --- 데이터베이스 서버 실행 통계 정보 출력
        diagdb [option] <database-name>  --- 내부 정보 출력
        plandump [option] <database-name>  --- 쿼리 플랜 캐시 정보 출력
        paramdump [option] <database-name>  --- 데이터베이스의 설정된 파라미터 값 확인
        changemode [option] <database-name>  --- 서버의 HA 모드 출력 또는 변경
        applyinfo [option] <database-name>   --- HA 환경에서 트랜잭션 로그 반영 정보를 확인하는 도구

rye 유틸리티 로깅
--------------------

Rye는 rye 유틸리티의 수행 결과에 대한 로깅 기능을 제공하며, 자세한 내용은 rye-utility-logging을 참고한다.

데이터베이스 사용자
===================

Rye 데이터베이스 사용자는 동일한 권한을 갖는 멤버를 가질 수 있다. 사용자에게 권한 **A**가 부여되면, 상기 사용자에게 속하는 모든 멤버에게도 권한 **A**가 동일하게 부여된다. 이와 같이 데이터베이스 사용자와 그에 속한 멤버를 '그룹'이라 하고, 멤버가 없는 사용자를 '사용자'라 한다.

Rye는 **DBA**와 **PUBLIC**이라는 사용자를 기본으로 제공한다.

-   **DBA**는 모든 사용자의 멤버가 되며 데이터베이스의 모든 객체에 접근할 수 있는 최고 권한 사용자이다. 또한, **DBA**만이 데이터베이스 사용자를 추가, 편집, 삭제할 수 있는 권한을 갖는다.
-   **DBA**를 포함한 모든 사용자는 **PUBLIC**의 멤버가 되므로 모든 데이터베이스 사용자는 **PUBLIC**에 부여된 권한을 가진다. 예를 들어, **PUBLIC** 사용자에 권한 **B**를 추가하면 데이터베이스의 모든 사용자에게 일괄적으로 권한 **B**가 부여된다.

databases.txt 파일
==================

Rye에 존재하는 모든 데이터베이스의 위치 정보는 **databases.txt** 파일에 저장하는데, 이를 데이터베이스 위치 정보 파일이라 한다. 이러한 데이터베이스 위치 정보 파일은 데이터베이스의 생성, 이름 변경, 삭제 및 복사에 관한 유틸리티를 수행하거나 각 데이터베이스를 구동할 때에 사용되며, 항상 **RYE\_DATABASES** 환경 변수로 지정한 디렉터리에 위치한다.

    db_name

데이터베이스 위치 정보 파일의 라인별 형식은 구문에 정의된 바와 같으며, 데이터베이스 이름 정보를 저장한다. 다음은 데이터베이스 위치 정보 파일의 내용을 확인한 예이다.

    % more databases.txt

    #db-name
    testdb
    demodb

데이터베이스 위치 정보 파일의 저장 디렉터리는 시스템 환경 변수 **RYE\_DATABASES**의 설정으로 지정된다. 데이터베이스 위치 정보 파일의 저장 디렉터리 경로가 유효해야 데이터베이스 관리를 위한 **rye** 유틸리티가 데이터베이스 위치 정보 파일에 접근할 수 있게 된다. 이를 위해서 사용자는 디렉터리 경로를 정확하게 입력해야 하고, 해당 디렉터리 경로에 대해 쓰기 권한을 가지는지 확인해야 한다. 다음은 **RYE\_DATABASES** 환경 변수에 설정된 값을 확인하는 예이다.

    % set | grep RYE_DATABASES
    RYE_DATABASES=/home1/user/Rye/databases

만약 **RYE\_DATABASES** 환경 변수에서 유효하지 않은 디렉터리 경로가 설정되는 경우에는 에러가 발생하며, 설정된 디렉터리 경로는 유효하나 데이터베이스 위치 정보 파일이 존재하지 않는 경우에는 새로운 위치 정보 파일을 생성한다.

데이터베이스 생성, 볼륨 추가, 삭제
==================================

Rye 데이터베이스의 볼륨은 크게 영구적 볼륨, 일시적 볼륨, 백업 볼륨으로 분류한다.

-   영구적 볼륨 중
    -   데이터베이스 볼륨에는 범용(generic), 데이터(data), 인덱스(index), 임시(temp) 볼륨이 있고,
    -   로그 볼륨에는 활성(active) 로그, 보관(archiving) 로그, 백그라운드 보관(background archiving) 로그가 있다.
-   일시적 볼륨에는 일시적 임시(temporary temp) 볼륨이 있다.

볼륨에 대한 자세한 내용은 database-volume-structure를 참고한다.

다음은 testdb 데이터베이스를 운영할 때 발생하는 데이터베이스 관련 파일의 예이다.

<table border="1" class="docutils">
<colgroup>
<col width="10%" />
<col width="4%" />
<col width="11%" />
<col width="10%" />
<col width="65%" />
</colgroup>
<thead valign="bottom">
<tr class="row-odd"><th class="head">파일 이름</th>
<th class="head">크기</th>
<th class="head">종류</th>
<th class="head">분류</th>
<th class="head">설명</th>
</tr>
</thead>
<tbody valign="top">
<tr class="row-even"><td>testdb</td>
<td>40MB</td>
<td>generic</td>
<td rowspan="7">데이터베이스
볼륨</td>
<td>DB 생성 시 최초로 생성되는 볼륨. <strong>generic</strong> 볼륨으로 사용되며, DB의 메타 정보를 포함한다.
cubrid.conf의 db_volume_size를 40M로 명시한 후 &quot;cubrid createdb&quot;를 수행했거나 &quot;cubrid createdb&quot;
수행 시 --db-volume-size를 40M로 명시했기 때문에 파일의 크기는 40MB가 되었다.</td>
</tr>
<tr class="row-odd"><td>testdb_x001</td>
<td>40MB</td>
<td>generic, data
index, temp
중 하나</td>
<td>자동으로 생성된 <strong>generic</strong> 파일 또는 사용자의 볼륨 추가 명령으로 생성된 파일.
cubrid.conf의 db_volume_size를 40M로 명시한 후 DB를 시작했기
때문에 자동으로 생성되는 <strong>generic</strong> 파일의 크기는 40MB가 되었다.</td>
</tr>
<tr class="row-even"><td>testdb_x002</td>
<td>40MB</td>
<td>generic, data
index, temp
중 하나</td>
<td>자동으로 생성된 <strong>generic</strong> 파일 또는 사용자의 볼륨 추가 명령으로 생성된 파일</td>
</tr>
<tr class="row-odd"><td>testdb_x003</td>
<td>40MB</td>
<td>generic, data
index, temp
중 하나</td>
<td>자동으로 생성된 <strong>generic</strong> 파일 또는 사용자의 볼륨 추가 명령으로 생성된 파일</td>
</tr>
<tr class="row-even"><td>testdb_x004</td>
<td>40MB</td>
<td>generic, data
index, temp
중 하나</td>
<td>자동으로 생성된 <strong>generic</strong> 파일 또는 사용자의 볼륨 추가 명령으로 생성된 파일</td>
</tr>
<tr class="row-odd"><td>testdb_x005</td>
<td>40MB</td>
<td>generic, data
index, temp
중 하나</td>
<td>자동으로 생성된 <strong>generic</strong> 파일 또는 사용자의 볼륨 추가 명령으로 생성된 파일</td>
</tr>
<tr class="row-even"><td>testdb_x006</td>
<td>2GB</td>
<td>generic, data
index, temp
중 하나</td>
<td>자동으로 생성된 <strong>generic</strong> 파일 또는 사용자의 볼륨 추가 명령으로 생성된 파일.
cubrid.conf의 db_volume_size를 2G로 변경한 후 DB를 재시작했거나
&quot;cubrid addvoldb&quot; 수행 시 --db-volume-size를 2G로 명시했기 때문에 크기가 2GB가 되었다.</td>
</tr>
<tr class="row-odd"><td>testdb_t32766</td>
<td>360MB</td>
<td>temporary temp</td>
<td>없음</td>
<td><strong>temp</strong> 볼륨이 필요한 질의(예: 정렬, 스캐닝, 인덱스 생성) 실행 중 <strong>temp</strong> 볼륨의 공간이
부족할 때 임시로 생성되는 파일. DB를 재시작하면 삭제된다. 하지만 임의로 삭제하면 안 된다.</td>
</tr>
<tr class="row-even"><td>testdb_lgar_t</td>
<td>40MB</td>
<td>background
archiving</td>
<td rowspan="3">로그 볼륨</td>
<td>백그라운드 보관(background archiving) 기능과 관련된 로그 파일.
보관 로그를 저장할 때 사용된다.</td>
</tr>
<tr class="row-odd"><td>testdb_lgar224</td>
<td>40MB</td>
<td>archiving</td>
<td>보관 로그(archiving log)가 계속 쌓이면서 세 자리 숫자로 끝나는 파일들이 생성되는데,
cubrid backupdb -r 옵션 또는 cubrid.conf의 log_max_archives 파라미터의 설정으로 인해 001~223까지의
보관 로그들은 정상적으로 삭제된 것으로 보인다. 보관 로그가  삭제되는 경우, lginf 파일의 REMOVE
섹션에서 삭제된 보관 로그 번호를 확인할 수 있다. <a class="reference internal" href="#managing-archive-logs"><em>보관 로그 관리</em></a>를 참고한다.</td>
</tr>
<tr class="row-even"><td>testdb_lgat</td>
<td>40MB</td>
<td>active</td>
<td>활성 로그(active log) 파일</td>
</tr>
</tbody>
</table>

-   데이터베이스 볼륨 파일
    -   위의 예에서 testdb, testdb\_x001 ~ testdb\_x006이 데이터베이스 볼륨 파일에 해당된다.
    -   "rye createdb", "rye addvoldb" 명령 수행 시 "--db-volume-size" 옵션에 의해 크기가 정해진다.
    -   자동으로 생성되는 볼륨은 항상 **generic** 타입이다.
-   로그 볼륨 파일
    -   위의 예에서 testdb\_lgar\_t, testdb\_lgar224, testdb\_lgat가 로그 볼륨 파일에 해당된다.
    -   "rye createdb" 명령 수행 시 "--log-volume-size" 옵션에 의해 크기가 정해진다.

임시 볼륨은 질의 처리 및 정렬(sorting)을 수행할 때 중간, 최종 결과를 임시로 저장하는 공간으로, 일시적 임시 볼륨과 영구적 임시 볼륨으로 구분한다.

영구적 또는 일시적 임시 볼륨을 사용할 수 있는 질의의 예는 다음과 같다.

-   **SELECT** 문 등 질의 결과가 생성되는 질의
-   **GROUP BY** 나 **ORDER BY** 가 포함된 질의
-   부질의(subquery)가 포함된 질의
-   정렬 병합(sort-merge) 조인이 수행되는 질의
-   **CREATE INDEX** 문이 포함된 질의

위와 같은 질의를 수행할 때 **SELECT** 결과를 저장하거나 데이터를 정렬하기 위해 지정한 메모리 공간 (**rye-auto.conf** 에서 지정하는 시스템 파라미터인 **temp\_file\_memory\_size\_in\_pages**에 의해 메모리 공간의 크기가 결정됨)을 소진하면 임시 볼륨 공간을 사용한다. 질의 처리 및 정렬 결과를 저장하기 위해 사용하는 저장 공간의 순서는 다음과 같으며, 현재의 저장 공간을 모두 소진하면 다음 저장 공간을 사용한다.

-   **temp\_file\_memory\_size\_in\_pages** 시스템 파라미터에 의해 확보된 메모리
-   영구적 임시 볼륨
-   일시적 임시 볼륨

(큰 크기의 임시 공간이 필요한 질의를 수행하면서 일시적 임시 볼륨이 기대 이상으로 증가함으로 인해) 디스크의 여유 공간이 부족해져 시스템 운영에 문제가 발생하는 것을 예방하려면,

-   예상하는 영구적 임시 볼륨을 미리 확보하고,
-   하나의 질의가 수행될 때 일시적 임시 볼륨에서 사용되는 공간의 최대 크기를 제한하는 것이 좋다.

영구적 임시 볼륨은 "rye addvoldb -p temp" 명령을 실행하여 확보하며, 하나의 질의가 수행되는 동안 차지하는 일시적 임시 공간의 최대 크기는 **rye.conf**의 **temp\_file\_max\_size\_in\_pages** 파라미터에 의해 제한한다(기본값은 -1로 무제한).

데이터베이스 생성
-----------------

**rye createdb** 유틸리티는 Rye 시스템에서 사용할 데이터베이스를 생성하고 미리 만들어진 Rye 시스템 테이블을 초기화한다. 일반적으로 데이터베이스 관리자만이 **rye createdb** 유틸리티를 사용한다. 데이터베이스의 위치는 **RYE\_DATABASES**/databases_name/ 이다.

    rye createdb [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **createdb**: 새로운 데이터베이스를 생성하기 위한 명령이다.
-   *database\_name*: 데이터베이스가 생성될 디렉터리 경로명을 포함하지 않고, 생성하고자 하는 데이터베이스의 이름을 고유하게 부여한다. 이 때, 지정한 데이터베이스 이름이 이미 존재하는 데이터베이스 이름과 중복되는 경우, Rye는 기존 파일을 보호하기 위하여 데이터베이스 생성을 더 이상 진행하지 않는다.

데이터베이스 이름의 최대 길이는 영문 17자이다.

다음은 **rye createdb**에 대한 \[options\]이다.

**--db-volume-size**=SIZE

데이터베이스를 생성할 때 첫 번째 데이터베이스 볼륨의 크기를 지정하는 옵션으로, 기본값은 cubrid.conf에 지정된 시스템 파라미터 **db_volume_size** 의 값이다. 최소값은 20M이다. K, M, G, T로 단위를 설정할 수 있으며, 각각 KB(kilobytes), MB(megabytes), GB(gigabytes), TB(terabytes)를 의미한다. 단위를 생략하면 바이트 단위가 적용된다.

다음은 첫 번째로 생성되는 testdb의 볼륨 크기를 512MB로 지정하는 구문이다.

    rye createdb --db-volume-size=512M testdb

**--log-volume-size**=SIZE 

생성되는 데이터베이스의 로그 볼륨 크기를 지정하는 옵션으로, 기본값은 데이터베이스 볼륨 크기와 같으며 최소값은 20M이다. K, M, G, T로 단위를 설정할 수 있으며, 각각 KB(kilobytes), MB(megabytes), GB(gigabytes), TB(terabytes)를 의미한다. 단위를 생략하면 바이트 단위가 적용된다.

다음은 *testdb*를 생성하고, *testdb*의 로그 볼륨 크기를 256M로 지정하는 구문이다.

    rye createdb --log-volume-size=256M testdb 

**-r, --replace**

**-r**은 지정된 데이터베이스 이름이 이미 존재하는 데이터베이스 이름과 중복되더라도 새로운 데이터베이스를 생성하고, 기존의 데이터베이스를 덮어쓰도록 하는 옵션이다.

다음은 *testdb* 라는 이름의 데이터베이스가 이미 존재하더라도 기존의 *testdb* 를 덮어쓰고 새로운 *testdb* 를 생성하는 구문이다.

    rye createdb -r testdb

**--rsql-initialization-file**=FILE

생성하고자 하는 데이터베이스에 대해 CSQL 인터프리터에서 구문을 실행하는 옵션으로, 파라미터로 지정된 파일에 저장된 SQL 구문에 따라 스키마를 생성할 수 있다.

다음은 *testdb*를 생성함과 동시에 table_schema.sql에 정의된 SQL 구문을 CSQL 인터프리터에서 실행시키는 구문이다.

    rye createdb --csql-initialization-file table_schema.sql testdb 

**-o, --output-file**=FILE

데이터베이스 생성에 관한 메시지를 파라미터로 지정된 파일에 저장하는 옵션이며, 파일은 데이터베이스와 동일한 디렉터리에 생성된다. **-o** 옵션이 지정되지 않으면 메시지는 콘솔 화면에 출력된다. **-o** 옵션은 데이터베이스가 생성되는 중에 출력되는 메시지를 지정된 파일에 저장함으로써 특정 데이터베이스의 생성 과정에 관한 정보를 활용할 수 있게 한다.

다음은 *testdb*를 생성하면서 이에 관한 유틸리티의 출력을 콘솔 화면이 아닌 db_output 파일에 저장하는 구문이다.

    rye createdb -o db_output testdb

**-v, --verbose**

데이터베이스 생성 연산에 관한 모든 정보를 화면에 출력하는 옵션으로서, **-o** 옵션과 마찬가지로 특정 데이터베이스 생성 과정에 관한 정보를 확인하는데 유용하다. 따라서, **-v** 옵션과 **-o** 옵션을 함께 지정하면, **-o** 옵션의 파라미터로 지정된 출력 파일에 **cubrid createdb** 유틸리티의 연산 정보와 생성 과정에 관한 출력 메시지를 저장할 수 있다.

다음은 *testdb*를 생성하면서 이에 관한 상세한 연산 정보를 화면에 출력하는 구문이다.

    rye createdb -v testdb

-   **temp\_file\_max\_size\_in\_pages**는 복잡한 질의문이나 정렬 수행에 사용되는 일시적 임시 볼륨(temporary temp volume)을 디스크에 저장하는 데에 할당되는 페이지의 최대 개수를 설정하는 파라미터이다. 기본값은 **-1**로, 디스크의 여유 공간까지 일시적 임시 볼륨(temporary temp volume)이 커질 수 있다. 0이면 일시적 임시 볼륨이 생성되지 않으므로 rye addvoldb &lt;adding-database-volume&gt; 유틸리티를 이용하여 영구적 임시 볼륨(permanent temp volume)을 충분히 추가해야 한다. 볼륨을 효율적으로 관리하려면 용도별로 볼륨을 추가하는 것을 권장한다.
-   rye spacedb &lt;spacedb&gt; 유틸리티를 사용하여 각 용도별 볼륨의 남은 공간을 검사할 수 있으며, rye addvoldb &lt;adding-database-volume&gt; 유틸리티를 사용하여 데이터베이스 운영 중에도 필요한 만큼 볼륨을 추가할 수 있다. 데이터베이스 운영 중에 볼륨을 추가하려면 가급적 시스템 부하가 적은 상태에서 추가할 것을 권장한다. 해당 용도의 볼륨 공간이 모두 사용되면 범용(**generic**) 볼륨이 생성되므로 여유 공간이 부족할 것으로 예상되는 용도의 볼륨을 미리 추가해 놓을 것을 권장한다.

다음은 데이터베이스를 생성하고 볼륨 용도를 구분하여 데이터(**data**), 인덱스(**index**), 임시(**temp**) 볼륨을 추가하는 예이다. 

    rye createdb --db-volume-size=512M --log-volume-size=256M ryedb
    rye addvoldb -S -p data -n ryedb_DATA01 --db-volume-size=512M ryedb
    rye addvoldb -S -p data -n ryedb_DATA02 --db-volume-size=512M ryedb
    rye addvoldb -S -p index -n ryedb_INDEX01 ryedb --db-volume-size=512M ryedb
    rye addvoldb -S -p temp -n ryedb_TEMP01 ryedb --db-volume-size=512M ryedb

데이터베이스 볼륨 추가
----------------------

전체 **generic** 볼륨의 여유 공간이 disk-parameters에 속한 **generic\_vol\_prealloc\_size** 파라미터에서 지정한 크기(기본값: 50M)보다 작아지면 자동으로 **generic** 볼륨이 추가된다. 볼륨 자동 추가는 새로운 페이지 할당 요청이 있을 때 이루어지며, SELECT만 수행되는 경우 볼륨이 확장되지 않는다.

Rye의 볼륨은 데이터 저장, 인덱스 저장, 임시 결과 저장 등 용도에 따라 구분되는데, **generic** 볼륨은 데이터 및 인덱스 저장 용도로 사용될 수 있다.

각 볼륨의 종류(용도)에 대해서는 database-volume-structure를 참고한다.

이에 비해, 사용자에 의해 수동으로 데이터베이스 볼륨을 추가하는 명령은 다음과 같다.

    rye addvoldb [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **addvoldb**: 지정된 데이터베이스에 지정된 페이지 수만큼 새로운 볼륨을 추가하기 위한 명령이다.
-   *database\_name*: 데이터베이스가 생성될 디렉터리 경로명을 포함하지 않고, 볼륨을 추가하고자 하는 데이터베이스의 이름을 지정한다.

다음은 데이터베이스를 생성하고 볼륨 용도를 구분하여 데이터(**data**), 인덱스(**index**), 임시(**temp**) 볼륨을 추가하는 예이다. :

    rye createdb --db-volume-size=512M --log-volume-size=256M ryedb
    rye addvoldb -S -p data -n ryedb_DATA01 --db-volume-size=512M ryedb
    rye addvoldb -S -p data -n ryedb_DATA02 --db-volume-size=512M ryedb
    rye addvoldb -S -p index -n ryedb_INDEX01 ryedb --db-volume-size=512M ryedb
    rye addvoldb -S -p temp -n ryedb_TEMP01 ryedb --db-volume-size=512M ryedb

다음은 rye addvoldb에 대한 \[options\]이다.

**--db-volume-size**=SIZE

추가되는 데이터베이스 볼륨의 크기를 지정하는 옵션으로, 기본값은 **rye.conf**에 지정된 시스템 파라미터 **db_volume_size** 의 값이다. K, M, G, T로 단위를 설정할 수 있으며, 각각 KB(kilobytes), MB(megabytes), GB(gigabytes), TB(terabytes)를 의미한다. 단위를 생략하면 바이트 단위가 적용된다.

다음은 *testdb*에 데이터 볼륨을 추가하며 볼륨 크기를 256MB로 지정하는 구문이다.

    rye addvoldb -p data --db-volume-size=256M testdb

**-p, --purpose**=PURPOSE

추가할 볼륨의 사용 목적에 따라 볼륨의 종류를 지정하는 옵션이다. 이처럼 볼륨의 사용 목적에 맞는 볼륨을 지정해야 볼륨 종류별로 디스크 드라이브에 분리 저장할 수 있어 I/O 성능을 높일 수 있다. **-p** 옵션의 파라미터로 가능한 값은 **data**, **index**, **temp**, **generic** 중 하나이며, 기본값은 **generic**이다. 각 볼륨 용도에 관해서는 database-volume-structure 를 참조한다.

다음은 독립모드(standalone) 상태에서 *testdb*\라는 데이터베이스에 256MB 인덱스 볼륨을 추가하는 구문이다.

    rye addvoldb -S -p index --db-volume-size=256M testdb

**-S, --SA-mode**

서버 프로세스를 구동하지 않고 데이터베이스에 접근하는 독립 모드(standalone)로 작업하기 위해 지정되며, 인수는 없다. **-S** 옵션을 지정하지 않으면, 시스템은 클라이언트/서버 모드로 인식한다.

    rye addvoldb -S --db-volume-size=256M testdb

**-C, --CS-mode**

서버 프로세스와 클라이언트 프로세스를 각각 구동하여 데이터베이스에 접근하는 클라이언트/서버 모드로 작업하기 위한 옵션이며, 인수는 없다. **-C** 옵션을 지정하지 않더라도 시스템은 기본적으로 클라이언트/서버 모드로 인식한다.

    rye addvoldb -C --db-volume-size=256M testdb

**--max_writesize-in-sec**=SIZE

데이터베이스에 볼륨을 추가할 때 디스크 출력량을 제한하여 시스템 운영 영향을 줄이도록 하는 옵션이다. 이 옵션을 통해 1초당 쓸 수 있는 최대 크기를 지정할 수 있으며, 단위는 K(kilobytes), M(megabytes)이다. 최소값은 160K이며, 이보다 작게 값을 설정하면 160K로 바뀐다. 단, 클라이언트/서버 모드(-C)에서만 사용 가능하다.
    
다음은 2GB 볼륨을 초당 1MB씩 쓰도록 하는 예이다. 소요 시간은 35분( = (2048MB / 1MB)  / 60초 )  정도가 예상된다.
    
    rye addvoldb -C --db-volume-size=2G --max-writesize-in-sec=1M testdb


데이터베이스 삭제
-----------------

**rye deletedb**는 데이터베이스를 삭제하는 유틸리티이다. 데이터베이스가 몇 개의 상호 의존적 파일들로 만들어지기 때문에, 데이터베이스를 제거하기 위해 운영체제 파일 삭제 명령이 아닌 **rye deletedb** 유틸리티를 사용해야 한다.

**rye deletedb** 유틸리티는 데이터베이스 위치 파일( **databases.txt** )에 지정된 데이터베이스에 대한 정보도 같이 삭제한다. **rye deletedb** 유틸리티는 오프라인 상에서 즉, 아무도 데이터베이스를 사용하지 않는 상태에서 독립 모드로 사용해야 한다. :

    rye deletedb [options] database_name 

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **deletedb**: 데이터베이스 및 관련 데이터, 로그, 백업 파일을 전부 삭제하기 위한 명령으로, 데이터베이스 서버가 구동 정지 상태인 경우에만 정상적으로 수행된다.
-   *database\_name*: 디렉터리 경로명을 포함하지 않고, 삭제하고자 하는 데이터베이스의 이름을 지정한다

다음은 **rye deletedb**에 대한 \[options\]이다.

**-o, --output-file**=FILE

데이터베이스를 삭제하면서 출력되는 메시지를 인자로 지정한 파일에 기록하는 명령이다. **rye deletedb** 유틸리티를 사용하면 데이터베이스 위치 정보 파일( **databases.txt** )에 기록된 데이터베이스 정보가 함께 삭제된다.

    rye deletedb -o deleted_db.out testdb

만약, 존재하지 않는 데이터베이스를 삭제하는 명령을 입력하면 다음과 같은 메시지가 출력된다.

    rye deletedb testdb
    Database "testdb" is unknown, or the file "databases.txt" cannot be accessed.

**-d, --delete-backup**

데이터베이스를 삭제하면서 백업 볼륨 및 백업 정보 파일도 함께 삭제할 수 있다. -**d** 옵션을 지정하지 않으면 백업 볼륨 및 백업 정보 파일은 삭제되지 않는다.

    rye deletedb -d testdb


데이터베이스 공간 확인
======================

사용 공간 확인
--------------

**rye spacedb** 유틸리티는 사용 중인 데이터베이스 볼륨의 공간을 확인하기 위해서 사용된다. **rye spacedb** 유틸리티는 데이터베이스에 있는 모든 영구 데이터 볼륨의 간략한 설명을 보여준다. **rye spacedb** 유틸리티에 의해 반환되는 정보는 볼륨 ID와 이름, 각 볼륨의 목적, 각 볼륨과 관련된 총(total) 공간과 빈(free) 공간이다.

    rye spacedb [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **spacedb**: 대상 데이터베이스에 대한 공간을 확인하는 명령으로 데이터베이스 서버가 구동 정지 상태인 경우에만 정상적으로 수행된다.
-   *database\_name*: 공간을 확인하고자 하는 데이터베이스의 이름이며, 데이터베이스가 생성될 디렉터리 경로명을 포함하지 않는다.

다음은 **rye spacedb**에 대한 \[options\]이다.

**-o FILE**

데이터베이스의 공간 정보에 대한 결과를 지정한 파일에 저장한다.
    
    rye spacedb -o db_output testdb

**-S, --SA-mode**
    
서버 프로세스를 구동하지 않고 데이터베이스에 접근하는 독립 모드(standalone)로 작업하기 위해 지정되며, 인수는 없다. **-S** 옵션을 지정하지 않으면, 시스템은 클라이언트/서버 모드로 인식한다. 

    rye spacedb --SA-mode testdb

**-C, --CS-mode**

**-C** 옵션은 서버 프로세스와 클라이언트 프로세스를 각각 구동하여 데이터베이스에 접근하는 클라이언트/서버 모드로 작업하기 위한 옵션이며, 인수는 없다. **-C** 옵션을 지정하지 않더라도 시스템은 기본적으로 클라이언트/서버 모드로 인식한다.

    rye spacedb --CS-mode testdb

**--size-unit**={PAGE|M|G|T|H}

데이터베이스 볼륨의 공간을 지정한 크기 단위로 출력하기 위한 옵션이며, 기본값은 H이다.
단위를 PAGE, M, G, T, H로 설정할 수 있으며, 각각 페이지, MB(megabytes), GB(gigabytes), TB(terabytes), 자동 지정을 의미한다. 자동 지정을 의미하는 H로 설정하면 데이터베이스 크기가 1MB 이상 1024MB 미만일 때 MB 단위로, 1GB 이상 1024GB 미만일 때 GB 단위로 결정된다.

    $ rye spacedb --size-unit=M testdb
    $ rye spacedb --size-unit=H testdb

    Space description for database 'testdb' with pagesize 16.0K. (log pagesize: 16.0K)

    Volid  Purpose    total_size   free_size  Vol Name

        0   GENERIC       20.0 M      17.0 M  /home1/rye/testdb
        1      DATA       20.0 M      19.5 M  /home1/rye/testdb_x001
        2     INDEX       20.0 M      19.6 M  /home1/rye/testdb_x002
        3      TEMP       20.0 M      19.6 M  /home1/rye/testdb_x003
        4      TEMP       20.0 M      19.9 M  /home1/rye/testdb_x004
    -------------------------------------------------------------------------------
        5                100.0 M      95.6 M
    Space description for temporary volumes for database 'testdb' with pagesize 16.0K.

    Volid  Purpose    total_size   free_size  Vol Name

**-s, --summarize**

데이터 볼륨(DATA), 인덱스 볼륨(INDEX), 범용 볼륨(GENERIC), 임시 볼륨(TEMP), 일시적 임시 볼륨(TEMP TEMP)별로 전체 공간(total_pages), 사용 공간(used_pages), 빈 공간(free_pages)을 합산하여 출력한다.

    $ rye spacedb -s testdb

    Summarized space description for database 'testdb' with pagesize 16.0K. (log pagesize: 16.0K)

    Purpose     total_size   used_size   free_size  volume_count
    -------------------------------------------------------------
          DATA      20.0 M       0.5 M      19.5 M          1
         INDEX      20.0 M       0.4 M      19.6 M          1
       GENERIC      20.0 M       3.0 M      17.0 M          1
          TEMP      40.0 M       0.5 M      39.5 M          2
     TEMP TEMP       0.0 M       0.0 M       0.0 M          0
    -------------------------------------------------------------
         TOTAL     100.0 M       4.4 M      95.6 M          5

**-p, --purpose**

사용 중인 디스크 공간을 data_size, index_size, temp_size로 구분하여 출력한다.

    Space description for database 'testdb' with pagesize 16.0K. (log pagesize: 16.0K)

    Volid  Purpose    total_size   free_size   data_size  index_size   temp_size  Vol Name

        0   GENERIC       20.0 M      17.0 M       2.1 M       0.9 M       0.0 M  /home1/rye/testdb
        1      DATA       20.0 M      19.5 M       0.4 M       0.0 M       0.0 M  /home1/rye/testdb_x001
        2     INDEX       20.0 M      19.6 M       0.0 M       0.4 M       0.0 M  /home1/rye/testdb_x002
        3      TEMP       20.0 M      19.6 M       0.0 M       0.0 M       0.3 M  /home1/rye/testdb_x003
        4      TEMP       20.0 M      19.9 M       0.0 M       0.0 M       0.1 M  /home1/rye/testdb_x004
    ----------------------------------------------------------------------------------------------------
        5                100.0 M      95.6 M       2.5 M       1.2 M       0.4 M
    Space description for temporary volumes for database 'testdb' with pagesize 16.0K.

    Volid  Purpose    total_size   free_size   data_size  index_size   temp_size  Vol Name


**-p**와 **-s**를 함께 사용하는 경우, 요약 정보를 출력할 때 사용 중인 디스크 공간을 data\_size, index\_size, temp\_size로 구분하여 출력한다.

    $ rye spacedb -s -p testdb
    Summarized space description for database 'testdb' with pagesize 16.0K. (log pagesize: 16.0K)

    Purpose     total_size   used_size   free_size   data_size  index_size   temp_size  volume_count
    -------------------------------------------------------------------------------------------------
          DATA      20.0 M       0.5 M      19.5 M       0.4 M       0.0 M       0.0 M          1
         INDEX      20.0 M       0.4 M      19.6 M       0.0 M       0.4 M       0.0 M          1
       GENERIC      20.0 M       3.0 M      17.0 M       2.1 M       0.9 M       0.0 M          1
          TEMP      40.0 M       0.5 M      39.5 M       0.0 M       0.0 M       0.4 M          2
     TEMP TEMP       0.0 M       0.0 M       0.0 M       0.0 M       0.0 M       0.0 M          0
    -------------------------------------------------------------------------------------------------
         TOTAL     100.0 M       4.4 M      95.6 M       2.5 M       1.2 M       0.4 M          5


질의 계획 확인
==============

질의 수행 계획 캐시 확인
------------------------

**rye plandump** 유틸리티를 사용해서 서버에 저장(캐시)되어 있는 질의 수행 계획들의 정보를 출력할 수 있다. :

    rye plandump [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **plandump**: 대상 데이터베이스에 대하여 현재 캐시에 저장되어 있는 질의 수행 계획을 출력하는 명령이다.
-   *database\_name*: 데이터베이스 서버 캐시로부터 질의 수행 계획을 확인 또는 제거하고자 하는 데이터베이스 이름이다

옵션 없이 사용하면 캐시에 저장된 질의 수행 계획을 확인한다. :

    rye plandump testdb

다음은 **rye plandump**에 대한 \[options\]이다.

**-d, --drop**

캐시에 저장된 질의 수행 계획을 제거한다.

    rye plandump -d testdb

**-o, --output-file**=FILE
        
캐시에 저장된 질의 수행 계획 결과 파일에 저장 ::

    rye plandump -o output.txt testdb


서버 실행 통계 정보 출력
------------------------

**rye statdump** 유틸리티를 이용해 Rye 데이터베이스 서버가 실행한 통계 정보를 확인할 수 있으며, 통계 정보 항목은 크게 File I/O 관련, 페이지 버퍼 관련, 로그 관련, 트랜잭션 관련, 동시성 관련, 인덱스 관련, 쿼리 수행 관련, 네트워크 요청 관련으로 구분된다.

RSQL의 해당 연결에 대해서만 통계 정보를 확인하려면 RSQL의 세션 명령어를 이용할 수 있으며 RSQL 실행 통계 정보 출력 &lt;rsql-execution-statistics&gt;를 참고한다.

    rye statdump [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **statdump**: 대상 데이터베이스 서버 실행 통계 정보를 출력하는 명령어이다. 데이터베이스가 동작 중일 때에만 정상 수행된다.
-   *database\_name*: 통계 자료를 확인하고자 하는 대상 데이터베이스 이름이다.

다음은 **rye statdump**에 대한 \[options\]이다.

**-i, --interval**=SECOND

지정한 초 단위로 주기적으로 출력한다. **-i** 옵션이 주어질 때만 정보가 갱신된다.
    
다음은 1초마다 누적된 정보 값을 출력한다.
    
    rye statdump -i 1 -c demodb
        
다음은 1초 마다 0으로 리셋하고 1초 동안 누적된 값을 출력한다.
    
    rye statdump -i 1 demodb
        
다음은 -i 옵션으로 가장 마지막에 실행한 값을 출력한다.
    
    rye statdump demodb
        
다음은 위와 같은 결과를 출력한다. **-c** 옵션은 **-i** 옵션과 같이 쓰이지 않으면 옵션을 설정하지 않은 것과 동일하다.
    
    rye statdump -c demodb

다음은 5초마다 결과를 출력한다.

    rye statdump -i 5 testdb
     
    Thu March 07 23:10:08 KST 2014
     
     *** SERVER EXECUTION STATISTICS ***
    Num_file_creates              =          0
    Num_file_removes              =          0
    Num_file_ioreads              =          0
    Num_file_iowrites             =          0
    Num_file_iosynches            =          0
    Num_file_page_allocs          =          0
    Num_file_page_deallocs        =          0
    Num_data_page_fetches         =          0
    Num_data_page_dirties         =          0
    Num_data_page_ioreads         =          0
    Num_data_page_iowrites        =          0
    Num_data_page_victims         =          0
    Num_data_page_iowrites_for_replacement =          0
    Num_log_page_ioreads          =          0
    Num_log_page_iowrites         =          0
    Num_log_append_records        =          0
    Num_log_archives              =          0
    Num_log_start_checkpoints     =          0
    Num_log_end_checkpoints       =          0
    Num_log_wals                  =          0
    Num_page_locks_acquired       =          0
    Num_object_locks_acquired     =          0
    Num_page_locks_converted      =          0
    Num_object_locks_converted    =          0
    Num_page_locks_re-requested   =          0
    Num_object_locks_re-requested =          0
    Num_page_locks_waits          =          0
    Num_object_locks_waits        =          0
    Num_tran_commits              =          0
    Num_tran_rollbacks            =          0
    Num_tran_savepoints           =          0
    Num_tran_start_topops         =          0
    Num_tran_end_topops           =          0
    Num_tran_interrupts           =          0
    Num_btree_inserts             =          0
    Num_btree_deletes             =          0
    Num_btree_updates             =          0
    Num_btree_covered             =          0
    Num_btree_noncovered          =          0
    Num_btree_resumes             =          0
    Num_btree_multirange_optimization =      0
    Num_btree_splits              =          0
    Num_btree_merges              =          0
    Num_query_selects             =          0
    Num_query_inserts             =          0
    Num_query_deletes             =          0
    Num_query_updates             =          0
    Num_query_sscans              =          0
    Num_query_iscans              =          0
    Num_query_lscans              =          0
    Num_query_setscans            =          0
    Num_query_methscans           =          0
    Num_query_nljoins             =          0
    Num_query_mjoins              =          0
    Num_query_objfetches          =          0
    Num_query_holdable_cursors    =          0
    Num_sort_io_pages             =          0
    Num_sort_data_pages           =          0
    Num_network_requests          =          1
    Num_adaptive_flush_pages      =          0
    Num_adaptive_flush_log_pages  =          0
    Num_adaptive_flush_max_pages  =        900
    Num_prior_lsa_list_size       =          0
    Num_prior_lsa_list_maxed      =          0
    Num_prior_lsa_list_removed    =          0
    Num_heap_stats_bestspace_entries =          0
    Num_heap_stats_bestspace_maxed =          0
    Time_ha_replication_delay     =          0
    Num_plan_cache_add            =          0
    Num_plan_cache_lookup         =          0
    Num_plan_cache_hit            =          0
    Num_plan_cache_miss           =          0
    Num_plan_cache_full           =          0
    Num_plan_cache_delete         =          0
    Num_plan_cache_invalid_xasl_id =          0
    Num_plan_cache_query_string_hash_entries =          0
    Num_plan_cache_xasl_id_hash_entries =          0
    Num_plan_cache_class_oid_hash_entries =          0
        
     *** OTHER STATISTICS ***
    Data_page_buffer_hit_ratio    =       0.00

다음은 위의 데이터베이스 서버 실행 통계 정보에 대한 설명이다.

<table border="1" class="docutils">
<colgroup>
<col width="12%" />
<col width="28%" />
<col width="59%" />
</colgroup>
<thead valign="bottom">
<tr class="row-odd"><th class="head">분류</th>
<th class="head">항목</th>
<th class="head">설명</th>
</tr>
</thead>
<tbody valign="top">
<tr class="row-even"><td rowspan="5">File I/O
관련</td>
<td>Num_file_removes</td>
<td>삭제한 파일 개수</td>
</tr>
<tr class="row-odd"><td>Num_file_creates</td>
<td>생성한 파일 개수</td>
</tr>
<tr class="row-even"><td>Num_file_ioreads</td>
<td>디스크로부터 읽은 횟수</td>
</tr>
<tr class="row-odd"><td>Num_file_iowrites</td>
<td>디스크로 저장한 횟수</td>
</tr>
<tr class="row-even"><td>Num_file_iosynches</td>
<td>디스크와 동기화를 수행한 횟수</td>
</tr>
<tr class="row-odd"><td rowspan="11">페이지 버퍼
관련</td>
<td>Num_data_page_fetches</td>
<td>가져오기(fetch)한 페이지 수</td>
</tr>
<tr class="row-even"><td>Num_data_page_dirties</td>
<td>더티 페이지 수</td>
</tr>
<tr class="row-odd"><td>Num_data_page_ioreads</td>
<td>디스크에서 읽은 페이지 수
(이 값이 클수록 덜 효율적이며, 히트율이 낮은 것과 상관됨)</td>
</tr>
<tr class="row-even"><td>Num_data_page_iowrites</td>
<td>디스크에서 기록한 페이지 수(이 값이 클수록 덜 효율적임)</td>
</tr>
<tr class="row-odd"><td>Num_data_page_victims</td>
<td>데이터 버퍼에서 디스크로 내려 쓰기(flush)하는 스레드가 깨어나는 회수
(내려 쓰기되는 페이지의 또는 희생자(victim)의 개수가 아님)</td>
</tr>
<tr class="row-even"><td>Num_data_page_iowrites_for_replacement</td>
<td>후보로 선정되어 디스크로 쓰여진 데이터 페이지 수</td>
</tr>
<tr class="row-odd"><td>Num_adaptive_flush_pages</td>
<td>데이터 버퍼로부터 디스크로 내려 쓰기(flush)한 데이터 페이지 수</td>
</tr>
<tr class="row-even"><td>Num_adaptive_flush_log_pages</td>
<td>로그 버퍼로부터 디스크로 내려 쓰기(flush)한 로그 페이지 수</td>
</tr>
<tr class="row-odd"><td>Num_adaptive_flush_max_pages</td>
<td>데이터 및 로그 버퍼로부터 디스크로 내려 쓰기(flush)를 허용하는 최대
페이지 수</td>
</tr>
<tr class="row-even"><td>Num_sort_io_pages</td>
<td>정렬하는 동안 디스크에서 페치한 페이지 개수(이 값이 클수록 덜 효율적임)</td>
</tr>
<tr class="row-odd"><td>Num_sort_data_pages</td>
<td>정렬하는 동안 페이지 버퍼에서 발견된 페이지 개수(이 값이 클수록 더 효율적임)</td>
</tr>
<tr class="row-even"><td rowspan="7">로그 관련</td>
<td>Num_log_page_ioreads</td>
<td>읽은 로그 페이지의 수</td>
</tr>
<tr class="row-odd"><td>Num_log_page_iowrites</td>
<td>저장한 로그 페이지의 수</td>
</tr>
<tr class="row-even"><td>Num_log_append_records</td>
<td>추가(append)한 로그 레코드의 수</td>
</tr>
<tr class="row-odd"><td>Num_log_archives</td>
<td>보관 로그의 개수</td>
</tr>
<tr class="row-even"><td>Num_log_start_checkpoints</td>
<td>체크포인트 시작 횟수</td>
</tr>
<tr class="row-odd"><td>Num_log_end_checkpoints</td>
<td>체크포인트 종료 횟수</td>
</tr>
<tr class="row-even"><td>Num_log_wals</td>
<td>현재 사용하지 않음</td>
</tr>
<tr class="row-odd"><td rowspan="6">트랜잭션
관련</td>
<td>Num_tran_commits</td>
<td>커밋한 횟수</td>
</tr>
<tr class="row-even"><td>Num_tran_rollbacks</td>
<td>롤백한 횟수</td>
</tr>
<tr class="row-odd"><td>Num_tran_savepoints</td>
<td>세이브포인트 횟수</td>
</tr>
<tr class="row-even"><td>Num_tran_start_topops</td>
<td>시작한 top operation의 개수</td>
</tr>
<tr class="row-odd"><td>Num_tran_end_topops</td>
<td>종료한 top operation의 개수</td>
</tr>
<tr class="row-even"><td>Num_tran_interrupts</td>
<td>인터럽트 개수</td>
</tr>
<tr class="row-odd"><td rowspan="8">동시성/잠금
관련</td>
<td>Num_page_locks_acquired</td>
<td>페이지 잠금을 획득한 횟수</td>
</tr>
<tr class="row-even"><td>Num_object_locks_acquired</td>
<td>오브젝트 잠금을 획득한 횟수</td>
</tr>
<tr class="row-odd"><td>Num_page_locks_converted</td>
<td>페이지 잠금 타입을 변환한 횟수</td>
</tr>
<tr class="row-even"><td>Num_object_locks_converted</td>
<td>오브젝트 잠금 타입을 변환한 횟수</td>
</tr>
<tr class="row-odd"><td>Num_page_locks_re-requested</td>
<td>페이지 잠금을 재요청한 횟수</td>
</tr>
<tr class="row-even"><td>Num_object_locks_re-requested</td>
<td>오브젝트 잠금을 재요청한 횟수</td>
</tr>
<tr class="row-odd"><td>Num_page_locks_waits</td>
<td>잠금을 대기하는 페이지 개수</td>
</tr>
<tr class="row-even"><td>Num_object_locks_waits</td>
<td>잠금을 대기하는 오브젝트 개수</td>
</tr>
<tr class="row-odd"><td rowspan="9">인덱스 관련</td>
<td>Num_btree_inserts</td>
<td>삽입된 항목의 개수</td>
</tr>
<tr class="row-even"><td>Num_btree_deletes</td>
<td>삭제된 항목의 개수</td>
</tr>
<tr class="row-odd"><td>Num_btree_updates</td>
<td>갱신된 항목의 개수</td>
</tr>
<tr class="row-even"><td>Num_btree_covered</td>
<td>질의 시 인덱스가 데이터를 모두 포함한 경우의 개수</td>
</tr>
<tr class="row-odd"><td>Num_btree_noncovered</td>
<td>질의 시 인덱스가 데이터를 일부분만 포함하거나 전혀 포함하지 않은 경우의 개수</td>
</tr>
<tr class="row-even"><td>Num_btree_resumes</td>
<td>index_scan_oid_buffer_pages를 초과한 인덱스 스캔 횟수</td>
</tr>
<tr class="row-odd"><td>Num_btree_multirange_optimization</td>
<td>WHERE ... IN ... LIMIT 조건 질의문에 대해 다중 범위
최적화(multi-range optimization)를 수행한 횟수</td>
</tr>
<tr class="row-even"><td>Num_btree_splits</td>
<td>B-tree 노드 분할 연산 회수</td>
</tr>
<tr class="row-odd"><td>Num_btree_merges</td>
<td>B-tree 노드 합병 연산 회수</td>
</tr>
<tr class="row-even"><td rowspan="12">쿼리 관련</td>
<td>Num_query_selects</td>
<td>SELECT 쿼리의 수행 횟수</td>
</tr>
<tr class="row-odd"><td>Num_query_inserts</td>
<td>INSERT 쿼리의 수행 횟수</td>
</tr>
<tr class="row-even"><td>Num_query_deletes</td>
<td>DELETE 쿼리의 수행 횟수</td>
</tr>
<tr class="row-odd"><td>Num_query_updates</td>
<td>UPDATE 쿼리의 수행 횟수</td>
</tr>
<tr class="row-even"><td>Num_query_sscans</td>
<td>순차 스캔(풀 스캔) 횟수</td>
</tr>
<tr class="row-odd"><td>Num_query_iscans</td>
<td>인덱스 스캔 횟수</td>
</tr>
<tr class="row-even"><td>Num_query_lscans</td>
<td>LIST 스캔 횟수</td>
</tr>
<tr class="row-odd"><td>Num_query_setscans</td>
<td>SET 스캔 횟수</td>
</tr>
<tr class="row-even"><td>Num_query_methscans</td>
<td>METHOD 스캔 횟수</td>
</tr>
<tr class="row-odd"><td>Num_query_nljoins</td>
<td>Nested Loop 조인 횟수</td>
</tr>
<tr class="row-even"><td>Num_query_mjoins</td>
<td>병합 조인 횟수</td>
</tr>
<tr class="row-odd"><td>Num_query_objfetches</td>
<td>객체를 가져오기(fetch)한 횟수</td>
</tr>
<tr class="row-even"><td>네트워크
요청 관련</td>
<td>Num_network_requests</td>
<td>네트워크 요청 횟수</td>
</tr>
<tr class="row-odd"><td rowspan="10">질의 계획
캐시 관련</td>
<td>Num_plan_cache_add</td>
<td>캐시 엔트리(entry)가 새로 추가된 횟수</td>
</tr>
<tr class="row-even"><td>Num_plan_cache_lookup</td>
<td>특정 키를 사용하여 룩업(lookup)을 시도한 횟수</td>
</tr>
<tr class="row-odd"><td>Num_plan_cache_hit</td>
<td>질의 문자열 해시 테이블에서 엔트리를 찾은(hit) 횟수</td>
</tr>
<tr class="row-even"><td>Num_plan_cache_miss</td>
<td>질의 문자열 해시 테이블에서 엔트리를 찾지 못한(miss) 횟수</td>
</tr>
<tr class="row-odd"><td>Num_plan_cache_full</td>
<td>캐시 엔트리의 개수가 허용된 최대 개수를 넘어 희생자(victim) 탐색을 시도한 횟수</td>
</tr>
<tr class="row-even"><td>Num_plan_cache_delete</td>
<td>캐시 엔트리가 삭제된(victimized) 횟수</td>
</tr>
<tr class="row-odd"><td>Num_plan_cache_invalid_xasl_id</td>
<td>xasl_id 해시 테이블에서 엔트리를 찾지 못한(miss) 횟수.
서버에서 특정 엔트리가 제거(victimized)되었는데, 해당 엔트리를 클라이언트에서
요청했을 때 발생하는 에러 횟수</td>
</tr>
<tr class="row-even"><td>Num_plan_cache_query_string_hash_entries</td>
<td>질의 문자열 해시 테이블의 현재 엔트리 개수</td>
</tr>
<tr class="row-odd"><td>Num_plan_cache_xasl_id_hash_entries</td>
<td>xasl id 해시 테이블의 현재 엔트리 개수</td>
</tr>
<tr class="row-even"><td>Num_plan_cache_class_oid_hash_entries</td>
<td>class oid 해시 테이블의 현재 엔트리 개수</td>
</tr>
<tr class="row-odd"><td>버퍼 히트율
관련</td>
<td>Data_page_buffer_hit_ratio</td>
<td>페이지 버퍼의 히트율
(Num_data_page_fetches - Num_data_page_ioreads)*100 / Num_data_page_fetches</td>
</tr>
<tr class="row-even"><td>HA 관련</td>
<td>Time_ha_replication_delay</td>
<td>복제 지연 시간(초)</td>
</tr>
</tbody>
</table>

**-o, --output-file**=FILE

대상 데이터베이스 서버의 실행 통계 정보를 지정된 파일에 저장한다.

    rye statdump -o statdump.log testdb

**-c, --cumulative**

**-c** 옵션을 이용하여 대상 데이터베이스 서버의 누적된 실행 통계 정보를 출력할 수 있다. **-i** 옵션과 결합하면, 지정된 시간 간격(interval)마다 실행 통계 정보를 확인할 수 있다.

    rye statdump -i 5 -c testdb

**-s, --substr**=STRING

**-s** 옵션 뒤에 문자열을 지정하면, 항목 이름 내에 해당 문자열을 포함하는 통계 정보만 출력할 수 있다.

다음 예는 항목 이름 내에 "data"를 포함하는 통계 정보만 출력한다.

    rye statdump -s data testdb

    *** SERVER EXECUTION STATISTICS ***
    Num_data_page_fetches         =        135
    Num_data_page_dirties         =          0
    Num_data_page_ioreads         =          0
    Num_data_page_iowrites        =          0
    Num_data_page_victims         =          0
    Num_data_page_iowrites_for_replacement =          0
     
     *** OTHER STATISTICS ***
    Data_page_buffer_hit_ratio    =     100.00

각 상태 정보는 64비트 **INTEGER**로 구성되어 있으며, 누적된 값이 한도를 넘으면 해당 실행 통계 정보가 유실될 수 있다.

잠금 확인, 트랜잭션 확인, 트랜잭션 제거
=======================================

잠금(Lock) 상태 확인
--------------------

**rye lockdb**는 대상 데이터베이스에 대하여 현재 트랜잭션에서 사용되고 있는 잠금 정보를 확인하는 유틸리티이다. :

    rye lockdb [<option>] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **lockdb**: 대상 데이터베이스에 대하여 현재 트랜잭션에서 사용되고 있는 잠금 정보를 확인하는 명령이다.
-   *database\_name*: 현재 트랜잭션의 잠금 정보를 확인하는 데이터베이스 이름이다.

다음 예는 옵션 없이 testdb 데이터베이스의 잠금 정보를 화면에 출력한다. :

    rye lockdb testdb

다음은 **rye lockdb**에 대한 \[option\]이다.

**-o, --output-file**=FILE

데이터베이스의 잠금 정보를 output.txt로 출력한다.

    rye lockdb -o output.txt testdb

### 출력 내용

**rye lockdb**의 출력 내용은 논리적으로 3개의 섹션으로 나뉘어져 있다.

-   서버에 대한 잠금 설정
-   현재 데이터베이스에 접속한 클라이언트들
-   객체 잠금 테이블의 내용

**서버에 대한 잠금 설정**

**rye lockdb** 출력 내용의 첫 번째 섹션은 데이터베이스 서버에 대한 잠금 설정이다.

    *** Lock Table Dump ***
     Run Deadlock interval = 0

위에서 교착 상태 탐지 간격은 0초로 설정되어 있다.

관련 시스템 파라미터인 **deadlock\_detection\_interval**에 대한 설명은 lock-parameters 를 참고한다.

**현재 데이터베이스에 접속한 클라이언트들**

**rye lockdb** 출력 내용의 두 번째 섹션은 데이터베이스에 연결된 모든 클라이언트의 정보를 포함한다. 이 정보에는 각각의 클라이언트에 대한 트랜잭션 인덱스, 프로그램 이름, 사용자 ID, 호스트 이름, 프로세스 ID, 격리 수준, 그리고 잠금 타임아웃 설정이 포함된다.

    Transaction (index 1, rsql, dba@ryedb|12854)
    Isolation READ COMMITTED CLASSES AND READ UNCOMMITTED INSTANCES
    State TRAN_ACTIVE
    Timeout_period : Infinite wait

위에서 트랜잭션 인덱스는 1이고, 프로그램 이름은 rsql, 사용자 이름은 dba, 호스트 이름은 ryedb, 클라이언트 프로세스 식별자는 12854, 격리 수준은 READ COMMITTED CLASSES AND READ UNCOMMITTED INSTANCES, 그리고 잠금 타임아웃은 무제한이다.

트랜잭션 인덱스가 0인 클라이언트는 내부적인 시스템 트랜잭션이다. 이것은 데이터베이스의 체크포인트 수행과 같이 특정한 시간에 잠금을 획득할 수 있지만 대부분의 경우 이 트랜잭션은 어떤 잠금도 획득하지 않을 것이다.

**rye lockdb** 유틸리티는 잠금 정보를 가져오기 위해 데이터베이스에 접속하기 때문에 **rye lockdb** 자체가 하나의 클라이언트이고 따라서 클라이언트의 하나로 출력된다.

**객체 잠금 테이블**

**rye lockdb** 출력 내용의 세 번째 섹션은 객체 잠금 테이블의 내용을 포함한다. 이것은 어떤 객체에 대해서 어떤 클라이언트가 어떤 모드로 잠금을 가지고 있는지, 어떤 객체에 대해서 어떤 클라이언트가 어떤 모드로 기다리고 있는지를 보여준다. 객체 잠금 테이블 결과물의 첫 부분에는 얼마나 많은 객체가 잠금되었는지가 출력된다.

    Object lock Table:
        Current number of ojbects which are locked = 3

**rye lockdb**는 잠금을 획득한 각각의 객체에 대한 객체의 OID와 Object type, 테이블 이름을 출력한다. 추가적으로 객체에 대해서 잠금을 보유하고 있는 트랜잭션의 개수(Num holders), 잠금을 보유하고 있지만 상위 잠금으로 변환(예를 들어 S\_LOCK에서 X\_LOCK으로 잠금 변환)하지 못해 차단된 트랜잭션의 개수(Num blocked-holders), 객체의 잠금을 기다리는 다른 트랜잭션의 개수(Num waiters)가 출력된다. 그리고 잠금을 보유하고 있는 클라이언트 트랜잭션, 차단된 클라이언트 트랜잭션, 기다리는 클라이언트 트랜잭션의 리스트가 출력된다.

Granted\_mode는 현재 획득한 잠금의 모드를 의미하고 Blocked\_mode는 차단된 잠금의 모드를 의미한다. Starting\_waiting\_at은 잠금을 요청한 시간을 의미하고 Wait\_for\_secs는 잠금을 기다리는 시간을 의미한다. Wait\_for\_secs의 값은 lock\_timeout 시스템 파라미터에 의해 설정된다.

트랜잭션 확인
-------------

**rye tranlist**는 대상 데이터베이스의 트랜잭션 정보를 확인하는 유틸리티로서, DBA 또는 DBA그룹 사용자만 수행할 수 있다. :

    rye tranlist [options] database_name

옵션을 생략하면 각 트랜잭션에 대한 전체 정보를 출력한다.

"rye tranlist demodb"는 "rye killtran -q demodb"와 비슷한 결과를 출력하나, 후자에 비해 "User name"과 "Host name"을 더 출력한다. "rye tranlist -s demodb"는 "rye killtran -d demodb"와 동일한 결과를 출력한다.

다음은 tranlist 출력 결과의 예이다.

    $ rye tranlist demodb

    Tran index          User name      Host name      Process id    Program name              Query time    Tran time       Wait for lock holder      SQL_ID       SQL Text
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
       1(ACTIVE)           public     test-server           1681    broker1_rye_cas_1               0.00         0.00                       -1     *** empty ***  
       2(ACTIVE)           public     test-server           1682    broker1_rye_cas_2               0.00         0.00                       -1     *** empty ***  
       3(ACTIVE)           public     test-server           1683    broker1_rye_cas_3               0.00         0.00                       -1     *** empty ***  
       4(ACTIVE)           public     test-server           1684    broker1_rye_cas_4               1.80         1.80                  3, 2, 1     e5899a1b76253   update ta set a = 5 where a > 0
       5(ACTIVE)           public     test-server           1685    broker1_rye_cas_5               0.00         0.00                       -1     *** empty ***  
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SQL_ID: e5899a1b76253
    Tran index : 4
    update ta set a = 5 where a > 0

위의 예는 3개의 트랜잭션이 각각 INSERT문을 실행 중일 때 또 다른 트랜잭션에서 UPDATE문의 실행을 시도한다. 위에서 Tran index가 4인 update문은 3,2,1(Wait for lock holder)번의 트랜잭션이 종료되기를 대기하고 있다.

화면에 출력되는 질의문(SQL Text)은 질의 계획 캐시에 저장되어 있는 것을 보여준다. 질의 수행이 완료되면 **empty**로 표시된다.

각 칼럼의 의미는 다음과 같다.

> -   Tran index: 트랜잭션 인덱스
> -   User name: 데이터베이스 사용자 이름
> -   Host name: 해당 트랜잭션이 수행되는 CAS의 호스트 이름
> -   Process id: 클라이언트 프로세스 ID
> -   Program name: 클라이언트 프로그램 이름
> -   Query time: 수행중인 질의의 총 수행 시간(단위: 초)
> -   Tran time: 현재 트랜잭션의 총 수행 시간(단위: 초)
> -   Wait for lock holder: 현재 트랜잭션이 락 대기중이면 해당 락을 소유하고 있는 트랜잭션의 리스트
> -   SQL\_ID: SQL Text에 대한 ID. rye killtran 명령의 --kill-sql-id 옵션에서 사용될 수 있다.
> -   SQL Text: 수행중인 질의문(최대 30자)

"Tran index"에 보여지는 transaction 상태 메시지는 다음과 같다.

> -   ACTIVE : 활성
> -   RECOVERY : 복구중인 트랜잭션
> -   COMMITTED : 커밋완료되어 종료될 트랜잭션
> -   COMMITTING : 커밋중인 트랜잭션
> -   ABORTED : 롤백되어 종료될 트랜잭션
> -   KILLED : 서버에 의해 강제 종료 중인 트랜잭션

다음은 **rye tranlist**에 대한 \[options\]이다.

**-u, --user**=USER

로그인할 사용자 ID. DBA및 DBA그룹 사용자만 허용한다.(기본값 : DBA)
    
**-p, --password**=PASSWORD

사용자 비밀번호
    
**-s, --summary**

요약 정보만 출력한다(질의 수행 정보 또는 잠금 관련 정보를 생략).
       
    $ rye tranlist -s demodb
        
    Tran index          User name      Host name      Process id      Program name
    -------------------------------------------------------------------------------
       1(ACTIVE)           public     test-server           1681 broker1_rye_cas_1
       2(ACTIVE)           public     test-server           1682 broker1_rye_cas_2
       3(ACTIVE)           public     test-server           1683 broker1_rye_cas_3
       4(ACTIVE)           public     test-server           1684 broker1_rye_cas_4
       5(ACTIVE)           public     test-server           1685 broker1_rye_cas_5
    -------------------------------------------------------------------------------

**--sort-key**=NUMBER
 
해당 NUMBER 위치의 칼럼에 대해 오름차순으로 정렬된 값을 출력한다. 칼럼의 타입이 숫자인 경우는 숫자로 정렬되고, 그렇지 않은 경우 문자열로 정렬된다. 생략되면 "Tran index"에 대한 정렬값을 보여준다.

다음은 네번째 칼럼인 "Process id"를 지정하여 정렬한 정보를 출력하는 예이다.
     
    $ cubrid tranlist --sort-key=4 demodb
 
    Tran index          User name      Host name      Process id    Program name              Query time    Tran time       Wait for lock holder      SQL_ID       SQL Text
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
       1(ACTIVE)           public     test-server           1681    broker1_rye_cas_1               0.00         0.00                       -1     *** empty ***
       2(ACTIVE)           public     test-server           1682    broker1_rye_cas_2               0.00         0.00                       -1     *** empty ***
       3(ACTIVE)           public     test-server           1683    broker1_rye_cas_3               0.00         0.00                       -1     *** empty ***
       4(ACTIVE)           public     test-server           1684    broker1_rye_cas_4               1.80         1.80                  3, 1, 2     e5899a1b76253   update ta set a = 5 where a > 0
       5(ACTIVE)           public     test-server           1685    broker1_rye_cas_5               0.00         0.00                       -1     *** empty ***
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SQL_ID: e5899a1b76253
    Tran index : 4
    update ta set a = 5 where a > 0
        
**--reverse**
 
역순으로 정렬된 값을 출력한다.
 
다음은 "Tran index"의 역순으로 정렬한 정보를 출력하는 예이다.
     
    Tran index          User name      Host name      Process id    Program name              Query time    Tran time     Wait for lock holder      SQL_ID       SQL Text
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
       5(ACTIVE)           public     test-server           1685    broker1_rye_cas_5               0.00         0.00                     -1     *** empty ***
       4(ACTIVE)           public     test-server           1684    broker1_rye_cas_4               1.80         1.80                3, 2, 1     e5899a1b76253   update ta set a = 5 where a > 0
       3(ACTIVE)           public     test-server           1683    broker1_rye_cas_3               0.00         0.00                     -1     *** empty ***
       2(ACTIVE)           public     test-server           1682    broker1_rye_cas_2               0.00         0.00                     -1     *** empty ***
       1(ACTIVE)           public     test-server           1681    broker1_rye_cas_1               0.00         0.00                     -1     *** empty ***
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SQL_ID: e5899a1b76253
    Tran index : 4
    update ta set a = 5 where a > 0


트랜잭션 제거
-------------

**rye killtran**은 대상 데이터베이스의 트랜잭션을 확인하거나 특정 트랜잭션을 강제 종료하는 유틸리티로서, **DBA** 사용자만 수행할 수 있다. :

    rye killtran [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **killtran**: 지정된 데이터베이스에 대해 트랜잭션을 관리하는 명령어이다.
-   *database\_name*: 대상 데이터베이스의 이름이다.

\[options\]에 따라 특정 트랜잭션을 지정하여 제거하거나, 현재 활성화된 트랜잭션을 화면 출력할 수 있다. 옵션이 지정되지 않으면, **-d** 옵션이 기본으로 적용되어 모든 트랜잭션을 화면 출력하며, rye tranlist 명령에 **-s** 옵션을 준 것과 동일하다.

    $ rye killtran demodb 

    Tran index      User name   Host name      Process id      Program name
    -------------------------------------------------------------------------------
       1(ACTIVE)          dba      myhost             664           rye_cas
       2(ACTIVE)          dba      myhost            6700              rsql
       3(ACTIVE)          dba      myhost            2188           rye_cas
       4(ACTIVE)          dba      myhost             696              rsql
       5(ACTIVE)       public      myhost            6944              rsql
    -------------------------------------------------------------------------------

다음은 **rye killtran**에 대한 \[options\]이다.

**-i, --kill-transaction-index**=ID1,ID2,ID3

지정한 인덱스에 해당하는 트랜잭션을 제거한다.  쉼표(,)로 구분하여 제거하고자 하는 트랜잭션 ID 여러 개를 지정할 수 있다. 제거할 트랜잭션 리스트에 유효하지 않은 트랜잭션 ID가 지정되면 무시된다.

    $ rye killtran -i 1,2 demodb
    Ready to kill the following transactions:

    Tran index          User name      Host name      Process id      Program name
    -------------------------------------------------------------------------------
       1(ACTIVE)              DBA         myhost           15771              rsql
       2(ACTIVE)              DBA         myhost            2171              rsql
    -------------------------------------------------------------------------------
    Do you wish to proceed ? (Y/N)y
    Killing transaction associated with transaction index 1
    Killing transaction associated with transaction index 2

**--kill-user-name**=ID

지정한 OS 사용자 ID에 해당하는 트랜잭션을 제거한다.

    rye killtran --kill-user-name=os_user_id demodb

**--kill-host-name**=HOST

지정한 클라이언트 호스트의 트랜잭션을 제거한다. 

    rye killtran --kill-host-name=myhost demodb

**--kill-program-name**=NAME

지정한 이름의 프로그램에 해당하는 트랜잭션을 제거한다. 

    rye killtran --kill-program-name=rye_cas demodb

**--kill-sql-id**=SQL_ID
        
지정한 SQL ID에 해당하는 트랜잭션을 제거한다. 

    rye killtran --kill-sql-id=5377225ebc75a demodb

**-p, --dba-password**=PASSWORD

이 옵션 뒤에 오는 값은 **DBA**\ 의 암호이며 생략하면 프롬프트에서 입력해야 한다.

**-q, --query-exec-info**

rye tranlist 명령에서 "User name" 칼럼과 "Host name" 칼럼이 출력되지 않는다는 점만 다르다. tranlist를 참고한다.

**-d, --display**

기본 지정되는 옵션으로 트랜잭션의 요약 정보를 출력한다. rye tranlist 명령의 -s 옵션을 지정하여 실행한 것과 동일한 결과를 출력한다. tranlist -s 를 참고한다.

**-f, --force**

중지할 트랜잭션을 확인하는 프롬프트를 생략한다.

    rye killtran -f -i 1 demodb
        

데이터베이스 진단/파라미터 출력
===============================

데이터베이스 내부 정보 출력
---------------------------

**rye diagdb** 유틸리티를 이용해 다양한 데이터베이스 내부 정보를 확인할 수 있다. **rye diagdb** 유틸리티가 제공하는 정보들은 현재 데이터베이스의 상태를 진단하거나 문제를 파악하는데 도움이 된다. :

    rye diagdb [option] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **diagdb**: Rye에 저장되는 바이너리 형태의 파일 정보를 텍스트 형태로 출력하여 현재의 데이터베이스 저장 상태를 확인하고자 할 때 사용하는 명령어이다. 데이터베이스가 구동 정지 상태인 경우에만 정상적으로 수행된다. 전체를 확인하거나 옵션을 사용하여 파일 테이블, 파일 용량, 힙 용량, 클래스 이름, 디스크 비트맵을 선택해 확인할 수 있다.
-   *database\_name*: 내부 정보를 확인하려는 데이터베이스 이름이다.

다음은 **rye diagdb**에서 사용하는 \[option\]이다.

**-d, --dump-type**=TYPE

데이터베이스의 전체 파일에 대한 기록 상태를 출력할 때 출력 범위를 지정한다. 생략하면 기본값인 -1이 지정된다.

    rye diagdb -d 1 demodb

**-d** 옵션에 적용되는 타입은 모두 9가지로, 그 종류는 다음과 같다.

<table border="1" class="docutils">
<colgroup>
<col width="18%" />
<col width="82%" />
</colgroup>
<thead valign="bottom">
<tr class="row-odd"><th class="head">타입</th>
<th class="head">설명</th>
</tr>
</thead>
<tbody valign="top">
<tr class="row-even"><td>-1</td>
<td>전체 데이터베이스 정보를 출력한다.</td>
</tr>
<tr class="row-odd"><td>1</td>
<td>파일 테이블 정보를 출력한다.</td>
</tr>
<tr class="row-even"><td>2</td>
<td>파일 용량 정보를 출력한다.</td>
</tr>
<tr class="row-odd"><td>3</td>
<td>힙 용량 정보를 출력한다.</td>
</tr>
<tr class="row-even"><td>4</td>
<td>인덱스 용량 정보를 출력한다.</td>
</tr>
<tr class="row-odd"><td>5</td>
<td>클래스 이름 정보를 출력한다.</td>
</tr>
<tr class="row-even"><td>6</td>
<td>디스크 비트맵 정보를 출력한다.</td>
</tr>
<tr class="row-odd"><td>7</td>
<td>카탈로그 정보를 출력한다.</td>
</tr>
<tr class="row-even"><td>8</td>
<td>로그 정보를 출력한다.</td>
</tr>
<tr class="row-odd"><td>9</td>
<td>힙(heap) 정보를 출력한다.</td>
</tr>
</tbody>
</table>


서버/클라이언트에서 사용하는 파라미터 출력
------------------------------------------

**rye paramdump** 유틸리티는 서버/클라이언트 프로세스에서 사용하는 파라미터 정보를 출력한다. :

    rye paramdump [options] database_name

-   **rye**: Rye 서비스 및 데이터베이스 관리를 위한 통합 유틸리티이다.
-   **paramdump**: 서버/클라이언트 프로세스에서 사용하는 파라미터 정보를 출력하는 명령이다.
-   *database\_name*: 파라미터 정보를 출력할 데이터베이스 이름이다.

다음은 **rye paramdump**에서 사용하는 \[options\]이다.

**-o, --output-file**=FILE

데이터베이스의 서버/클라이언트 프로세스에서 사용하는 파라미터 정보를 지정된 파일에 저장하는 옵션이며, 파일은 현재 디렉터리에 생성된다. **-o** 옵션이 지정되지 않으면 메시지는 콘솔 화면에 출력한다.

        rye paramdump -o db_output demodb

**-b, --both**

데이터베이스의 서버/클라이언트 프로세스에서 사용하는 파라미터 정보를 콘솔 화면에 출력하는 옵션이며, **-b** 옵션을 사용하지 않으면 서버 프로세스의 파라미터 정보만 출력한다. 

    rye paramdump -b demodb

**-S, --SA-mode**

독립 모드에서 서버 프로세스의 파라미터 정보를 출력한다. 

    rye paramdump -S demodb

**-C, --CS-mode**

클라이언트-서버 모드에서 서버 프로세스의 파라미터 정보를 출력한다.

    rye paramdump -C demodb
        

HA 모드 변경, 로그 복제/반영
============================

**rye changemode** 유틸리티는 서버의 HA 모드 출력 또는 변경하는 유틸리티이다.

**rye applyinfo** 유틸리티는 HA 환경에서 트랜잭션 로그 반영 정보를 확인하는 유틸리티이다.

자세한 사용법은 rye-service-util 을 참고한다.
