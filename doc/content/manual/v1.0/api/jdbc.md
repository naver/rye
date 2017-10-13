JDBC 드라이버
=============

JDBC 개요
---------

Rye JDBC 드라이버(rye_jdbc.jar)를 사용하면 Java로 작성된 응용 프로그램에서 Rye 데이터베이스에 접속할 수 있다. 
Rye JDBC 드라이버는 &lt;*Rye 설치 디렉터리*&gt; **/jdbc** 디렉터리에 위치한다. Rye JDBC 드라이버는 JDBC 2.0 스펙을 기준으로 개발되었으며, JDK 1.6에서 컴파일한 것을 기본으로 제공한다.


**Rye JDBC 드라이버 버전 확인**

JDBC 드라이버 버전은 다음과 같은 방법으로 확인할 수 있다. :

``` bash
    % jar -tf rye_jdbc.jar
    META-INF/
    META-INF/MANIFEST.MF
    rye/
    rye/jdbc/
    rye/jdbc/driver/
    rye/jdbc/jci/
    ...
    Rye-JDBC-1.0.0.0508
```

**Rye JDBC 드라이버 등록**

JDBC 드라이버 등록은 **Class.forName()** 메서드를 사용하며, 아래는 Rye JDBC 드라이버를 등록하기 위해 rye.jdbc.driver.RyeDriver 클래스를 로드하는 예제이다.

``` java
import java.sql.*;
import rye.jdbc.driver.*;

public class LoadDriver {
   public static void main(String[] Args) {
       try {
           Class.forName("rye.jdbc.driver.RyeDriver");
       } catch (Exception e) {
           System.err.println("Unable to load driver.");
           e.printStackTrace();
       }
       ...
```

JDBC 설치 및 설정
-----------------

**기본 환경**

-   JDK 1.6 이상

**JDBC 드라이버 설정**

JDBC를 사용하려면 Rye JDBC 드라이버가 존재하는 경로를 환경 변수 **CLASSPATH** 에 추가해야 한다.

Rye JDBC 드라이버(**rye_jdbc.jar**)는 Rye 설치 디렉터리 아래의 jdbc 디렉터리에 위치한다.

JDBC 프로그래밍
---------------

### 연결 설정

**DriverManager** 는 JDBC 드라이버를 관리하기 위한 기본적인 인터페이스이며, JDBC 드라이버를 선택하고 새로운 데이터베이스 연결을 생성하는 기능을 한다. Rye JDBC 드라이버가 등록되어 있다면 **DriverManager.getConnection()** 메서드를 호출하여 데이터베이스에 접속한다.

연결 설정을 위한 *db-url* 인자의 구성은 다음과 같다.

```
    jdbc:rye://host:port[,...]/db-name[:user-id[:password]]/broker_name?<property>[&...]

    <property> ::=
                 | rcTime=<second>
                 | loadBalance=<bool_type>
                 | connectTimeout=<second>
                 | queryTimeout=<second>
                 | charSet=<character_set>
                 | zeroDateTimeBehavior=<behavior_type>
                 | logFile=<file_name>
                 | logOnException=<bool_type>
                 | logSlowQueries=<bool_type>
                 | slowQueryThresholdMillis=<millisecond>
                 | useLazyConnection=<bool_type>

        <alternative_hosts> ::=
        <standby_broker1_host>:<port> [,<standby_broker2_host>:<port>]
        <behavior_type> ::= exception | round | convertToNull
        <bool_type> ::= true | false
```

-   *host*: Rye 브로커가 동작하고 있는 서버의 IP 주소 또는 호스트 이름
-   *port*: Rye 브로커의 포트 번호
	* 브로커가 이중화 된 경우 접속할 host:port 정보를 콤마(,)로 구분하여 여러개 지정할 수 있다.
-   *db-name*: 접속할 데이터베이스 이름
-   *user-id*: 데이터베이스에 접속할 사용자 ID이다. 기본적으로 데이터베이스에는 **dba** 와 **public** 두 개의 사용자가 존재한다. 지정하지 않거나 빈 문자열("")을 입력하면 **public** 사용자로 데이터베이스에 접속한다.
-   *password*: 데이터베이스에 접속할 사용자의 암호이다. 해당 사용자에 암호가 설정되어 있지 않으면, 빈 문자열("")을 입력하거나, *password*를 지정하지 않는다.
-   *broker_name*: 접속할 브로커 이름. 데이터에 접속하는 모드를 구분하기 위해 기본적으로 **rw**, **ro**, **so** 브로커가 구동되어 있다.
    * **rw** : 마스터 데이터베이스에 접속하기 위한 모드이다.
    * **ro** : 슬레이브 데이터베이스에 접속하기 위한 모드이다. 슬레이브 데이터베이스에 접속할 수 없는 상황이면 마스터 데이터베이스에 접속한다. READ-ONLY 쿼리만 허용된다.
    * **so** : 슬레이브 데이터베이스에 접속하기 위한 모드이다. 슬레이브 데이터베이스에 접속 할 수 없는 상황이면 커넥션 오류가 발생한다. READ-ONLY 쿼리만 허용된다.

-   &lt;*property*&gt;
    -   **rcTime**: 브로커 커넥션 절체가 발생한 경우 **rcTime** 시간 후에 최초 접속했던 브로커가 복구 되었는지 판단해서 정상인 경우 최초 브로커로 재연결한다. (기본값: 60(초)).
    -   **loadBalance**: *host:port* 설정이 여러 개 일때 **loadBalance** 값이 *true* 면 커넥션 생성시 *host:port* 설정 중 랜덤한 값이 사용된다. *false* 면 처음 설정된 *host:port* 값이 사용된다. (기본값: false).
    -   **connectTimeout**: 데이터베이스 접속에 대한 타임아웃 시간을 초 단위로 설정한다. 기본값은 30초이다. 이 값이 0인 경우 무한 대기를 의미한다. 이 값은 최초 접속 이후 내부적인 재접속이 발생하는 경우에도 적용된다. **DriverManger.setLoginTimeout** () 메서드로 LoginTimeout이 설정된 경우 **DriverManager**를 통해 설정한 값이 우선한다.
    -   **queryTimeout**: 질의 수행에 대한 타임아웃 시간을 초 단위로 설정한다(기본값: 0, 무제한). 최대값은 2,000,000이다. 이 값은 **Statement.setQueryTimeout** () 메서드에 의해 변경될 수 있다. executeBatch() 메서드를 수행하는 경우 한 개의 질의에 대한 타임아웃이 아닌 한 번의 메서드 호출에 대한 타임아웃이 적용된다.
    -   **charSet**: 접속하고자 하는 DB의 문자셋(charSet)이다. (기본값:utf-8)
    -   **zeroDateTimeBehavior**: JDBC에서는 java.sql.Date 형 객체에 날짜와 시간 값이 모두 0인 값을 허용하지 않으므로 이 값을 출력해야 할 때 어떻게 처리할 것인지를 정하는 속성으로 기본 동작은 **exception** 이다. 설정값에 따른 동작은 다음과 같다.
        -   **exception**: 기본 동작. SQLException 예외로 처리한다.
        -   **round**: 반환할 타입의 최소값으로 변환한다.
        -   **convertToNull**: **NULL** 로 변환한다.
    -   **logFile**: 디버깅용 로그 파일 이름(기본값: rye_jdbc.log). 별도의 경로 설정이 없으면 응용 프로그램을 실행하는 위치에 저장된다.
    -   **logOnException**: 디버깅용 예외 처리 로깅 여부(기본값: false)
    -   **logSlowQueries**: 디버깅용 슬로우 쿼리 로깅 여부(기본값: false)
        -   **slowQueryThresholdMillis**: 디버깅용 슬로우 쿼리 로깅 시 슬로우 쿼리 제한 시간(기본값: 60000). 단위는 밀리 초이다.
    -   **useLazyConnection**: 이 값이 true이면 사용자의 연결 요청 시 브로커 연결 없이 성공을 반환(기본값: false)하고, prepare나 execute 등의 함수를 호출할 때 브로커에 연결한다. 이 값을 true로 설정하면 많은 응용 클라이언트가 동시에 재시작되면서 연결 풀(connection pool)을 생성할 때 접속이 지연되거나 실패하는 현상을 피할 수 있다.

**URL 예제** :

``` java
    jdbc:rye://192.168.0.1:4000/testdb:public:/rw
    jdbc:rye://192.168.0.1:4000/testdb:public/rw
    jdbc:rye://192.168.0.1:4000/testdb/rw

    jdbc:rye://192.168.0.1:4000,192.168.0.2:4000/testdb/rw

    jdbc:rye://192.168.0.1:4000,192.168.0.2:4000/testdb/rw?zeroDateTimeBehavior=convertToNull&queryTimeout=60
```

**예제 2**

``` java
String url = "jdbc:rye://192.168.0.1:4000/testdb/rw";
String userid = "";
String password = "";

try {
   Connection conn =
           DriverManager.getConnection(url,userid,password);
   // Do something with the Connection

   ...

   } catch (SQLException e) {
       System.out.println("SQLException:" + e.getMessage());
       System.out.println("SQLState: " + e.getSQLState());
   }
   ...
```

-   URL 문자열에서 콜론(:)과 물음표(?)는 구분자로 사용되므로, URL 문자열에 암호를 포함하는 경우 암호의 일부에 콜론이나 물음표를 사용할 수 없다. 암호에 콜론이나 물음표를 사용하려면 getConnection 함수에서 사용자 이름(*user-id*)과 암호(*password*)를 별도의 인자로 지정해야 한다.
-   자동 커밋 모드에서 SELECT 문 수행 이후 모든 결과 셋이 fetch되지 않으면 커밋이 되지 않는다. 따라서, 자동 커밋 모드라 하더라도 프로그램 내에서 결과 셋에 대한 fetch 도중 어떠한 오류가 발생한다면 반드시 커밋 또는 롤백을 수행하여 트랜잭션을 종료 처리하도록 한다.

### JDBC 에러 코드와 에러 메시지

SQLException에서 발생하는 JDBC 에러 코드는 다음과 같다.

-   모든 에러 번호는 0보다 작은 음수이다.
-   SQLException 발생 시 에러 번호는 SQLException.getErrorCode(), 에러 메시지는 SQLException.getMessage()를 통해 확인할 수 있다.
-   에러 번호가 -21001부터 -21999 사이이면, Rye JDBC 메서드에서 발생하는 에러이다.
-   에러 번호가 -10000부터 -10999 사이이면, CAS에서 발생하는 에러를 JDBC가 전달받아 반환하는 에러이다. CAS 에러는 cas-error를 참고한다.
-   에러 번호가 0부터 -9999 사이이면, DB 서버에서 발생하는 에러이다. DB 서버 에러는 database-server-error를 참고한다.

|error code|error message|
|---|---|
|-21001|Index's Column is Not Object|
|-21002|Server error|
|-21003|Cannot communicate with the broker|
|-21004|Invalid cursor position|
|-21005|Type conversion error|
|-21006|Missing or invalid position of the bind variable provided|
|-21007|Attempt to execute the query when not all the parameters are binded|
|-21008|Internal Error: NULL value|
|-21009|Column index is out of range|
|-21010|Data is truncated because receive buffer is too small|
|-21011|Internal error: Illegal schema type|
|-21012|File access failed|
|-21013|Cannot connect to a broker|
|-21014|Unknown transaction isolation level|
|-21015|Internal error: The requested information is not available|
|-21016|The argument is invalid|
|-21017|Connection or Statement might be closed|
|-21018|Internal error: Invalid argument|
|-21019|Cannot communicate with the broker or received invalid packet|
|-21020|No More Result|
|-21021|This ResultSet do not include the OID|
|-21022|Command is not insert|
|-21023|Error|
|-21024|Request timed out|
|-21101|Attempt to operate on a closed Connection.|
|-21102|Attempt to access a closed Statement.|
|-21103|Attempt to access a closed PreparedStatement.|
|-21104|Attempt to access a closed ResultSet.|
|-21105|Not supported method|
|-21106|Unknown transaction isolation level.|
|-21107|invalid URL -|
|-21108|The database name should be given.|
|-21109|The query is not applicable to the executeQuery(). Use the executeUpdate() instead.|
|-21110|The query is not applicable to the executeUpdate(). Use the executeQuery() instead.|
|-21111|The length of the stream cannot be negative.|
|-21112|An IOException was caught during reading the inputstream.|
|-21113|Not supported method, because it is deprecated.|
|-21114|The object does not seem to be a number.|
|-21115|Missing or invalid position of the bind variable provided.|
|-21116|The column name is invalid.|
|-21117|Invalid cursor position.|
|-21118|Type conversion error.|
|-21119|Internal error: The number of attributes is different from the expected.|
|-21120|The argument is invalid.|
|-21121|The type of the column should be a collection type.|
|-21122|Attempt to operate on a closed DatabaseMetaData.|
|-21123|Attempt to call a method related to scrollability of non-scrollable ResultSet.|
|-21124|Attempt to call a method related to sensitivity of non-sensitive ResultSet.|
|-21125|Attempt to call a method related to updatability of non-updatable ResultSet.|
|-21126|Attempt to update a column which cannot be updated.|
|-21127|The query is not applicable to the executeInsert().|
|-21128|The argument row can not be zero.|
|-21129|Given InputStream object has no data.|
|-21130|Given Reader object has no data.|
|-21131|Insertion query failed.|
|-21132|Attempt to call a method related to scrollability of TYPE_FORWARD_ONLY Statement.|
|-21133|Authentication failure|
|-21134|Attempt to operate on a closed PooledConnection.|
|-21135|Attempt to operate on a closed XAConnection.|
|-21136|Illegal operation in a distributed transaction|
|-21137|Attempt to access a CUBRIDOID associated with a Connection which has been closed.|
|-21138|The table name is invalid.|
|-21139|Lob position to write is invalid.|
|-21140|Lob is not writable.|
|-21141|Request timed out.|

JDBC 예제 프로그램
------------------

다음은 JDBC 드라이버를 통해 Rye에 접속하여 데이터를 조회, 삽입하는 것을 간단하게 구성한 예제이다. 예제를 실행하려면 먼저 접속하고자 하는 데이터베이스와 Rye 브로커가 구동되어 있어야 한다.

**JDBC 드라이버 로드**

Rye에 접속하기 위해서는 **Class.forName()** 메서드를 사용하여 JDBC 드라이버를 로드해야 한다.

``` java
Class.forName("rye.jdbc.driver.RyeDriver");
```

**데이터베이스 연결**

JDBC 드라이버를 로드한 후 **DriverManager.getConnection()** 메서드를 사용하여 데이터베이스와 연결한다. **Connection** 객체를 생성하기 위해서는 데이터베이스의 위치를 기술하기 위한 URL, 데이터베이스의 사용자 이름, 암호 등의 정보가 지정되어야 한다. 

``` java
String url = "jdbc:rye://192.168.0.1:4000/testdb/rw";
String userid = "dba";
String password = "";

Connection conn = DriverManager.getConnection(url,userid,password);
```

**데이터베이스 조작(질의 수행 및 ResultSet 처리)**

접속된 데이터베이스에 질의문을 전달하고 실행시키기 위하여 **Statement** , **PrepardStatement** 객체를 생성한다. **Statement** 객체가 생성되면, **Statement** 객체의 **executeQuery** () 메서드나 **executeUpdate** () 메서드를 사용하여 질의문을 실행한다. **next** () 메서드를 사용하여 **executeQuery** () 메서드의 결과로 반환된 **ResultSet** 의 다음 행을 처리할 수 있다.

다음은 *testdb*에 접속하여 테이블을 생성하고, **INSERT**, **SELECT** 문을 실행시키는 예제이다.

``` java
import java.util.*;
import java.sql.*;

public class Basic {
   public static Connection connect() throws SQLException {
      Class.forName("rye.jdbc.driver.RyeDriver");
      return DriverManager.getConnection("jdbc:rye://192.168.0.1:4000/testdb/rw","dba","");
   }

   public static void printdata(ResultSet rs) throws SQLException {
          ResultSetMetaData rsmd = null;

          rsmd = rs.getMetaData();
          int numberofColumn = rsmd.getColumnCount();

          while (rs.next ()) {
              for(int j=1; j<=numberofColumn; j++ )
                  System.out.printf("%s ", rs.getString(j));
              System.out.println("");
          }
   }

   public static void main(String[] args) throws Exception {
      Connection conn = null;
      Statement stmt = null;
      ResultSet rs = null;
      PreparedStatement preStmt = null;

      try {
           conn = connect();

           stmt = conn.createStatement();
           stmt.executeUpdate("CREATE GLOBAL TABLE xoo ( a INT PRIMARY KEY, b INT, c CHAR(10))");

           preStmt = conn.prepareStatement("INSERT INTO xoo VALUES(?,?,?)");
           preStmt.setInt (1, 1) ;
           preStmt.setInt (2, 10) ;
           preStmt.setString(3, "abc")
           int rst = preStmt.executeUpdate () ;

           rs = stmt.executeQuery("select a,b,c from xoo" );

           printdata(rs);

           rs.close();
           stmt.close();
           PreStmt.close();
           conn.close();
      } catch ( Exception e ) {
           System.err.println("SQLException : " + e.getMessage());
      } finally {
           if ( conn != null ) conn.close();
      }
   }
}
```

JDBC API
--------

JDBC API에 대한 자세한 내용은 Java API Specification 문서(http://docs.oracle.com/javase/7/docs/api)를 참고한다.

다음은 Rye에서 지원하는 JDBC 표준 인터페이스를 및 확장 인터페이스를 정리한 목록이다. JDBC 2.0 스펙에 포함된 메서드 중 일부는 지원하지 않으므로 프로그램 작성 시 주의한다.

**JDBC 인터페이스 지원 여부**

| JDBC 인터페이스 | |
|-----------------------------|----|
| java.sql.Blob               | 미지원 |
| java.sql.CallableStatement  | 미지원 |
| java.sql.Clob               | 미지원 |
| java.sql.Connection         | 지원 |
| java.sql.DatabaseMetaData   | 지원 |
| java.sql.Driver             | 지원 |
| java.sql.PreparedStatement  | 지원 |
| java.sql.ResultSet          | 지원 |
| java.sql.ResultSetMetaData  | 지원 |
| java.sql.Statement          | 지원 |
| java.sql.DriverManager      | 지원 |
| Java.sql.SQLException       | 지원 |
| java.sql.Array              | 미지원 |
| java.sql.ParameterMetaData  | 미지원 |
| java.sql.Ref                | 미지원 |
| java.sql.Savepoint          | 미지원 |
| java.sql.SQLData            | 미지원 |
| java.sql.SQLInput           | 미지원 |
| java.sql.Struct             | 미지원 |

-   Statement.RETURN_GENERATED_KEYS: 미지원
-   ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE: 미지원
-   ResultSet.CONCUR_UPDATABLE: 미지원

쿼리 분산
--------

샤딩 환경에서 샤드키에 의한 쿼리 분산은 JDBC드라이버 안에서 수행된다. JDBC를 통해 쿼리와 바인딩 되는 값이 주어지면 드라이버 내에서 shard-key 컬럼을 구분하고 대응되는 값을 찾아 shard-key값으로 사용한다.

**shard-key 추출**
- WHERE 절에 equal(=)  조건이 사용된 경우 대응되는 값이 shard-key로 사용된다.
  WHERE shard_key_col = ?
- WHERE 절에 **in** 조건이 사용된 경우 대응 되는 n개의 값이 shard-key로 사용된다.
  WHERE shard_key_col in (?, ?, ?)
- subquery를 포함하는 질의인 경우 최상위 레벨의 쿼리문에 대해서만 shard-key를 추출한다.
- shard-key 컬럼에 대한 equal 조건이나 in 조건이 아닌 경우 shard-key 컬럼에 대한 조건이 없는것과 동일하다.
- shard-key 값은 다음과 같이 상수값의 형태로만 지정가능하다.
  - WHERE shard_key_col = ?
  - WHERE shard_key_col = 'abc'

**쿼리 종류별 쿼리 분산**

쿼리 종류별 쿼리 분산은 다음과 같이 이루어진다.

- SELECT
   - FROM 절에 명시된 테이블에 SHARD 테이블을 포함하는 경우
      - shard-key가 한 개 지정된 경우 1개의 노드에서 쿼리를 수행한다.
      - shard-key가 n 개 지정된 경우 n개의 노드에서 쿼리를 수행한다.
      - shard-key가 지정되지 않은 경우 모든 샤드에서 쿼리를 수행한다.

   > Note:
   > n개의 노드에서 쿼리가 수행될 때 쿼리 결과는 각 노드의 ResultSet을 단순하게 합쳐서 하나의 ResultSet으로 반환한다. n 개의 노드에 대해 쿼리문의 UNION ALL 형태라고 보면 된다.  쿼리에 ORDER BY가 포함된 경우 한 노드안의 결과는 ORDER BY에 의해 정렬되어 있으나 전체 노드결과를 합친 최종 결과는 ORDER BY 절에 따라 정렬되지 않는다. COUNT(*) 쿼리와 같이 한개의 노드에서 한개의 레코드만 반환하는 쿼리인 경우 각 노드의 결과가 하나의 ResultSet으로 반환되어 n개의 레코드가 반환된다. ORDER BY, GROUP BY, 집계 함수가 사용되는 경우 주의가 필요하다.


   - FROM 절에 명시된 테이블에 SHARD 테이블이 포함되지 않는 경우. 
      - GLOBAL 테이블 또는 GLOBAL 테이블간의 조인 쿼리로서 모든 노드의 수행 결과는 동일하다. 임의의 한개 노드가 선택되어 쿼리를 수행하여 결과를 반환한다.

- SELECT를 제외한 DML, SELECT FOR UPDATE
   - SHARD 테이블에 대한 질의
      - shard-key 조건에 의해 1개의 shard-key값이 명시되어야 한다. 쿼리에서 shard-key값이 명시되지 않거나, 2개 이상이 지정된 경우 에러를 반환한다. 1개의 shard-key가 지정된 경우 해당 노드에서 쿼리가 수행된다.
      - 여러개의 쿼리가 하나의 트랜잭션으로 처리되는 경우 서로 다른 shard-key에 대한 DML은 한 트랜잭션으로 처리될 수 없다.
      - SHARD 테이블과 GLOBAL 테이블에 대한 DML은 하나의 트랜잭션으로 처리될 수 없다.

  - GLOBAL 테이블에 대한 질의
      - 모든 노드에 대해 질의를 수행한다. 
      - auto-commit false 모드로 모든 노드에 대해 DML을 수행한 후 error가 발생하지 않으면 모든 노드에 대해 커밋을 수행하고, 에러가 있으면 전체 노드에 대해 롤백을 수행한다.
      - Rye는 2 phase commit을 지원하지 않기 때문에 커밋을 하는 중에 일부 노드의 장애가 발생한 경우 노드간 데이터 불일치가 발생 할 수 있다. 이 경우 응용에서 수행된 DML에 대한 보상쿼리를 수행시켜 데이터를 일치시키는것이 권고된다.

- DDL
   - 모든 노드에 대해 질의를 수행한다.
   - DDL은 auto-commit 모드로 수행되기 때문에 특정 노드에서 DDL 이 실패한 경우 노드간 스키마 불일치가 발생할 수 있다.  DDL 구문의 성공 여부에 관계없이 모든 노드에 대해 쿼리를 수행한다.

