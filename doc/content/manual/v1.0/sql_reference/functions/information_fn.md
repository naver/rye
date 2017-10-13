정보 함수
=========

CURRENT_USER, USER
-------------------
**CURRENT_USER**
**USER**

**CURRENT_USER**와 **USER** 의사 칼럼(pseudo column)은 동일하며, 현재 데이터베이스에 로그인한 사용자의 이름을 문자열로 반환한다.

기능이 비슷한 **SYSTEM_USER()** 함수와 **USER()** 함수는 사용자 이름을 호스트 이름과 함께 반환한다.

``` sql
--selecting the current user on the session
SELECT USER;
```
```
  CURRENT_USER   
=================
  'DBA'          
```

``` sql
SELECT USER(), CURRENT_USER;
```
```
  user()              CURRENT_USER   
=====================================
  'DBA@test_server'   'DBA'          
```

``` sql
--selecting all users of the current database from the system table
SELECT name, id, password FROM db_user;
```
```
  name                 id     password
========================================
  'DBA'                NULL   NULL
  'PUBLIC'             NULL   NULL
```

DATABASE, SCHEMA
----------------

**DATABASE()**
**SCHEMA()**

**DATABASE** 함수와 **SCHEMA** 함수는 동일하며, 현재 연결된 데이터베이스 이름을 **VARCHAR** 타입의 문자열로 반환한다.

``` sql
SELECT DATABASE(), SCHEMA();
```
```
  database()   schema()    
===========================
  'testdb'    'testdb'   
```

DEFAULT
-------

**DEFAULT(column_name)**
**DEFAULT**

**DEFAULT**와 **DEFAULT** 함수는 칼럼에 정의된 기본값을 반환한다. 해당 칼럼에 기본값이 지정되지 않으면 NULL 또는 에러를 출력한다. **DEFAULT**는 인자가 없는 반면, **DEFAULT** 함수는 칼럼 이름을 입력 인자로 하는 차이가 있다. **DEFAULT**는 **INSERT** 문의 입력 데이터, **UPDATE** 문의 **SET** 절에서 사용될 수 있고, **DEFAULT** 함수는 모든 곳에서 사용될 수 있다.

기본값이 정의되지 않은 칼럼에 어떠한 제약 조건이 정의되어 있지 않거나 UNIQUE 제약 조건이 정의된 경우에는 NULL을 반환하고, 해당 칼럼에 NOT NULL 또는 PRIMARY KEY 제약 조건이 정의된 경우에는 에러를 반환한다.

``` sql
CREATE GLOBAL TABLE info_tbl(pk INT PRIMARY KEY, id INT DEFAULT 0, name VARCHAR);
INSERT INTO info_tbl VALUES (1, 1,'a'),(2, 2,'b'),(3, NULL,'c');

SELECT id, DEFAULT(id) FROM info_tbl;
```
```
  id     default(id)   
=======================
  1      0             
  2      0             
  NULL   0             
```

``` sql
UPDATE info_tbl SET id = DEFAULT WHERE id IS NULL;
DELETE FROM info_tbl WHERE id = DEFAULT(id);
INSERT INTO info_tbl VALUES (4, DEFAULT,'d');
```

INDEX_CARDINALITY
------------------

**INDEX_CARDINALITY(table, index, key_pos)**

**INDEX_CARDINALITY** 함수는 테이블에서 인덱스 카디널리티(cardinality)를 반환한다. 인덱스 카디널리티는 인덱스를 정의하는 고유한 값의 개수이다. 인덱스 카디널리티는 다중 칼럼 인덱스의 부분 키에 대해서도 적용할 수 있는데, 이때 세 번째 인자로 칼럼의 위치를 지정하여 부분 키에 대한 고유 값의 개수를 나타낸다. 이 값은 근사치임에 유의한다.

갱신된 결과를 얻으려면 **UPDATE STATISTICS** 문을 먼저 수행해야 한다.

리턴 값은 0 또는 양의 정수이며, 입력 인자 중 하나라도 **NULL** 이면 **NULL** 을 반환한다. 입력 인자인 테이블이나 인덱스가 발견되지 않거나 *key_pos* 가 지정된 범위를 벗어나면 **NULL** 을 리턴한다.

``` sql
CREATE GLOBAL TABLE t1( id VARCHAR PRIMARY KEY, 
i1 INTEGER ,
i2 INTEGER not null,
i3 INTEGER unique,
s1 VARCHAR(10),
s2 VARCHAR(10),
s3 VARCHAR(10) UNIQUE);

CREATE INDEX i_t1_i1 ON t1(i1 DESC);
CREATE INDEX i_t1_s1 ON t1(s1);
CREATE INDEX i_t1_i1_s1 on t1(i1,s1);

INSERT INTO t1 VALUES ('user1', 1,1,1,'abc','abc','abc');
INSERT INTO t1 VALUES ('user2', 2,2,2,'zabc','zabc','zabc');
INSERT INTO t1 VALUES ('user3', 2,3,3,'+abc','+abc','+abc');

UPDATE STATISTICS ON t1;
```

``` sql
SELECT INDEX_CARDINALITY('t1','i_t1_i1_s1',0);
```
```
  index_cardinality('t1', 'i_t1_i1_s1', 0)   
=============================================
  2                                          
```

``` sql
SELECT INDEX_CARDINALITY('t1','i_t1_i1_s1',1);
```
```
  index_cardinality('t1', 'i_t1_i1_s1', 1)   
=============================================
  3                                          
```

``` sql
SELECT INDEX_CARDINALITY('t1','i_t1_i1_s1',2);
```
```
  index_cardinality('t1', 'i_t1_i1_s1', 2)   
=============================================
  NULL                                       
```

``` sql
SELECT INDEX_CARDINALITY('t123','i_t1_i1_s1',1);
```
```
  index_cardinality('t123', 'i_t1_i1_s1', 1)   
===============================================
  NULL                                         
```

INET_ATON
----------

**INET_ATON(ip_string)**

**INET_ATON** 함수는 IPv4 주소의 문자열을 입력받아 이에 해당하는 숫자를 반환한다. 'a.b.c.d' 형식의 IP 주소 문자열을 입력하면 "a * 256 ^ 3 + b * 256 ^ 2 + c * 256 + d"가 반환된다. 반환 타입은 **BIGINT** 이다.

다음 예제에서 192.168.0.10은 "192 * 256 ^ 3 + 168 * 256 ^ 2 + 0 * 256 + 10"으로 계산된다.

``` sql
SELECT INET_ATON('192.168.0.10');
```
```
  inet_aton('192.168.0.10')   
==============================
  3232235530                  
```

INET_NTOA
----------

**INET_NTOA(expr)**

**INET_NTOA** 함수는 숫자를 입력받아 IPv4 주소 형식의 문자열을 반환한다. 반환 타입은 **VARCHAR** 이다.

``` sql
SELECT INET_NTOA(3232235530);
```
```
  inet_ntoa(3232235530)   
==========================
  '192.168.0.10'          
```

USER, SYSTEM_USER
------------------

**USER()**
**SYSTEM_USER()**

**USER** 함수와 **SYSTEM_USER** 함수는 동일하며, 사용자 이름을 호스트 이름과 함께 반환한다.

기능이 비슷한 **USER**와 **CURRENT_USER** 의사 칼럼(pseudo column)은 현재 데이터베이스에 로그인한 사용자의 이름을 문자열로 반환한다.

``` sql
--selecting the current user on the session
SELECT SYSTEM_USER ();
```
```
  user()              
======================
  'DBA@test_server'   
```

``` sql
SELECT USER(), CURRENT_USER;
```
```
  user()              CURRENT_USER   
=====================================
  'DBA@test_server'   'DBA'          
```

``` sql
--selecting all users of the current database from the system table
SELECT name, id, password FROM db_user;
```
```
  name                 id     password
=========================================
  'DBA'                NULL   NULL
  'PUBLIC'             NULL   NULL
```

VERSION
-------

**VERSION()**

버전 문자열을 반환한다.

``` sql
SELECT VERSION();
```
```
  version()      
=================
  '1.0.0.0508'   
```
