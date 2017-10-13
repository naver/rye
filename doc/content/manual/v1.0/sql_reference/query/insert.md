INSERT
======

**INSERT** 문을 사용하여 데이터베이스에 존재하는 테이블에 새로운 레코드를 삽입할 수 있다. Rye는 **INSERT ... VALUES** 문, **INSERT ... SET** 문, **INSERT ... SELECT** 문을 지원한다.

**INSERT ... VALUES** 문과 **INSERT ... SET** 문은 명시적으로 지정된 값을 기반으로 새로운 레코드를 삽입하며, **INSERT ... SELECT** 문은 다른 테이블에서 조회한 결과 레코드를 삽입할 수 있다. 단일 **INSERT** 문을 이용하여 여러 행을 삽입하기 위해서는 **INSERT ... VALUES** 문 또는 **INSERT ... SELECT** 문을 사용한다.
```
    <INSERT ... VALUES statement>
    INSERT [INTO] table_name [(column_name, ...)]
        {VALUES | VALUE}({expr | DEFAULT}, ...)[,({expr | DEFAULT}, ...),...]
        [ON DUPLICATE KEY UPDATE column_name = expr, ... ]
    INSERT [INTO] table_name DEFAULT [ VALUES ]

    <INSERT ... SET statement>
    INSERT [INTO] table_name
        SET column_name = {expr | DEFAULT}[, column_name = {expr | DEFAULT},...]
        [ON DUPLICATE KEY UPDATE column_name = expr, ... ]

    <INSERT ... SELECT statement>
    INSERT [INTO] table_name [(column_name, ...)]
        SELECT...
        [ON DUPLICATE KEY UPDATE column_name = expr, ... ]
```
-   *table\_name*: 새로운 레코드를 삽입할 대상 테이블 이름을 지정한다.
-   *column\_name*: 값을 삽입할 칼럼 이름을 지정한다. 이 값을 생략하면, 테이블에 정의된 모든 칼럼이 명시된 것으로 간주되므로 모든 칼럼에 대한 값을 **VALUES** 뒤에 명시해야 한다. 테이블에 정의된 칼럼 중 일부 칼럼만 명시하면 나머지 칼럼에는 **DEFAULT** 로 정의된 값이 할당되며, 정의된 기본값이 없는 경우 **NULL** 값이 할당된다.
-   *expr* | **DEFAULT**: **VALUES** 뒤에는 칼럼에 대응하는 칼럼 값을 명시하며, 표현식 또는 **DEFAULT** 키워드를 값으로 지정할 수 있다. 명시된 칼럼 리스트의 순서와 개수는 칼럼 값 리스트와 대응되어야 하며, 하나의 레코드에 대해 칼럼 값 리스트는 괄호로 처리된다.
-   **DEFAULT**: 기본값을 칼럼 값으로 명시하기 위하여 **DEFAULT** 키워드를 사용할 수 있다. **VALUES** 키워드 뒤의 칼럼 값 리스트 내에 **DEFAULT** 를 명시하면 해당 칼럼에 기본값을 저장하고, **VALUES** 키워드 앞에 **DEFAULT** 를 명시하면 테이블 내 모든 칼럼에 대해 기본값을 저장한다. 기본값이 정의되지 않은 칼럼에 대해서는 **NULL** 을 저장한다.
-   **ON DUPLICATE KEY UPDATE**: **PRIMARY KEY** 또는 **UNIQUE** 속성이 정의된 칼럼에 중복 값이 삽입되어 제약 조건 위반이 발생하면, **ON DUPLICATE KEY UPDATE** 절에 명시된 액션을 수행하면서 제약 조건 위반을 발생시킨 값을 특정 값으로 변경한다.

``` sql
CREATE GLOBAL TABLE a_tbl1(
    id INT PRIMARY KEY,
    name VARCHAR,
    phone VARCHAR DEFAULT '000-0000'
);

--insert multiple rows
INSERT INTO a_tbl1 VALUES (1,'aaa', DEFAULT),(2,'bbb', DEFAULT);

--insert a single row specifying column values for all
INSERT INTO a_tbl1 VALUES (3,'ccc', '333-3333');

--insert two rows specifying column values for only
INSERT INTO a_tbl1(id) VALUES (4), (5);

--insert a single row with SET clauses
INSERT INTO a_tbl1 SET id=6, name='eee';
INSERT INTO a_tbl1 SET id=7, phone='777-7777';

SELECT * FROM a_tbl1;
```
```
  id   name    phone        
============================
  1    'aaa'   '000-0000'   
  2    'bbb'   '000-0000'   
  3    'ccc'   '333-3333'   
  4    NULL    '000-0000'   
  5    NULL    '000-0000'   
  6    'eee'   '000-0000'   
  7    NULL    '777-7777'   
```

``` sql
--insert duplicated value violating UNIQUE constraint
INSERT INTO a_tbl1 VALUES(2, 'bbb', '222-2222');
```
```
ERROR: Operation would have caused one or more unique constraint violations. INDEX pk_a_tbl1_id(B+tree: 0|366|2500) ON TABLE a_tbl1(CLASS_OID: 0|301|85). key: {2, 0|2511|8}(OID: 0|2511|8).
```


**INSERT ... SET** 문
--------------------

``` sql
CREATE GLOBAL TABLE tbl (a INT PRIMARY KEY, b INT, c INT);
INSERT INTO tbl SET a=1, b=2, c=3;
SELECT * FROM tbl;
```
```
  a   b   c   
==============
  1   2   3   
```


``` sql
CREATE GLOBAL TABLE tbl2 (a INT PRIMARY KEY, b INT, c INT);
INSERT INTO tbl2 SET a=1, c=3;
SELECT * FROM tbl2;
```
```
  a   b      c   
=================
  1   NULL   3   
```

b의 기본값이 없으므로 b의 값은 **NULL**이 된다.


``` sql
CREATE GLOBAL TABLE tbl3 (a INT PRIMARY KEY, b INT default 10, c INT);
INSERT INTO tbl3 SET a=1, c=3;
SELECT * FROM tbl3;
```
```
  a   b    c   
===============
  1   10   3   
```
b의 기본값이 10이므로 b의 값은 10이 된다.

INSERT ... SELECT 문
--------------------

**INSERT** 문에 **SELECT** 질의를 사용하면 하나 이상의 테이블로부터 특정 검색 조건을 만족하는 질의 결과를 대상 테이블에 삽입할 수 있다.

    INSERT [INTO] table_name [(column_name, ...)]
        SELECT...
        [ON DUPLICATE KEY UPDATE column_name = expr, ... ]

**SELECT** 문은 **VALUES** 키워드 대신 사용하거나 **VALUES** 뒤의 칼럼 값 리스트 내에 부질의로서 포함될 수 있다. **VALUES** 키워드를 대신하여 **SELECT** 문을 명시하면, 질의 결과로 얻은 다수의 레코드를 한 번에 대상 테이블 칼럼에 삽입할 수 있다. 그러나, **SELECT** 문을 칼럼 값 리스트 내에 부질의로 사용하려면 질의 결과 레코드가 하나여야 한다.

``` sql
--creating an empty table which schema replicated from a_tbl1
CREATE GLOBAL TABLE a_tbl2 LIKE a_tbl1;

--inserting multiple rows from SELECT query results
INSERT INTO a_tbl2 SELECT * FROM a_tbl1 WHERE id > 1;

--inserting column value with SELECT subquery specified in the value list
INSERT INTO a_tbl2 VALUES(8, SELECT name FROM a_tbl1 WHERE name <'bbb', DEFAULT);

SELECT * FROM a_tbl2;
```
```
  id   name    phone        
============================
  2    'bbb'   '000-0000'   
  3    'ccc'   '333-3333'   
  4    NULL    '000-0000'   
  5    NULL    '000-0000'   
  6    'eee'   '000-0000'   
  7    NULL    '777-7777'   
  8    'aaa'   '000-0000'   
```

ON DUPLICATE KEY UPDATE 절
--------------------------

**INSERT** 문에 **ON DUPLICATE KEY UPDATE** 절을 명시하여 **UNIQUE** 인덱스 또는 **PRIMARY KEY** 제약 조건이 설정된 칼럼에 중복된 값이 삽입되는 상황에서 에러를 출력하지 않고 새로운 값으로 갱신할 수 있다.

-   **PRIMARY KEY**와 **UNIQUE** 또는 다수의 **UNIQUE**가 한 테이블에 같이 존재하는 경우, 둘 중 하나에 의해 제약 조건 위반이 발생할 수 있으므로 **ON DUPLICATE KEY UPDATE** 절의 사용을 권장하지 않는다.
```
    <INSERT ... VALUES statement>
    <INSERT ... SET statement>
    <INSERT ... SELECT statement>
        INSERT ...
        [ON DUPLICATE KEY UPDATE column_name = expr, ... ]
```
-   *column\_name* = *expr*: **ON DUPLICATE KEY UPDATE** 뒤에 칼럼 값을 변경하고자 하는 칼럼 이름을 명시하고, 등호 부호를 이용하여 새로운 칼럼 값을 명시한다.

``` sql
--creating a new table having the same schema as a_tbl1
CREATE GLOBAL TABLE a_tbl3 LIKE a_tbl1;
INSERT INTO a_tbl3 SELECT * FROM a_tbl1 WHERE name IS NOT NULL;
SELECT * FROM a_tbl3;
```
```
  id   name    phone        
============================
  1    'aaa'   '000-0000'   
  2    'bbb'   '000-0000'   
  3    'ccc'   '333-3333'   
  6    'eee'   '000-0000'   
```
``` sql
INSERT INTO a_tbl3 SET id=6, phone='000-0000'
ON DUPLICATE KEY UPDATE phone='666-6666';
SELECT * FROM a_tbl3 WHERE id=6;
```
```
  id   name    phone        
============================
  6    'eee'   '666-6666'   
```

``` sql
INSERT INTO a_tbl3 SELECT * FROM a_tbl3 WHERE id=6 ON DUPLICATE KEY UPDATE name='ggg';
SELECT * FROM a_tbl3 WHERE id=6;
```
```
  id   name    phone        
============================
  6    'ggg'   '666-6666'   
```
