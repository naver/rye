REPLACE
=======

**REPLACE** 문은 [INSERT](insert.md) 문과 유사하지만, **PRIMARY KEY** 또는 **UNIQUE** 제약 조건이 정의된 칼럼에 중복된 값을 삽입하면 에러 출력 없이 기존 레코드를 삭제한 후 새로운 레코드를 삽입한다. **REPLACE** 문은 삽입 또는 삭제 후 삽입을 수행하므로, **REPLACE** 문을 사용하기 위해서는 테이블에 대한 **INSERT**와 **DELETE** 권한을 동시에 가지고 있어야 한다. 권한 설정에 관한 자세한 내용은 [권한 부여](../authorization.md) 절을 참고하면 된다.

```
    <REPLACE ... VALUES statement>
    REPLACE [INTO] table_name [(column_name, ...)]
        {VALUES | VALUE}({expr | DEFAULT}, ...)[,({expr | DEFAULT}, ...),...]

    <REPLACE ... SET statement>
    REPLACE [INTO] table_name
        SET column_name = {expr | DEFAULT}[, column_name = {expr | DEFAULT},...]

    <REPLACE ... SELECT statement>
    REPLACE [INTO] table_name [(column_name, ...)]
        SELECT...
```

-   *table\_name*: 새로운 레코드를 삽입할 대상 테이블 이름을 지정한다.
-   *column\_name*: 값을 삽입할 칼럼 이름을 지정한다. 이 값을 생략하면, 테이블에 정의된 모든 칼럼이 명시된 것으로 간주되므로 모든 칼럼에 대한 값을 **VALUES** 뒤에 명시해야 한다. 테이블에 정의된 칼럼 중 일부 칼럼만 명시하면 나머지 칼럼에는 **DEFAULT**로 정의된 값이 할당되며, 정의된 기본값이 없는 경우 **NULL** 값이 할당된다.
-   *expr* | **DEFAULT**: **VALUES** 뒤에는 칼럼에 대응하는 칼럼 값을 명시하며, 표현식 또는 **DEFAULT** 키워드를 값으로 지정할 수 있다. 명시된 칼럼 리스트의 순서와 개수는 칼럼 값 리스트와 대응되어야 하며, 하나의 레코드에 대해 칼럼 값 리스트는 괄호로 처리된다.

**REPLACE** 문은 새로운 레코드에 의한 **PRIMARY KEY** 또는 **UNIQUE** 인덱스 칼럼 값의 중복을 판단하므로, **PRIMARY KEY** 또는 **UNIQUE** 인덱스가 정의되지 않은 테이블에 대해서는 **INSERT** 문을 사용하는 것이 성능 상 유리하다.

``` sql
--creating a new table having the same schema as a_tbl1
CREATE GLOBAL TABLE a_tbl1 (id INT PRIMARY KEY, name VARCHAR, phone VARCHAR);
INSERT INTO a_tbl1 VALUES (1, 'aaa', '000-0000'), (2, 'bbb', '000-0000'), (3, 'ccc', '333-3333'), (6, 'eee', '000-0000');
CREATE GLOBAL TABLE a_tbl4 LIKE a_tbl1;
INSERT INTO a_tbl4 SELECT * FROM a_tbl1 WHERE id IS NOT NULL and name IS NOT NULL;

SELECT * FROM a_tbl4;
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
--insert duplicated value violating UNIQUE constraint
REPLACE INTO a_tbl4 VALUES(1, 'aaa', '111-1111'),(2, 'bbb', '222-2222');
REPLACE INTO a_tbl4 SET id=6, name='fff', phone=DEFAULT;

SELECT * FROM a_tbl4;
```
```
  id   name    phone        
============================
  1    'aaa'   '111-1111'   
  2    'bbb'   '222-2222'   
  3    'ccc'   '333-3333'   
  6    'fff'   NULL         
```
