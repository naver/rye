DELETE
======

**DELETE** 문을 사용하여 테이블 내에 레코드를 삭제할 수 있으며, [WHERE 절](select.md#where-절)과 결합하여 삭제 조건을 명시할 수 있다. 하나의 **DELETE** 문으로 하나 이상의 테이블을 삭제할 수 있다.
```
<DELETE table>
DELETE table_name [ FROM <table_specifications> ] [ WHERE <search_condition> ] [LIMIT row_count]

<DELETE table USING>
DELETE [FROM] table_name [ USING <table_specifications> ] [ WHERE <search_condition> ] [LIMIT row_count]
```

-   &lt;*table\_specifications*&gt;: **SELECT** 문의 **FROM** 절과 같은 형태의 구문을 지정할 수 있으며, 하나 이상의 테이블을 지정할 수 있다.
-   *table\_name*: 삭제할 데이터가 포함되어 있는 테이블의 이름을 지정한다. **FROM** 키워드를 생략할 수 있다.
-   *search\_condition*:[WHERE 절](select.md#where-절)을 이용하여 *search\_condition*을 만족하는 데이터만 삭제한다. 생략할 경우 지정된 테이블의 모든 데이터를 삭제한다.
-   *row\_count*: [LIMIT 절](select.md#limit-절)에 삭제할 레코드 수를 명시하며, 0보다 큰 정수를 지정할 수 있다. [WHERE 절](select.md#where-절)을 만족하는 레코드 개수가 *row\_count*를 초과하면 *row\_count* 개의 레코드만 삭제된다.

``` sql
CREATE GLOBAL TABLE my_tbl(
    id INT PRIMARY KEY,
    phone VARCHAR(10));
INSERT INTO my_tbl VALUES(1,'111-1111'), (2,'222-2222'), (3, '333-3333'), (4, NULL), (5, NULL);

--delete one record only from my_tbl
DELETE FROM my_tbl WHERE phone IS NULL LIMIT 1;
SELECT * FROM my_tbl;
```
```
  id   phone        
====================
  1    '111-1111'   
  2    '222-2222'   
  3    '333-3333'   
  5    NULL         
```
``` sql
--delete all records from my_tbl
DELETE FROM my_tbl;
```

아래 테이블들은 **DELETE JOIN**을 설명하기 위해 생성한 것이다.

``` sql
CREATE GLOBAL TABLE a_tbl(
    id INT PRIMARY KEY,
    phone VARCHAR(10));
CREATE GLOBAL TABLE b_tbl(
    id INT PRIMARY KEY,
    phone VARCHAR(10));
CREATE GLOBAL TABLE c_tbl(
    id INT PRIMARY KEY,
    phone VARCHAR(10));

INSERT INTO a_tbl VALUES(1,'111-1111'), (2,'222-2222'), (3, '333-3333'), (4, NULL), (5, NULL);
INSERT INTO b_tbl VALUES(1,'111-1111'), (2,'222-2222'), (3, '333-3333'), (4, NULL);
INSERT INTO c_tbl VALUES(1,'111-1111'), (2,'222-2222'), (10, '333-3333'), (11, NULL), (12, NULL);
```

다음 질의들은 여러 개의 테이블들을 조인한 후 삭제를 수행하며, 모두 같은 결과를 보여준다.

``` sql
-- Below four queries show the same result.
--  <DELETE table FROM ...>

DELETE a FROM a_tbl a, b_tbl b, c_tbl c
WHERE a.id=b.id AND b.id=c.id;

DELETE a FROM a_tbl a INNER JOIN b_tbl b ON a.id=b.id
INNER JOIN c_tbl c ON b.id=c.id;

-- <DELETE FROM table USING ...>

DELETE FROM a USING a_tbl a, b_tbl b, c_tbl c
WHERE a.id=b.id AND b.id=c.id;

DELETE FROM a USING a_tbl a INNER JOIN b_tbl b ON a.id=b.id
INNER JOIN c_tbl c ON b.id=c.id;

SELECT id, phone FROM a_tbl;
```
```
  id   phone        
====================
  3    '333-3333'   
  4    NULL         
  5    NULL         
```

조인 구문에 대한 자세한 설명은 [조인 질의](select.md#조인-질의)를 참고한다.
