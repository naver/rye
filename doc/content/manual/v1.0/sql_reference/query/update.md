UPDATE
======

**UPDATE** 문을 사용하면 대상 테이블에 저장된 레코드의 칼럼 값을 새로운 값으로 업데이트할 수 있다. **SET** 절에는 업데이트할 칼럼 이름과 새로운 값을 명시하며, [WHERE 절](select.md#where-절)에는 업데이트할 레코드를 추출하기 위한 조건을 명시한다.

```
<UPDATE table>
UPDATE <table_specifications> SET column_name = {<expr> | DEFAULT} [, column_name = {<expr> | DEFAULT} ...]
        [WHERE <search_condition>]
        [ORDER BY {col_name | <expr>}]
        [LIMIT row_count]
```

-   &lt;*table\_specifications*&gt; : **SELECT** 문의 **FROM** 절과 같은 형태의 구문을 지정할 수 있으며, 하나 이상의 테이블을 지정할 수 있다.
-   *column\_name*: 업데이트할 칼럼 이름을 지정한다. 하나의 테이블에 대한 칼럼들을 지정할 수 있다.
-   &lt;*expr*&gt; | **DEFAULT**: 해당 칼럼의 새로운 값을 지정하며, 표현식 또는 **DEFAULT** 키워드를 값으로 지정할 수 있다. 단일 결과 레코드를 반환하는 **SELECT** 질의를 지정할 수도 있다.
-   &lt;*search\_condition*&gt;: [WHERE 절](select.md#where-절)에 조건식을 명시하면, 조건식을 만족하는 레코드에 대해서만 칼럼 값을 업데이트한다.
-   *col\_name* | &lt;*expr*&gt;: 업데이트할 순서의 기준이 되는 칼럼을 지정한다.
-   *row\_count*: [LIMIT 절](select.md#limit-절)에 업데이트할 레코드 수를 명시하며, 0보다 큰 정수를 지정할 수 있다.

[ORDER BY 절](select.md#order-by-절)이나 [LIMIT 절](select.md#limit-절)을 지정할 수 있다. [LIMIT 절](select.md#limit-절)을 명시하면 업데이트할 레코드 수를 한정할 수 있다.[ORDER BY 절](select.md#order-by-절)을 명시하면 해당 칼럼의 순서로 레코드를 업데이트한다.

다음은 업데이트를 수행하는 예이다.

``` sql
--creating a new table having all records copied from a_tbl1
CREATE GLOBAL TABLE a_tbl1
 (id INT PRIMARY KEY,
  name VARCHAR,
  phone VARCHAR);
INSERT INTO a_tbl1 VALUES (3, NULL, '000-0000'), (4, NULL, '000-0000'), (5, NULL, '000-0000'), (7, NULL, '777-7777');
CREATE GLOBAL TABLE a_tbl5 LIKE a_tbl1;
INSERT INTO a_tbl5 SELECT * FROM a_tbl1;
SELECT * FROM a_tbl5 WHERE name IS NULL;
```
```
  id   name   phone        
===========================
  3    NULL   '000-0000'   
  4    NULL   '000-0000'   
  5    NULL   '000-0000'   
  7    NULL   '777-7777'   
```
``` sql
UPDATE a_tbl5 SET name='yyy', phone='999-9999' WHERE name IS NULL LIMIT 3;
SELECT id, name, phone FROM a_tbl5;
```
```
  id   name    phone        
============================
  3    'yyy'   '999-9999'   
  4    'yyy'   '999-9999'   
  5    'yyy'   '999-9999'   
  7    NULL    '777-7777'   
```

다음은 여러 개의 테이블들에 대해 조인한 후 *b\_tbl* 업데이트를 수행하는 예이다.

``` sql
CREATE GLOBAL TABLE a_tbl(id INT PRIMARY KEY, charge DOUBLE);
CREATE GLOBAL TABLE b_tbl(id INT PRIMARY KEY, rate_id INT, rate DOUBLE);
INSERT INTO a_tbl VALUES (1, 100.0), (2, 1000.0), (3, 10000.0);
INSERT INTO b_tbl VALUES (1, 1, 0.1), (2, 2, 0.0), (3, 3, 0.2), (4, 3, 0.5);

UPDATE a_tbl INNER JOIN b_tbl ON a_tbl.id=b_tbl.rate_id
SET b_tbl.rate = b_tbl.rate * (0.9)
WHERE a_tbl.charge > 900.0;

SELECT rate_id, rate FROM b_tbl;
```
```
  rate_id   rate                  
==================================
  1         0.1                   
  2         0.0                   
  3         0.18000000000000002   
  3         0.45                  
```

**UPDATE** 문에서 업데이트하는 테이블 *a\_tbl* 에 대해 *a\_tbl*의 행 하나당 조인하는 *b\_tbl*의 행의 개수가 두 개 이상이면 오류를 반환한다.

``` sql
UPDATE a_tbl INNER JOIN b_tbl ON a_tbl.id=b_tbl.rate_id
SET a_tbl.charge = a_tbl.charge * (1 + b_tbl.rate)
WHERE a_tbl.charge > 900.0;
```
```
ERROR: Cannot update/delete without exactly one key-preserved table.
```

조인 구문에 대한 자세한 설명은 [조인 질의](select.md#조인-질의)를 참고한다.
