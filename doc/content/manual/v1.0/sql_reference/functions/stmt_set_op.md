문장 집합 연산자
================

UNION, DIFFERENCE, INTERSECTION
-------------------------------

피연산자로 지정된 하나 이상의 질의문의 결과에 대해 합집합(**UNION**), 차집합(**DIFFERENCE**), 교집합(**INTERSECTION**)을 구하기 위하여 문장 집합 연산자(Statement Set Operator)를 이용한다. 단, 두 질의문의 대상 테이블에서 조회하고자 하는 데이터 타입이 동일하거나, 묵시적으로 변환 가능해야 한다.

    query_term statement_set_operator [qualifier] <query_term>
    [{statement_set_operator [qualifier] <query_term>}];  

        <query_term> ::=
            query_specification
            subquery

-   *qualifier*
    -   DISTINCT, DISTINCTROW 또는 UNIQUE(결과로 반환되는 레코드가 서로 다르다는 것을 보장)
    -   ALL (모든 레코드가 반환, 중복 허용)
-   *statement\_set\_operator*
    -   UNION (합집합)
    -   DIFFERENCE (차집합)
    -   INTERSECT | INTERSECTION (교집합)

다음은 Rye가 지원하는 문장 집합 연산자를 나타낸 표이다.

**문장 집합 연산자**

문장 집합 연산자 | 설명 | 비고
--- | --- | ---
**UNION** | 합집합 중복을 허용하지 않음 | **UNION ALL** 이면 중복된 값을 포함한 모든 결과 인스턴스 출력
**DIFFERENCE** | 차집합 중복을 허용하지 않음 | **EXCEPT** 연산자와 동일. **DIFFERENCE ALL** 이면 중복된 값을 포함한 모든 결과 인스턴스 출력
**INTERSECTION** | 교집합 중복을 허용하지 않음 | **INTERSECT** 연산자와 동일. **INTERSECTION ALL** 이면 중복된 값을 포함한 모든 결과 인스턴스 출력

다음은 문장 집합 연산자를 가지고 질의를 수행하는 예이다.

``` sql
CREATE GLOBAL TABLE nojoin_tbl_1 (ID INT PRIMARY KEY, Name VARCHAR(32));

INSERT INTO nojoin_tbl_1 VALUES (1,'Kim');
INSERT INTO nojoin_tbl_1 VALUES (2,'Moy');
INSERT INTO nojoin_tbl_1 VALUES (3,'Jonas');
INSERT INTO nojoin_tbl_1 VALUES (4,'Smith');
INSERT INTO nojoin_tbl_1 VALUES (5,'Kim');
INSERT INTO nojoin_tbl_1 VALUES (6,'Smith');
INSERT INTO nojoin_tbl_1 VALUES (7,'Brown');

CREATE GLOBAL TABLE nojoin_tbl_2 (id INT PRIMARY KEY, Name VARCHAR(32));

INSERT INTO nojoin_tbl_2 VALUES (5,'Kim');
INSERT INTO nojoin_tbl_2 VALUES (6,'Smith');
INSERT INTO nojoin_tbl_2 VALUES (7,'Brown');
INSERT INTO nojoin_tbl_2 VALUES (8,'Lin');
INSERT INTO nojoin_tbl_2 VALUES (9,'Edwin');
INSERT INTO nojoin_tbl_2 VALUES (10,'Edwin');

--Using UNION to get only distinct rows
SELECT id, name FROM nojoin_tbl_1
UNION
SELECT id, name FROM nojoin_tbl_2;
```
```
  id   name      
=================
  1    'Kim'     
  2    'Moy'     
  3    'Jonas'   
  4    'Smith'   
  5    'Kim'     
  6    'Smith'   
  7    'Brown'   
  8    'Lin'     
  9    'Edwin'   
  10   'Edwin'   
```
``` sql
--Using UNION ALL not eliminating duplicate selected rows
SELECT id, name FROM nojoin_tbl_1
UNION ALL
SELECT id, name FROM nojoin_tbl_2;
```
```
  id   name      
=================
  1    'Kim'     
  2    'Moy'     
  3    'Jonas'   
  4    'Smith'   
  5    'Kim'     
  6    'Smith'   
  7    'Brown'   
  5    'Kim'     
  6    'Smith'   
  7    'Brown'   
  8    'Lin'     
  9    'Edwin'   
  10   'Edwin'   
```
``` sql
--Using DEFFERENCE to get only rows returned by the first query but not by the second
SELECT id, name FROM nojoin_tbl_1
DIFFERENCE
SELECT id, name FROM nojoin_tbl_2;
```
```
  id   name      
=================
  1    'Kim'     
  2    'Moy'     
  3    'Jonas'   
  4    'Smith'   
```
``` sql
--Using INTERSECTION to get only those rows returned by both queries
SELECT id, name FROM nojoin_tbl_1
INTERSECT
SELECT id, name FROM nojoin_tbl_2;
```
```
  id   name      
=================
  5    'Kim'     
  6    'Smith'   
  7    'Brown'   
```
