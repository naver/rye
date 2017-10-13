SELECT
======

**SELECT** 문은 지정된 테이블에서 원하는 칼럼을 조회한다.
```
    SELECT [ <qualifier> ] <select_expressions>
        [FROM <extended_table_specification_comma_list>]
        [WHERE <search_condition>]
        [GROUP BY {col_name | expr} [ASC | DESC], ...[WITH ROLLUP]]
        [HAVING  <search_condition> ]
        [ORDER BY {col_name | expr} [ASC | DESC], ... [NULLS {FIRST | LAST}]
        [LIMIT [offset,] row_count]
        [USING INDEX { index_name [,index_name, ...] | NONE }]
        [FOR UPDATE [OF <spec_name_comma_list>]]

        <qualifier> ::= ALL | DISTINCT | UNIQUE

        <select_expressions> ::= * | <expression_comma_list> | *, <expression_comma_list>

        <extended_table_specification_comma_list> ::=
            <table_specification>   [
                                        {, <table_specification> } ... |
                                        <join_table_specification> ... |
                                        <join_table_specification2> ...
                                    ]

    <table_specification> ::=
        <table_name> [<correlation>] |
        <subquery> <correlation> |
        TABLE ( <expression> ) <correlation>

    <correlation> ::= [AS] <identifier> [(<identifier_comma_list>)]

    <join_table_specification> ::=
        [INNER | {LEFT | RIGHT} [OUTER]] JOIN <table_specification> ON <search_condition>

    <join_table_specification2> ::= CROSS JOIN <table_specification>
```

-   *qualifier*: 한정어. 생략이 가능하며 지정하지 않을 경우에는 **ALL** 로 지정된다.
    -   **ALL**: 테이블의 모든 레코드를 조회한다.
    -   **DISTINCT**: 중복을 허용하지 않고 유일한 값을 갖는 레코드에 대해서만 조회한다. **UNIQUE** 와 동일하다.
-   <*select_expressions*>
    -   * 구문을 사용하면 **FROM** 절에서 명시한 테이블에 대한 모든 칼럼을 조회할 수 있다.
    -   *expression_comma_list* : *expression* 은 칼럼 이름이나 변수, 테이블 이름이 될 수 있으며 산술 연산을 포함하는 일반적인 표현식도 모두 사용될 수 있다. 쉼표(,)는 리스트에서 개별 표현식을 구분하는데 사용된다. 조회하고자 하는 칼럼 또는 연산식에 대해 **AS** 키워드를 사용하여 별칭(alias)를 지정할 수 있으며, 지정된 별칭은 칼럼 이름으로 사용되어 **GROUP BY**, **HAVING**, **ORDER BY** 절 내에서 사용될 수 있다. 칼럼의 위치 인덱스(position)는 칼럼이 명시된 순서대로 부여되며, 시작 값은 1이다.

        *expression*에는 **AVG**, **COUNT**, **MAX**, **MIN**, **SUM** 과 같이 조회된 데이터를 조작하는 집계 함수가 사용될 수 있다.

-   *table_name*.\*: 테이블 이름을 지정한다. \*을 사용하면 명시한 테이블의 모든 칼럼을 지정하는 것과 같다.

다음은 역대 올림픽이 개최된 국가를 중복 없이 조회한 예제이다. 이 예제는 *demodb* 의 *olympic* 테이블을 대상으로 수행하였다. **DISTINCT** 또는 **UNIQUE** 키워드는 질의 결과가 유일한 값만을 갖도록 만든다. 예를 들어 *host_nation* 값이 'Greece'인 *olympic* 인스턴스가 여러 개일 때 질의 결과에는 하나의 값만 나타나도록 할 경우에 사용된다.

``` sql
SELECT DISTINCT host_nation
FROM olympic;
```
```
  host_nation        
=====================
  'Australia'        
  'Belgium'          
  'Canada'           
  'England'          
  'Finland'          
  'France'           
  'Germany'          
  'Greece'           
  'Italy'            
  'Japan'            
  'Korea'            
  'Mexico'           
  'Netherlands'      
  'Spain'            
  'Sweden'           
  'United Kingdom'   
  'USA'              
  'USSR'             
```

다음은 조회하고자 하는 칼럼에 칼럼 별칭을 부여하고, **ORDER BY** 절에서 칼럼 별칭을 이용하여 결과 레코드를 정렬하는 예제이다. 이때, **LIMIT** 절을 사용하여 결과 레코드 수를 5개로 제한한다.

``` sql
SELECT host_year as col1, host_nation as col2
FROM olympic
ORDER BY col2 LIMIT 5;
```
```
  col1   col2          
=======================
  1956   'Australia'   
  2000   'Australia'   
  1920   'Belgium'     
  1976   'Canada'      
  1948   'England'     
```
``` sql
SELECT CONCAT(host_nation, ', ', host_city) AS host_place
FROM olympic
ORDER BY host_place LIMIT 5;
```
```
  host_place               
===========================
  'Australia, Melbourne'   
  'Australia, Sydney'      
  'Belgium, Antwerp'       
  'Canada, Montreal'       
  'England, London'        
```

FROM 절
-------

**FROM** 절은 질의에서 데이터를 조회하고자 하는 테이블을 지정한다. 어떤 테이블도 참조하지 않는 경우에는 **FROM** 절을 생략할 수도 있다. 조회할 수 있는 경로는 다음과 같다.

-   개별 테이블(single table)
-   부질의(subquery)
-   유도 테이블(derived table)

<!-- -->

    SELECT [<qualifier>] <select_expressions>
    [
        FROM <table_specification> [ {, <table_specification> | <join_table_specification> }... ]
    ]

    <select_expressions> ::= * | <expression_comma_list> | *, <expression_comma_list>

    <table_specification> ::=
        <table_name> [<correlation>] |
        <subquery> <correlation> |
        TABLE (<expression>) <correlation>

    <correlation> ::= [AS] <identifier> [(<identifier_comma_list>)]

-   <*select_expressions*>: 조회하고자 하는 칼럼 또는 연산식을 하나 이상 지정할 수 있으며, 테이블 내 모든 칼럼을 조회할 때에는 \* 를 지정한다. 조회하고자 하는 칼럼 또는 연산식에 대해 **AS** 키워드를 사용하여 별칭(alias)를 지정할 수 있으며, 지정된 별칭은 칼럼 이름으로 사용되어 **GROUP BY**, **HAVING**, **ORDER BY** 절 내에서 사용될 수 있다. 칼럼의 위치 인덱스(position)는 칼럼이 명시된 순서대로 부여되며, 시작 값은 1이다.
-   <*table_specification*>: **FROM** 절 뒤에 하나 이상의 테이블 이름이 명시되며, 부질의와 유도 테이블도 지정될 수 있다. 부질의 유도 테이블에 대한 설명은 subquery-derived-table을 참고한다.

``` sql
--FROM clause can be omitted in the statement
SELECT 1+1 AS sum_value;
```
```
  sum_value   
==============
  2           
```
``` sql
SELECT CONCAT('Rye', '2017' , 'v1.0') AS db_version;
```
```
  db_version      
==================
  'Rye2017v1.0'   
```

###유도 테이블

질의문에서 **FROM** 절의 테이블 명세 부분에 부질의가 사용될 수 있다. 이런 형태의 부질의는 부질의 결과가 테이블로 취급되는 유도 테이블(derived table)을 만든다.

또한 유도 테이블은 집합 값을 갖는 속성의 개별 원소를 접근하는데 사용된다. 이 경우 집합 값의 한 원소는 유도 테이블에서 하나의 레코드로 생성된다.

###부질의 유도 테이블

유도 테이블의 각 레코드는 **FROM** 절에 주어진 부질의의 결과로부터 만들어진다. 부질의로부터 생성되는 유도 테이블은 임의의 개수의 칼럼과 레코드를 가질 수 있다.

    FROM (subquery) [AS] [derived_table_name [(column_name [{, column_name } ... ])]]

-   *column_name* 파라미터의 개수와 *subquery* 에서 만들어지는 칼럼의 개수는 일치해야 한다.
-   *derived_table_name*을 생략할 수 있다.

다음은 한국이 획득한 금메달 개수와 일본이 획득한 은메달 개수를 더한 값을 조회하는 예제이다. 이 예제는 유도 테이블을 이용하여 부질의의 중간 결과를 모으고 하나의 결과로 처리하는 방법을 보여준다. 이 질의는 *nation_code* 칼럼이 'KOR'인 *gold* 값과 *nation_code* 칼럼이 'JPN'인 *silver* 값의 전체 합을 반환한다.

``` sql
SELECT SUM (n)
FROM (SELECT gold FROM participant WHERE nation_code = 'KOR'
      UNION ALL
      SELECT silver FROM participant WHERE nation_code = 'JPN') AS t(n);
```
```
  sum(n)   
===========
  82       
```

부질의 유도 테이블은 외부 질의와 연관되어 있을 때 유용하게 사용할 수 있다. 예를 들어 **WHERE** 절에서 사용된 부질의의 **FROM** 절에 유도 테이블이 사용될 수 있다. 다음은 은메달 및 동메달을 하나 이상 획득한 경우, 해당 은메달과 동메달의 합의 평균보다 많은 수의 금메달을 획득한 *nation_code*, *host_year*, *gold* 필드를 보여주는 질의 예제이다. 이 예제에서는 질의(외부 **SELECT** 절)와 부질의(내부 **SELECT** 절)가 *nation_code* 속성으로 연결되어 있다.

``` sql
SELECT nation_code, host_year, gold
FROM participant p
WHERE gold > (SELECT AVG(s)
              FROM (SELECT silver + bronze
                    FROM participant
                    WHERE nation_code = p.nation_code
                    AND silver > 0
                    AND bronze > 0)
                   AS t(s));
```
```
  nation_code   host_year   gold   
===================================
  'ESP'         1992        13     
  'DEN'         1996        4      
  'CHN'         2004        32     
  'JPN'         2004        16     
```

WHERE 절
--------

질의에서 칼럼은 조건에 따라 처리될 수 있다. **WHERE** 절은 조회하려는 데이터의 조건을 명시한다.

    WHERE <search_condition>

        <search_condition> ::=
            <comparison_predicate>
            <between_predicate>
            <exists_predicate>
            <in_predicate>
            <null_predicate>
            <like_predicate>

**WHERE** 절은 *search_condition* 또는 질의에서 조회되는 데이터를 결정하는 조건식을 지정한다. 조건식이 참인 데이터만 질의 결과로 조회된다(**NULL** 값은 알 수 없는 값으로서 질의 결과로 조회되지 않는다).

-   *search_condition*: 자세한 내용은 다음의 항목을 참고한다.
    -   [단순 비교 조건식](../functions/condition_op.md#단순-비교-조건식)
    -   [BETWEEN](../functions/condition_op.md#between)
    -   [EXISTS](../functions/condition_op.md#exists)
    -   [IN](../functions/condition_op.md#in)
    -   [IS NULL](../functions/condition_op.md#is-null)
    -   [LIKE](../functions/condition_op.md#like)

복수의 조건은 논리연산자 **AND**, **OR** 를 사용할 수 있다. **AND** 가 지정된 경우 모든 조건이 참이어야 하고, **OR** 로 지정된 경우에는 하나의 조건만 참이어도 된다. 만약 키워드 **NOT** 이 조건 앞에 붙는다면 조건은 반대의 의미를 갖는다. 논리 연산이 평가되는 순서는 다음 표와 같다.

 우선순위 | 연산자    | 기능
--------|---------|---------------------------------------------
 1      | **( )** | 괄호 내에 포함된 논리 표현식은 첫 번째로 평가된다.
 2      | **NOT** | 논리 표현식의 결과를 부정한다.
 3      | **AND** | 논리 표현식에 포함된 모든 조건이 참이어야 한다.
 4      | **OR**  | 논리 표현식에 포함된 조건 중 하나의 조건은 참이어야 한다.

GROUP BY ... HAVING 절
----------------------

**SELECT** 문으로 검색한 결과를 특정 칼럼을 기준으로 그룹화하기 위해 **GROUP BY** 절을 사용하며, 그룹별로 정렬을 수행하거나 집계 함수를 사용하여 그룹별 집계를 구할 때 사용한다. 그룹이란 **GROUP BY** 절에 명시된 칼럼에 대해 동일한 칼럼 값을 가지는 레코드들을 의미한다.

**GROUP BY** 절 뒤에 **HAVING** 절을 결합하여 그룹 선택을 위한 조건식을 설정할 수 있다. 즉, **GROUP BY** 절로 구성되는 모든 그룹 중 **HAVING** 절에 명시된 조건식을 만족하는 그룹만 조회한다.

```
    SELECT ...
    GROUP BY {col_name | expr | position} [ASC | DESC], ...
              [WITH ROLLUP] [HAVING <search_condition>]
```

-   *col_name* | *expr* | *position*: 하나 이상의 칼럼 이름, 표현식, 별칭 또는 칼럼 위치가 지정될 수 있으며, 각 항목은 쉼표로 구분된다. 이를 기준으로 칼럼들이 정렬된다.
-   \[**ASC** | **DESC**\]: **GROUP BY** 절 내에 명시된 칼럼 뒤에 **ASC** 또는 **DESC** 의 정렬 옵션을 명시할 수 있다. 정렬 옵션이 명시되지 않으면 기본 옵션은 **ASC** 가 된다.
-   <*search_condition*>: **HAVING** 절에 검색 조건식을 명시한다. **HAVING** 절에서는 **GROUP BY** 절 내에 명시된 칼럼과 별칭, 또는 집계 함수에서 사용되는 칼럼을 참조할 수 있다.

-   **WITH ROLLUP**: **GROUP BY** 절에 **WITH ROLLUP** 수정자를 명시하면, **GROUP BY** 된 칼럼 각각에 대한 결과 값이 그룹별로 집계되고 나서, 해당 그룹 행의 전체를 집계한 결과 값이 추가로 출력된다. 즉, 그룹별로 집계한 값에 대해 다시 전체 집계를 수행한다. 그룹 대상 칼럼이 두 개 이상일 경우 앞의 그룹을 큰 단위, 뒤의 그룹을 작은 단위로 간주하여 작은 단위 별 전체 집계 행과 큰 단위의 전체 집계 행이 추가된다. 예를 들어 부서별, 사람별 영업 실적의 집계를 하나의 질의문으로 확인할 수 있다.

``` sql
-- creating a new table
CREATE GLOBAL TABLE sales_tbl (
	dept_no INT,
    name VARCHAR(20),
    sales_month INT,
    sales_amount INT DEFAULT 100,
    PRIMARY KEY (dept_no, name, sales_month)
);

INSERT INTO sales_tbl VALUES
	(201, 'George' , 1, 450), (201, 'George' , 2, 250), (201, 'Laura'  , 1, 100),
    (201, 'Laura'  , 2, 500), (301, 'Max'    , 1, 300), (301, 'Max'    , 2, 300),
	(501, 'Stephan', 1, 300), (501, 'Stephan', 2, DEFAULT), (501, 'Chang'  , 1, 150),
    (501, 'Chang'  , 2, 150), (501, 'Sue'    , 1, 150), (501, 'Sue'    , 2, 200);

-- selecting rows grouped by dept_no
SELECT dept_no, avg(sales_amount)
FROM sales_tbl
GROUP BY dept_no;
```
```
  dept_no   avg(sales_amount)   
================================
  201       325.0               
  301       300.0               
  501       175.0               
```
``` sql
-- conditions in WHERE clause operate first before GROUP BY
SELECT dept_no, avg(sales_amount)
FROM sales_tbl
WHERE sales_amount > 100
GROUP BY dept_no;
```
```
  dept_no   avg(sales_amount)   
================================
  201       400.0               
  301       300.0               
  501       190.0               
```
``` sql
-- conditions in HAVING clause operate last after GROUP BY
SELECT dept_no, avg(sales_amount)
FROM sales_tbl
WHERE sales_amount > 100
GROUP BY dept_no HAVING avg(sales_amount) > 200;
```
```
  dept_no   avg(sales_amount)   
================================
  201       400.0               
  301       300.0               
```
``` sql
-- selecting and sorting rows with using column alias
SELECT dept_no AS a1, avg(sales_amount) AS a2
FROM sales_tbl
WHERE sales_amount > 200 GROUP
BY a1 HAVING a2 > 200
ORDER BY a2;
```
```
  a1    a2      
================
  301   300.0   
  501   300.0   
  201   400.0   
```
``` sql
-- selecting rows grouped by dept_no, name with WITH ROLLUP modifier
SELECT dept_no AS a1, name AS a2, avg(sales_amount) AS a3
FROM sales_tbl
WHERE sales_amount > 100
GROUP BY a1, a2 WITH ROLLUP;
```
```
  a1     a2          a3      
=============================
  201    'George'    350.0   
  201    'Laura'     500.0   
  201    NULL        400.0   
  301    'Max'       300.0   
  301    NULL        300.0   
  501    'Chang'     150.0   
  501    'Stephan'   300.0   
  501    'Sue'       175.0   
  501    NULL        190.0   
  NULL   NULL        275.0   
```

ORDER BY 절
-----------

**ORDER BY** 절은 질의 결과를 오름차순 또는 내림차순으로 정렬하며, **ASC** 또는 **DESC** 와 같은 정렬 옵션을 명시하지 않으면 오름차순으로 정렬한다. **ORDER BY** 절을 지정하지 않으면, 조회되는 레코드의 순서는 질의에 따라 다르다.

    SELECT ...
    ORDER BY {col_name | expr | position} [ASC | DESC], ...] [NULLS {FIRST | LAST}]

-   *col_name* | *expr* | *position*: 정렬 기준이 되는 칼럼 이름, 표현식, 별칭 또는 칼럼 위치를 지정한다. 하나 이상의 값을 지정할 수 있으며 각 항목은 쉼표로 구분한다. **SELECT** 칼럼 리스트에 명시되지 않은 칼럼도 지정할 수 있다.
-   \[**ASC** | **DESC**\]: **ASC** 은 오름차순, **DESC** 은 내림차순으로 정렬하며, 정렬 옵션이 명시되지 않으면 오름차순으로 정렬한다.
-   \[**NULLS** {**FIRST** | **LAST**}\]: **NULLS FIRST**는 NULL을 앞에 정렬하며, **NULLS LAST**는 NULL을 뒤에 정렬한다. 이 구문이 생략될 경우 **ASC**는 NULL을 앞에 정렬하며, **DESC**는 NULL을 뒤에 정렬한다.

``` sql
-- selecting rows sorted by ORDER BY clause
SELECT *
FROM sales_tbl
ORDER BY dept_no DESC, name ASC;
```
```
  dept_no   name        sales_month   sales_amount   
=====================================================
  501       'Chang'     1             150            
  501       'Chang'     2             150            
  501       'Stephan'   1             300            
  501       'Stephan'   2             100            
  501       'Sue'       1             150            
  501       'Sue'       2             200            
  301       'Max'       1             300            
  301       'Max'       2             300            
  201       'George'    1             450            
  201       'George'    2             250            
  201       'Laura'     1             100            
  201       'Laura'     2             500            
```
``` sql
-- sorting reversely and limiting result rows by LIMIT clause
SELECT dept_no AS a1, avg(sales_amount) AS a2
FROM sales_tbl
GROUP BY a1
ORDER BY a2 DESC
LIMIT 3;
```
```
  a1    a2      
================
  201   325.0   
  301   300.0   
  501   175.0   
```
다음은 ORDER BY 절 뒤에 NULLS FIRST, NULLS LAST 구문을 지정하는 예제이다.

``` sql
CREATE GLOBAL TABLE tbl (a INT PRIMARY KEY, b VARCHAR);

INSERT INTO tbl VALUES
(1,NULL), (2,NULL), (3,'AB'), (4,NULL), (5,'AB'),
(6,NULL), (7,'ABCD'), (8,NULL), (9,'ABCD'), (10,NULL);

SELECT * FROM tbl ORDER BY b NULLS FIRST;
```
```
  a    b        
================
  1    NULL     
  2    NULL     
  4    NULL     
  6    NULL     
  8    NULL     
  10   NULL     
  3    'AB'     
  5    'AB'     
  7    'ABCD'   
  9    'ABCD'   
```
``` sql
SELECT * FROM tbl ORDER BY b NULLS LAST;
```
```
  a    b        
================
  3    'AB'     
  5    'AB'     
  7    'ABCD'   
  9    'ABCD'   
  1    NULL     
  2    NULL     
  4    NULL     
  6    NULL     
  8    NULL     
  10   NULL     
```

**GROUP BY 별칭(alias)의 해석**

``` sql
CREATE GLOBAL TABLE t1(a INT PRIMARY KEY, b INT, c INT);
INSERT INTO t1 VALUES(1,1,1);
INSERT INTO t1 VALUES(2,NULL,2);
INSERT INTO t1 VALUES(3,2,2);

SELECT a, NVL(b,2) AS b
FROM t1
GROUP BY a, b;  -- Q1
```
```
  a   b   
==========
  1   1   
  2   2   
  3   2   
```

위의 SELECT 질의를 수행할 때 "GROUP BY a, b"는 "GROUP BY a, b"(칼럼 이름 b)로 해석되며, 아래 Q2와 동일한 결과를 출력한다.

``` sql
    SELECT a, NVL(b,2) AS bxxx
    FROM t1
    GROUP BY a, b;  -- Q2
```
```
  a   bxxx   
=============
  1   1      
  2   2      
  3   2      
```

LIMIT 절
--------

**LIMIT** 절은 출력되는 레코드의 개수를 제한할 때 사용한다. **LIMIT** 절은 바인드 파라미터를 사용할 수 있다.

**LIMIT** 절을 포함하는 질의에서는 **WHERE** 절에 **INST_NUM** (), **ROWNUM** 을 포함할 수 없으며, **HAVING GROUPBY_NUM** ()과 함께 사용할 수 없다.

    LIMIT {[offset,] row_count | row_count [OFFSET offset]}

-   *offset*: 출력할 레코드의 시작 행 오프셋 값을 지정한다. 결과 셋의 시작 행 오프셋 값은 0이다. 생략할 수 있으며, 기본값은 **0** 이다.
-   *row_count*: 출력하고자 하는 레코드 개수를 명시한다. 0보다 큰 정수를 지정할 수 있다.

``` sql
-- LIMIT clause can be used in prepared statement
SELECT * FROM sales_tbl LIMIT 1, 2;

-- selecting rows with LIMIT clause
SELECT *
FROM sales_tbl
WHERE sales_amount > 100
LIMIT 5;
```
```
  dept_no   name       sales_month   sales_amount   
====================================================
  201       'George'   2             250            
  201       'Laura'    1             100            

  dept_no   name       sales_month   sales_amount   
====================================================
  201       'George'   1             450            
  201       'George'   2             250            
  201       'Laura'    2             500            
  301       'Max'      1             300            
  301       'Max'      2             300            
```
``` sql
-- LIMIT clause can be used in subquery
SELECT t1.*
FROM (SELECT * FROM sales_tbl AS t2 WHERE sales_amount > 100 LIMIT 5) AS t1
LIMIT 1,3;

-- above query and below query shows the same result
SELECT t1.*
FROM (SELECT * FROM sales_tbl AS t2 WHERE sales_amount > 100 LIMIT 5) AS t1
LIMIT 3 OFFSET 1;
```
```
  dept_no   name       sales_month   sales_amount   
====================================================
  201       'George'   2             250            
  201       'Laura'    2             500            
  301       'Max'      1             300            

  dept_no   name       sales_month   sales_amount   
====================================================
  201       'George'   2             250            
  201       'Laura'    2             500            
  301       'Max'      1             300            
```

조인 질의
---------

조인은 두 개 이상의 테이블 또는 뷰(view)에 대해 행(row)을 결합하는 질의이다. 조인 질의에서 두 개 이상의 테이블에 공통인 칼럼을 비교하는 조건을 조인 조건이라고 하며, 조인된 각 테이블로부터 행을 가져와 지정된 조인 조건을 만족하는 경우에만 결과 행을 결합한다.

조인 질의에서 동등 연산자( **=** )를 이용한 조인 조건을 포함하는 조인 질의를 동등 조인(equi-join)이라 하고, 조인 조건이 없는 조인 질의를 카티션 곱(cartesian products)이라 한다. 또한, 하나의 테이블을 조인하는 경우를 자체 조인(self join)이라 하는데, 자체 조인에서는 **FROM** 절에 같은 테이블이 두 번 사용되므로 테이블 별칭(alias)을 사용하여 칼럼을 구분한다.

조인된 테이블에 대해 조인 조건을 만족하는 행만 결과를 출력하는 경우를 내부 조인(inner join) 또는 간단 조인(simple join)이라고 하는 반면, 조인된 테이블에 대해 조인 조건을 만족하는 행은 물론 조인 조건을 만족하지 못하는 행도 포함하여 출력하는 경우를 외부 조인(outer join)이라 한다.

외부 조인은 왼쪽 테이블의 모든 행이 결과로 출력되는(조건과 일치하지 않는 오른쪽 테이블의 칼럼들은 NULL로 출력됨) 왼쪽 외부 조인과(left outer join)과 오른쪽 테이블의 모든 행이 결과로 출력되는(조건과 일치하지 않는 왼쪽 테이블의 칼럼들은 NULL로 출력됨) 오른쪽 외부 조인(right outer join)이 있으며, 양쪽의 행이 모두 출력되는 완전 외부 조인(full outer join)이 있다. 외부 조인 질의 결과에서 한쪽 테이블에 대해 대응되는 칼럼 값이 없는 경우, 이는 모두 **NULL**을 반환된다.

```
    FROM <table_specification> [{, <table_specification>
        | { <join_table_specification> | <join_table_specification2> } ...]

    <table_specification> ::=
        <table_name> [<correlation>] |
        <subquery> <correlation> |
        TABLE (<expression>) <correlation>

    <join_table_specification> ::=
        [INNER | {LEFT | RIGHT} [OUTER]] JOIN <table_specification> ON <search_condition>

    <join_table_specification2> ::=
        CROSS JOIN <table_specification>
```

-   <*join_table_specification*>
    -   \[**INNER**\] **JOIN**: 내부 조인에 사용되며 조인 조건이 반드시 필요하다.
    -   {**LEFT** | **RIGHT**} \[**OUTER**\] **JOIN**: **LEFT** 는 왼쪽 외부 조인을 수행하는 질의를 만드는데 사용되고, **RIGHT** 는 오른쪽 외부 조인을 수행하는 질의를 만드는데 사용된다.
-   <*join\_table\_specification2*>
    -   **CROSS JOIN**: 교차 조인에 사용되며, 조인 조건을 사용하지 않는다.

### 내부 조인

내부 조인은 조인을 위한 조건이 반드시 필요하다. **INNER JOIN** 키워드는 생략할 수 있으며, 생략하면 테이블 사이를 쉼표(,)로 구분하고, **ON** 조인 조건을 **WHERE** 조건으로 대체할 수 있다.

다음은 내부 조인을 이용하여 1950년 이후에 열린 올림픽 중에서 신기록이 세워진 올림픽의 개최연도와 개최국가를 조회하는 예제이다. 다음 질의는 *history* 테이블의 *host_year* 가 1950보다 큰 범위에서 값이 존재하는 레코드를 가져온다. 다음 두 개의 질의는 같은 결과를 출력한다.

``` sql
SELECT DISTINCT h.host_year, o.host_nation
FROM history h INNER JOIN olympic o ON h.host_year = o.host_year AND o.host_year > 1950;

SELECT DISTINCT h.host_year, o.host_nation
FROM history h, olympic o
WHERE h.host_year = o.host_year AND o.host_year > 1950;
```
```
  host_year   host_nation   
============================
  1968        'Mexico'      
  1980        'USSR'        
  1984        'USA'         
  1988        'Korea'       
  1992        'Spain'       
  1996        'USA'         
  2000        'Australia'   
  2004        'Greece'      

  host_year   host_nation   
============================
  1968        'Mexico'      
  1980        'USSR'        
  1984        'USA'         
  1988        'Korea'       
  1992        'Spain'       
  1996        'USA'         
  2000        'Australia'   
  2004        'Greece'      
```

### 외부 조인

Rye는 외부 조인 중 왼쪽 외부 조인과 오른쪽 외부 조인만 지원하며, 완전 외부 조인(full outer join)을 지원하지 않는다. 또한, 외부 조인에서 조인 조건에 부질의와 하위 칼럼을 포함하는 경로 표현식을 사용할 수 없다.

외부 조인의 경우 조인 조건은 내부 조인의 경우와는 다른 방법으로 지정된다. 내부 조인의 조인 조건은 **WHERE** 절에서도 표현될 수 있지만, 외부 조인의 경우에는 조인 조건이 **FROM** 절 내의 **ON** 키워드 뒤에 나타난다. 다른 검색 조건은 **WHERE** 절이나 **ON** 절에서 사용할 수 있지만 검색 조건이 **WHERE** 절에 있을 때와 **ON** 절에 있을 때 질의 결과가 달라질 수 있다.

**FROM** 절에 명시된 순서대로 테이블 실행 순서가 고정되므로, 외부 조인을 사용하는 경우 테이블 순서에 주의하여 질의문을 작성한다. 외부 조인 연산자 '**(+)**'를 **WHERE** 절에 명시하여 Oracle 스타일의 조인 질의문도 작성 가능하나, 실행 결과나 실행 계획이 원하지 않는 방향으로 유도될 수 있으므로 {**LEFT** | **RIGHT**} \[**OUTER**\] **JOIN**을 이용한 표준 구문을 사용할 것을 권장한다.

다음은 오른쪽 외부 조인을 이용하여 1950년 이후에 열린 올림픽에서 신기록이 세워진 올림픽의 개최국가와 개최연도를 조회하되, 신기록이 세워지지 않은 올림픽에 대한 정보도 포함하는 예제이다. 이 예제는 오른쪽 외부 조인이므로, *olympic* 테이블의 *host_nation* 의 모든 레코드를 포함하고, 값이 존재하지 않는 *history* 테이블의 *host_year*에 대해서는 칼럼 값으로 **NULL**을 반환한다.

``` sql
SELECT DISTINCT h.host_year, o.host_year, o.host_nation
FROM history h RIGHT OUTER JOIN olympic o ON h.host_year = o.host_year
WHERE o.host_year > 1950;
```
```
  host_year   host_year   host_nation   
========================================
  NULL        1952        'Finland'     
  NULL        1956        'Australia'   
  NULL        1960        'Italy'       
  NULL        1964        'Japan'       
  NULL        1972        'Germany'     
  NULL        1976        'Canada'      
  1968        1968        'Mexico'      
  1980        1980        'USSR'        
  1984        1984        'USA'         
  1988        1988        'Korea'       
  1992        1992        'Spain'       
  1996        1996        'USA'         
  2000        2000        'Australia'   
  2004        2004        'Greece'      
```
다음은 왼쪽 외부 조인을 이용하여 위와 동일한 결과를 출력하는 예제이다. **FROM** 절에서 두 테이블의 순서를 바꾸어 명시한 후, 왼쪽 외부 조인을 수행한다.

``` sql
SELECT DISTINCT h.host_year, o.host_year, o.host_nation
FROM olympic o LEFT OUTER JOIN history h ON h.host_year = o.host_year
WHERE o.host_year > 1950;
```
```
  host_year   host_year   host_nation   
========================================
  NULL        1952        'Finland'     
  NULL        1956        'Australia'   
  NULL        1960        'Italy'       
  NULL        1964        'Japan'       
  NULL        1972        'Germany'     
  NULL        1976        'Canada'      
  1968        1968        'Mexico'      
  1980        1980        'USSR'        
  1984        1984        'USA'         
  1988        1988        'Korea'       
  1992        1992        'Spain'       
  1996        1996        'USA'         
  2000        2000        'Australia'   
  2004        2004        'Greece'      
```
다음은 **WHERE** 절에서 **(+)**를 사용해서 외부 조인 질의를 작성한 예이며, 위와 같은 결과를 출력한다. 단, **(+)** 연산자를 이용한 Oracle 스타일의 외부 조인 질의문은 ISO/ANSI 표준이 아니며 모호한 상황을 만들어 낼 수 있으므로 허용하지 않는다. 표준 구문인 **LEFT OUTER JOIN**(또는 **RIGHT OUTER JOIN**)을 사용할 것을 권장한다.

``` sql
SELECT DISTINCT h.host_year, o.host_year, o.host_nation
FROM history h, olympic o
WHERE o.host_year = h.host_year(+) AND o.host_year > 1950;
```
```
        host_year    host_year  host_nation
    ================================================
             NULL         1952  'Finland'
             NULL         1956  'Australia'
             NULL         1960  'Italy'
             NULL         1964  'Japan'
             NULL         1972  'Germany'
             NULL         1976  'Canada'
             1968         1968  'Mexico'
             1980         1980  'USSR'
             1984         1984  'USA'
             1988         1988  'Korea'
             1992         1992  'Spain'
             1996         1996  'USA'
             2000         2000  'Australia'
             2004         2004  'Greece'
```
이상의 예에서 *h.host_year* = *o.host_year* 는 외부 조인 조건이고 *o.host_year* > 1950은 검색 조건이다. 만약 검색 조건이 **WHERE** 절이 아닌 **ON** 절에서 조인 조건으로 사용될 경우 질의의 의미와 결과는 달라진다. 다음 질의는 *o.host_year* 가 1950보다 크지 않은 값도 질의 결과에 포함된다.

``` sql
SELECT DISTINCT h.host_year, o.host_year, o.host_nation
FROM olympic o LEFT OUTER JOIN history h ON h.host_year = o.host_year AND o.host_year > 1950;
```
```
  host_year   host_year   host_nation        
=============================================
  NULL        1896        'Greece'           
  NULL        1900        'France'           
  NULL        1904        'USA'              
  NULL        1908        'United Kingdom'   
  NULL        1912        'Sweden'           
  NULL        1920        'Belgium'          
  NULL        1924        'France'           
  NULL        1928        'Netherlands'      
  NULL        1932        'USA'              
  NULL        1936        'Germany'          
  NULL        1948        'England'          
  NULL        1952        'Finland'          
  NULL        1956        'Australia'        
  NULL        1960        'Italy'            
  NULL        1964        'Japan'            
  NULL        1972        'Germany'          
  NULL        1976        'Canada'           
  1968        1968        'Mexico'           
  1980        1980        'USSR'             
  1984        1984        'USA'              
  1988        1988        'Korea'            
  1992        1992        'Spain'            
  1996        1996        'USA'              
  2000        2000        'Australia'        
  2004        2004        'Greece'           
```
위의 예에서 **LEFT OUTER JOIN**은 왼쪽 테이블의 행이 조건에 부합하지 않더라도 모든 행을 결과 행에 결합해야 하므로, 왼쪽 테이블의 칼럼 조건인 "AND o.host_year > 1950"는 이므로 무시된다. 그러나 "WHERE o.host_year > 1950"는 조인이 완료된 이후에 적용된다. **OUTER JOIN**에서는 **ON** 절 뒤의 조건과 **WHERE** 절 뒤의 조건이 다르게 적용될 수 있음에 주의해야 한다.

### 교차 조인

교차 조인은 아무런 조건 없이 두 개의 테이블을 결합한 것, 즉 카티션 곱(cartesian product)이다. 교차 조인에서 **CROSS JOIN** 키워드는 생략할 수 있으며, 생략하려면 테이블 사이를 쉼표(,)로 구분한다.

다음은 교차 조인을 작성한 예이다.

``` sql
SELECT DISTINCT h.host_year, o.host_nation 
FROM history h CROSS JOIN olympic o;

SELECT DISTINCT h.host_year, o.host_nation 
FROM history h, olympic o;
```
```
  host_year   host_nation        
=================================
  1968        'Australia'        
  1968        'Belgium'          
  1968        'Canada'           
  1968        'England'          
  1968        'Finland'          
  1968        'France'           
  1968        'Germany'          
  1968        'Greece'           
  1968        'Italy'            
  1968        'Japan'            
  1968        'Korea'            
  1968        'Mexico'           
  1968        'Netherlands'      
  1968        'Spain'            
  1968        'Sweden'           
  1968        'United Kingdom'   
  1968        'USA'              
  1968        'USSR'             
  1980        'Australia'        
  1980        'Belgium'          
  1980        'Canada'           
  1980        'England'          
  1980        'Finland'          
  1980        'France'           
  1980        'Germany'          
  1980        'Greece'           
  1980        'Italy'            
  1980        'Japan'            
  1980        'Korea'            
  1980        'Mexico'           
  1980        'Netherlands'      
  1980        'Spain'            
  1980        'Sweden'           
  1980        'United Kingdom'   
  1980        'USA'              
  1980        'USSR'             
  1984        'Australia'        
  1984        'Belgium'          
  1984        'Canada'           
  1984        'England'          
  1984        'Finland'          
  1984        'France'           
  1984        'Germany'          
  1984        'Greece'           
  1984        'Italy'            
  1984        'Japan'            
  1984        'Korea'            
  1984        'Mexico'           
  1984        'Netherlands'      
  1984        'Spain'            
  1984        'Sweden'           
  1984        'United Kingdom'   
  1984        'USA'              
  1984        'USSR'             
  1988        'Australia'        
  1988        'Belgium'          
  1988        'Canada'           
  1988        'England'          
  1988        'Finland'          
  1988        'France'           
  1988        'Germany'          
  1988        'Greece'           
  1988        'Italy'            
  1988        'Japan'            
  1988        'Korea'            
  1988        'Mexico'           
  1988        'Netherlands'      
  1988        'Spain'            
  1988        'Sweden'           
  1988        'United Kingdom'   
  1988        'USA'              
  1988        'USSR'             
  1992        'Australia'        
  1992        'Belgium'          
  1992        'Canada'           
  1992        'England'          
  1992        'Finland'          
  1992        'France'           
  1992        'Germany'          
  1992        'Greece'           
  1992        'Italy'            
  1992        'Japan'            
  1992        'Korea'            
  1992        'Mexico'           
  1992        'Netherlands'      
  1992        'Spain'            
  1992        'Sweden'           
  1992        'United Kingdom'   
  1992        'USA'              
  1992        'USSR'             
  1996        'Australia'        
  1996        'Belgium'          
  1996        'Canada'           
  1996        'England'          
  1996        'Finland'          
  1996        'France'           
  1996        'Germany'          
  1996        'Greece'           
  1996        'Italy'            
  1996        'Japan'            
  1996        'Korea'            
  1996        'Mexico'           
  1996        'Netherlands'      
  1996        'Spain'            
  1996        'Sweden'           
  1996        'United Kingdom'   
  1996        'USA'              
  1996        'USSR'             
  2000        'Australia'        
  2000        'Belgium'          
  2000        'Canada'           
  2000        'England'          
  2000        'Finland'          
  2000        'France'           
  2000        'Germany'          
  2000        'Greece'           
  2000        'Italy'            
  2000        'Japan'            
  2000        'Korea'            
  2000        'Mexico'           
  2000        'Netherlands'      
  2000        'Spain'            
  2000        'Sweden'           
  2000        'United Kingdom'   
  2000        'USA'              
  2000        'USSR'             
  2004        'Australia'        
  2004        'Belgium'          
  2004        'Canada'           
  2004        'England'          
  2004        'Finland'          
  2004        'France'           
  2004        'Germany'          
  2004        'Greece'           
  2004        'Italy'            
  2004        'Japan'            
  2004        'Korea'            
  2004        'Mexico'           
  2004        'Netherlands'      
  2004        'Spain'            
  2004        'Sweden'           
  2004        'United Kingdom'   
  2004        'USA'              
  2004        'USSR'             

  host_year   host_nation        
=================================
  1968        'Australia'        
  1968        'Belgium'          
  1968        'Canada'           
  1968        'England'          
  1968        'Finland'          
  1968        'France'           
  1968        'Germany'          
  1968        'Greece'           
  1968        'Italy'            
  1968        'Japan'            
  1968        'Korea'            
  1968        'Mexico'           
  1968        'Netherlands'      
  1968        'Spain'            
  1968        'Sweden'           
  1968        'United Kingdom'   
  1968        'USA'              
  1968        'USSR'             
  1980        'Australia'        
  1980        'Belgium'          
  1980        'Canada'           
  1980        'England'          
  1980        'Finland'          
  1980        'France'           
  1980        'Germany'          
  1980        'Greece'           
  1980        'Italy'            
  1980        'Japan'            
  1980        'Korea'            
  1980        'Mexico'           
  1980        'Netherlands'      
  1980        'Spain'            
  1980        'Sweden'           
  1980        'United Kingdom'   
  1980        'USA'              
  1980        'USSR'             
  1984        'Australia'        
  1984        'Belgium'          
  1984        'Canada'           
  1984        'England'          
  1984        'Finland'          
  1984        'France'           
  1984        'Germany'          
  1984        'Greece'           
  1984        'Italy'            
  1984        'Japan'            
  1984        'Korea'            
  1984        'Mexico'           
  1984        'Netherlands'      
  1984        'Spain'            
  1984        'Sweden'           
  1984        'United Kingdom'   
  1984        'USA'              
  1984        'USSR'             
  1988        'Australia'        
  1988        'Belgium'          
  1988        'Canada'           
  1988        'England'          
  1988        'Finland'          
  1988        'France'           
  1988        'Germany'          
  1988        'Greece'           
  1988        'Italy'            
  1988        'Japan'            
  1988        'Korea'            
  1988        'Mexico'           
  1988        'Netherlands'      
  1988        'Spain'            
  1988        'Sweden'           
  1988        'United Kingdom'   
  1988        'USA'              
  1988        'USSR'             
  1992        'Australia'        
  1992        'Belgium'          
  1992        'Canada'           
  1992        'England'          
  1992        'Finland'          
  1992        'France'           
  1992        'Germany'          
  1992        'Greece'           
  1992        'Italy'            
  1992        'Japan'            
  1992        'Korea'            
  1992        'Mexico'           
  1992        'Netherlands'      
  1992        'Spain'            
  1992        'Sweden'           
  1992        'United Kingdom'   
  1992        'USA'              
  1992        'USSR'             
  1996        'Australia'        
  1996        'Belgium'          
  1996        'Canada'           
  1996        'England'          
  1996        'Finland'          
  1996        'France'           
  1996        'Germany'          
  1996        'Greece'           
  1996        'Italy'            
  1996        'Japan'            
  1996        'Korea'            
  1996        'Mexico'           
  1996        'Netherlands'      
  1996        'Spain'            
  1996        'Sweden'           
  1996        'United Kingdom'   
  1996        'USA'              
  1996        'USSR'             
  2000        'Australia'        
  2000        'Belgium'          
  2000        'Canada'           
  2000        'England'          
  2000        'Finland'          
  2000        'France'           
  2000        'Germany'          
  2000        'Greece'           
  2000        'Italy'            
  2000        'Japan'            
  2000        'Korea'            
  2000        'Mexico'           
  2000        'Netherlands'      
  2000        'Spain'            
  2000        'Sweden'           
  2000        'United Kingdom'   
  2000        'USA'              
  2000        'USSR'             
  2004        'Australia'        
  2004        'Belgium'          
  2004        'Canada'           
  2004        'England'          
  2004        'Finland'          
  2004        'France'           
  2004        'Germany'          
  2004        'Greece'           
  2004        'Italy'            
  2004        'Japan'            
  2004        'Korea'            
  2004        'Mexico'           
  2004        'Netherlands'      
  2004        'Spain'            
  2004        'Sweden'           
  2004        'United Kingdom'   
  2004        'USA'              
  2004        'USSR'             
```

위 두 개의 질의는 같은 결과를 출력한다.

부질의
------

부질의는 질의 내에서 **SELECT** 절이나 **WHERE** 절 등 표현식이 가능한 모든 곳에서 사용할 수 있다. 부질의가 표현식으로 사용될 경우에는 반드시 단일 칼럼을 반환해야 하지만, 표현식이 아닌 경우에는 하나 이상의 행이 반환될 수 있다.

표현식으로 사용된 부질의는 하나의 칼럼을 갖는 하나의 행을 만든다. 부질의에 의해 행이 반환되지 않을 경우에 부질의 표현식은 **NULL** 을 가진다. 만약 부질의가 두 개 이상의 행을 반환하도록 만들어진 경우에는 에러가 발생한다.

다음은 역대 기록 테이블을 조회하는데, 신기록을 수립한 올림픽이 개최된 국가도 함께 조회하는 예제이다. 이 예제는 표현식으로 사용된 단일 행 부질의를 보여준다. 이 예에서 부질의는 *olympic* 테이블에서 *host_year* 칼럼 값이 *history* 테이블의 *host_year* 칼럼 값과 같은 행에 대해 *host_nation* 값을 반환한다. 조건에 일치되는 값이 없을 경우 부질의 결과는 **NULL** 이 표시된다.

``` sql
SELECT h.host_year,
      (SELECT host_nation FROM olympic o WHERE o.host_year=h.host_year) AS host_nation,
       h.event_code, h.score, h.unit
FROM history h;
```
```
  host_year   host_nation   event_code   score       unit      
===============================================================
  2004        'Greece'      20005        '12.37'     'time'    
  2004        'Greece'      20006        '27:05.0'   'time'    
  2000        'Australia'   20007        '30:17.0'   'time'    
  1996        'USA'         20008        '9.84'      'time'    
  1988        'Korea'       20009        '10.62'     'time'    
  2004        'Greece'      20010        '12.91'     'time'    
  2000        'Australia'   20012        '03:32.0'   'time'    
  1988        'Korea'       20013        '03:54.0'   'time'    
  1996        'USA'         20015        '19.32'     'time'    
  1988        'Korea'       20016        '21.34'     'time'    
  2000        'Australia'   20017        '1:18:59'   'time'    
  2000        'Australia'   20018        '1:29:05'   'time'    
  1988        'Korea'       20020        '08:06.0'   'time'    
  1992        'Spain'       20021        '37.4'      'time'    
  1992        'Spain'       20021        '37.4'      'time'    
  1992        'Spain'       20021        '37.4'      'time'    
  1992        'Spain'       20021        '37.4'      'time'    
  1980        'USSR'        20022        '41.6'      'time'    
  1980        'USSR'        20022        '41.6'      'time'    
  1980        'USSR'        20022        '41.6'      'time'    
  1980        'USSR'        20022        '41.6'      'time'    
  1992        'Spain'       20023        '02:56.0'   'time'    
  1992        'Spain'       20023        '02:56.0'   'time'    
  1992        'Spain'       20023        '02:56.0'   'time'    
  1992        'Spain'       20023        '02:56.0'   'time'    
  1988        'Korea'       20024        '03:15.0'   'time'    
  1988        'Korea'       20024        '03:15.0'   'time'    
  1988        'Korea'       20024        '03:15.0'   'time'    
  1988        'Korea'       20024        '03:15.0'   'time'    
  1996        'USA'         20025        '43.49'     'time'    
  1996        'USA'         20026        '48.25'     'time'    
  1992        'Spain'       20027        '46.78'     'time'    
  2004        'Greece'      20028        '52.77'     'time'    
  1984        'USA'         20029        '13:06.0'   'time'    
  2000        'Australia'   20030        '14:41.0'   'time'    
  1988        'Korea'       20031        '3:38:29'   'time'    
  1996        'USA'         20032        '01:43.0'   'time'    
  1980        'USSR'        20033        '01:53.0'   'time'    
  2004        'Greece'      20035        '8893'      'score'   
  2004        'Greece'      20036        '69.89'     'meter'   
  1988        'Korea'       20037        '72.3'      'meter'   
  1988        'Korea'       20038        '84.8'      'meter'   
  2004        'Greece'      20039        '75.02'     'meter'   
  1988        'Korea'       20040        '7291'      'meter'   
  1996        'USA'         20041        '2.39'      'meter'   
  2004        'Greece'      20042        '2.06'      'meter'   
  2000        'Australia'   20043        '90.17'     'meter'   
  2004        'Greece'      20044        '71.53'     'meter'   
  1968        'Mexico'      20045        '8.9'       'meter'   
  1988        'Korea'       20046        '7.4'       'meter'   
  1984        'USA'         20047        '2:09:21'   'time'    
  2000        'Australia'   20048        '2:23:14'   'time'    
  2004        'Greece'      20049        '5.95'      'meter'   
  2004        'Greece'      20050        '4.91'      'meter'   
  1988        'Korea'       20051        '22.47'     'meter'   
  1980        'USSR'        20052        '22.41'     'meter'   
  1996        'USA'         20053        '18.09'     'meter'   
  1996        'USA'         20054        '15.33'     'meter'   
  2004        'Greece'      20084        '01:01.0'   'time'    
  2004        'Greece'      20085        '33.952'    'time'    
  2004        'Greece'      20088        '04:15.0'   'time'    
  2004        'Greece'      20089        '03:25.0'   'time'    
  1996        'USA'         20099        '10.129'    'time'    
  1996        'USA'         20100        '11.21'     'time'    
  2004        'Greece'      20101        '03:57.0'   'time'    
  2004        'Greece'      20101        '03:57.0'   'time'    
  2004        'Greece'      20101        '03:57.0'   'time'    
  2004        'Greece'      20101        '03:57.0'   'time'    
  2004        'Greece'      20239        '591'       'score'   
  1996        'USA'         20240        '390'       'score'   
  2004        'Greece'      20241        '599'       'score'   
  2004        'Greece'      20242        '399'       'score'   
  2004        'Greece'      20243        '590'       'score'   
  2000        'Australia'   20245        '590'       'score'   
  1996        'USA'         20246        '596'       'score'   
  1980        'USSR'        20247        '581'       'score'   
  2000        'Australia'   20248        '1177'      'score'   
  1996        'USA'         20249        '600'       'score'   
  1996        'USA'         20250        '589'       'score'   
  2004        'Greece'      20251        '144'       'score'   
  2000        'Australia'   20252        '112'       'score'   
  1996        'USA'         20253        '125'       'score'   
  2000        'Australia'   20254        '73'        'score'   
  1996        'USA'         20256        '124'       'score'   
  2000        'Australia'   20257        '71'        'score'   
  2004        'Greece'      20259        '53.45'     'time'    
  2004        'Greece'      20260        '59.68'     'time'    
  2004        'Greece'      20261        '01:00.0'   'time'    
  2004        'Greece'      20262        '01:07.0'   'time'    
  2004        'Greece'      20263        '51.25'     'time'    
  2000        'Australia'   20264        '56.61'     'time'    
  2000        'Australia'   20265        '47.84'     'time'    
  2004        'Greece'      20266        '53.52'     'time'    
  2004        'Greece'      20267        '14:43.0'   'time'    
  2004        'Greece'      20268        '01:55.0'   'time'    
  1992        'Spain'       20269        '02:07.0'   'time'    
  2004        'Greece'      20270        '02:09.0'   'time'    
  2004        'Greece'      20271        '02:23.0'   'time'    
  2004        'Greece'      20272        '01:54.0'   'time'    
  2000        'Australia'   20273        '02:06.0'   'time'    
  2004        'Greece'      20274        '01:45.0'   'time'    
  1988        'Korea'       20275        '01:58.0'   'time'    
  2004        'Greece'      20276        '01:57.0'   'time'    
  2000        'Australia'   20277        '02:11.0'   'time'    
  2004        'Greece'      20278        '03:13.0'   'time'    
  2004        'Greece'      20278        '03:13.0'   'time'    
  2004        'Greece'      20278        '03:13.0'   'time'    
  2004        'Greece'      20278        '03:13.0'   'time'    
  2004        'Greece'      20279        '03:36.0'   'time'    
  2004        'Greece'      20279        '03:36.0'   'time'    
  2004        'Greece'      20279        '03:36.0'   'time'    
  2004        'Greece'      20279        '03:36.0'   'time'    
  2004        'Greece'      20280        '03:31.0'   'time'    
  2004        'Greece'      20280        '03:31.0'   'time'    
  2004        'Greece'      20280        '03:31.0'   'time'    
  2004        'Greece'      20280        '03:31.0'   'time'    
  2004        'Greece'      20281        '03:57.0'   'time'    
  2004        'Greece'      20281        '03:57.0'   'time'    
  2004        'Greece'      20281        '03:57.0'   'time'    
  2004        'Greece'      20281        '03:57.0'   'time'    
  2000        'Australia'   20282        '07:07.0'   'time'    
  2000        'Australia'   20282        '07:07.0'   'time'    
  2000        'Australia'   20282        '07:07.0'   'time'    
  2000        'Australia'   20282        '07:07.0'   'time'    
  2004        'Greece'      20283        '07:53.0'   'time'    
  2004        'Greece'      20283        '07:53.0'   'time'    
  2004        'Greece'      20283        '07:53.0'   'time'    
  2004        'Greece'      20283        '07:53.0'   'time'    
  2000        'Australia'   20284        '03:41.0'   'time'    
  1988        'Korea'       20285        '04:04.0'   'time'    
  2004        'Greece'      20286        '04:08.0'   'time'    
  2000        'Australia'   20287        '04:34.0'   'time'    
  1992        'Spain'       20288        '21.91'     'time'    
  2000        'Australia'   20289        '24.13'     'time'    
  2000        'Australia'   20290        '08:20.0'   'time'    
  2000        'Australia'   20318        '472.5'     'kg'      
  2004        'Greece'      20321        '305'       'kg'      
  2004        'Greece'      20326        '210'       'kg'      
  2000        'Australia'   20328        '225'       'kg'      
  2000        'Australia'   20330        '305'       'kg'      
  2004        'Greece'      20331        '237.5'     'kg'      
  2000        'Australia'   20334        '325'       'kg'      
  2000        'Australia'   20335        '242.5'     'kg'      
  2000        'Australia'   20338        '357.5'     'kg'      
  2004        'Greece'      20339        '275'       'kg'      
  2004        'Greece'      20341        '272.5'     'kg'      
  2004        'Greece'      20344        '375'       'kg'      
```

FOR UPDATE
----------

**FOR UPDATE** 절은 **UPDATE/DELETE** 문을 수행하기 위해 **SELECT** 문에서 반환되는 행들에 잠금을 부여하기 위해 사용될 수 있다.

```
    SELECT ... [FOR UPDATE [OF <spec_name_comma_list>]]

        <spec_name_comma_list> ::= <spec_name> [, <spec_name>, ... ]
            <spec_name> ::= table_name | view_name
```

-   <*spec_name_comma_list*>: FROM 절에서 참조하는 테이블/뷰들의 목록

<*spec_name_comma_list*>에서 참조되는 테이블/뷰에만 잠금이 적용된다. FOR UPDATE 절에 <*spec_name_comma_list*>가 명시되지 않는 경우, FROM 절의 모든 테이블/뷰가 잠금 대상인 것으로 간주한다.

제약 사항

-   부질의 안에서 **FOR UPDATE** 절을 사용할 수 없다. 단, **FOR UPDATE** 절이 부질의를 참조할 수는 있다.
-   **GROUP BY, DISTINCT** 또는 집계 함수를 가진 질의문에서 사용할 수 없다.
-   **UNION**을 참조할 수 없다.

다음은 **SELECT ... FOR UPDATE** 문을 사용하는 예이다.

``` sql
CREATE GLOBAL TABLE t1(i INT PRIMARY KEY);
INSERT INTO t1 VALUES (1), (2), (3), (4), (5);

CREATE GLOBAL TABLE t2(i INT PRIMARY KEY);
INSERT INTO t2 VALUES (1), (2), (3), (4), (5);
CREATE INDEX idx_t2_i ON t2(i);

CREATE VIEW v12 AS SELECT t1.i AS i1, t2.i AS i2 FROM t1 INNER JOIN t2 ON t1.i=t2.i;

SELECT * FROM t1 ORDER BY 1 FOR UPDATE;
SELECT * FROM t1 ORDER BY 1 FOR UPDATE OF t1;
SELECT * FROM t1 INNER JOIN t2 ON t1.i=t2.i ORDER BY 1 FOR UPDATE OF t1, t2;

SELECT * FROM t1 INNER JOIN (SELECT * FROM t2 WHERE t2.i > 0) r ON t1.i=r.i WHERE t1.i > 0 ORDER BY 1 FOR UPDATE;

SELECT * FROM v12 ORDER BY 1 FOR UPDATE;
SELECT * FROM t1, (SELECT * FROM v12, t2 WHERE t2.i > 0 AND t2.i=v12.i1) r WHERE t1.i > 0 AND t1.i=r.i ORDER BY 1 FOR UPDATE OF r;
```
