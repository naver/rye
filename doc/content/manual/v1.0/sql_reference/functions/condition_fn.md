비교 함수
===========

COALESCE
--------

**COALESCE(*expression* [, *expression* ] ...)**

**COALESCE** 함수는 하나 이상의 연산식 리스트가 인자로 지정되며, 첫 번째 인자가 **NULL** 이 아닌 값이면 해당 값을 결과로 반환하고, **NULL** 이면 두 번째 인자를 반환한다. 만약 인자로 지정된 모든 연산식이 **NULL** 이면 **NULL** 을 결과로 반환한다. 이러한 **COALESCE** 함수는 주로 **NULL** 값을 다른 기본값으로 대체할 때 사용한다.

Parameters:
- *expression*: 하나 이상의 연산식을 지정하며, 서로 비교 가능한 타입이어야 한다.

Return type: 첫번째 *expression* 의 타입

**COALESCE** 함수는 첫번째 인자의 타입으로 모든 인자를 변환하여 연산을 수행한다.

**COALESCE** (*a, b*)는 다음의 **CASE** 조건식과 같은 의미를 가진다.

```
    CASE WHEN a IS NOT NULL
    THEN a
    ELSE b
    END
```
``` sql
CREATE GLOBAL TABLE case_tbl (id INT PRIMARY KEY, a INT);
INSERT INTO case_tbl VALUES (1, 1);
INSERT INTO case_tbl VALUES (2, 2);
INSERT INTO case_tbl VALUES (3, 3);
INSERT INTO case_tbl VALUES (4, NULL);
```
``` sql
--substituting a default value 10 for a NULL value
SELECT a, COALESCE(a, 10) FROM case_tbl;
```
```
  a      coalesce(a, 10)   
===========================
  1      1                 
  2      2                 
  3      3                 
  NULL   10                
```

DECODE
------

**DECODE(*expression*, *search*, *result* [, *search*, *result*]\* [,*default*])**

**DECODE** 함수는 **CASE** 연산식과 마찬가지로 **IF** ... **THEN** ... **ELSE** 문과 동일한 기능을 수행한다. 인자로 지정된 *expression* 과 *search* 를 비교하여, 같은 값을 가지는 *search* 에 대응하는 *result* 를 결과로 반환한다. 만약, 같은 값을 가지는 *search* 가 없다면 *default* 값을 반환하고, *default* 값이 생략된 경우에는 **NULL** 을 반환한다. 비교 연산의 대상이 되는 *expression* 과 *search* 는 데이터 타입이 동일하거나 서로 변환 가능해야 한다.

Parameters:
- *expression*, *search*: 비교 가능한 타입의 연산식
- *result*: 매칭되었을 때 반환할 값
- *default*: 매치가 발견되지 않았을 때 반환할 값

Return type: 첫번째 *result* 의 타입

**DECODE**(*a*, *b*, *c*, *d*, *e*, *f*)는 다음의 **CASE** 조건식과 같은 의미를 가진다.
```
    CASE WHEN a = b THEN c
    WHEN a = d THEN e
    ELSE f
    END
```
``` sql
--Using DECODE function to compare expression and search values one by one
SELECT a, DECODE(a, 1, 'one', 2, 'two', 'other') FROM case_tbl;
```
```
  a      decode(a, 1, 'one', 2, 'two', 'other')   
==================================================
  1      'one'                                    
  2      'two'                                    
  3      'other'                                  
  NULL   'other'                                  
```

``` sql
--result types are converted to a single type containing all of significant figures
SELECT a, DECODE(a, 1, 1.000000000, 2, 1.2345, 1.234567890) FROM case_tbl;
```
```
  a      decode(a, 1, 1.000000000, 2, 1.2345, 1.234567890)   
=============================================================
  1      1.000000000                                         
  2      1.234500000                                         
  3      1.234567890                                         
  NULL   1.234567890                                         
```

``` sql
SELECT a, DECODE(a, 1, 'one', 2, 'two', 1.2345) FROM case_tbl;
```
```
  a      decode(a, 1, 'one', 2, 'two', 1.2345)   
=================================================
  1      'one'                                   
  2      'two'                                   
  3      '1.2345'                                
  NULL   '1.2345'                                
```

GREATEST
--------

**GREATEST(*expression* [, *expression*] ...)**

**GREATEST** 함수는 인자로 지정된 하나 이상의 연산식을 서로 비교하여 가장 큰 값을 반환한다. 만약, 하나의 연산식만 지정되면 서로 비교할 대상이 없으므로 해당 연산식의 값을 그대로 반환한다.

Parameters:
 - *expression*: 하나 이상의 연산식을 지정하며, 서로 비교 가능한 타입이어야 한다. 인자 중 어느 하나가 **NULL** 값이면 **NULL** 을 반환한다.

Return type: 첫번째 *expression* 의 타입

다음은 *demodb* 에서 한국이 획득한 각 메달의 수와 최대 메달의 수를 반환하는 예제이다.

``` sql
SELECT gold, silver, bronze, GREATEST (gold, silver, bronze)
FROM participant
WHERE nation_code = 'KOR';
```
```
  gold   silver   bronze   greatest(gold, silver, bronze)   
============================================================
  12     10       11       12                               
  12     5        12       12                               
  7      15       5        15                               
  8      10       10       10                               
  9      12       9        12                               
```

IF
---

**IF(*expression1*, *expression2*, *expression3*)**

**IF** 함수는 첫 번째 인자로 지정된 연산식의 값이 **TRUE** 이면 *expression2* 를 반환하고, **FALSE** 이거나 **NULL** 이면 *expression3* 를 반환한다. 결과로 반환되는 *expression2* 와 *expression3* 은 데이터 타입이 동일하거나 *expression2* 의 타입으로 변환 가능해야 한다. 둘 중 하나가 명확하게 **NULL** 이면, 함수의 결과 타입은 **NULL** 이 아닌 인자의 타입을 따른다.

Parameters:
 - *expression1*: 비교 조건식
 - *expression2*: *expression1* 이 참일 때 반환할 값
 - *expression3*: *expression1* 이 참이 아닐 때 반환할 값

Return type: *expression2* 의 타입

**IF**(*a*, *b*, *c*)는 다음의 **CASE** 연산식과 같은 의미를 가진다. 
```
    CASE WHEN a IS TRUE THEN b
    ELSE c
    END
```
``` sql
--IF function returns the second expression when the first is TRUE
SELECT a, IF(a=1, 'one', 'other') FROM case_tbl;
```
```
  a      if(a=1, 'one', 'other')   
===================================
  1      'one'                     
  2      'other'                   
  3      'other'                   
  NULL   'other'                   
```

``` sql
--If function in WHERE clause
SELECT a FROM case_tbl WHERE IF(a=1, 1, 2) = 1;
```
```
  a   
======
  1   
```

IFNULL, NVL
-----------

**IFNULL(*expr1*, *expr2*)**

**NVL(*expr1*, *expr2*)**

**IFNULL** 함수와 **NVL** 함수는 유사하게 동작한다. 두 개의 인자가 지정되며, 첫 번째 인자 *expr1* 이 **NULL** 이 아니면 *expr1* 을 반환하고, **NULL** 이면 두 번째 인자인 *expr2* 를 반환한다.

Parameters:
 - *expr1*: 조건식
 - *expr2*: *expr1* 이 **NULL** 일 때 반환할 값

Return type: *expr1* 의 타입

**IFNULL**(*a*, *b*) 또는 **NVL**(*a*, *b*)는 다음의 **CASE** 조건식과 같은 의미를 가진다.
```
    CASE WHEN a IS NULL THEN b
    ELSE a
    END
```
``` sql
--returning a specific value when a is NULL
SELECT a, NVL(a, 10) FROM case_tbl;
```
```
  a      nvl(a, 10)   
======================
  1      1            
  2      2            
  3      3            
  NULL   10           
```
``` sql
--IFNULL can be used instead of NVL and return values are converted to the string type
 SELECT a, IFNULL(CAST (a AS VARCHAR), 'UNKNOWN') FROM case_tbl;
```
```
  a      ifnull( cast(a as varchar), 'UNKNOWN')   
==================================================
  1      '1'                                      
  2      '2'                                      
  3      '3'                                      
  NULL   'UNKNOWN'                                
```

ISNULL
------

**ISNULL(*expression*)**

**ISNULL** 함수는 조건절 내에서 사용할 수 있으며, 인자로 지정된 표현식의 결과가 **NULL** 인지 비교하여 **NULL** 이면 1을 반환하고, 아니면 0을 반환한다. 이 함수를 이용하여 어떤 값이 **NULL** 인지 아닌지를 테스트할 수 있으며, **IS NULL** 조건식과 유사하게 동작한다.

Parameters:
 - *expression*: 단일 값을 가지는 칼럼, 상수 값 또는 단일 값을 생성하는 산술 함수를 입력한다.

Return type: INT

``` sql
CREATE GLOBAL TABLE condition_tbl (id INT PRIMARY KEY, name VARCHAR, dept_name VARCHAR, salary DOUBLE);
INSERT INTO condition_tbl VALUES (7, 'Brown', 'account', NULL);
```
``` sql
--Using ISNULL function to select rows with NULL value
SELECT * FROM condition_tbl WHERE ISNULL(salary);
```
```
  id   name      dept_name   salary   
======================================
  7    'Brown'   'account'   NULL     
```

LEAST
-----

**LEAST(*expression* [, *expression*] ...)**

**LEAST** 함수는 인자로 지정된 하나 이상의 연산식을 비교하여 가장 작은 값을 반환한다. 만약, 하나의 연산식만 지정되면 서로 비교할 대상이 없으므로 해당 연산식의 값을 그대로 반환한다.

Parameters:
 - *expression*: 하나 이상의 연산식을 지정하며, 서로 비교 가능한 타입이어야 한다. 인자 중 어느 하나가 **NULL** 값이면 **NULL** 을 반환한다.

Return type: 첫번째 *expression* 의 타입

다음은 *demodb* 에서 한국이 획득한 각 메달의 수와 최소 메달의 수를 반환하는 예제이다.

``` sql
SELECT gold, silver, bronze, LEAST(gold, silver, bronze) FROM participant
WHERE nation_code = 'KOR';
```
```
  gold   silver   bronze   least(gold, silver, bronze)   
=========================================================
  12     10       11       10                            
  12     5        12       5                             
  7      15       5        5                             
  8      10       10       8                             
  9      12       9        9                             
```

NULLIF
------

**NULLIF(*expr1*, *expr2*)**

**NULLIF** 함수는 인자로 지정된 두 개의 연산식이 동일하면 **NULL** 을 반환하고, 다르면 첫 번째 인자 값을 반환한다.

Parameters:
 - *expr1*: 비교할 연산식
 - *expr2*: 비교할 연산식

Return type: *expr1* 의 타입

**NULLIF** (*a, b*)는 다음의 **CASE** 조건식과 같은 의미를 가진다.
```
    CASE
    WHEN a = b THEN NULL
    ELSE a
    END
```
``` sql
--returning NULL value when a is 1
SELECT a, NULLIF(a, 1) FROM case_tbl;
```
```
  a      nullif(a, 1)   
========================
  1      NULL           
  2      2              
  3      3              
  NULL   NULL           
```

NVL2
----

**NVL2(*expr1*, *expr2*, *expr3*)**

**NVL2** 함수는 세 개의 인자가 지정되며, 첫 번째 연산식(*expr1*)이 **NULL** 이 아니면 두 번째 연산식(*expr2*)을 반환하고, **NULL** 이면 세 번째 연산식(*expr3*)을 반환한다.

Parameters:
 - *expr1*: 조건식
 - *expr2*: *expr1* 이 **NULL** 이 아닐 때 반환할 값
 - *expr3*: *expr1* 이 **NULL** 일 때 반환할 값

Return type: *expr2* 의 타입

``` sql
--returning a specific value of INT type
SELECT a, NVL2(a, a+1, 10.5678) FROM case_tbl;
```
```
  a      nvl2(a, a+1, 10.5678)   
=================================
  1      2                       
  2      3                       
  3      4                       
  NULL   11                      
```
