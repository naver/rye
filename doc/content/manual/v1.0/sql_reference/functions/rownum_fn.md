ROWNUM 함수
===========

ROWNUM, INST\_NUM
-----------------

일반적인 **SELECT** 문에서는 **ROWNUM**, **INST\_NUM()**을, **ORDER BY** 절을 포함한 **SELECT** 문에서는 **ORDERBY\_NUM()**을, **GROUP BY** 절을 포함한 **SELECT** 문에서는 **GROUPBY\_NUM()**을 사용할 수 있다. **ROWNUM** 함수를 사용하면 질의의 결과 레코드 수를 다양한 방법으로 제한할 수 있다. 예를 들어, 질의 결과의 처음 10건만 조회한다거나, 짝수 번째 또는 홀수 번째 레코드만 반환하도록 할 수 있다.

**ROWNUM** 함수는 결과 타입이 정수형이고, **SELECT** 절과 **WHERE** 절과 같이 질의 내에 수식이 위치할 수 있는 모든 곳에 사용할 수 있다. 하지만, **ROWNUM** 함수 결과를 속성 또는 연관된 부질의(correlated subquery)와 비교하는 것은 허용되지 않는다.

-   **WHERE** 절에 명시된 **ROWNUM** 함수는 **INST\_NUM()** 함수와 같은 의미를 가진다.
-   **ROWNUM** 함수는 각각의 **SELECT** 문장에 종속된다. 즉, **ROWNUM** 함수가 부질의에 쓰인 경우, 부질의를 수행하는 동안에 부질의 결과에 대하여 일련 번호를 반환한다. 내부적으로, **ROWNUM** 함수 결과는 조회된 레코드를 질의 결과 셋에 쓰기 직전에 생성된다. 이 순간에 질의 결과 셋의 레코드에 대한 일련 번호를 생성하는 카운터 값이 증가된다.
-   **SELECT** 결과에 일련번호를 부여할 목적으로 사용하는 경우, 정렬 과정이 없는 경우 **ROWNUM**을, ORDER BY 절이 있으면 **ORDERBY\_NUM()** 함수를, GROUP BY 절이 있으면 **GROUPBY\_NUM()** 함수를 사용한다.
-   **SELECT** 문에 **ORDER BY** 절이 포함된 경우 **WHERE** 절에 명시된 **ORDERBY\_NUM()** 함수의 값은 **ORDER BY** 절 처리를 위한 정렬 과정 이후에 생성된다. 정렬 과정 이후에 결과 행의 개수를 제한하려면 **FOR ORDERBY\_NUM()** 절을 사용한다.
-   **SELECT** 문에 **GROUP BY** 절이 포함된 경우 **HAVING** 절에 명시된 **GROUPBY\_NUM()** 함수의 값은 질의 결과가 그룹화된 이후에 생성된다. 그룹화된 이후에 결과 행의 개수를 제한하려면 **HAVING GROUPBY\_NUM()** 절을 사용한다.
-   정렬된 결과 행들의 개수를 제한하는 목적으로 **FOR ORDERBY\_NUM()** 또는 **HAVING GROUPBY\_NUM()** 구문 대신 **LIMIT** 절을 사용할 수 있다.
-   **ROWNUM** 함수는 **SELECT** 문 뿐만 아니라 **INSERT**, **DELETE**, **UPDATE** 와 같은 SQL 문에도 쓸 수 있다. 예를 들어, **INSERT INTO** *table\_name* **SELECT** ... **FROM** ... **WHERE** ... 질의와 같이 한 테이블의 행(row) 중 일부를 조회하여 다른 테이블에 삽입하고자 할 때, **WHERE** 절에 **ROWNUM** 함수를 사용할 수 있다.

다음은 *demodb* 에서 1988 올림픽에서 금메달 개수를 기준으로 4위권 국가 이름을 반환하는 예제이다.

``` sql
--Limiting 4 rows using ROWNUM in the WHERE condition
SELECT  * FROM
(SELECT nation_code FROM participant WHERE host_year = 1988
     ORDER BY gold DESC) AS T
WHERE ROWNUM <5;
```
```

    nation_code

> ======================  
> 'URS' 'GDR' 'USA' 'KOR'
>
```
LIMIT 절은 정렬된 결과 행의 개수를 제한하므로 아래의 질의 결과는 위의 결과와 동일하다.

``` sql
--Limiting 4 rows using LIMIT
SELECT ROWNUM, nation_code FROM participant WHERE host_year = 1988
ORDER BY gold DESC
LIMIT 4;
```
```

    rownum  nation_code

> ===================================  
> 156 'URS' 155 'GDR' 154 'USA' 153 'KOR'
>
```
아래의 ROWNUM 조건은 정렬되기 이전에 행의 개수를 제한하므로 질의 결과가 위와 다르다.

``` sql
--Unexpected results : ROWNUM operated before ORDER BY
SELECT ROWNUM, nation_code FROM participant
WHERE host_year = 1988 AND ROWNUM < 5
ORDER BY gold DESC;
```
```

    rownum  nation_code

> ===================================  
> 1 'ZIM' 2 'ZAM' 3 'ZAI' 4 'YMD'
>
```

ORDERBY\_NUM
------------

다음은 *demodb* 의 *history* 테이블에서 3위에서 5위까지의 선수 이름과 기록을 조회하는 예제이다.

``` sql
--Ordering first and then limiting rows using FOR ORDERBY_NUM()
SELECT ORDERBY_NUM(), athlete, score FROM history
ORDER BY score FOR ORDERBY_NUM() BETWEEN 3 AND 5;
```
```

    orderby_num()  athlete               score

> ==============================================================  
> 3 'Luo Xuejuan' '01:07.0' 4 'Rodal Vebjorn' '01:43.0' 5 'Thorpe Ian' '01:45.0'
>
```
아래의 LIMIT 절을 사용한 질의는 위의 질의와 동일한 결과를 출력한다.

``` sql
SELECT ORDERBY_NUM(), athlete, score FROM history
ORDER BY score LIMIT 2, 3;
```

아래의 ROWNUM을 사용하여 결과 행의 개수를 제한한 질의는 정렬 이전에 개수를 제한한 이후에 ORDER BY 정렬을 수행한다.

``` sql
--Limiting rows first and then Ordering using ROWNUM
SELECT ROWNUM athlete, score FROM history
WHERE ROWNUM BETWEEN 3 AND 5 ORDER BY score;
```
```

    athlete               score

> ============================================  
> 'Thorpe Ian' '01:45.0' 'Thorpe Ian' '03:41.0' 'Hackett Grant' '14:43.0'
>
```

GROUPBY\_NUM
------------

다음은 *demodb* 의 *history* 테이블에서 과거 5개의 올림픽에 대해서 최단 기록을 조회하는 예제이다.

``` sql
--Group-ordering first and then limiting rows using GROUPBY_NUM()
SELECT  GROUPBY_NUM(), host_year, MIN(score) FROM history  
GROUP BY host_year HAVING GROUPBY_NUM() BETWEEN 1 AND 5;
```
```

    groupby_num()    host_year  min(score)

> =====================================================  
> 1 1968 '8.9' 2 1980 '01:53.0' 3 1984 '13:06.0' 4 1988 '01:58.0' 5 1992 '02:07.0'
>
```
아래의 LIMIT 절을 사용한 질의는 위의 질의와 동일한 결과를 출력한다.

``` sql
SELECT  GROUPBY_NUM(), host_year, MIN(score) FROM history  
GROUP BY host_year LIMIT 5;
```

아래의 ROWNUM을 사용하여 결과 행의 개수를 제한한 질의는 그룹핑 이전에 개수를 제한한 이후에 GROUP BY 연산을 수행한다.

``` sql
--Limiting rows first and then Group-ordering using ROWNUM
SELECT host_year, MIN(score) FROM history
WHERE ROWNUM BETWEEN 1 AND 5 GROUP BY host_year;
```
```

    host_year  min(score)

> ===================================  
> 2000 '03:41.0' 2004 '01:45.0'
>
```