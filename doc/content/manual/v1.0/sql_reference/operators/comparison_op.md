비교 연산자
===========

비교 연산자(comparison operator)는 왼쪽 피연산자와 오른쪽 피연산자를 비교하여 1 또는 0을 반환한다. 비교 연산의 피연산자들은 같은 데이터 타입이어야 하므로, 시스템에 의해서 묵시적으로 타입이 변환되거나 사용자에 의해 명시적으로 타입이 변환되어야 한다.

**구문 1**
```
    <expression> <comparison_operator> <expression>

        <expression> ::=
            character_string |
            numeric_value |
            NULL

        <comparison_operator> ::=
            = |
            <=> |
            <> |
            != |
            > |
            < |
            >= |
            <=
```
**구문 2**
```
    <expression> IS [NOT] <boolean_value>

        <expression> ::=
            character_string |
            numeric_value |
            NULL

    <boolean_value> ::=
        { UNKNOWN | NULL } |
        TRUE |
        FALSE
```
-   &lt;*expression*&gt;: 비교할 수식을 선언한다.
    -   *character\_string*: 문자 코드와 연관된 정렬 체계(collation)에 의해 비교 순서가 결정된다. 서로 다른 길이의 문자열을 비교할 때는, 비교 전에 길이가 긴 문자열의 길이와 같아지도록 길이가 짧은 문자열 뒤에 공백을 추가한다.
    -   *numeric\_value*: 모든 숫자 값에 대해 불리언(Boolean)을 수행할 수 있으며, 모든 비교 연산자를 이용하여 비교 연산을 수행할 수 있다. 서로 다른 숫자 타입을 비교할 때에는 시스템이 묵시적으로 타입을 변환한다. 예를 들어, **INTEGER** 값을 **DECIMAL** 값과 비교할 때 시스템은 먼저 **INTEGER** 를 **DECIMAL** 로 변환한 후 비교한다.
    -   **NULL**: **NULL** 값은 모든 데이터 타입의 값 범위 내에 포함되지 않는다. 따라서, **NULL** 값의 비교는 주어진 값이 **NULL** 값인지 아닌지에 대한 비교만 가능하다. **NULL** 값이 다른 데이터 타입으로 할당될 때 묵시적인 타입 변경은 일어나지 않는다. 예를 들어, **INTEGER** 타입의 칼럼이 **NULL** 값을 가지고 있고 부동 소수점 타입과 비교할 때, 비교하기 전에 **NULL** 값을 **DOUBLE** 형으로 변환하지 않는다. **NULL** 값에 대한 비교 연산은 결과를 반환하지 않는다.

다음은 Rye에서 지원되는 비교 연산자의 설명 및 리턴 값을 나타낸 표이다.

**비교 연산자**

<table>
<colgroup>
<col width="16%" />
<col width="61%" />
<col width="11%" />
<col width="11%" />
</colgroup>
<thead>
<tr class="header">
<th>비교 연산자</th>
<th>설명</th>
<th>조건식</th>
<th>리턴 값</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><strong>=</strong></td>
<td>일반 등호이며, 두 피연산자의 값이 같은지 비교한다. 하나 이상의 피연산자가 <strong>NULL</strong> 이면 <strong>NULL</strong> 을 반환한다.</td>
<td>1=2 1=NULL</td>
<td>0 NULL</td>
</tr>
<tr class="even">
<td><strong>&lt;=&gt;</strong></td>
<td>NULL safe 등호이며, <strong>NULL</strong> 을 포함하여 두 피연산자의 값이 같은지 비교한다. 피연산자가 모두 <strong>NULL</strong> 이면 1을 반환한다.</td>
<td>1&lt;=&gt;2 1&lt;=&gt;NULL</td>
<td>0 0</td>
</tr>
<tr class="odd">
<td><strong>&lt;&gt;, !=</strong></td>
<td>두 피연산자의 값이 다른지 비교한다. 하나 이상의 피연산자가 <strong>NULL</strong> 이면 <strong>NULL</strong> 을 반환한다.</td>
<td>1&lt;&gt;2</td>
<td>1</td>
</tr>
<tr class="even">
<td><strong>&gt;</strong></td>
<td>왼쪽 피연산자가 오른쪽 피연산자보다 값이 큰지 비교한다. 하나 이상의 피연산자가 <strong>NULL</strong> 이면 <strong>NULL</strong> 을 반환한다.</td>
<td>1&gt;2</td>
<td>0</td>
</tr>
<tr class="odd">
<td><strong>&lt;</strong></td>
<td>왼쪽 피연산자가 오른쪽 피연산자보다 값이 작은지 비교한다. 하나 이상의 피연산자가 <strong>NULL</strong> 이면 <strong>NULL</strong> 을 반환한다.</td>
<td>1&lt;2</td>
<td>1</td>
</tr>
<tr class="even">
<td><strong>&gt;=</strong></td>
<td>왼쪽 피연산자가 오른쪽 피연산자보다 값이 크거나 같은지 비교한다. 하나 이상의 피연산자가 <strong>NULL</strong> 이면 <strong>NULL</strong> 을 반환한다.</td>
<td>1&gt;=2</td>
<td>0</td>
</tr>
<tr class="odd">
<td><strong>&lt;=</strong></td>
<td>왼쪽 피연산자가 오른쪽 피연산자보다 값이 작거나 같은지 비교한다. 하나 이상의 피연산자가 <strong>NULL</strong> 이면 <strong>NULL</strong> 을 반환한다.</td>
<td>1&lt;=2</td>
<td>1</td>
</tr>
<tr class="even">
<td><strong>IS</strong> <em>boolean_value</em></td>
<td>왼쪽 피연산자가 오른쪽 불리언 값과 같은지 비교한다. 불리언 값은 <strong>TRUE</strong>, <strong>FALSE</strong>, <strong>NULL</strong> 이 될 수 있다.</td>
<td>1 IS FALSE</td>
<td>0</td>
</tr>
<tr class="odd">
<td><strong>IS NOT</strong> <em>boolean_value</em></td>
<td>왼쪽 피연산자가 오른쪽 불리언 값과 다른지 비교한다. 불리언 값은 <strong>TRUE</strong>, <strong>FALSE</strong>, <strong>NULL</strong> 이 될 수 있다.</td>
<td>1 IS NOT FALSE</td>
<td>1</td>
</tr>
</tbody>
</table>

다음은 비교 연산자를 사용하는 예이다.

``` sql
SELECT (1 <> 0); -- TRUE이므로 1을 출력한다.
SELECT (1 != 0); -- TRUE이므로 1을 출력한다.
SELECT (1 = '1'); -- 문자열 타입을 숫자로 변경해서 비교하므로 1을 출력한다.
SELECT (1 = NULL); -- NULL을 출력한다.
SELECT (1 <=> NULL); -- FALSE이므로 0을 출력한다.
SELECT (1.000 = 1); -- TRUE이므로 1을 출력한다.
SELECT ('rye' = 'Rye'); -- 대소문자를 구분하지 않으므로 1을 출력한다.
SELECT ('rye' = 'rye'); -- TRUE이므로 1을 출력한다.
```
```
  (1<>0)   
===========
  1        

  (1<>0)   
===========
  1        

  (1='1')   
============
  1         

  (1=null)   
=============
  NULL       

  (1<=>null)   
===============
  0            

  (1.000=1)   
==============
  1           

  ('rye'='Rye')   
==================
  1               

  ('rye'='rye')   
==================
  1               
```
