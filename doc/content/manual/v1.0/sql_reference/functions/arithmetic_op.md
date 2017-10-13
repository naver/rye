산술 연산자
===========

산술 연산자
-----------

산술 연산자는 덧셈, 뺄셈, 곱셈, 나눗셈을 위한 이항(binary) 연산자와 양수, 음수를 나타내기 위한 단항(unary) 연산자가 있다. 양수/음수의 부호를 나타내는 단항 연산자의 연산 우선순위가 이항 연산자보다 높다.

    <expression>  <mathematical_operator>  <expression>

        <expression> ::=
            bit_string |
            character_string |
            numeric_value |
            date-time_value |
            collection_value |
            NULL

        <mathematical_operator> ::=
            <set_arithmetic_operator> |
            <arithmetic_operator>

                <arithmetic_operator> ::=
                    + |
                    - |
                    * |
                    { / | DIV } |
                    { % | MOD }

                <set_arithmetic_operator> ::=
                    UNION |
                    DIFFERENCE |
                    { INTERSECT | INTERSECTION }

-   &lt;*expression*&gt;: 연산을 수행할 수식을 선언한다.
-   &lt;*mathematical\_operator*&gt;: 수학적 연산을 지정하는 연산자로서, 산술 연산자와 집합 연산자가 있다.
    -   &lt;*set\_arithmetic\_operator*&gt;: 컬렉션 타입의 피연산자에 대해 합집합, 차집합, 교집합을 수행하는 집합 산술 연산자이다.
    -   &lt;*arithmetic\_operator*&gt;: 사칙 연산을 수행하기 위한 연산자이다.

다음은 Rye가 지원하는 산술 연산자의 설명 및 리턴 값을 나타낸 표이다.

**산술 연산자**

<table style="width:100%;">
<colgroup>
<col width="10%" />
<col width="66%" />
<col width="9%" />
<col width="12%" />
</colgroup>
<thead>
<tr class="header">
<th>산술 연산자</th>
<th><strong>설명</strong></th>
<th>연산식</th>
<th>리턴 값</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><strong>+</strong></td>
<td>더하기 연산</td>
<td>1+2</td>
<td>3</td>
</tr>
<tr class="even">
<td><strong>-</strong></td>
<td>빼기 연산</td>
<td>1-2</td>
<td>-1</td>
</tr>
<tr class="odd">
<td><strong>*</strong></td>
<td>곱하기 연산</td>
<td>1*2</td>
<td>2</td>
</tr>
<tr class="even">
<td><strong>/</strong></td>
<td>나누기 연산 후, 몫을 반환한다.</td>
<td>1/2.0</td>
<td>0.500000000</td>
</tr>
<tr class="odd">
<td><strong>DIV</strong></td>
<td>나누기 연산 후, 몫을 반환한다. 피연산자는 정수 타입이어야 하며, 정수를 반환한다.</td>
<td>1 DIV 2</td>
<td>0</td>
</tr>
<tr class="even">
<td><strong>%</strong> , <strong>MOD</strong></td>
<td>나누기 연산 후, 나머지를 반환한다. 피연산자는 정수 타입이어야 하며, 정수를 반환한다. 피연산자가 실수이면 <strong>MOD</strong> 함수를 이용한다.</td>
<td>1 % 2 1 MOD 2</td>
<td>1</td>
</tr>
</tbody>
</table>

수치형 데이터 타입의 산술 연산과 타입 변환
------------------------------------------

모든 수치형 데이터 타입을 산술 연산에 사용할 수 있으며, 연산 결과 타입은 피연산자의 데이터 타입과 연산의 종류에 따라 다르다. 아래는 피연산자 타입별 덧셈/뺄셈/곱셈 연산의 결과 데이터 타입을 정리한 표이다.

**피연산자의 타입별 결과 데이터 타입**

<table>
<colgroup>
<col width="20%" />
<col width="20%" />
<col width="20%" />
<col width="20%" />
</colgroup>
<thead>
<tr class="header">
<th></th>
<th><strong>INT</strong></th>
<th><strong>NUMERIC</strong></th>
<th><strong>DOUBLE</strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><strong>INT</strong></td>
<td>INT 또는 BIGINT</td>
<td>NUMERIC</td>
<td>DOUBLE</td>
</tr>
<tr class="even">
<td><strong>NUMERIC</strong></td>
<td>NUMERIC</td>
<td>NUMERIC (p와 s도 변환됨)</td>
<td>DOUBLE</td>
</tr>
<tr class="odd">
<td><strong>DOUBLE</strong></td>
<td>DOUBLE</td>
<td>DOUBLE</td>
<td>DOUBLE</td>
</tr>
</tbody>
</table>

피연산자가 모두 동일한 데이터 타입이면 연산 결과의 타입이 변환되지 않으나, 나누기 연산의 경우 예외적으로 타입이 변환되므로 주의해야 한다. 분모, 즉 제수(divisor)가 0이면 에러가 발생한다.

아래는 피연산자가 모두 **NUMERIC** 타입인 경우, 연산 결과의 전체 자릿수(*p*)와 소수점 아래 자릿수(*s*)를 정리한 표이다.

**NUMERIC 타입의 연산 결과**

<table>
<colgroup>
<col width="16%" />
<col width="64%" />
<col width="19%" />
</colgroup>
<thead>
<tr class="header">
<th>연산</th>
<th>결과의 최대 자릿수</th>
<th>결과의 소수점 이하 자릿수</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>N(p1, s1) + N(p2, s2)</td>
<td>max(p1-s1, p2-s2)+max(s1, s2) +1</td>
<td>max(s1, s2)</td>
</tr>
<tr class="even">
<td>N(p1, s1) - N(p2, s2)</td>
<td>max(p1-s1, p2-s2)+max(s1, s2)</td>
<td>max(s1, s2)</td>
</tr>
<tr class="odd">
<td>N(p1, s1) * N(p2, s2)</td>
<td>p1+p2+1</td>
<td>s1+s2</td>
</tr>
<tr class="even">
<td>N(p1, s1) / N(p2, s2)</td>
<td>s2 &gt; 0 이면 Pt = p1+max(s1, s2) + s2 - s1, 그 외에는 Pt = p1라 하고, s1 &gt; s2 이면 St = s1, 그 외에는 s2라 하면, 소수점 이하 자릿수는 St &lt; 9 이면 min(9-St, 38-Pt) + St, 그 외에는 St</td>
<td></td>
</tr>
</tbody>
</table>

**예제**

```sql
--int * int
SELECT 123*123;
```
```
    123*123

 =============  
 15129
```
``` sql
-- int * int returns overflow error
SELECT (1234567890123*1234567890123);
```
```
    ERROR: Data overflow on data type bigint.
```

``` sql
-- int * numeric returns numeric type  
SELECT (1234567890123*CAST(1234567890123 AS NUMERIC(15,2)));
```
```
    (1234567890123* cast(1234567890123 as numeric(15,2)))

 ======================  
 1524157875322755800955129.00
```
``` sql
-- int * double returns double type
SELECT (1234567890123*CAST(1234567890123 AS DOUBLE));
```
```
    (1234567890123* cast(1234567890123 as double))

 ================================================  
 1.524157875322756e+024
```
``` sql
-- numeric * numeric returns numeric type   
SELECT (CAST(1234567890123 AS NUMERIC(15,2))*CAST(1234567890123 AS NUMERIC(15,2)));
```
```
    ( cast(1234567890123 as numeric(15,2))* cast(1234567890123 as numeric(15,2)))

 ======================  
 1524157875322755800955129.0000
```
``` sql
-- numeric * double returns double type  
SELECT (CAST(1234567890123 AS NUMERIC(15,2))*CAST(1234567890123 AS DOUBLE));
```
```
    ( cast(1234567890123 as numeric(15,2))* cast(1234567890123 as double))

 ========================================================================  
 1.524157875322756e+024
```
``` sql
-- double * double returns double type  
SELECT (CAST(1234567890123 AS DOUBLE)*CAST(1234567890123 AS DOUBLE));
```
```
    ( cast(1234567890123 as double)* cast(1234567890123 as double))

 =================================================================  
 1.524157875322756e+024
```
``` sql
-- int / int returns int type without type conversion or rounding
SELECT 100100/100000;
```
```
    100100/100000

 ===============  
 1
```
``` sql
-- int / int returns int type without type conversion or rounding
SELECT 100100/200200;
```
```
    100100/200200

 ===============  
 0
```
``` sql
-- int / zero returns error
SELECT 100100/(100100-100100);
```
```
    ERROR: Attempt to divide by zero.
```
