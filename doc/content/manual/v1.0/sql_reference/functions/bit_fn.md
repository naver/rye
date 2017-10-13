비트 함수와 연산자
==================

비트 연산자
-----------

비트 연산자(Bitwise operator)는 비트 단위로 연산을 수행하며 산술 연산식에서 이용될 수 있다. 피연산자로 정수 타입이 지정되며, **BIT** 타입은 지정될 수 없다. 연산 결과로 **BIGINT** 타입 정수(64비트 정수)를 반환한다. 이때, 하나 이상의 피연산자가 **NULL** 이면 **NULL** 을 반환한다.

아래는 Rye가 지원하는 비트 연산자의 종류에 관한 표이다.

**비트 연산자**

<table>
<colgroup>
<col width="11%" />
<col width="71%" />
<col width="8%" />
<col width="9%" />
</colgroup>
<thead>
<tr class="header">
<th>비트 연산자</th>
<th>설명</th>
<th>조건식</th>
<th>리턴 값</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>&amp;</td>
<td>비트 단위로 <strong>AND</strong> 연산을 수행하고, <strong>BIGINT</strong> 정수를 반환한다.</td>
<td>17 &amp; 3</td>
<td>1</td>
</tr>
<tr class="even">
<td>|</td>
<td>비트 단위로 <strong>OR</strong> 연산을 수행하고, <strong>BIGINT</strong> 정수를 반환한다.</td>
<td>17 | 3</td>
<td>19</td>
</tr>
<tr class="odd">
<td>^</td>
<td>비트 단위로 <strong>XOR</strong> 연산을 수행하고, <strong>BIGINT</strong> 정수를 반환한다.</td>
<td>17 ^ 3</td>
<td>18</td>
</tr>
<tr class="even">
<td>~</td>
<td>단항 연산자이며, 피연산자의 비트를 역으로 전환(<strong>INVERT</strong>)하는 보수 연산을 수행하고, <strong>BIGINT</strong> 정수를 반환한다.</td>
<td>~17</td>
<td>-18</td>
</tr>
<tr class="odd">
<td>&lt;&lt;</td>
<td>왼쪽 피연산자의 비트를 오른쪽 피연산자만큼 왼쪽으로 이동시키는 연산을 수행하고, <strong>BIGINT</strong> 정수를 반환한다.</td>
<td>17 &lt;&lt; 3</td>
<td>136</td>
</tr>
<tr class="even">
<td>&gt;&gt;</td>
<td>왼쪽 피연산자의 비트를 오른쪽 피연산자만큼 오른쪽으로 이동시키는 연산을 수행하고, <strong>BIGINT</strong> 정수를 반환한다.</td>
<td>17 &gt;&gt; 3</td>
<td>2</td>
</tr>
</tbody>
</table>

BIT\_COUNT
----------

**BIT_COUNT (*expr*)**

*expr* 의 모든 비트 중 1로 설정된 비트의 개수를 반환하는 함수이며, 집계 함수는 아니다.

Parameters:
- expr: 정수 타입의 임의의 연산식이다.

Return type: BIGINT
    
``` sql
CREATE GLOBAL TABLE bit_tbl(id INT PRIMARY KEY);
INSERT INTO bit_tbl VALUES (1), (2), (3), (4), (5);

SELECT 1&3&5, 1|3|5, 1^3^5, id, BIT_COUNT(id) FROM bit_tbl WHERE id in(1,3,5);
```
```
  1&3&5   1|3|5   1^3^5   id   bit_count(id)   
===============================================
  1       7       7       1    1               
  1       7       7       3    2               
  1       7       7       5    2               
```
