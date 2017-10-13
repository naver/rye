논리 연산자
===========

논리 연산자(logical operator)는 피연산자로 불리언(boolean) 연산식 또는 **INTEGER** 값으로 평가되는 표현식이 지정되며, 연산 결과로 **TRUE**, **FALSE**, **NULL** 을 반환한다. **INTEGER** 값이 논리식에 사용되는 경우 0은 **FALSE**, 0이 아닌 나머지는 **TRUE** 로 사용된다.

논리 연산자의 종류 및 진리표는 아래와 같다.

**논리 연산자**

<table>
<colgroup>
<col width="15%" />
<col width="70%" />
<col width="13%" />
</colgroup>
<thead>
<tr class="header">
<th>논리 연산자</th>
<th>설명</th>
<th>조건식</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>AND</strong></td>
<td>피연산자가 모두 <strong>TRUE</strong>이면 <strong>TRUE</strong>를 반환한다.</td>
<td>a <strong>AND</strong> b</td>
</tr>
<tr>
<td><strong>OR</strong></td>
<td>피연산자가 모두 <strong>NULL</strong>이 아니고, 하나 이상의 피연산자가 <strong>TRUE</strong>이면 <strong>TRUE</strong>를 반환한다.</td>
<td>a <strong>OR</strong> b</td>
</tr>
<tr>
<td><strong>XOR</strong></td>
<td>피연산자가 모두 <strong>NULL</strong>이 아니고, 두 피연산자의 값이 다르면 <strong>TRUE</strong>를 반환한다.</td>
<td>a <strong>XOR</strong> b</td>
</tr>
<tr>
<td><strong>NOT</strong></td>
<td>단항 연산자이며, 피연산자가 <strong>FALSE</strong>이면 <strong>TRUE</strong>, 피연산자가 <strong>TRUE</strong>이면 <strong>FALSE</strong>를 반환한다.</td>
<td><strong>NOT</strong> a</td>
</tr>
</tbody>
</table>

**논리 연산자의 진리표**

<table>
<colgroup>
<col width="15%" />
<col width="15%" />
<col width="18%" />
<col width="16%" />
<col width="15%" />
<col width="18%" />
</colgroup>
<thead>
<tr class="header">
<th>a</th>
<th>b</th>
<th>a AND b</th>
<th>a OR b</th>
<th>NOT a</th>
<th>a XOR b</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><strong>TRUE</strong></td>
<td><strong>TRUE</strong></td>
<td>TRUE</td>
<td>TRUE</td>
<td>FALSE</td>
<td>FALSE</td>
</tr>
<tr class="even">
<td><strong>TRUE</strong></td>
<td><strong>FALSE</strong></td>
<td>FALSE</td>
<td>TRUE</td>
<td>FALSE</td>
<td>TRUE</td>
</tr>
<tr class="odd">
<td><strong>TRUE</strong></td>
<td><strong>NULL</strong></td>
<td>NULL</td>
<td>TRUE</td>
<td>FALSE</td>
<td>NULL</td>
</tr>
<tr class="even">
<td><strong>FALSE</strong></td>
<td><strong>TRUE</strong></td>
<td>FALSE</td>
<td>TRUE</td>
<td>TRUE</td>
<td>TRUE</td>
</tr>
<tr class="odd">
<td><strong>FALSE</strong></td>
<td><strong>FALSE</strong></td>
<td>FALSE</td>
<td>FALSE</td>
<td>TRUE</td>
<td>FALSE</td>
</tr>
<tr class="even">
<td><strong>FALSE</strong></td>
<td><strong>NULL</strong></td>
<td>FALSE</td>
<td>NULL</td>
<td>TRUE</td>
<td>NULL</td>
</tr>
</tbody>
</table>


| 테이블 | 설명 | 비고 |
|--------|--------|--------|
| **UNION**   | 합집합  | **UNION ALL** 이면        |
| **DIFFERENCE**   | 차집합  | **EXCEPT** 연산자와 동일        |


