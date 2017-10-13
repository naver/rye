날짜/시간 함수와 연산자
=======================

ADDDATE, DATE_ADD
------------------

- **ADDDATE (date, INTERVAL expr unit)**
- **ADDDATE (date, days)**
- **DATE_ADD (date, INTERVAL expr unit)**

**ADDDATE** 함수와 **DATE_ADD** 함수는 동일하며, 특정 **DATE** 값에 대해 덧셈 또는 뺄셈을 실행한다. 리턴 값은 **DATE** 타입 또는 **DATETIME** 타입이다. **DATETIME** 타입을 반환하는 경우는 다음과 같다.

 - 첫 번째 인자가 **DATETIME** 타입인 경우
 - 첫 번째 인자가 **DATE** 타입이고 **INTERVAL** 값의 단위가 날짜 단위 미만으로 지정된 경우

위의 경우 외에 **DATETIME** 타입의 결과 값을 반환하려면 :ref:`CAST` 함수를 이용하여 첫 번째 인자 값의 타입을 변환해야 한다. 연산 결과의 날짜가 해당 월의 마지막 날짜를 초과하면, 해당 월의 말일을 적용하여 유효한 **DATE** 값을 반환한다.

입력 인자의 날짜와 시간 값이 모두 0이면 에러를 반환한다.

계산 결과가 '0000-00-00 00:00:00'과 '0001-01-01 00:00:00' 사이이면, 날짜와 시간 값이 모두 0인 **DATE** 또는 **DATETIME** 타입의 값을 반환한다. 그러나 JDBC 프로그램에서는 연결 URL 속성인 zeroDateTimeBehavior의 설정에 따라 동작이 달라진다. JDBC의 연결 URL 속성은 :ref:`jdbc-connection-conf` 을 참고한다.

 - date: **DATE** 또는 **DATETIME** 타입의 연산식이며, 시작 날짜를 의미한다. 만약, '2006-07-00'와 같이 유효하지 않은 **DATE** 값이 지정되면, 에러를 반환한다.
 - expr: 시작 날짜로부터 더할 시간 간격 값(interval value)을 의미하며, **INTERVAL** 키워드 뒤에 음수가 명시되면 시작 날짜로부터 시간 간격 값을 뺀다.
 - unit: *expr* 수식에 명시된 시간 간격 값의 단위를 의미하며, 아래의 테이블을 참고하여 시간 간격 값 해석을 위한 형식을 지정할 수 있다. *expr* 의 단위 값이 *unit* 에서 요구하는 단위 값의 개수보다 적을 경우 가장 작은 단위부터 채운다. 예를 들어, **HOUR_SECOND** 의 경우 'HOURS:MINUTES:SECONDS'와 같이 3개의 값이 요구되는데, "1:1" 처럼 2개의 값만 주어지면 'MINUTES:SECONDS'로 간주한다.
 - return type: **DATE** 또는 **DATETIME** 
    
**unit에 대한 expr 형식**

<table>
<colgroup>
<col width="16%" />
<col width="34%" />
<col width="49%" />
</colgroup>
<thead>
<tr class="header">
<th>unit 값</th>
<th>expr 형식</th>
<th>예</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>MILLISECOND</td>
<td>MILLISECONDS</td>
<td>ADDDATE(SYSDATE, INTERVAL 123 MILLISECOND)</td>
</tr>
<tr class="even">
<td>SECOND</td>
<td>SECONDS</td>
<td>ADDDATE(SYSDATE, INTERVAL 123 SECOND)</td>
</tr>
<tr class="odd">
<td>MINUTE</td>
<td>MINUTES</td>
<td>ADDDATE(SYSDATE, INTERVAL 123 MINUTE)</td>
</tr>
<tr class="even">
<td>HOUR</td>
<td>HOURS</td>
<td>ADDDATE(SYSDATE, INTERVAL 123 HOUR)</td>
</tr>
<tr class="odd">
<td>DAY</td>
<td>DAYS</td>
<td>ADDDATE(SYSDATE, INTERVAL 123 DAY)</td>
</tr>
<tr class="even">
<td>WEEK</td>
<td>WEEKS</td>
<td>ADDDATE(SYSDATE, INTERVAL 123 WEEK)</td>
</tr>
<tr class="odd">
<td>MONTH</td>
<td>MONTHS</td>
<td>ADDDATE(SYSDATE, INTERVAL 12 MONTH)</td>
</tr>
<tr class="even">
<td>QUARTER</td>
<td>QUARTERS</td>
<td>ADDDATE(SYSDATE, INTERVAL 12 QUARTER)</td>
</tr>
<tr class="odd">
<td>YEAR</td>
<td>YEARS</td>
<td>ADDDATE(SYSDATE, INTERVAL 12 YEAR)</td>
</tr>
<tr class="even">
<td>SECOND_MILLISECOND</td>
<td>'SECONDS.MILLISECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12.123' SECOND_MILLISECOND)</td>
</tr>
<tr class="odd">
<td>MINUTE_MILLISECOND</td>
<td>'MINUTES:SECONDS.MILLISECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12:12.123' MINUTE_MILLISECOND)</td>
</tr>
<tr class="even">
<td>MINUTE_SECOND</td>
<td>'MINUTES:SECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12:12' MINUTE_SECOND)</td>
</tr>
<tr class="odd">
<td>HOUR_MILLISECOND</td>
<td>'HOURS:MINUTES:SECONDS.MILLISECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12:12:12.123' HOUR_MILLISECOND)</td>
</tr>
<tr class="even">
<td>HOUR_SECOND</td>
<td>'HOURS:MINUTES:SECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12:12:12' HOUR_SECOND)</td>
</tr>
<tr class="odd">
<td>HOUR_MINUTE</td>
<td>'HOURS:MINUTES'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12:12' HOUR_MINUTE)</td>
</tr>
<tr class="even">
<td>DAY_MILLISECOND</td>
<td>'DAYS HOURS:MINUTES:SECONDS.MILLISECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12 12:12:12.123' DAY_MILLISECOND)</td>
</tr>
<tr class="odd">
<td>DAY_SECOND</td>
<td>'DAYS HOURS:MINUTES:SECONDS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12 12:12:12' DAY_SECOND)</td>
</tr>
<tr class="even">
<td>DAY_MINUTE</td>
<td>'DAYS HOURS:MINUTES'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12 12:12' DAY_MINUTE)</td>
</tr>
<tr class="odd">
<td>DAY_HOUR</td>
<td>'DAYS HOURS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12 12' DAY_HOUR)</td>
</tr>
<tr class="even">
<td>YEAR_MONTH</td>
<td>'YEARS-MONTHS'</td>
<td>ADDDATE(SYSDATE, INTERVAL '12-13' YEAR_MONTH)</td>
</tr>
</tbody>
</table>

``` sql
SELECT ADDDATE('2017-07-03',INTERVAL 24 HOUR), ADDDATE('2017-07-03', 1);
```
```
  date_add('2017-07-03', INTERVAL 24 HOUR)   adddate('2017-07-03', 1)   
========================================================================
  '2017-07-04 00:00:00.000'                  '2017-07-04'               
```

``` sql
--it subtracts days when argument < 0
SELECT ADDDATE('2017-07-03',INTERVAL -24 HOUR), ADDDATE('2017-07-03', -1);
```
```
  date_add('2017-07-03', INTERVAL -24 HOUR)   adddate('2017-07-03', -1)   
==========================================================================
  '2017-07-02 00:00:00.000'                   '2017-07-02'                
```

``` sql
--when expr is not fully specified for unit
SELECT ADDDATE('2017-07-03 12:00:00', INTERVAL '1:20' HOUR_SECOND);
```
```
  date_add('2017-07-03 12:00:00', INTERVAL '1:20' HOUR_SECOND)   
=================================================================
  '2017-07-03 12:01:20.000'                                      
```

``` sql
SELECT ADDDATE('0000-00-00', 1 );
```
```
ERROR: Conversion error in date format.
```

``` sql
SELECT ADDDATE('0001-01-01 00:00:00', -1);
```
```
  adddate('0001-01-01 00:00:00', -1)   
=======================================
  '0000-00-00 00:00:00.000'            
```

CURDATE, CURRENT_DATE, SYS_DATE, SYSDATE
------------------------------------------

- **CURDATE ()**
- **CURRENT_DATE ()**
- **CURRENT_DATE**
- **SYS_DATE**
- **SYSDATE**

**CURDATE** (), **CURRENT_DATE** (), **CURRENT_DATE**, **SYS_DATE**, **SYSDATE** 는 모두 동일하며, 현재 날짜를 **DATE** 타입(*YYYY*-*MM*-*DD*)으로 반환한다. 
입력 인자의 연, 월, 일이 모두 0이면 에러를 반환한다.
 - return type: DATE
    
``` sql
SELECT CURDATE(), CURRENT_DATE(), CURRENT_DATE, SYS_DATE, SYSDATE;
```
```
  SYS_DATE       SYS_DATE       SYS_DATE       SYS_DATE       SYS_DATE       
=============================================================================
  '2017-07-03'   '2017-07-03'   '2017-07-03'   '2017-07-03'   '2017-07-03'   
```

CURRENT_DATETIME, NOW, SYS_DATETIME, SYSDATETIME
--------------------------------------------------

- **CURRENT_DATETIME ()**
- **CURRENT_DATETIME**
- **NOW ()**
- **SYS_DATETIME**
- **SYSDATETIME**

**CURRENT_DATETIME**, **CURRENT_DATETIME** (), **NOW** (), **SYS_DATETIME**, **SYSDATETIME** 는 동일하며, 현재 날짜를 **DATETIME** 타입으로 반환한다. 

 - return type: DATETIME
    
``` sql
SELECT NOW(), SYS_DATETIME;
```
```
  SYS_DATETIME                SYS_DATETIME                
==========================================================
  '2017-07-03 17:51:26.622'   '2017-07-03 17:51:26.622'   
```

CURTIME, CURRENT_TIME, SYS_TIME, SYSTIME
------------------------------------------

- **CURTIME ()**
- **CURRENT_TIME**
- **CURRENT_TIME ()**
- **SYS_TIME**
- **SYSTIME**

**CURTIME** (), **CURRENT_TIME**, **CURRENT_TIME** (), **SYS_TIME**, **SYSTIME** 는 모두 동일하며, 현재 시간을 **TIME** 타입(*HH*:*MI*:*SS*)으로 반환한다. 

 - return type: TIME
    
``` sql
SELECT CURTIME(), CURRENT_TIME(), CURRENT_TIME, SYS_TIME, SYSTIME;
```
```
  SYS_TIME     SYS_TIME     SYS_TIME     SYS_TIME     SYS_TIME     
===================================================================
  '17:51:26'   '17:51:26'   '17:51:26'   '17:51:26'   '17:51:26'   
```

DATE
----

**DATE (date)**

**DATE** 함수는 지정된 인자로부터 날짜 부분을 추출하여 '*MM*/*DD*/*YYYY*' 형식 문자열로 반환한다. 지정 가능한 인자는 **DATE**, **DATETIME** 타입이며, 리턴 값은 **VARCHAR** 타입이다

인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 날짜와 시간이 모두 0인 값을 입력한 경우에는 연, 월, 일 값이 모두 0인 문자열을 반환한다.

 - date: **DATE**, **DATETIME** 타입이 지정될 수 있다.
 - return type: STRING
    
``` sql
SELECT DATE('2010-02-27 15:10:23');
```
```
  date('2010-02-27 15:10:23')   
================================
  '02/27/2010'                  
```

``` sql
SELECT DATE(NOW());
```
```
  date( SYS_DATETIME )   
=========================
  '07/03/2017'           
```

``` sql
SELECT DATE('0000-00-00 00:00:00');
```
```
  date('0000-00-00 00:00:00')   
================================
  '00/00/0000'                  
```

DATEDIFF
--------

**DATEDIFF (date1, date2)**

**DATEDIFF** 함수는 주어진 두 개의 인자로부터 날짜 부분을 추출하여 두 값의 차이를 일 단위 정수로 반환한다. 지정 가능한 인자는 **DATE**, **DATETIME** 타입이며, 리턴 값의 타입은 **INTEGER** 이다.

입력 인자의 날짜와 시간 값이 모두 0이면 에러를 반환한다.

 - date1,date2: 날짜를 포함하는 타입(**DATE**, **TIMESTAMP**, **DATETIME**) 또는 해당 타입의 값을 나타내는 문자열이 지정될 수 있다. 유효하지 않은 문자열이 지정되면 에러를 반환한다.
 - return type: INT
    
``` sql
SELECT DATEDIFF('2010-2-28 23:59:59','2010-03-02');
```
```
  datediff('2010-2-28 23:59:59', '2010-03-02')   
=================================================
  -2                                             
```

``` sql
SELECT DATEDIFF('0000-00-00 00:00:00', '2010-2-28 23:59:59');
```
```
  datediff('0000-00-00 00:00:00', '2010-2-28 23:59:59')   
==========================================================
  -734196                                                 
```

DATE_SUB, SUBDATE
------------------

- **DATE_SUB (date, INTERVAL expr unit)**
- **SUBDATE(date, INTERVAL expr unit)**
- **SUBDATE(date, days)**

**DATE_SUB** ()와 **SUBDATE** ()는 동일하며, 특정 **DATE** 값에 대해 뺄셈 또는 덧셈을 실행한다. 리턴 값은 **DATE** 타입 또는 **DATETIME** 타입이다. 연산 결과의 날짜가 해당 월의 마지막 날짜를 초과하면, 해당 월의 말일을 적용하여 유효한 **DATE** 값을 반환한다.

입력 인자의 날짜와 시간 값이 모두 0이면 에러를 반환한다.

계산 결과가 '0000-00-00 00:00:00'과 '0001-01-01 00:00:00' 사이이면, 날짜와 시간 값이 모두 0인 **DATE** 또는 **DATETIME** 타입의 값을 반환한다. 그러나 JDBC 프로그램에서는 연결 URL 속성인 zeroDateTimeBehavior의 설정에 따라 동작이 달라진다("API 레퍼런스 > JDBC API > JDBC 프로그래밍 > 연결 설정" 참고).

 - date: **DATE** 또는 **DATETIME** 타입의 연산식이며, 시작 날짜를 의미한다. 만약, '2006-07-00'와 같이 유효하지 않은 **DATE** 값이 지정되면, 에러를 반환한다.
 - expr: 시작 날짜로부터 뺄 시간 간격 값(interval value)을 의미하며, **INTERVAL** 키워드 뒤에 음수가 명시되면 시작 날짜로부터 시간 간격 값을 더한다.
 - unit: *expr* 수식에 명시된 시간 간격 값의 단위를 의미하며, *unit* 값에 대한 *expr* 인자의 값은 :func:`ADDDATE` 의 표를 참고한다.
 - return type: DATE or DATETIME
    
``` sql
SELECT SUBDATE('2017-07-03',INTERVAL 24 HOUR), SUBDATE('2017-07-03', 1);
```
```
  date_sub('2017-07-03', INTERVAL 24 HOUR)   subdate('2017-07-03', 1)   
========================================================================
  '2017-07-02 00:00:00.000'                  '2017-07-02'               
```

``` sql
--it adds days when argument < 0
SELECT SUBDATE('2017-07-03',INTERVAL -24 HOUR), SUBDATE('2017-07-03', -1);
```
```
  date_sub('2017-07-03', INTERVAL -24 HOUR)   subdate('2017-07-03', -1)   
==========================================================================
  '2017-07-04 00:00:00.000'                   '2017-07-04'                
```

``` sql
SELECT SUBDATE('0000-00-00 00:00:00', -50);
```
```
ERROR: Conversion error in date format.
```

``` sql
SELECT SUBDATE('0001-01-01 00:00:00', 10);
```
```
  subdate('0001-01-01 00:00:00', 10)   
=======================================
  '0000-00-00 00:00:00.000'            
```

DAY, DAYOFMONTH
---------------

- **DAY (date)**
- **DAYOFMONTH (date)**

**DAY** 함수와 **DAYOFMONTH** 함수는 동일하며, 지정된 인자로부터 1~31 범위의 일(day)을 반환한다. 인자로 **DATE**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 연, 월, 일이 모두 0인 값을 입력한 경우에는 0을 반환한다.

- date: 날짜
- return type: INT

``` sql
SELECT DAYOFMONTH('2010-09-09');
```
```
  dayofmonth('2010-09-09')   
=============================
  9                          
```

``` sql
SELECT DAY('2010-09-09 19:49:29');
```
```
  day('2010-09-09 19:49:29')   
===============================
  9                            
```

``` sql
SELECT DAYOFMONTH('0000-00-00 00:00:00');
```
```
  dayofmonth('0000-00-00 00:00:00')   
======================================
  0                                   
```

DAYOFWEEK
---------

**DAYOFWEEK (date)**

**DAYOFWEEK** 함수는 지정된 인자로부터 1~7 범위의 요일(1: 일요일, 2: 월요일, ..., 7: 토요일)을 반환한다. 요일 인덱스는 ODBC 표준과 같다. 인자로 **DATE**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

입력 인자의 연, 월, 일이 모두 0이면 에러를 반환한다.

- date: 날짜
- return type: INT
    
``` sql
SELECT DAYOFWEEK('2010-09-09');
```
```
  dayofweek('2010-09-09')   
============================
  5                         
```

``` sql
SELECT DAYOFWEEK('2010-09-09 19:49:29');
```
```
  dayofweek('2010-09-09 19:49:29')   
=====================================
  5                                  
```

``` sql
SELECT DAYOFWEEK('0000-00-00');
```
```
ERROR: Conversion error in date format.
```

DAYOFYEAR
---------

**DAYOFYEAR (date)**

**DAYOFYEAR** 함수는 지정된 인자로부터 1~366 범위의 일(day of year)을 반환한다. 인자로 **DATE**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

입력 인자의 날짜 값이 모두 0이면 에러를 반환한다.

- date: 날짜
- return type: INT


``` sql
SELECT DAYOFYEAR('2010-09-09');
```
```
  dayofyear('2010-09-09')   
============================
  252                       
```

``` sql
SELECT DAYOFYEAR('2010-09-09 19:49:29');
```
```
  dayofyear('2010-09-09 19:49:29')   
=====================================
  252                                
```

``` sql
SELECT DAYOFYEAR('0000-00-00');
```
```
ERROR: Conversion error in date format.
```

EXTRACT
-------

**EXTRACT ( field FROM date-time_argument )**

**EXTRACT** 연산자는 날짜/시간 값을 반환하는 연산식 *date-time_argument* 중 일부분을 추출하여 **INTEGER** 타입으로 반환한다. 
    
입력 인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 날짜와 시간이 모두 0인 값을 입력한 경우에는 0을 반환한다.

- field: 날짜/시간 수식에서 추출할 값을 지정한다. (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND)
- date-time_argument: 날짜/시간 값을 반환하는 연산식이다. 이 연산식의 값은 **TIME**, **DATE**, **DATETIME** 타입 중 하나여야 하며, **NULL** 이 지정된 경우에는 **NULL** 값이 반환된다.
- return type: INT

``` sql
SELECT EXTRACT(MONTH FROM DATETIME '2008-12-25 10:30:20.123' );
```
```
  extract(month  from datetime '2008-12-25 10:30:20.123')   
============================================================
  12                                                        
```

``` sql
SELECT EXTRACT(HOUR FROM DATETIME '2008-12-25 10:30:20.123' );
```
```
  extract(hour  from datetime '2008-12-25 10:30:20.123')   
===========================================================
  10                                                       
```

``` sql
SELECT EXTRACT(MILLISECOND FROM DATETIME '2008-12-25 10:30:20.123' );
```
```
  extract(millisecond  from datetime '2008-12-25 10:30:20.123')   
==================================================================
  123                                                             
```

``` sql
SELECT EXTRACT(MONTH FROM '0000-00-00 00:00:00');
```
```
  extract(month  from '0000-00-00 00:00:00')   
===============================================
  0                                            
```

FROM_DAYS
----------

**FROM_DAYS (N)**

**FROM_DAYS** 함수는 **INTEGER** 타입을 인자로 입력하면 **DATE** 타입의 날짜를 반환한다.

**FROM_DAYS** 함수는 그레고리력(Gregorian Calendar) 출현(1582년) 이전은 고려하지 않았으므로 1582년 이전의 날짜에 대해서는 사용하지 않는 것을 권장한다.

인자로 0~3,652,424 범위의 정수를 입력할 수 있다. 0~365 범위의 값을 인자로 입력하면 0을 반환한다. 최대값인 3,652,424는 9999년의 마지막 날을 의미한다.

- N: 0~3,652,424 범위의 정수
- return type: DATE
    
``` sql
SELECT FROM_DAYS(719528);
```
```
  from_days(719528)   
======================
  '1970-01-01'        
```

``` sql
SELECT FROM_DAYS('366');
```
```
  from_days('366')   
=====================
  '0001-01-03'       
```

``` sql
SELECT FROM_DAYS(3652424);
```
```
  from_days(3652424)   
=======================
  '9999-12-31'         
```

``` sql
SELECT FROM_DAYS(0);
```

FROM_UNIXTIME
--------------

**FROM_UNIXTIME ( unix_timestamp[, format] )**

**FROM_UNIXTIME** 함수는 *format* 인자가 명시된 경우 **VARCHAR** 타입으로 해당 형식의 문자열을 반환하며, *format* 인자가 생략될 경우 **DATETIME** 타입의 값을 반환한다. *unix_timestamp* 인자로 UNIX의 타임스탬프에 해당하는 **INTEGER** 타입을 입력한다. 리턴 값은 현재의 타임 존으로 표현된다.

*format* 에 입력하는 시간 형식은 :ref:`DATE_FORMAT` 의 날짜/시간 형식 2를 따른다.

**DATETIME** 과 UNIX 타임스탬프는 일대일 대응 관계가 아니기 때문에 변환할 때 :func:`UNIX_TIMESTAMP` 함수나 **FROM_UNIXTIME** 함수를 사용하면 값의 일부가 유실될 수 있다. 자세한 설명은 :ref:`UNIX_TIMESTAMP` 를 참고한다.

인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 날짜와 시간이 모두 0인 값을 입력한 경우에는 날짜와 시간 값이 모두 0인 문자열을 반환한다. 그러나 JDBC 프로그램에서는 연결 URL 속성인 zeroDateTimeBehavior의 설정에 따라 동작이 달라진다 :ref:("API 레퍼런스 > JDBC API > JDBC 프로그래밍 > 연결 설정" 참고).

- unix_timestamp: 양의 정수
- format: 시간 형식. :ref:`DATE_FORMAT` 의 날짜/시간 형식 2를 따른다.
- return type: STRING, INT


``` sql
SELECT FROM_UNIXTIME(1234567890);
```
```
  from_unixtime(1234567890)   
==============================
  '2009-02-14 08:31:30.0'     
```

``` sql
SELECT FROM_UNIXTIME('1000000000');
```
```
  from_unixtime('1000000000')   
================================
  '2001-09-09 10:46:40.0'       
```

``` sql
SELECT FROM_UNIXTIME(1234567890,'%M %Y %W');
```
```
  from_unixtime(1234567890, '%M %Y %W')   
==========================================
  'February 2009 Saturday'                
```

``` sql
SELECT FROM_UNIXTIME('1234567890','%M %Y %W');
```
```
  from_unixtime('1234567890', '%M %Y %W')   
============================================
  'February 2009 Saturday'                  
```

``` sql
SELECT FROM_UNIXTIME(0);
```
```
  from_unixtime(0)          
============================
  '1970-01-01 09:00:00.0'   
```

HOUR
----

**HOUR (time)**

**HOUR** 함수는 지정된 인자로부터 시(hour) 부분을 추출한 정수를 반환한다. 인자로 **TIME**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

- time: 시간
- return type: INT
    
``` sql
SELECT HOUR('12:34:56');
```
```
  hour('12:34:56')   
=====================
  12                 
```

``` sql
SELECT HOUR('2010-01-01 12:34:56');
```
```
  hour('2010-01-01 12:34:56')   
================================
  12                            
```

``` sql
SELECT HOUR(datetime'2010-01-01 12:34:56');
```
```
  hour(datetime '2010-01-01 12:34:56')   
=========================================
  12                                     
```

LAST_DAY
---------

**LAST_DAY ( date_argument )**

**LAST_DAY** 함수는 인자로 지정된 **DATE** 값에서 해당 월의 마지막 날짜 값을 **DATE** 타입으로 반환한다. 
    
입력 인자의 연, 월, 일이 모두 0이면 에러를 반환한다. 
    
- date_argument: **DATE** 타입의 연산식을 지정한다. **DATETIME** 값을 지정하려면 **DATE** 타입으로 명시적 변환을 해야 한다. 값이 **NULL** 이면 **NULL** 을 반환한다.
- return type: DATE
    
``` sql
--it returns last day of the month in DATE type
SELECT LAST_DAY(DATE '1980-02-01'), LAST_DAY(DATE '2010-02-01');
```
```
  last_day(date '1980-02-01')   last_day(date '2010-02-01')   
==============================================================
  '1980-02-29'                  '2010-02-28'                  
```

``` sql
SELECT LAST_DAY(CAST (datetime'2017-07-03 12:00:00' AS DATE));
```
```
  last_day( cast(datetime '2017-07-03 12:00:00' as date))   
============================================================
  '2017-07-31'                                              
```

``` sql
SELECT LAST_DAY('0000-00-00');
```
```
ERROR: Conversion error in date format.
```

MAKEDATE
--------

**MAKEDATE (year, dayofyear)**

**MAKEDATE** 함수는 지정된 인자로부터 날짜를 반환한다. 인자로 1~9999 범위의 연도와 일(day of year)에 해당하는 **INTEGER** 타입을 지정할 수 있으며, 1/1/1~12/31/9999 범위의 **DATE** 타입을 반환한다. 일(day of year)이 해당 연도를 넘어가면 다음 연도가 된다. 예를 들어, MAKEDATE(1999, 366)은 2000-01-01을 반환한다. 단, 연도에 0~69 범위의 값을 입력하면 2000년~2069년으로 처리하고, 70~99 범위의 값을 입력하면 1970년~1999년으로 처리한다.

*year* 와 *dayofyear* 가 모두 0이면 에러를 반환한다.

- year: 1~9999 범위의 연도
- dayofyear: 연도에 0~99의 값을 입력하면 예외적으로 처리하므로, 실제로는 100년 이후의 연도만 사용된다. 따라서 *dayofyear* 의 최대값은 3,615,902이며, MAKEDATE(100, 3615902)는 9999/12/31을 반환한다.
- return type: DATE

``` sql
SELECT MAKEDATE(2010,277);
```
```
  makedate(2010, 277)   
========================
  '2010-10-04'          
```

``` sql
SELECT MAKEDATE(10,277);
```
```
  makedate(10, 277)   
======================
  '2010-10-04'        
```

``` sql
SELECT MAKEDATE(70,277);
```
```
  makedate(70, 277)   
======================
  '1970-10-04'        
```

``` sql
SELECT MAKEDATE(100,3615902);
```
```
  makedate(100, 3615902)   
===========================
  '9999-12-31'             
```

``` sql
SELECT MAKEDATE(9999,365);
```
```
  makedate(9999, 365)   
========================
  '9999-12-31'          
```

``` sql
SELECT MAKEDATE(0,0);
```
```
ERROR: Conversion error in date format.
```

MAKETIME
--------

**MAKETIME(hour, min, sec)**

**MAKETIME** 함수는 지정된 인자로부터 시간을 AM/PM 형태로 반환한다. 인자로 시각, 분, 초에 해당하는 **INTEGER** 타입을 지정할 수 있으며, **DATETIME** 타입을 반환한다.

- hour: 시를 나타내는 0~23 범위의 정수
- min: 분을 나타내는 0~59 범위의 정수
- sec: 초를 나타내는 0~59 범위의 정수
- return type: DATETIME
    
``` sql
SELECT MAKETIME(13,34,4);
```
```
  maketime(13, 34, 4)   
========================
  '13:34:04'            
```

``` sql
SELECT MAKETIME('1','34','4');
```
```
  maketime('1', '34', '4')   
=============================
  '01:34:04'                 
```

``` sql
SELECT MAKETIME(24,0,0);
```
```
ERROR: Conversion error in time format.
```

MINUTE
------

**MINUTE (time)**

**MINUTE** 함수는 지정된 인자로부터 0~59 범위의 분(minute)을 반환한다. 인자로 **TIME**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

- time: 시간
- return type: INT

``` sql
SELECT MINUTE('12:34:56');
```
```
  minute('12:34:56')   
=======================
  34                   
```

``` sql
SELECT MINUTE('2010-01-01 12:34:56');
```
```
  minute('2010-01-01 12:34:56')   
==================================
  34                              
```

``` sql
SELECT MINUTE('2010-01-01 12:34:56.7890');
```
```
  minute('2010-01-01 12:34:56.7890')   
=======================================
  34                                   
```

MONTH
-----

**MONTH (date)**

**MONTH** 함수는 지정된 인자로부터 1~12 범위의 월(month)을 반환한다. 인자로 **DATE**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 날짜가 모두 0인 값을 입력한 경우에는 0을 반환한다.    

- date: 날짜
- return type: INT

``` sql
SELECT MONTH('2010-01-02');
```
```
  month('2010-01-02')   
========================
  1                     
```

``` sql
SELECT MONTH('2010-01-02 12:34:56');
```
```
  month('2010-01-02 12:34:56')   
=================================
  1                              
```

``` sql
SELECT MONTH('2010-01-02 12:34:56.7890');
```
```
  month('2010-01-02 12:34:56.7890')   
======================================
  1                                   
```

``` sql
SELECT MONTH('0000-00-00');
```
```
  month('0000-00-00')   
========================
  0                     
```

MONTHS_BETWEEN
---------------

**MONTHS_BETWEEN (date_argument, date_argument)**

**MONTHS_BETWEEN** 함수는 주어진 두 개의 **DATE** 값 간의 차이를 월 단위로 반환하며, 리턴 값은 **DOUBLE** 타입이다. 인자로 지정된 두 날짜가 동일하거나, 해당 월의 말일인 경우에는 정수 값을 반환하지만, 그 외의 경우에는 날짜 차이를 31로 나눈 값을 반환한다.

- date_argument:  **DATE** 타입의 연산식을 지정한다. **DATETIME** 값을 지정하려면 **DATE** 타입으로 명시적 변환을 해야 한다. 값이 **NULL** 이면 **NULL** 을 반환한다.
- return type: DOUBLE
    
``` sql
SELECT MONTHS_BETWEEN(DATE '2008-12-31', DATE '2010-6-30');
```
```
  months_between(date '2008-12-31', date '2010-6-30')   
========================================================
  -18.0                                                 
```

``` sql
SELECT MONTHS_BETWEEN(DATE '2010-6-30', DATE '2008-12-31');
```
```
  months_between(date '2010-6-30', date '2008-12-31')   
========================================================
  18.0                                                  
```

``` sql
SELECT MONTHS_BETWEEN(CAST ('2017-07-03 12:00:00' AS DATE), DATE '2008-12-25');
```
```
  months_between( cast('2017-07-03 12:00:00' as date), date '2008-12-25')   
============================================================================
  102.29032258064517                                                        
```

QUARTER
-------

**QUARTER (date)**

**QUARTER** 함수는 지정된 인자로부터 1~4 범위의 분기(quarter)를 반환한다. 인자로 **DATE**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

- date: 날짜
- return type: INT
    
``` sql
SELECT QUARTER('2010-05-05');
```
```
  quarter('2010-05-05')   
==========================
  2                       
```

``` sql
SELECT QUARTER('2010-05-05 12:34:56.7890');
```
```
  quarter('2010-05-05 12:34:56.7890')   
========================================
  2                                     
```

SEC_TO_TIME
-------------

**SEC_TO_TIME(second)**

**SEC_TO_TIME** 함수는 지정된 인자로부터 시, 분, 초를 포함한 시간을 반환한다. 인자로 0~86399 범위의 **INTEGER** 타입을 지정할 수 있으며, **TIME** 타입을 반환한다.

- second: second -- 0~86399 범위의 초
- return type: INT

``` sql
SELECT SEC_TO_TIME(82800);
```
```
  sec_to_time(82800)   
=======================
  '23:00:00'           
```
``` sql
SELECT SEC_TO_TIME('82800.3');
```
```
  sec_to_time('82800.3')   
===========================
  '23:00:00'               
```
``` sql
SELECT SEC_TO_TIME(86399);
```
```
  sec_to_time(86399)   
=======================
  '23:59:59'           
```

SECOND
------

**SECOND(time)**

**SECOND** 함수는 지정된 인자로부터 0~59 범위의 초(second)를 반환한다. 인자로 **TIME**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

- time -- 시간
- return type: INT

``` sql
SELECT SECOND('12:34:56');
```
```
  second('12:34:56')   
=======================
  56                   
```
``` sql
SELECT SECOND('2010-01-01 12:34:56.7890');
```
```
  second('2010-01-01 12:34:56.7890')   
=======================================
  56                                   
```

TIME
----

**TIME(time)**

**TIME** 함수는 지정된 인자로부터 시간 부분을 추출하여 'HH:MI:SS' 형태의 **VARCHAR** 타입 문자열을 반환한다. 인자로 **TIME**,  **DATETIME** 타입을 지정할 수 있다.

- time -- 시간
- return type: STRING

``` sql
SELECT TIME('12:34:56');
```
```
  time('12:34:56')   
=====================
  '12:34:56'         
```
``` sql
SELECT TIME('2010-01-01 12:34:56');
```
```
  time('2010-01-01 12:34:56')   
================================
  '12:34:56'                    
```
``` sql
SELECT TIME(datetime'2010-01-01 12:34:56');
```
```
  time(datetime '2010-01-01 12:34:56')   
=========================================
  '12:34:56'                             
```

TIME_TO_SEC
-------------

**TIME_TO_SEC(time)**

**TIME_TO_SEC** 함수는 지정된 인자로부터 0~86399 범위의 초를 반환한다. 인자로 **TIME**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

- time -- 시간
- return type: INT

``` sql
SELECT TIME_TO_SEC('23:00:00');
```
```
  time_to_sec('23:00:00')   
============================
  82800                     
```
``` sql
SELECT TIME_TO_SEC('2010-10-04 23:00:00.1234');
```
```
  time_to_sec('2010-10-04 23:00:00.1234')   
============================================
  82800                                     
```

TIMEDIFF
--------

**TIMEDIFF(expr1, expr2)**

**TIMEDIFF** 함수는 지정된 두 개의 시간 인자의 시간 차를 반환한다. 날짜/시간 타입인 **TIME**, **DATE**, **DATETIME** 타입을 인자로 입력할 수 있으며, 두 인자의 데이터 타입은 같아야 한다. TIME 타입을 반환하며, 두 인자의 시간 차이는 00:00:00~23:59:59 범위여야 한다. 이 범위를 벗어나면 에러를 반환한다.

- expr1, expr2  -- 시간. 두 인자의 데이터 타입은 같아야 한다.
- return type: TIME

``` sql
SELECT TIMEDIFF(time '17:18:19', time '12:05:52');
```
```
  timediff(time '17:18:19', time '12:05:52')   
===============================================
  '05:12:27'                                   
```
``` sql
SELECT TIMEDIFF('17:18:19','12:05:52');
```
```
  timediff('17:18:19', '12:05:52')   
=====================================
  '05:12:27'                         
```
``` sql
SELECT TIMEDIFF('2010-01-01 06:53:45', '2010-01-01 03:04:05');
```
```
  timediff('2010-01-01 06:53:45', '2010-01-01 03:04:05')   
===========================================================
  '03:49:40'                                               
```

TO_DAYS
--------

**TO_DAYS(date)**

**TO_DAYS** 함수는 지정된 인자로부터 0년 이후의 날 수를 366~3652424 범위의 값으로 반환한다. 인자로 **DATE** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

**TO_DAYS** 함수는 그레고리력(Gregorian Calendar) 출현(1582년) 이전은 고려하지 않았으므로, 1582년 이전의 날짜에 대해서는 사용하지 않는 것을 권장한다.

``` sql
SELECT TO_DAYS('2010-10-04');
```
```
  to_days('2010-10-04')   
==========================
  734414                  
```
``` sql
SELECT TO_DAYS('2010-10-04 12:34:56');
```
```
  to_days('2010-10-04 12:34:56')   
===================================
  734414                           
```
``` sql
SELECT TO_DAYS('2010-10-04 12:34:56.7890');
```
```
  to_days('2010-10-04 12:34:56.7890')   
========================================
  734414                                
```
``` sql
SELECT TO_DAYS('1-1-1');
```
```
  to_days('1-1-1')   
=====================
  366                
```
``` sql
SELECT TO_DAYS('9999-12-31');
```
```
  to_days('9999-12-31')   
==========================
  3652424                 
```

UNIX_TIMESTAMP
---------------

**UNIX_TIMESTAMP(date)**

**UNIX_TIMESTAMP**수는 인자를 생략할 수 있으며, 인자를 생략하면 '1970-01-01 00:00:00' UTC 이후 현재 시스템 날짜/시간까지의 초 단위 시간 간격(interval)을 반환한다. *date* 인자가 지정되면 '1970-01-01 00:00:00' UTC 이후 지정된 날짜/시간까지의 초 단위 시간 간격을 반환한다.

인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 날짜와 시간이 모두 0인 값을 입력한 경우에는 0을 반환한다.

``` sql
SELECT UNIX_TIMESTAMP('1970-01-02'), UNIX_TIMESTAMP();
```
```
  unix_timestamp('1970-01-02')   unix_timestamp()   
====================================================
  54000                          1499071887         
```
``` sql
SELECT UNIX_TIMESTAMP ('0000-00-00 00:00:00');
```
```
  unix_timestamp('0000-00-00 00:00:00')   
==========================================
  0                                       
```

UTC_DATE
---------

**UTC_DATE()**

**UTC_DATE** 함수는 UTC 날짜를 'YYYY-MM-DD' 형태로 반환한다.

``` sql
SELECT UTC_DATE();
```
```
  utc_date()     
=================
  '2017-07-03'   
```

UTC_TIME
---------

**UTC_TIME()**

**UTC_TIME** 함수는 UTC 시간을 'HH:MI:SS' 형태로 반환한다.

``` sql
SELECT UTC_TIME();
```
```
  utc_time()   
===============
  '08:51:27'   
```

WEEK
----
**WEEK (date, mode)**

함수의 두 번째 인자인 *mode* 는 생략할 수 있으며, 0~7 범위의 값을 입력한다. 이 값으로 한 주가 일요일부터 시작하는지 월요일부터 시작하는지, 리턴 값의 범위가 0~53인지 1~53인지 설정한다. *mode* 를 생략하면 0이 사용된다. *mode* 값의 의미는 다음과 같다.

<table>
<colgroup>
<col width="14%" />
<col width="16%" />
<col width="12%" />
<col width="57%" />
</colgroup>
<thead>
<tr class="header">
<th>mode</th>
<th>시작 요일</th>
<th>범위</th>
<th>해당 연도의 첫 번째 주</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>0</td>
<td>일요일</td>
<td>0~53</td>
<td>일요일이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="even">
<td>1</td>
<td>월요일</td>
<td>0~53</td>
<td>3일 이상이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="odd">
<td>2</td>
<td>일요일</td>
<td>1~53</td>
<td>일요일이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="even">
<td>3</td>
<td>월요일</td>
<td>1~53</td>
<td>3일 이상이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="odd">
<td>4</td>
<td>일요일</td>
<td>0~53</td>
<td>3일 이상이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="even">
<td>5</td>
<td>월요일</td>
<td>0~53</td>
<td>월요일이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="odd">
<td>6</td>
<td>일요일</td>
<td>1~53</td>
<td>3일 이상이 해당 연도에 속하는 첫 번째 주</td>
</tr>
<tr class="even">
<td>7</td>
<td>월요일</td>
<td>1~53</td>
<td>월요일이 해당 연도에 속하는 첫 번째 주</td>
</tr>
</tbody>
</table>

*mode* 값이 0, 1, 4, 5 중 하나이고 날짜가 이전 연도의 마지막 주에 해당하면 **WEEK** 함수는 0을 반환한다. 이때의 목적은 해당 연도에서 해당 주가 몇 번째 주인지를 아는 것이므로, 1999년의 52번째 주에 해당해도 2000년의 날짜가 0번째 주에 해당되는 0을 반환한다.

``` sql
SELECT YEAR('2000-01-01'), WEEK('2000-01-01',0);
```
```
  year('2000-01-01')   week('2000-01-01', 0)   
===============================================
  2000                 0                       
```
시작 요일이 속해있는 주의 연도를 기준으로 해당 날짜가 몇 번째 주인지 알려면, *mode* 값으로 0, 2, 5, 7 중 하나의 값을 사용한다.

``` sql
SELECT WEEK('2000-01-01',2);
```
```
  week('2000-01-01', 2)   
==========================
  52                      
```
``` sql
SELECT WEEK('2010-04-05');
```
```
  week('2010-04-05', 0)   
==========================
  14                      
```
``` sql
SELECT WEEK('2010-04-05 12:34:56',2);
```
```
  week('2010-04-05 12:34:56', 2)   
===================================
  14                               
```
``` sql
SELECT WEEK('2010-04-05 12:34:56.7890',4);
```
```
  week('2010-04-05 12:34:56.7890', 4)   
========================================
  14                                    
```

WEEKDAY
-------

**WEEKDAY(date)**

**WEEKDAY** 함수는 지정된 인자로부터 0~6 범위의 요일(0: 월요일, 1: 화요일, ..., 6: 일요일)을 반환한다. 인자로 **DATE**,  **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

``` sql
SELECT WEEKDAY('2010-09-09');
```
```
  weekday('2010-09-09')   
==========================
  3                       
```
``` sql
SELECT WEEKDAY('2010-09-09 13:16:00');
```
```
  weekday('2010-09-09 13:16:00')   
===================================
  3                                
```

YEAR
----

**YEAR(date)**

**YEAR** 함수는 지정된 인자로부터 1~9999 범위의 연도를 반환한다. 인자로 **DATE**, **DATETIME** 타입을 지정할 수 있으며, **INTEGER** 타입을 반환한다.

``` sql
SELECT YEAR('2010-10-04');
```
```
  year('2010-10-04')   
=======================
  2010                 
```
``` sql
SELECT YEAR('2010-10-04 12:34:56');
```
```
  year('2010-10-04 12:34:56')   
================================
  2010                          
```
``` sql
SELECT YEAR('2010-10-04 12:34:56.7890');
```
```
  year('2010-10-04 12:34:56.7890')   
=====================================
  2010                               
```
