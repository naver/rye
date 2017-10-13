데이터 타입 변환 함수와 연산자
==============================

CAST
----

**CAST (cast_operand AS cast_target)**

**CAST** 연산자를 **SELECT** 문에서 어떤 값의 데이터 타입을 다른 데이터 타입으로 명시적으로 변환하는 데 사용할 수 있다. 조회 리스트 또는 **WHERE** 절의 값 수식을 다른 데이터 타입으로 변환할 수 있다.

Parameters:
 - cast_operand: 다른 타입으로 변환할 값을 선언한다.
 - cast_target: 변환할 타입을 지정한다.

Return type: cast_target

경우에 따라 **CAST** 연산자를 쓰지 않고 데이터 타입이 자동으로 변환될 수 있다. 이에 대한 자세한 내용은 implicit-type-conversion을 참고한다.
문자열을 날짜/시간 타입으로 변환하는 것에 대한 자세한 내용은 cast-string-to-datetime 을 참고한다.

**CAST** 연산자를 사용한 명시적인 타입 변환에 대해서 정리하면 다음의 표와 같다.

| **From \ To**  | **EN** | **AN** | **VC** | **VB** | **D** | **T** | **DT** |
|----------------|--------|--------|--------|--------|-------|-------|--------|
| **EN**         | Yes    | Yes    | Yes    | No     | No    | No    | No     |
| **AN**         | Yes    | Yes    | Yes    | No     | No    | No    | No     |
| **VC**         | Yes    | Yes    | Yes    | NO     | Yes   | Yes   | Yes    |
| **VB**         | No     | No     | No     | Yes    | No    | No    | No     |
| **D**          | No     | No     | Yes    | No     | Yes   | No    | Yes    |
| **T**          | No     | No     | Yes    | No     | No    | Yes   | No     |
| **DT**         | No     | No     | Yes    | No     | Yes   | Yes   | Yes    |

 - **데이터 타입 키**
  - **EN**: 정확한 수치(**INTEGER**, **BIGINT**, **NUMERIC**)
  - **AN**: 근사값 수치(**DOUBLE**)
  - **VC**: 가변 길이 문자열(**VARCHAR** (*n*))
  - **VB**: 가변 길이 바이트열(**VARBINARY** (*n*))
  - **D**: **DATE**
  - **T**: **TIME**
  - **DT**: **DATETIME**

``` sql
--operation after casting character as INT type returns 2
SELECT (1+CAST ('1' AS INT));
```
```
  (1+ cast('1' as integer))   
==============================
  2                           
```
``` sql
--operation after casting returns 1+1234567890
SELECT (1+CAST('1234567890' AS INT));
```
```
  (1+ cast('1234567890' as integer))   
=======================================
  1234567891                           
```
``` sql
--'1234.567890' is casted to 1235 after rounding up
SELECT (1+CAST('1234.567890' AS INT));
```
```
  (1+ cast('1234.567890' as integer))   
========================================
  1236                                  
```
``` sql
--'1234.567890' is casted to string containing only first 5 letters.
SELECT (CAST('1234.567890' AS CHAR(5)));
```
```
  ( cast('1234.567890' as varchar(5)))   
=========================================
  '1234.'                                
```
``` sql
--numeric type can be casted to CHAR type only when enough length is specified
SELECT (CAST(1234.567890 AS CHAR(5)));
```
```
ERROR: Cannot coerce value of type "numeric" to type "character varying".
```

``` sql
--numeric type can be casted to CHAR type only when enough length is specified
SELECT (CAST(1234.567890 AS CHAR(11)));
```
```
  ( cast(1234.567890 as varchar(11)))   
========================================
  '1234.567890'                         
```
``` sql
--numeric type can be casted to CHAR type only when enough length is specified
SELECT (CAST(1234.567890 AS VARCHAR));
```
```
  ( cast(1234.567890 as varchar))   
====================================
  '1234.567890'                     
```
``` sql
SELECT (CAST('10:30:20' AS TIME));
```
```
  ( cast('10:30:20' as time))   
================================
  '10:30:20'                    
```
``` sql
--string can be casted to TIME type when its literal is same as TIME's.
SELECT (CAST('2008-12-25 10:30:20' AS TIME));
```
```
  ( cast('2008-12-25 10:30:20' as time))   
===========================================
  '10:30:20'                               
```
``` sql
SELECT CAST(B'11010000' as varchar(10));
```
```
ERROR: Cannot coerce value of type "varbinary" to type "character varying".
```

NOTE:
 - **CAST** 변환은 같은 문자셋을 가지는 데이터 타입끼리만 허용된다.
 - 근사치 데이터 타입(DOUBLE)이 정수형으로 변환되는 경우, 소수점 아래 자리가 반올림 처리된다.
 - 정확한 수치 데이터 타입(NUMERIC)이 정수형으로 변환되는 경우, 소수점 아래 자리가 반올림 처리된다.
 - 수치 데이터 타입을 문자열 타입으로 변환하는 경우, 문자열의 길이가 (모든 유효 숫자 자리 + 소수점) 이상이 되도록 충분하게 지정해야 한다. 그렇지 않으면 에러가 발생한다.
 - 문자열 타입 *A*를 문자열 타입 *B*로 변환하는 경우, *A*의 길이 이상이 되도록 충분하게 지정되지 않으면 문자열 끝 부분이 삭제(truncate)되어 저장된다.
 - 문자열 타입 *A*를 날짜/시간 데이터 타입 *B*로 변환하는 경우, *A*의 리터럴이 *B* 타입과 일치하는 경우에만 변환된다. 그렇지 않을 경우 에러가 발생한다.
 - 문자열로 저장된 수치 데이터는 명시적으로 타입 변환을 해주어야 산술 연산이 가능하다.

DATE_FORMAT
-----------

**DATE_FORMAT (date, format)**

**DATE_FORMAT** 함수는 **DATE** 형식('*YYYY*-*MM*-*DD*')를 포함하는 문자열 또는 날짜/시간 타입(**DATE**, **DATETIME**) 값을 지정된 날짜/시간 형식으로 변환하여 문자열로 출력하며, 리턴 값은 **VARCHAR** 타입이다. 지정할 *format* 인자는 아래의 **날짜/시간 형식 2** 표를 참고한다. **날짜/시간 형식 2** 표는 [DATE_FORMAT](#date_format) 함수, [TIME_FORMAT](#time_format) 함수, [STR_TO_DATE](#str_to_date) 함수에서 사용된다.

Parameters:
 - date: **DATE** 형식('*YYYY*-*MM*-*DD*')를 포함하는 문자열 또는 날짜/시간 타입(**DATE**, **DATETIME**) 값이 지정될 수 있다.
 - format: 출력 형식을 지정한다. '%'로 시작하는 형식 지정자(specifier)를 사용한다.

Return type: STRING

<p id="datetime-format2"><strong>날짜/시간 형식 2</strong></p>

 format 값 | 의미
----------|------------------------------------------------------------------------------
 %a       | Weekday, 영문 약어 (Sun, ..., Sat)
 %b       | Month, 영문 약어 (Jan, ..., Dec)
 %c       | Month(1, ..., 12)
 %D       | Day of the month, 서수 영문 문자열(1st, 2nd, 3rd, ...)
 %d       | Day of the month, 두 자리 숫자(01, ..., 31)
 %e       | Day of the month (1, ..., 31)
 %f       | Milliseconds, 세 자리 숫자 (000, ..., 999)
 %H       | Hour, 24시간 기준, 두 자리 수 이상 (00, ..., 23, ..., 100, ...)
 %h       | Hour, 12시간 기준 두 자리 숫자 (01, ..., 12)
 %I       | Hour, 12시간 기준 두 자리 숫자 (01, ..., 12)
 %i       | Minutes, 두 자리 숫자 (00, ..., 59)
 %j       | Day of year, 세 자리 숫자 (001, ..., 366)
 %k       | Hour, 24시간 기준, 한 자리 수 이상 (0, ..., 23, ..., 100, ...)
 %l       | Hour, 12시간 기준 (1, ..., 12)
 %M       | Month, 영문 문자열 (January, ..., December)
 %m       | Month, 두 자리 숫자 (01, ..., 12)
 %p       | AM or PM
 %r       | Time, 12 시간 기준, 시:분:초 (hh:mi:ss AM or hh:mi:ss PM)
 %S       | Seconds, 두 자리 숫자 (00, ..., 59)
 %s       | Seconds, 두 자리 숫자 (00, ..., 59)
 %T       | Time, 24시간 기준, 시:분:초 (hh:mi:ss)
 %U       | Week, 두 자리 숫자, 일요일이 첫날인 주 단위 (00, ..., 53)
 %u       | Week, 두 자리 숫자, 월요일이 첫날인 주 단위 (00, ..., 53)
 %V       | Week, 두 자리 숫자, 일요일이 첫날인 주 단위 (01, ..., 53), %X와 결합되어 사용 가능
 %v       | Week, 두 자리 숫자, 월요일이 첫날인 주 단위 (01, ..., 53), %x와 결합되어 사용 가능
 %W       | Weekday, 영문 문자열 (Sunday, ..., Saturday)
 %w       | Day of the week, 숫자 인덱스 (0=Sunday, ..., 6=Saturday)
 %X       | Year, 네 자리 숫자, 일요일이 첫날인 주 단위로 계산(0000, ..., 9999), %V와 결합되어 사용 가능
 %x       | Year, 네 자리 숫자, 월요일이 첫날인 주 단위로 계산(0000, ..., 9999), %v와 결합되어 사용 가능
 %Y       | Year, 네 자리 숫자 (0001, ..., 9999)
 %y       | Year, 두 자리 숫자 (00, 01, ..., 99)
 %%       | 특수문자 "%"를 그대로 출력하는 경우
 %x       | 형식 지정자로 쓰이지 않는 영문자 중 임의의 문자 x를 그대로 출력하는 경우

``` sql
SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
```
```
  date_format('2009-10-04 22:23:00', '%W %M %Y')   
===================================================
  'Sunday October 2009'                            
```
``` sql
SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
```
```
  date_format('2007-10-04 22:23:00', '%H:%i:%s')   
===================================================
  '22:23:00'                                       
```
``` sql
SELECT DATE_FORMAT('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
```
```
  date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j')   
===============================================================
  '4th 00 Thu 04 10 Oct 277'                                   
```
``` sql
SELECT DATE_FORMAT('1999-01-01', '%X %V');
```
```
  date_format('1999-01-01', '%X %V')   
=======================================
  '1998 52'                            
```

FORMAT
------

**FORMAT ( x , dec )**

**FORMAT** 함수는 숫자 *x* 의 형식이 *#,###,###.#####* 이 되도록, 소수점 위 세 자리마다 자릿수 구분 기호로 구분하고 소수점 기호 아래 숫자가 *dec* 만큼 표현되도록 *dec* 의 아랫자리에서 반올림을 수행한 결과를 **VARCHAR** 타입으로 반환한다.

Parameters:
 - x: 수치 값을 반환하는 임의의 연산식이다.
 - dec: 소수점 이하 자릿수

Return type: STRING

``` sql
SELECT FORMAT(12000.123456,3), FORMAT(12000.123456,0);
```
```
  format(12000.123456, 3)   format(12000.123456, 0)   
======================================================
  '12,000.123'              '12,000'                  
```

STR_TO_DATE
-----------

**STR_TO_DATE (string, format)**

**STR_TO_DATE** 함수는 인자로 주어진 문자열을 지정된 형식에 따라 해석하여 날짜/시간 값으로 변환하며, [DATE_FORMAT](#date_format) 함수와 반대로 동작한다. 리턴 값은 문자열에 포함된 날짜 또는 시간 부분에 따라 타입이 결정되며, **DATETIME**, **DATE**, **TIME** 타입 중 하나이다.

Parameters:
 - x: string: 모든 문자열 타입이 지정될 수 있다.
 - format: 문자열 해석을 위한 형식을 지정한다. %를 포함하는 문자열을 형식 지정자(specifier)로 사용한다. [DATE_FORMAT](#date_format) 함수의 [날짜/시간 형식 2](#datetime-format2) 표를 참고한다.

Return type: DATETIME, DATE, TIME

*string*에 유효하지 않은 날짜/시간 값이 포함되거나, *format*에 지정된 형식 지정자를 적용하여 문자열을 해석할 수 없으면 에러를 리턴한다.

인자의 연, 월, 일에는 0을 입력할 수 없으나, 예외적으로 날짜와 시간이 모두 0인 값을 입력한 경우에는 날짜와 시간 값이 모두 0인 **DATE**, **DATETIME** 타입의 값을 반환한다. 그러나 JDBC 프로그램에서는 연결 URL 속성인 zeroDateTimeBehavior의 설정에 따라 동작이 달라진다. 이에 관한 자세한 내용은 jdbc-connection-conf을 참고하면 된다.

``` sql
SELECT STR_TO_DATE('01,5,2013','%d,%m,%Y');
```
```
  str_to_date('01,5,2013', '%d,%m,%Y')   
=========================================
  '2013-05-01'                           
```
``` sql
SELECT STR_TO_DATE('May 1, 2013','%M %d,%Y');
```
```
  str_to_date('May 1, 2013', '%M %d,%Y')   
===========================================
  '2013-05-01'                             
```
``` sql
SELECT STR_TO_DATE('13:30:17','%H:%i');
```
```
  str_to_date('13:30:17', '%H:%i')   
=====================================
  '13:30:00'                         
```
``` sql
SELECT STR_TO_DATE('09:30:17 PM','%r');
```
```
  str_to_date('09:30:17 PM', '%r')   
=====================================
  '21:30:17'                         
```

TIME_FORMAT
-----------

**TIME_FORMAT (time, format)**

**TIME_FORMAT** 함수는 **TIME** 형식(*HH*:*MI*:*SS*)을 포함하는 문자열 또는 **TIME** 을 포함하는 날짜/시간 타입(**TIME**, **DATETIME**) 값을 지정된 시간 형식으로 변환하여 문자열로 출력하며, 리턴 값은 **VARCHAR** 타입이다.

Parameters:
 - time: **TIME** 형식(*HH*:*MI*:*SS*)을 포함하는 문자열, **TIME** 을 포함하는 날짜/시간 타입(**TIME**, **DATETIME**) 값을 지정할 수 있다.
 - format: 문자열 해석을 위한 형식을 지정한다. %를 포함하는 문자열을 형식 지정자(specifier)로 사용한다. [DATE_FORMAT](#date_format) 함수의 [날짜/시간 형식 2](#datetime-format2) 표를 참고한다.

Return type: STRING

``` sql
SELECT TIME_FORMAT('22:23:00', '%H %i %s');
```
```
  time_format('22:23:00', '%H %i %s')   
========================================
  '22 23 00'                            
```
``` sql
SELECT TIME_FORMAT('23:59:00', '%H %h %i %s %f');
```
```
  time_format('23:59:00', '%H %h %i %s %f')   
==============================================
  '23 11 59 00 000'                           
```
``` sql
SELECT SYSTIME, TIME_FORMAT(SYSTIME, '%p');
```
```
  SYS_TIME     time_format( SYS_TIME , '%p')   
===============================================
  '16:29:28'   'PM'                            
```

TO_CHAR(date_time)
--------------------

**TO_CHAR ( date_time [, format[, date_lang_string_literal ]] )**

**TO_CHAR** (date_time) 함수는 날짜/시간 타입(**TIME**, **DATE**, **DATETIME**) 값을 [날짜/시간 형식 1](#datetime-format1) 표에 따라 문자열로 변환하여 이를 반환하며, 리턴 값의 타입은 **VARCHAR** 이다.

Parameters:
 - date_time: 날짜/시간 타입의 연산식을 지정한다. 값이 **NULL** 인 경우에는 **NULL** 이 반환된다.
 - format: 리턴 값의 형식을 지정한다. 값이 **NULL** 인 경우에는 **NULL** 이 반환된다.
 - date_lang_string_literal: 리턴 값에 적용할 언어를 지정한다.

Return type: STRING

*format* 인자가 지정되면 지정한 언어에 맞는 형식으로 *date_time* 을 출력한다. 자세한 형식은 [날짜/시간 형식 1](#datetime-format1) 표를 참고하면 된다. 언어는 *date_lang_string_literal* 인자에 의해 정해진다. *date_lang_string_literal* 인자가 생략되면 "en_US"가 적용된다.

주어진 문자열과 대응하지 않는 *format* 인자가 지정되면 에러를 반환한다.

*format* 인자가 생략되면 "en_US"의 기본 출력 형식을 따라 *date_time*을 문자열로 출력한다

**날짜/시간 타입에 대한 기본 출력 형식**

 언어   | DATE           | TIME          | DATETIME
-------|----------------|---------------|----------------------------
 en_US | 'MM/DD/YYYY'   | 'HH:MI:SS AM' | 'HH:MI:SS.FF AM MM/DD/YYYY'
 ko_KR | 'YYYY.MM.DD'   | 'HH24:MI:SS'  | 'HH24:MI:SS.FF YYYY.MM.DD'

<p id="datetime-format1"><strong>날짜/시간 형식 1</strong></p>

 format 값             | 의미
-----------------------|-----------------------------
 **CC**                | 세기(Century)
 **YYYY**, **YY**      | 4자리 연도, 2자리 연도
 **Q**                 | 분기(1, 2, 3, 4; 1월~3월 = 1)
 **MM**                | 월(01-12; 1월 = 01)
 **MONTH**             | 월 이름
 **MON**               | 축약된 월 이름
 **DD**                | 날(1-31)
 **DAY**               | 요일 이름
 **DY**                | 축약된 요일 이름
 **D** 또는 **d**       | 요일(1-7)
 **AM** 또는 **PM**     | 오전/오후
 **A.M.** 또는 **P.M.** | 마침표가 포함된 오전/오후
 **HH** 또는 **HH12**   | 시(1-12)
 **HH24**              | 시(0-23)
 **MI**                | 분(0-59)
 **SS**                | 초(0-59)
 **FF**                | 밀리초(0-999)
 \- / , . ; : "텍스트"   | 구두점과 인용구는 그대로 결과에 표현됨


**date_lang_string_literal 예**

 **형식 구성 요소** |**'en_US'** | **'ko_KR'**
----------------|-------------|-------------
 **MONTH**    | JANUARY       | 1월
 **MON**      | JAN           | 1
 **DAY**      | MONDAY        | 월요일
 **DY**       | MON           | 월
 **Month**    | January       | 1월
 **Mon**      | Jan           | 1
 **Day**      | Monday        | 월요일
 **Dy**       | Mon           | 월
 **month**    | january       | 1월
 **mon**      | jan           | 1
 **day**      | monday        | 월요일
 **Dy**       | mon           | 월
 **AM**       | AM            | 오전
 **Am**       | Am            | 오전
 **am**       | am            | 오전
 **A.M.**     | A.M.          | 오전
 **A.m.**     | A.m.          | 오전
 **a.m.**     | a.m.          | 오전
 **PM**       | PM            | 오후
 **Pm**       | Pm            | 오후
 **pm**       | pm            | 오후
 **P.M.**     | P.M.          | 오후
 **P.m.**     | P.m.          | 오후
 **p.m.**     | p.m.          | 오후

**리턴 값 형식의 자릿수의 예**

| 형식 구성 요소             | en_US 자릿수                | ko_KR 자릿수
|-------------------------|---------------------------|---------------------
| **MONTH(Month, month)** | 9                         | 4
| **MON(Mon, mon)**       | 3                         | 2
| **DAY(Day, day)**       | 9                         | 6
| **DY(Dy, dy)**          | 3                         | 2
| **HH12, HH24**          | 2                         | 2
| "텍스트"                  | 텍스트의 길이                | 텍스트의 길이
| 나머지 형식                | 주어진 형식의 길이와 같음       | 주어진 형식의 길이와 같음

``` sql
--creating a table having date/time type columns
CREATE GLOBAL TABLE datetime_tbl(a TIME PRIMARY KEY, b DATE, c DATETIME);
INSERT INTO datetime_tbl VALUES(SYSTIME, SYSDATE, SYSDATETIME);

--selecting a VARCHAR type string from the data in the specified format
SELECT TO_CHAR(b, 'DD, DY , MON, YYYY') FROM datetime_tbl;
```
```
  to_char(b, 'DD, DY , MON, YYYY')   
=====================================
  '07, FRI , JUL, 2017'              
```

``` sql
SELECT TO_CHAR(c, 'HH24:MI, DD, MONTH, YYYY') FROM datetime_tbl;
```
```
  to_char(c, 'HH24:MI, DD, MONTH, YYYY')   
===========================================
  '16:29, 07, JULY     , 2017'             
```

``` sql
SELECT TO_CHAR(c, 'HH12:MI:SS:FF pm, YYYY-MM-DD-DAY') FROM datetime_tbl;
```
```
  to_char(c, 'HH12:MI:SS:FF pm, YYYY-MM-DD-DAY')   
===================================================
  '04:29:29:034 pm, 2017-07-07-FRIDAY   '          
```

``` sql
SELECT TO_CHAR(DATETIME'2009-10-04 22:23:00', 'Day Month yyyy');
```
```
  to_char(datetime '2009-10-04 22:23:00', 'Day Month yyyy')   
==============================================================
  'Sunday    October   2009'                                  
```

다음은 위에서 생성한 데이터베이스에서 **TO_CHAR** 함수에 언어 인자를 별도로 부여한 예이다.

``` sql
SELECT TO_CHAR(DATETIME'2009-10-04 22:23:00', 'Day Month yyyy','ko_KR');
```
```
  to_char(datetime '2009-10-04 22:23:00', 'Day Month yyyy', 'ko_KR')   
=======================================================================
  '일요일       10월 2009'                                                 
```

언어에 따라 월 이름, 일 이름, 요일 이름, 오전/오후 이름의 해석이 변경되는 함수에서 "en_US" 외에 변경할 수 있는 언어는 "ko_KR"뿐이다(위의 예 참고).

첫번째 인자가 zerodate이고 두번째 인자에 'Month', 'Day'와 같은 리터럴 형식이 지정되면 TO_CHAR 함수는 NULL을 반환한다.

``` sql
    SELECT TO_CHAR(datetime '0000-00-00 00:00:00', 'Month Day YYYY');
```
```
  to_char(datetime '0000-00-00 00:00:00', 'Month Day YYYY')   
==============================================================
  NULL                                                        
```

TO_CHAR(number)
---------------

**TO_CHAR(number[, format[, number_lang_string_literal ] ])**

**TO_CHAR** (number) 함수는 수치형 데이터 타입을 [숫자 형식](#tochar-number-format) 에 맞는 문자열로 변환하여 **VARCHAR** 타입으로 반환한다.

Parameters:
 - number: 숫자를 반환하는 수치형 데이터 타입의 연산식을 지정한다. 입력값이 NULL이면 결과로 NULL이 반환된다. 입력값이 문자열 타입이면 해당 문자열을 그대로 반환한다.
 - format: 리턴 값의 형식을 지정한다. 값이 **NULL** 인 경우에는 **NULL** 이 반환된다.
 - number_lang_string_literal: 입력 숫자를 출력할 때 적용할 언어를 지정한다.

Return type: STRING

*format* 인자가 지정되면 지정한 언어에 맞는 형식으로 *number*를 출력한다. 이때 언어는 *number_lang_string_literal* 인자에 의해 정해진다. *number\_lang\_string\_literal* 인자가 생략되면 "en_US"가 적용된다.

주어진 문자열과 대응하지 않는 *format* 인자가 지정되면 에러를 반환한다.

*format* 인자가 생략되면 지정된 언어의 기본 출력에 따라 *number* 를 문자열로 출력한다. (언어별 숫자의 기본 출력 표 참고)

<p id="tochar-number-format"><strong>숫자 형식</strong></p>

 형식 구성 요소 | 예제      | 설명 
-------------|----------|--------------------------------------------------------------------------------------
 **9**       | 9999     | "9"의 개수는 반환될 유효숫자 자릿수를 나타낸다. 숫자 인자에 대해 형식에서 지정된 유효숫자 자릿수가 부족하면, 소수부에 대해서는 반올림 연산을 수행한다. 숫자 인자의 정수부 자릿수보다 유효숫자 자릿수가 부족하면 #을 출력한다. 
 **0**       | 0999     | 형식에서 지정된 유효숫자 자릿수가 충분한 경우, 정수부 앞 부분을 공백이 아닌 0으로 채워 반환한다.
 **S**       | S9999    | 지정된 위치에 양수/음수 부호를 출력한다. 부호는 문자열의 시작부분에만 사용할 수 있다.
 **C**       | C9999    | 지정된 위치에 ISO 통화 기호를 반환한다.
 **,**       | 9,999    | 지정된 위치에 쉼표(",")를 반환한다. 언어의 설정에 따라 쓰임이 달라지는데, 자릿수 구분 기호로 사용될 경우 여러 개가 허용되며, 소수점 기호로 사용될 경우 한 개만 허용된다. :ref:`언어별 숫자의 기본 출력 <tochar-default-number-format>` 표 참고
 **.**       | 9.999    | 지정된 위치에 마침표를 출력한다. 언어의 설정에 따라 쓰임이 달라지는데, 자릿수 구분 기호로 사용될 경우 여러 개가 허용되며, 소수점 기호로 사용될 경우 한 개만 허용된다. :ref:`언어별 숫자의 기본 출력 <tochar-default-number-format>` 표 참고
 **EEEE**    | 9.99EEEE | 과학적 기수법(scientific notation)을 반환한다.

**언어별 숫자의 기본 출력**

 언어      | 로캘 이름  | 자릿수 구분 기호  | 소수점 기호    | 숫자 표기 예
----------|---------|---------------|--------------|-----------
 영어      | en_US   | ,(쉼표)         | .(마침표)     | 123,456,789.012
 한국어     | ko_KR   | ,(쉼표)        | .(마침표)      | 123,456,789.012

``` sql
--selecting a string casted from a number in the specified format
SELECT TO_CHAR(12345,'S999999'), TO_CHAR(12345,'S099999');
```
```
  to_char(12345, 'S999999')   to_char(12345, 'S099999')   
==========================================================
  ' +12345'                   '+012345'                   
```
``` sql
SELECT TO_CHAR(1234567,'9,999,999,999');
```
```
  to_char(1234567, '9,999,999,999')   
======================================
  '    1,234,567'                     
```
``` sql
SELECT TO_CHAR(1234567,'9.999.999.999');
```
```
  to_char(1234567, '9.999.999.999')   
======================================
  '#############'                     
```
``` sql
SELECT TO_CHAR(123.4567,'99'), TO_CHAR(123.4567,'999.99999'), TO_CHAR(123.4567,'99999.999');
```
```
  to_char(123.4567, '99')   to_char(123.4567, '999.99999')   to_char(123.4567, '99999.999')   
==============================================================================================
  '##'                      '123.45670'                      '  123.457'                      
```

TO_DATE
-------

**TO_DATE(string [,format [,date_lang_string_literal]])**

**TO_DATE** 함수는 인자로 지정된 날짜 형식을 기준으로 문자열을 해석하여, 이를 **DATE** 타입의 값으로 변환하여 반환한다. 날짜 형식은 [날짜/시간 형식 1](#datetime-format1)을 참고한다.

Parameters:
 - string: 문자열을 반환하는 임의의 연산식이다. 값이 NULL이면 결과로 NULL이 반환된다.
 - format: 날짜 타입으로 변환할 값의 형식을 지정하며, [날짜/시간 형식 1](#datetime-format1) 표를 참고한다. 값이 **NULL** 이면 결과로 **NULL** 이 반환된다.
 - date_lang_string_literal: 입력 값에 적용할 언어를 지정한다.

Return type: DATE

*format* 인자가 지정되면 지정한 언어에 맞는 형식으로 *string* 을 해석한다. 이때 언어는 *date_lang_string_literal* 인자에 의해 정해진다. *date_lang_string_literal* 인자가 생략되면 "en_US"가 적용된다.

주어진 문자열과 대응하지 않는 *format* 인자가 지정되면 에러를 반환한다.

*format* 인자가 생략되면 먼저 Rye 기본 형식에 따라 *string*을 해석한다.

``` sql
--selecting a date type value casted from a string in the specified format

SELECT TO_DATE('12/25/2008');
```
```
  to_date('12/25/2008')   
==========================
  '2008-12-25'            
```
``` sql
SELECT TO_DATE('25/12/2008', 'DD/MM/YYYY');
```
```
  to_date('25/12/2008', 'DD/MM/YYYY')   
========================================
  '2008-12-25'                          
```
``` sql
SELECT TO_DATE('081225', 'YYMMDD');
```
```
  to_date('081225', 'YYMMDD')   
================================
  '2008-12-25'                  
```
``` sql
SELECT TO_DATE('2008-12-25', 'YYYY-MM-DD');
```
```
  to_date('2008-12-25', 'YYYY-MM-DD')   
========================================
  '2008-12-25'                          
```

TO_DATETIME
-----------

**TO_DATETIME (string [,format [,date_lang_string_literal]])**

**TO_DATETIME** 함수는 인자로 지정된 **DATETIME** 형식을 기준으로 문자열을 해석하여, 이를 **DATETIME** 타입의 값으로 변환하여 반환한다. **DATETIME** 형식은 [TO_CHAR](#to_char) 함수의 [날짜/시간 형식 1](#datetime-format1) 을 참고한다.

Parameters:
 - string: 문자열을 반환하는 임의의 연산식이다. 값이 NULL이면 결과로 NULL이 반환된다.
 - format: DATETIME 타입으로 변환할 값의 형식을 지정하며, [날짜/시간 형식 1](#datetime-format1)을 참고한다. 값이 **NULL** 이면 결과로 **NULL** 이 반환된다.
 - date_lang_string_literal: 입력 값에 적용할 언어를 지정한다.

Return type: DATETIME

*format* 인자가 지정되면 지정한 언어에 맞는 형식으로 *string* 을 해석한다.

*date_lang_string_literal* 인자가 생략되면 "en_US"가 적용된다. 주어진 문자열과 대응하지 않는 *format* 인자가 지정되면 에러를 반환한다.

*format* 인자가 생략되면 먼저 Rye 기본 형식에 따라 *string*을 해석한다.

``` sql
--selecting a datetime type value casted from a string in the specified format

SELECT TO_DATETIME('13:10:30 12/25/2008');
```
```
  to_datetime('13:10:30 12/25/2008')   
=======================================
  '2008-12-25 13:10:30.0'              
```
``` sql
SELECT TO_DATETIME('08-Dec-25 13:10:30.999', 'YY-Mon-DD HH24:MI:SS.FF');
```
```
  to_datetime('08-Dec-25 13:10:30.999', 'YY-Mon-DD HH24:MI:SS.FF')   
=====================================================================
  '2008-12-25 13:10:30.999'                                          
```
``` sql
SELECT TO_DATETIME('DATE: 12-25-2008 TIME: 13:10:30.999', '"DATE:" MM-DD-YYYY "TIME:" HH24:MI:SS.FF');
```
```
  to_datetime('DATE: 12-25-2008 TIME: 13:10:30.999', '"DATE:" MM-DD-YYYY "TIME:" HH24:MI:SS.FF')   
===================================================================================================
  '2008-12-25 13:10:30.999'                                                                        
```

TO_NUMBER
---------

**TO_NUMBER(string [, format ])**

**TO_NUMBER** 함수는 인자로 지정된 숫자 형식을 기준으로 문자열을 해석하여, 이를 **NUMERIC** 타입으로 변환하여 반환한다.

Parameters:
 - string: 문자열을 반환하는 임의의 연산식이다. 값이 NULL이면 결과로 NULL이 반환된다.
 - format: 숫자로 반환할 값의 형식을 지정하며, [숫자 형식](#tochar-number-format) 표를 참고한다. 생략되면 NUMERIC(38,0) 값이 리턴된다.

Return type: NUMERIC

*format* 인자가 지정되면 "en_US"지정한 언어""에 맞는 형식으로 *string* 을 해석한다.

주어진 문자열과 대응하지 않는 *format* 인자가 지정되면 에러를 반환한다.

``` sql
--selecting a number casted from a string in the specified format
SELECT TO_NUMBER('-1234');
```
```
  to_number('-1234')   
=======================
  -1234                
```
``` sql
SELECT TO_NUMBER('12345','999999');
```
```
  to_number('12345', '999999')   
=================================
  12345                          
```
``` sql
SELECT TO_NUMBER('12,345.67','99,999.999');
```
```
  to_number('12,345.67', '99,999.999')   
=========================================
  12345.670                              
```
``` sql
SELECT TO_NUMBER('12345.67','99999.999');
```
```
  to_number('12345.67', '99999.999')   
=======================================
  12345.670                            
```

TO_TIME
-------

**TO_TIME(string [,format [,date_lang_string_literal]])**

**TO_TIME** 함수는 인자로 지정된 시간 형식을 기준으로 문자열을 해석하여, 이를 TIME 타입의 값으로 변환하여 반환한다. 시간 형식은 [날짜/시간 형식 1](#datetime-format1) 을 참고한다.

Parameters:
 - string: 문자열을 반환하는 임의의 연산식이다. 값이 NULL이면 결과로 NULL이 반환된다.
 - format: TIME 타입으로 변환할 값의 형식을 지정하며, [날짜/시간 형식 1](#datetime-format1) 표를 참고한다. 값이 **NULL** 이면 결과로 **NULL** 이 반환된다.
 - date_lang_string_literal: 입력 값에 적용할 언어를 지정한다.

Return type: TIME

*format* 인자가 지정되면 지정한 언어에 맞는 형식으로 *string* 을 해석한다. 이때 언어는 *date_lang_string_literal* 인자에 의해 정해진다. *date_lang_string_literal* 인자가 생략되면 "en_US"가 적용된다. 주어진 문자열과 대응하지 않는 *format* 인자가 지정되면 에러를 반환한다.

*format* 인자가 생략되면 먼저 Rye 기본 형식에 따라 *string*을 해석한다.

``` sql
--selecting a time type value casted from a string in the specified format
SELECT TO_TIME ('13:10:30');
```
```
  to_time('13:10:30')   
========================
  '13:10:30'            
```
``` sql
SELECT TO_TIME('HOUR: 13 MINUTE: 10 SECOND: 30', '"HOUR:" HH24 "MINUTE:" MI "SECOND:" SS');
```
```
  to_time('HOUR: 13 MINUTE: 10 SECOND: 30', '"HOUR:" HH24 "MINUTE:" MI "SECOND:" SS')   
========================================================================================
  '13:10:30'                                                                            
```
``` sql
SELECT TO_TIME ('13:10:30', 'HH24:MI:SS');
```
```
  to_time('13:10:30', 'HH24:MI:SS')   
======================================
  '13:10:30'                          
```
``` sql
SELECT TO_TIME ('13:10:30', 'HH12:MI:SS');
```
```
ERROR: Conversion error in date format.
```
