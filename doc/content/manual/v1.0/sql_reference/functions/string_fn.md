문자열 함수와 연산자
====================

병합 연산자
-----------

병합 연산자는 피연산자로 문자열 데이터 타입이 지정되며, 병합(concatenation)된 문자열을 반환한다. 문자열 데이터의 병합 연산자로 두 개의 파이프 기호(**||**)가 제공된다. 피연산자로 **NULL** 이 지정된 경우는 **NULL** 값이 반환된다.

``` sql
SELECT 'Rye' || ',' || '2017';
```
```
  'Rye'||','||'2017'   
=======================
  'Rye,2017'           
```
``` sql
SELECT EXTRACT(YEAR FROM SYS_DATETIME)||EXTRACT(MONTH FROM SYS_DATETIME);
```
```
  extract(year  from  SYS_DATETIME )|| extract(month  from  SYS_DATETIME )   
=============================================================================
  '20177'                                                                    
```
``` sql
SELECT 'Rye' || ',' || NULL;
```
```
  'Rye'||','||null   
=====================
  NULL               
```

ASCII
-----

**ASCII(*str*)**

ASCII 함수는 인자로 지정된 문자열의 가장 좌측 문자에 대한 ASCII 코드 값을 숫자로 반환한다. 입력 문자열이 NULL 이면 NULL 을 반환한다. ASCII 함수는 1바이트 문자에 대해 동작한다. 숫자가 입력되면 문자열로 변환한 후 가장 왼쪽 문자의 ASCII 코드 값을 반환한다.

Parameters:
- str: 입력 문자열

Return type:	STRING

``` sql
SELECT ASCII('5');
```
```
  ascii('5')   
===============
  53           
```
``` sql
SELECT ASCII('ab');
```
```
  ascii('ab')   
================
  97            
```

BIN
---

**BIN(*n*)**

BIN 함수는 BIGINT 타입의 숫자를 이진 문자열로 표현한다. 입력 인자가 NULL 이면 NULL 을 반환한다. BIGNIT로 변환되지 않는 문자열을 입력할 때 에러를 반환한다.

Parameters:
- n: BIGINT 타입의 숫자

Return type:	STRING

``` sql
SELECT BIN(12);
```
```
  bin(12)   
============
  '1100'    
```

BIT\_LENGTH
-----------

**BIT_LENGTH(*string*)**

BIT_LENGTH 함수는 비트열의 길이(bit)를 정수값으로 반환한다.

Parameters:
- string: 비트 단위로 길이를 구할 비트열을 지정한다. NULL 이 지정된 경우는 NULL 값이 반환된다.

Return type:	INT

``` sql
SELECT BIT_LENGTH(B'010101010');
```
```
  bit_length(B'010101010')   
=============================
  9                          
```

CHAR\_LENGTH, CHARACTER\_LENGTH, LENGTHB, LENGTH
------------------------------------------------

**CHAR_LENGTH(*string*)**

**CHARACTER_LENGTH(*string*)**

**LENGTHB(*string*)**

**LENGTH(*string*)**

문자의 개수를 정수 값으로 반환한다. CHAR_LENGTH, CHARACTER_LENGTH, LENGTHB, LENGTH 함수는 동일하다.

Parameters:
- string: 문자 개수 단위로 길이를 구할 문자열을 지정한다. NULL 이 지정된 경우는 NULL 값이 반환된다.

Return type:	INT

Note:
-   문자열 내에 포함된 공백 문자(space)의 길이는 1바이트이다.
-   공백 문자를 표현하기 위한 빈 따옴표('')의 길이는 0이다.

``` sql
--character set is UTF-8 for Korean characters
SELECT LENGTH('');
```
```
  char_length('')   
====================
  0                 
```
``` sql
SELECT LENGTH('Rye');
```
```
  char_length('Rye')   
=======================
  3                    
```
``` sql
SELECT LENGTH('라이');
```
```
  char_length('라이')   
======================
  2                   
```
``` sql
CREATE GLOBAL TABLE length_tbl (id INT PRIMARY KEY, varchar_1 VARCHAR, varchar_2 VARCHAR);
INSERT INTO length_tbl VALUES(1, '', ''); --Length of empty string
INSERT INTO length_tbl VALUES(2, 'a', 'a'); --English character
INSERT INTO length_tbl VALUES(3, NULL, '라'); --Korean character and NULL
INSERT INTO length_tbl VALUES(4, ' ', ' 라'); --Korean character and space

SELECT LENGTH(varchar_1), LENGTH(varchar_2) FROM length_tbl;
```
```
  char_length(varchar_1)   char_length(varchar_2)   
====================================================
  0                        0                        
  1                        1                        
  NULL                     1                        
  1                        2                        
```

CHR
---

**CHR(*number_operand*)**

CHR 함수는 인자로 지정된 연산식의 리턴 값에 대응하는 문자를 반환하는 함수이다. 유효하지 않은 범위의 코드 값을 입력할 때 에러를 반환한다.

Parameters:
- number_operand: 수치값을 반환하는 임의의 연산식을 지정한다.

Return type: STRING

``` sql
SELECT CHR(68) || CHR(68-2);
```
```
  chr(68)|| chr(68-2)   
========================
  'DB'                  
```
**CHR** 함수를 사용해서 멀티바이트 문자를 반환하려면 utf8 문자셋에 대해 유효한 범위의 숫자를 입력한다.

``` sql
SELECT CHR(14909886);
```
```
  chr(14909886)   
==================
  'ま'             
```
문자를 16진수 문자열로 반환하려면 **HEX** 함수를 사용한다.

``` sql
SELECT HEX('ま');
```
```
  hex('ま')   
=============
  'E381BE'   
```
16진수 문자열을 10진수로 반환하려면 **CONV** 함수를 사용한다.

``` sql
SELECT CONV('E381BE',16,10);
```
```
  conv('E381BE', 16, 10)   
===========================
  '14909886'               
```

CONCAT
------

**CONCAT(*string1*, *string2* [,*string3* [, ... [, *stringN*]...]])**

CONCAT 함수는 두 개 이상의 인자가 지정되며, 모든 인자 값을 연결한 문자열을 결과로 반환한다. 지정 가능한 인자의 개수는 제한이 없으며, 문자열 타입이 아닌 인자가 지정되는 경우 자동으로 타입 변환이 수행된다. 인자 중에 NULL 이 포함되면 결과로 NULL 을 반환한다.

인자로 지정된 문자열 사이에 구분자(separator)를 삽입하여 연결하려면, `CONCAT_WS()` 함수를 사용한다.

Parameters:
- strings: 연결할 문자열들

Return type:	STRING

``` sql
SELECT CONCAT('Rye', '2017' , 'R1.0');
```
```
  concat('Rye', '2017', 'R1.0')   
==================================
  'Rye2017R1.0'                   
```
``` sql
--it returns null when null is specified for one of parameters
SELECT CONCAT('Rye', '2016' , 'R1.0', NULL);
```
```
  concat('Rye', '2016', 'R1.0', null)   
========================================
  NULL                                  
```
``` sql
--it converts number types and then returns concatenated strings
SELECT CONCAT(2017, 1.0);
```
```
  concat(2017, 1.0)   
======================
  '20171.0'           
```

CONCAT\_WS
----------

**CONCAT_WS(*string1*, *string2* [,*string3* [, ... [, *stringN*]...]])**

CONCAT_WS 함수는 두 개 이상의 인자가 지정되며, 첫 번째 인자 값을 구분자로 이용하여 나머지 인자 값을 연결한 문자열을 결과로 반환한다. 지정 가능한 인자의 개수에는 제한이 없으며, 문자열 타입이 아닌 인자가 지정되는 경우 자동으로 타입 변환이 수행된다. 만약, 구분자로 NULL 이 지정되면 NULL 을 반환하고, 구분자 다음에 위치하는 나머지 인자에 NULL 이 지정되면 이를 무시하고 문자열을 반환한다.

Parameters:
- strings: 연결할 문자열들

Return type:	STRING

``` sql
SELECT CONCAT_WS(' ', 'Rye', '2017' , 'R1.0');
```
```
  concat_ws(' ', 'Rye', '2017', 'R1.0')   
==========================================
  'Rye 2017 R1.0'                         
```
``` sql
--it returns strings even if null is specified for one of parameters
SELECT CONCAT_WS(' ', 'Rye', '2017', NULL, 'R1.0');
```
```
  concat_ws(' ', 'Rye', '2017', null, 'R1.0')   
================================================
  'Rye 2017 R1.0'                               
```

``` sql
--it converts number types and then returns concatenated strings with separator
SELECT CONCAT_WS(' ',2017, 1.0);
```
```
  concat_ws(' ', 2017, 1.0)   
==============================
  '2017 1.0'                  
```

ELT
---

**ELT(*N*, *string1*, *string2*, ...)**

ELT 함수는 N이 1이면 string1을 반환하고, N이 2이면 string2를 반환한다. 리턴 값은 VARCHAR 타입이다. 조건식은 필요에 따라 늘릴 수 있다.

문자열의 최대 바이트 길이는 33,554,432이며 이를 초과하면 NULL을 반환한다.

N이 0 또는 음수이면 빈 문자열을 반환한다. N이 입력 문자열의 개수보다 크면 범위를 벗어나므로 NULL을 반환한다. N이 정수로 변환할 수 없는 타입이면 에러를 반환한다.

Parameters:
- N: 문자열 리스트 중 반환할 문자열의 위치
- strings: 문자열 리스트

Return type: STRING

``` sql
SELECT ELT(3,'string1','string2','string3');
```
```
  elt(3, 'string1', 'string2', 'string3')   
============================================
  'string3'                                 
```
``` sql
SELECT ELT('3','1/1/1','23:00:00','2001-03-04');
```
```
  elt('3', '1/1/1', '23:00:00', '2001-03-04')   
================================================
  '2001-03-04'                                  
```
``` sql
SELECT ELT(-1, 'string1','string2','string3');
```
```
  elt(-1, 'string1', 'string2', 'string3')   
=============================================
  NULL                                       
```
``` sql
SELECT ELT(4,'string1','string2','string3');
```
```
  elt(4, 'string1', 'string2', 'string3')   
============================================
  NULL                                      
```
``` sql
SELECT ELT(3.2,'string1','string2','string3');
```
```
  elt(3.2, 'string1', 'string2', 'string3')   
==============================================
  'string3'                                   
```
``` sql
SELECT ELT('a','string1','string2','string3');
```
```
    Error: Cannot coerce value of type "character varying" to type "integer".
```

FIELD
-----

**FIELD( *search_string*, *string1* [,*string2* [, ... [, *stringN*]...]])**

FIELD 함수는 string1 , string2 등의 인자 중 search_string과 동일한 인자의 위치 인덱스 값(포지션)을 반환한다. search_string과 동일한 인자가 없으면 0을 반환한다. search_string이 NULL이면 다른 인자와 비교 연산을 수행할 수 없으므로 0을 반환한다.

FIELD 함수에서 지정된 모든 인자가 문자열 타입이면 문자열 비교 연산을 수행하고, 모두 수치 타입이면 수치 비교 연산을 수행한다. 어느 한 인자의 타입이 나머지와 다른 경우, 모든 인자를 첫 번째 인자의 타입으로 변환하여 비교 연산을 수행한다. 각 인자와의 비교 연산 도중 타입 변환에 실패하면 비교 연산의 결과를 FALSE로 간주하고, 나머지 연산을 계속 진행한다.

Parameters:
- search_string: 검색할 문자열 패턴
- strings: 검색되는 문자열들의 리스트

Return type:	INT

``` sql
SELECT FIELD('abc', 'a', 'ab', 'abc', 'abcd', 'abcde');
```
```
  field('abc', 'a', 'ab', 'abc', 'abcd', 'abcde')   
====================================================
  3                                                 
```
``` sql
--it returns 0 when no same string is found in the list
SELECT FIELD('abc', 'a', 'ab', NULL);
```
```
  field('abc', 'a', 'ab', null)   
==================================
  0                               
```
``` sql
--it returns 0 when null is specified in the first parameter
SELECT FIELD(NULL, 'a', 'ab', NULL);
```
```
  field(null, 'a', 'ab', null)   
=================================
  0                              
```
``` sql
SELECT FIELD('123', 1, 12, 123.0, 1234, 12345);
```
```
  field('123', 1, 12, 123.0, 1234, 12345)   
============================================
  0                                         
```
``` sql
SELECT FIELD(123, 1, 12, '123.0', 1234, 12345);
```
```
  field(123, 1, 12, '123.0', 1234, 12345)   
============================================
  0                                         
```

FIND\_IN\_SET
-------------

**FIND_IN_SET(*str*, *strlist*)**

FIND_IN_SET 함수는 여러 개의 문자열을 쉼표(,)로 연결하여 구성한 문자열 리스트 strlist 에서 특정 문자열 str 이 존재하면 str 의 위치를 반환한다. strlist 에 str 이 존재하지 않거나 strlist 가 빈 문자열이면 0을 반환한다. 둘 중 하나의 인자가 NULL 이면 NULL 을 반환한다. str 이 쉼표를 포함하면 제대로 동작하지 않는다.

Parameters:
- str: 검색 대상 문자열
- strlist: 쉼표로 구분한 문자열의 집합

Return type:	INT

``` sql
SELECT FIND_IN_SET('b','a,b,c,d');
```
```
  find_in_set('b', 'a,b,c,d')   
================================
  2                             
```

FROM\_BASE64
------------

**FROM_BASE64(*str*)**

FROM_BASE64 함수는 TO_BASE64 함수에서 사용되는 base-64 암호화 규칙으로 암호화된 문자열을 인자로 입력받아 복호화된 결과를 바이너리 문자열로 반환한다. 입력 인자가 NULL이면 NULL을 반환한다. 유효하지 않은 base-64 문자열일 때 에러를 반환한다. 암호화 규칙에 대한 상세 내용은 `TO_BASE64()`를 참고한다.

Parameters:
- str -- 입력 문자열

Return type:	STRING

``` sql
SELECT TO_BASE64('abcd'), FROM_BASE64(TO_BASE64('abcd'));
```
```
  to_base64('abcd')   from_base64( to_base64('abcd'))   
========================================================
  'YWJjZA=='          'abcd'                            
```

INSERT
------

**INSERT(*str*, *pos*, *len*, *string*)**

INSERT 함수는 입력 문자열의 특정 위치부터 정해진 길이만큼 부분 문자열을 삽입한다. 리턴 값은 VARCHAR 타입이다. 문자열의 최대 길이는 33,554,432이며 이를 초과하면 NULL 을 반환한다.

Parameters:
- str: 입력 문자열
- pos: str 의 위치. 1부터 시작한다. pos 가 1보다 작거나 string 의 길이+1보다 크면, string 을 삽입하지 않고 str 을 리턴한다.
- len: str 의 pos 에 삽입할 string 의 길이. len 이 부분 문자열의 길이를 초과하면, str 의 pos 에서 string 만큼 삽입한다. len 이 음수이면 str 이 문자열의 끝이 된다.
- string: str 에 삽입할 부분 문자열

Return type:	STRING

``` sql
SELECT INSERT('rye',2,1,'dbsql');
```
```
  insert('rye', 2, 1, 'dbsql')   
=================================
  'rdbsqle'                      
```
``` sql
SELECT INSERT('rye',0,3,'db');
```
```
  insert('rye', 0, 3, 'db')   
==============================
  'rye'                       
```
``` sql
SELECT INSERT('rye',-3,3,'db');
```
```
  insert('rye', -3, 3, 'db')   
===============================
  'rye'                        
```
``` sql
SELECT INSERT('rye',3,100,'db');
```
```
  insert('rye', 3, 100, 'db')   
================================
  'rydb'                        
```
``` sql
SELECT INSERT('rye',3,100,'db');
```
```
  insert('rye', 3, 100, 'db')   
================================
  'rydb'                        
```
``` sql
SELECT INSERT('rye',3,-1,'db');
```
```
  insert('rye', 3, -1, 'db')   
===============================
  'rydb'                       
```

INSTR
-----

**INSTR(*string*, *substring*[, *position*])**

INSTR 함수는 POSITION 함수와 유사하게 문자열 string 내에서 문자열 substring 의 위치를 반환한다. 단, INSTR 함수는 substring 의 검색을 시작할 위치를 지정할 수 있으므로 중복된 substring 을 검색할 수 있다.

Parameters:
- string: 입력 문자열을 지정한다.
- substring: 위치를 반환할 문자열을 지정한다.
- position: 선택 사항으로 탐색을 시작할 string 의 위치를 나타내며, 문자 개수 단위로 지정된다. 이 인자가 생략되면 기본값인 1 이 적용된다. string 의 첫 번째 위치는 1로 지정된다. 값이 음수이면 string 의 끝에서부터 지정된 값만큼 떨어진 위치에서 역방향으로 string 을 탐색한다.

Return type:	INT

``` sql
--character set is UTF-8 for Korean characters
--it returns position of the first 'b'
SELECT INSTR ('12345abcdeabcde','b');
```
```
  instr('12345abcdeabcde', 'b', 1)   
=====================================
  7                                  
```
``` sql
-- it returns position of the first '나' on UTF-8 Korean charset
SELECT INSTR ('12345가나다라마가나다라마', '나' );
```
```
  instr('12345가나다라마가나다라마', '나', 1)   
=====================================
  7                                  
```
``` sql
-- it returns position of the second '나' on UTF-8 Korean charset
SELECT INSTR ('12345가나다라마가나다라마', '나', 11 );
```
```
  instr('12345가나다라마가나다라마', '나', 11)   
======================================
  12                                  
```
``` sql
--it returns position of the 'b' searching from the 8th position
SELECT INSTR ('12345abcdeabcde','b', 8);
```
```
  instr('12345abcdeabcde', 'b', 8)   
=====================================
  12                                 
```
``` sql
--it returns position of the 'b' searching backwardly from the end
SELECT INSTR ('12345abcdeabcde','b', -1);
```
```
  instr('12345abcdeabcde', 'b', -1)   
======================================
  12                                  
```
``` sql
--it returns position of the 'b' searching backwardly from a specified position
SELECT INSTR ('12345abcdeabcde','b', -8);
```
```
  instr('12345abcdeabcde', 'b', -8)   
======================================
  7                                   
```

LCASE, LOWER
------------

**LCASE(*string*)**

**LOWER(*string*)**

LCASE 함수와 LOWER 함수는 동일하며, 문자열에 포함된 대문자를 소문자로 변환한다.

Parameters:
- string: 소문자로 변환할 문자열을 지정한다. 값이 NULL 이면 결과는 NULL 이 반환된다.

Return type:	STRING

``` sql
SELECT LOWER('');
```
```
  lower('')   
==============
  ''          
```
``` sql
SELECT LOWER(NULL);
```
```
  lower(null)   
================
  NULL          
```
``` sql
SELECT LOWER('Rye');
```
```
  lower('Rye')   
=================
  'rye'          
```
``` sql
SELECT LOWER('Ă');
```
```
  lower('Ă')   
===============
  'Ă'          
```

LEFT
----

**LEFT(*string*, *length*)**

LEFT 함수는 string 의 가장 왼쪽에서부터 length 개의 문자를 반환한다. 어느 하나의 인자가 NULL 인 경우 NULL 이 반환되고, string 길이보다 큰 값이나 음수가 length 로 지정되면 문자열 전체를 반환한다. 문자열의 가장 오른쪽에서부터 length 길이의 문자열을 추출하려면 RIGHT() 를 사용한다.

Parameters:
- string: 입력 문자열
- length: 반환할 문자열의 길이

Return type:	STRING

``` sql
SELECT LEFT('Rye', 2);
```
```
  left('Rye', 2)   
===================
  'Ry'             
```
``` sql
SELECT LEFT('Rye', 10);
```
```
  left('Rye', 10)   
====================
  'Rye'             
```

LOCATE
------

**LOCATE(*substring*, *string*[, *position*])**

LOCATE 함수는 문자열 string 내에서 문자열 substring 의 위치 인덱스 값을 반환한다. 세 번째 인자 position 은 생략할 수 있으며, 이 인자가 지정되면 해당 위치에서부터 substring 을 검색하여 처음 검색한 위치 인덱스 값을 반환한다. substring 이 string 내에서 검색되지 않으면 0을 반환한다. LOCATE 함수는 `POSITION()` 와 유사하게 동작하지만, 비트열에 대해서는 LOCATE 함수를 적용할 수 없다.

Parameters:
- substring: 검색 대상 문자열의 패턴
- string: 전체 문자열
- position: 검색 시작 위치

Return type:	INT

``` sql
--it returns 1 when substring is empty space
SELECT LOCATE ('', '12345abcdeabcde');
```
```
  locate('', '12345abcdeabcde')   
==================================
  1                               
```
``` sql
--it returns position of the first 'abc'
SELECT LOCATE ('abc', '12345abcdeabcde');
```
```
  locate('abc', '12345abcdeabcde')   
=====================================
  6                                  
```
``` sql
--it returns position of the second 'abc'
SELECT LOCATE ('abc', '12345abcdeabcde', 8);
```
```
  locate('abc', '12345abcdeabcde', 8)   
========================================
  11                                    
```
``` sql
--it returns position of the first 'abc'
SELECT LOCATE ('ABC', '12345abcdeabcde');
```
```
  locate('ABC', '12345abcdeabcde')   
=====================================
  6                                  
```

LPAD
----

**LPAD(*char1*, *n*[, *char2*])**

LPAD 함수는 문자열이 일정 길이가 될 때까지 왼쪽에 특정 문자를 덧붙인다.

Parameters:
- char1: 덧붙이는 대상 문자열을 지정한다. char1 의 길이보다 작은 n 이 지정되면, 패딩을 수행하지 않고 char1 을 길이 n 으로 잘라내어 반환한다. 값이 NULL 이면 결과는 NULL 이 반환된다.
- n: char1 의 전체 문자 개수를 지정한다. 값이 NULL 이면 결과는 NULL 이 반환된다.
- char2: char1 의 길이가 n 이 될 때까지 왼쪽에 덧붙일 문자열을 지정한다. 이를 지정하지 않으면 공백 문자(' ')가 char2 의 기본값으로 사용된다. 값이 NULL 이면 결과는 NULL 이 반환된다.

Return type:	STRING

``` sql
--character set is UTF-8 for Korean characters
--it returns only 2 characters if not enough length is specified
SELECT LPAD ('Rye', 2, '?');
```
```
  lpad('Rye', 2, '?')   
========================
  'Ry'                  
```
``` sql
SELECT LPAD ('라이', 1, '?');
```
```
  lpad('라이', 1, '?')   
=======================
  '라'                  
```
``` sql
--padding spaces on the left till char_length is 10
SELECT LPAD ('Rye', 10);
```
```
  lpad('Rye', 10)   
====================
  '       Rye'      
```
``` sql
--padding specific characters on the left till char_length is 10
SELECT LPAD ('Rye', 10, '?');
```
```
  lpad('Rye', 10, '?')   
=========================
  '???????Rye'           
```
``` sql
--padding specific characters on the left till char_length is 10
SELECT LPAD ('라이', 10, '?');
```
```
  lpad('라이', 10, '?')   
========================
  '????????라이'          
```
``` sql
--padding 4 characters on the left
SELECT LPAD ('라이', LENGTH('라이')+4, '?');
```
```
  lpad('라이',  char_length('라이')+4, '?')   
==========================================
  '????라이'                                
```

LTRIM
-----

**LTRIM(*string*[, *trim_string*])**

LTRIM 함수는 문자열의 왼쪽(앞 부분)에 위치한 특정 문자를 제거한다.

Parameters:
- string: 트리밍할 문자열 또는 문자열 타입의 칼럼을 입력하며, 이 값이 NULL 이면 결과는 NULL 이 반환된다.
- trim_string: string 의 왼쪽에서 제거하고자 하는 특정 문자열을 지정할 수 있으며, 이를 지정하지 않으면 공백 문자(' ')가 자동으로 지정되어 대상 문자열의 왼쪽에 위치한 공백이 제거된다.

Return type:	STRING

``` sql
--trimming spaces on the left
SELECT LTRIM ('     Olympic     ');
```
```
  ltrim('     Olympic     ')   
===============================
  'Olympic     '               
```
``` sql
--If NULL is specified, it returns NULL
SELECT LTRIM ('iiiiiOlympiciiiii', NULL);
```
```
  ltrim('iiiiiOlympiciiiii', null)   
=====================================
  'iiiiiOlympiciiiii'                
```
``` sql
-- trimming specific strings on the left
SELECT LTRIM ('iiiiiOlympiciiiii', 'i');
```
```
  ltrim('iiiiiOlympiciiiii', 'i')   
====================================
  'Olympiciiiii'                    
```

MID
---

**MID(*string*, *position*, *substring_length*)**

MID 함수는 문자열 string 내의 position 위치로부터 substring_length 길이의 문자열을 추출하여 반환한다. 만약, position 값으로 음수가 지정되면, 문자열의 끝에서부터 역방향으로 위치를 산정한다. substring_length 는 생략할 수 없으며, 음수가 지정되는 경우 이를 0으로 간주하여 공백 문자열을 반환한다.

MID 함수는 `SUBSTR()` 와 유사하게 동작하나, 비트열에 대해서는 적용할 수 없고, substring_length 인자를 생략할 수 없으며, substring_length 에 음수가 지정되면 공백 문자열을 반환한다는 차이점이 있다.

Parameters:
- string: 입력 문자열을 지정한다. 입력 값이 NULL 이면 결과로 NULL 이 반환된다.
- position: 문자열을 추출할 시작 위치를 지정한다. 첫 번째 문자의 위치는 1이며, 0으로 지정되더라도 1로 간주된다. 입력 값이 NULL 이면 결과로 NULL 이 반환된다.
- substring_length: 추출할 문자열의 길이를 지정한다. 0 또는 음수가 지정되는 경우 공백 문자열이 반환되고, 입력 값이 NULL 이면 결과로 NULL 이 반환된다.

Return type: STRING

``` sql
CREATE GLOBAL TABLE mid_tbl(id INT PRIMARY KEY, a VARCHAR);
INSERT INTO mid_tbl VALUES(1, '12345abcdeabcde');

--it returns empty string when substring_length is 0
SELECT MID(a, 6, 0), SUBSTR(a, 6, 0), SUBSTRING(a, 6, 0) FROM mid_tbl;
```
```
  mid(a, 6, 0)   substr(a, 6, 0)   substring(a from 6 for 0)   
===============================================================
  ''             ''                ''                          
```
``` sql
--it returns 4-length substrings counting from the 6th position
SELECT MID(a, 6, 4), SUBSTR(a, 6, 4), SUBSTRING(a, 6, 4) FROM mid_tbl;
```
```
  mid(a, 6, 4)   substr(a, 6, 4)   substring(a from 6 for 4)   
===============================================================
  'abcd'         'abcd'            'abcd'                      
```
``` sql
--it returns an empty string when substring_length < 0
SELECT MID(a, 6, -4), SUBSTR(a, 6, -4), SUBSTRING(a, 6, -4) FROM mid_tbl;
```
```
  mid(a, 6, -4)   substr(a, 6, -4)   substring(a from 6 for -4)   
==================================================================
  ''              NULL               'abcdeabcde'                 
```
``` sql
--it returns 4-length substrings at 6th position counting backward from the end
SELECT MID(a, -6, 4), SUBSTR(a, -6, 4), SUBSTRING(a, -6, 4) FROM mid_tbl;
```
```
  mid(a, -6, 4)   substr(a, -6, 4)   substring(a from -6 for 4)   
==================================================================
  'eabc'          'eabc'             '1234'                       
```

OCTET\_LENGTH
-------------

**OCTET_LENGTH(*string*)**

OCTET_LENGTH 함수는 문자열의 바이트(byte) 길이를 정수로 반환한다.

Parameters:
- string: 바이트 단위로 길이를 구할 문자열을 지정한다. NULL 이 지정된 경우는 NULL 값이 반환된다.

Return type:	INT

``` sql
--character set is UTF-8 for Korean characters
SELECT OCTET_LENGTH('');
```
```
  octet_length('')   
=====================
  0                  
```
``` sql
SELECT OCTET_LENGTH('Rye');
```
```
  octet_length('Rye')   
========================
  3                     
```
``` sql
SELECT OCTET_LENGTH('라이');
```
```
  octet_length('라이')   
=======================
  6                    
```

POSITION
--------

**POSITION(*substring* IN *string*)**

POSITION 함수는 문자열 string 내에서 문자열 substring 의 위치를 반환한다.

이 함수의 인자로 문자열을 반환하는 임의의 연산식을 지정할 수 있으며, 리턴 값은 0 이상의 정수이다. 문자열에 대해서는 문자 개수 단위로 위치 값을 반환한다.

POSITION 함수는 가끔 다른 함수와 연결되어서 사용된다. 예를 들어, 특정 문자열에서 일부 문자열을 추출하고 싶은 경우에 POSITION 함수의 결과를 SUBSTRING 함수의 입력으로 사용할 수 있다.

Parameters:
- substring: 위치를 반환할 문자열을 지정한다. 값이 공백 문자열이면 1이 반환된다. NULL 이면 NULL 이 반환된다.

Return type:	INT

``` sql
--character set is UTF-8 for Korean characters
--it returns 1 when substring is empty space
SELECT POSITION ('' IN '12345abcdeabcde');
```
```
  position('' in '12345abcdeabcde')   
======================================
  1                                   
```
``` sql
--it returns position of the first 'b'
SELECT POSITION ('b' IN '12345abcdeabcde');
```
```
  position('b' in '12345abcdeabcde')   
=======================================
  7                                    
```
``` sql
-- it returns position of the first '나'
SELECT POSITION ('나' IN '12345가나다라마가나다라마');
```
```
  position('나' in '12345가나다라마가나다라마')   
=======================================
  7                                    
```
``` sql
--it returns 0 when no substring found in the string
SELECT POSITION ('f' IN '12345abcdeabcde');
```
```
  position('f' in '12345abcdeabcde')   
=======================================
  0                                    
```

REPEAT
------

**REPEAT(*string*, *count*)**

REPEAT 함수는 입력 문자열에 대해 반복 횟수만큼의 문자열을 반환한다. 리턴 값은 VARCHAR 타입이다. 문자열의 최대 길이는 33,554,432이며, 이를 초과하면 NULL 을 반환한다. 입력 인자 중 하나가 NULL 이면 NULL 을 반환한다.

Parameters:
- substring: 문자열
- count: 반복 횟수. 0 또는 음수를 입력하면 빈 문자열을 반환하고, 숫자가 아닌 다른 데이터 타입을 입력하면 에러를 반환한다.

Return type:	STRING

``` sql
SELECT REPEAT('rye',3);
```
```
  repeat('rye', 3)   
=====================
  'ryeryerye'        
```
``` sql
SELECT REPEAT('rye',32000000);
```
```
    Error: Trying to create a string requiring 96000000 bytes of memory, while the maximum allowed is 33554432 bytes.
```
``` sql
SELECT REPEAT('rye',-1);
```
```
  repeat('rye', -1)   
======================
  ''                  
```
``` sql
SELECT REPEAT('rye','a');
```
```
    Error: Cannot coerce value of type "character varying" to type "integer".
```

REPLACE
-------

**REPLACE(*string*, *search_string*[, *replacement_string*])**

REPLACE 함수는 주어진 문자열 string 내에서 문자열 search_string 을 검색하여 이를 문자열 replacement_string 으로 대체한다. 이때, 대체할 문자열 replacement_string 이 생략되면 string 내에서 검색된 search_string 이 모두 제거된다. 만약, 인자에 NULL 이 지정되면, NULL 이 반환된다.

Parameters:
- string: 원본 문자열을 지정한다. 값이 NULL 이면 결과로 NULL 이 반환된다.
- search_string: 검색할 문자열을 지정한다. 값이 NULL 이면 결과로 NULL 이 반환된다.
- replacement_string: search_string 을 대체할 문자열을 지정한다. 값이 생략되면 string 에서 search_string 을 제거하여 반환한다. 값이 NULL 이면 결과로 NULL 이 반환된다.

Return type:	STRING

``` sql
--it returns NULL when an argument is specified with NULL value
SELECT REPLACE('12345abcdeabcde','abcde',NULL);
```
```
  replace('12345abcdeabcde', 'abcde', null)   
==============================================
  NULL                                        
```
``` sql
--not only the first substring but all substrings into 'ABCDE' are replaced
SELECT REPLACE('12345abcdeabcde','abcde','ABCDE');
```
```
  replace('12345abcdeabcde', 'abcde', 'ABCDE')   
=================================================
  '12345ABCDEABCDE'                              
```
``` sql
--it removes all of substrings when replace_string is omitted
SELECT REPLACE('12345abcdeabcde','abcde');
```
```
  replace('12345abcdeabcde', 'abcde')   
========================================
  '12345'                               
```

다음은 개행 문자(newline)를 "\\n"으로 출력하도록 하는 예이다.

``` sql
CREATE GLOBAL TABLE tbl (cmt_no INT PRIMARY KEY, cmt VARCHAR(1024));
INSERT INTO tbl VALUES (1234,
'This is a test for

 new line.');

SELECT REPLACE(cmt, CHR(10), '\n')
FROM tbl
WHERE cmt_no=1234;
```
```
  replace(cmt,  chr(10), '\n')         
=======================================
  'This is a test for\n\n new line.'   
```

REVERSE
-------

**REVERSE(*string*)**

REVERSE 함수는 문자열 string을 역순으로 변환한 후 반환한다.

Parameters:
- string: 입력 문자열을 지정한다. 입력 값이 공백 문자열이면 공백 문자열을 반환하고, NULL 이면 NULL 을 반환한다.

Return type:	STRING

``` sql
SELECT REVERSE('Rye');
```
```
  reverse('Rye')   
===================
  'eyR'            
```

RIGHT
-----

**RIGHT(*string*, *length*)**

RIGHT 함수는 string 의 가장 오른쪽에서부터 length 개의 문자를 반환한다. 어느 하나의 인자가 NULL 인 경우 NULL 이 반환되고, string 길이보다 큰 값이나 음수가 length 로 지정되면 문자열 전체를 반환한다. 문자열의 가장 왼쪽에서부터 length 길이의 문자열을 추출하려면 `LEFT()` 를 사용한다.

Parameters:
- string: 입력 문자열
- length: 반환할 문자열의 길이

Return type:	STRING

``` sql
SELECT RIGHT('Rye', 2);
```
```
  right('Rye', 2)   
====================
  'ye'              
```
``` sql
SELECT RIGHT ('Rye', 10);
```
```
  right('Rye', 10)   
=====================
  'Rye'              
```

RPAD
----

**RPAD(*char1*, *n*[, *char2*])**

RPAD 함수는 문자열이 일정 길이가 될 때까지 오른쪽에 특정 문자를 덧붙인다.

Parameters:
- char1: 덧붙이는 대상 문자열을 지정한다. char1 의 길이보다 작은 n 이 지정되면, 패딩을 수행하지 않고 char1 을 길이 n 으로 잘라내어 반환한다. 값이 NULL 이면 결과는 NULL 이 반환된다.
- n: char1 의 전체 길이를 지정한다. 값이 NULL 이면 결과는 NULL 이 반환된다.
- char2: char1 의 길이가 n 이 될 때까지 오른쪽에 덧붙일 문자열을 지정한다. 이를 지정하지 않으면 공백 문자(' ')가 char2 의 기본값으로 사용된다. 값이 NULL 이면 결과는 NULL 이 반환된다.

Return type:	STRING

``` sql
--character set is UTF-8 for Korean characters
--it returns only 2 characters if not enough length is specified
SELECT RPAD ('Rye', 2, '?');
```
```
  rpad('Rye', 2, '?')   
========================
  'Ry'                  
```
``` sql
--on multi-byte charset, it returns the first character only with a right-padded space
SELECT RPAD ('라이', 1, '?');
```
```
  rpad('라이', 1, '?')   
=======================
  '라'                  
```
``` sql
--padding spaces on the right till char_length is 10
SELECT RPAD ('Rye', 10);
```
```
  rpad('Rye', 10)   
====================
  'Rye       '      
```
``` sql
--padding specific characters on the right till char_length is 10
SELECT RPAD ('Rye', 10, '?');
```
```
  rpad('Rye', 10, '?')   
=========================
  'Rye???????'           
```
``` sql
--padding specific characters on the right till char_length is 10
SELECT RPAD ('라이', 10, '?');
```
```
  rpad('라이', 10, '?')   
========================
  '라이????????'          
```
``` sql
--padding 4 characters on the right
SELECT RPAD ('라이', LENGTH('라이')+4, '?');
```
```
  rpad('라이',  char_length('라이')+4, '?')   
==========================================
  '라이????'                                
```

RTRIM
-----

**RTRIM(*string*[, *trim_string*])**

RTRIM 함수는 문자열의 오른쪽(뒷 부분)에 위치한 특정 문자를 제거한다.

Parameters:
- string: 트리밍할 문자열 또는 문자열 타입의 칼럼을 입력하며, 이 값이 NULL 이면 결과는 NULL 이 반환된다.
- trim_string: string 의 오른쪽에서 제거하고자 하는 특정 문자열을 지정할 수 있으며, 이를 지정하지 않으면 공백 문자(' ')가 자동으로 지정되어 대상 문자열의 오른쪽에 위치한 공백이 제거된다.

Return type:	STRING

``` sql
SELECT RTRIM ('     Olympic     ');
```
```
  rtrim('     Olympic     ')   
===============================
  '     Olympic'               
```
``` sql
--If NULL is specified, it returns NULL
SELECT RTRIM ('iiiiiOlympiciiiii', NULL);
```
```
  rtrim('iiiiiOlympiciiiii', null)   
=====================================
  'iiiiiOlympiciiiii'                
```
``` sql
-- trimming specific strings on the right
SELECT RTRIM ('iiiiiOlympiciiiii', 'i');
```
```
  rtrim('iiiiiOlympiciiiii', 'i')   
====================================
  'iiiiiOlympic'                    
```

SPACE
-----

**SPACE(*N*)**

SPACE 함수는 지정한 숫자만큼의 공백 문자열을 반환한다. 리턴 값은 VARCHAR 타입이다.

Parameters:
- N: 공백 개수. 최대값은 33,554,432이며 이를 초과하면 에러를 반환한다. 0 또는 음수를 입력하면 빈 문자열을 반환하고, 숫자로 변환할 수 없는 타입을 입력하면 에러를 반환한다.

Return type:	STRING

``` sql
SELECT SPACE(8);
```
```
  space(8)     
===============
  '        '   
```
``` sql
SELECT LENGTH(space(1048576));
```
```
  char_length( space(1048576))   
=================================
  1048576                        
```
``` sql
SELECT LENGTH(space('33554432'));
```
```
  char_length( space('33554432'))   
====================================
  33554432                          
```
``` sql
SELECT SPACE('aaa');
```
```
    Error: Cannot coerce value of type "character varying" to type "integer".
```

STRCMP
------

**STRCMP(*string1*, *string2*)**

STRCMP 함수는 두 개의 문자열 string1, string2 을 비교하여 동일하면 0을 반환하고, string1 이 더 크면 1을 반환하고, string1 이 더 작은 경우에는 -1을 반환한다. 어느 하나의 인자가 NULL 이면 NULL 을 반환한다.

Parameters:
- string1: 비교 대상 문자열
- string2: 비교 대상 문자열

Return type:	INT

``` sql
SELECT STRCMP('abc', 'abc');
```
```
  strcmp('abc', 'abc')   
=========================
  0                      
```

``` sql
SELECT STRCMP ('acc', 'abc');
```
```
  strcmp('acc', 'abc')   
=========================
  1                      
```
``` sql
SELECT STRCMP ('ABC','abc');
```
```
  strcmp('ABC', 'abc')   
=========================
  0                      
```

SUBSTR
------

**SUBSTR(*string*, *position*[, *substring_length*])**

SUBSTR 함수는 문자열 string 내의 position 위치로부터 substring_length 길이의 문자열을 추출하여 반환한다. 만약, position 값으로 음수가 지정되면, 문자열의 끝에서부터 역방향으로 위치를 산정한다. 또한, substring_length 가 생략되는 경우, 주어진 position 위치로부터 마지막까지 문자열을 추출하여 반환한다.

Parameters:
- string: 입력 문자열을 지정한다. 입력 값이 NULL 이면 결과로 NULL 이 반환된다.
- position: 문자열을 추출할 시작 위치를 지정한다. 첫 번째 문자의 위치는 1이며, 0으로 지정되더라도 1로 간주된다. string 길이보다 큰 값을 지정하거나 NULL 을 지정하면 결과로 NULL 이 반환된다.
- substring_length: 추출할 문자열의 길이를 지정한다. 이 인자가 생략되면 position 위치로부터 마지막까지 문자열을 추출한다. 이 인자의 값으로 NULL 이 지정될 수 없으며, 0이 지정되는 경우 공백 문자열이 반환되고, 음수가 지정되는 경우 NULL 이 반환된다.

Return type:	STRING

``` sql
--character set is UTF-8 for Korean characters
--it returns empty string when substring_length is 0
SELECT SUBSTR('12345abcdeabcde',6, 0);
```
```
  substr('12345abcdeabcde', 6, 0)   
====================================
  ''                                
```
``` sql
--it returns 4-length substrings counting from the position
SELECT SUBSTR('12345abcdeabcde', 6, 4), SUBSTR('12345abcdeabcde', -6, 4);
```
```
  substr('12345abcdeabcde', 6, 4)   substr('12345abcdeabcde', -6, 4)   
=======================================================================
  'abcd'                            'eabc'                             
```
``` sql
--it returns substrings counting from the position to the end
SELECT SUBSTR('12345abcdeabcde', 6), SUBSTR('12345abcdeabcde', -6);
```
```
  substr('12345abcdeabcde', 6)   substr('12345abcdeabcde', -6)   
=================================================================
  'abcdeabcde'                   'eabcde'                        
```
``` sql
-- it returns 4-length substrings counting from 11th position
SELECT SUBSTR ('12345가나다라마가나다라마', 11 , 4);
```
```
  substr('12345가나다라마가나다라마', 11, 4)   
=====================================
  '가나다라'                             
```

SUBSTRING
---------

**SUBSTRING ( *string*, *position* [, *substring_length*])**

**SUBSTRING( *string* FROM *position* [FOR *substring_length*] )**

SUBSTRING 함수는 SUBSTR 함수와 유사하며, 문자열 string 내의 position 위치로부터 substring_length 길이의 문자열을 추출하여 반환한다. position 값에 음수가 지정되면, SUBSTRING 함수는 문자열의 처음으로 검색 위치를 산정하고, SUBSTR 함수는 문자열의 끝에서부터 역방향으로 위치를 산정한다. substring_length 값에 음수가 지정되면, SUBSTRING 함수는 해당 인자가 생략된 것으로 처리하지만, SUBSTR 함수는 NULL 을 반환한다.

Parameters:
- string: 입력 문자열을 지정한다. 입력 값이 NULL 이면 결과로 NULL 이 반환된다.
- position: 문자열을 추출할 시작 위치를 지정한다. 0이나 음수가 지정되면, 첫 번째 문자의 위치인 1로 간주된다. string 길이보다 큰 값을 지정하면 공백 문자열이 반환되고, NULL 을 지정하면 NULL 이 반환된다.
- substring_length: 추출할 문자열의 길이를 지정한다. 이 인자가 생략되면 position 위치로부터 마지막까지 문자열을 추출한다. 이 인자의 값으로 NULL 이 지정될 수 없으며, 0이 지정되는 경우 공백 문자열이 반환되고, 음수를 지정하면 무시한다.

Return type:	STRING

``` sql
SELECT SUBSTRING('12345abcdeabcde', -6 ,4), SUBSTR('12345abcdeabcde', -6 ,4);
```
```
  substring('12345abcdeabcde' from -6 for 4)   substr('12345abcdeabcde', -6, 4)   
==================================================================================
  '1234'                                       'eabc'                             
```
``` sql
SELECT SUBSTRING('12345abcdeabcde', 16), SUBSTR('12345abcdeabcde', 16);
```
```
  substring('12345abcdeabcde' from 16)   substr('12345abcdeabcde', 16)   
=========================================================================
  ''                                     NULL                            
```
``` sql
SELECT SUBSTRING('12345abcdeabcde', 6, -4), SUBSTR('12345abcdeabcde', 6, -4);
```
```
  substring('12345abcdeabcde' from 6 for -4)   substr('12345abcdeabcde', 6, -4)   
==================================================================================
  'abcdeabcde'                                 NULL                               
```

SUBSTRING\_INDEX
----------------

**SUBSTRING_INDEX(*string*, *delim*, *count*)**

SUBSTRING_INDEX 함수는 문자열에 포함된 구분자를 세어 count 번째 구분자 앞까지의 부분 문자열을 반환한다. 리턴 값은 VARCHAR 타입이다.

Parameters:
- string: 입력 문자열. 최대 길이는 33,554,432이며, 이를 초과하면 NULL 을 반환한다.
- delim: 구분자. 대소문자를 구분한다.
- count: 구분자가 나타나는 횟수. 양수를 입력하면 문자열의 왼쪽부터 세고, 음수를 입력하면 오른쪽부터 센다. 0이면 빈 문자열을 반환한다. 정수로 변환할 수 없는 타입을 입력하면 에러를 반환한다.

Return type:	STRING

``` sql
SELECT SUBSTRING_INDEX('www.rye.org','.','2');
```
```
  substring_index('www.rye.org', '.', '2')   
=============================================
  'www.rye'                                  
```
``` sql
SELECT SUBSTRING_INDEX('www.rye.org','.','2.3');
```
```
  substring_index('www.rye.org', '.', '2.3')   
===============================================
  'www.rye'                                    
```
``` sql
SELECT SUBSTRING_INDEX('www.rye.org',':','2.3');
```
```
  substring_index('www.rye.org', ':', '2.3')   
===============================================
  'www.rye.org'                                
```
``` sql
SELECT SUBSTRING_INDEX('www.rye.org','rye',1);
```
```
  substring_index('www.rye.org', 'rye', 1)   
=============================================
  'www.'                                     
```
``` sql
SELECT SUBSTRING_INDEX('www.rye.org','.',100);
```
```
  substring_index('www.rye.org', '.', 100)   
=============================================
  'www.rye.org'                              
```

TO\_BASE64
----------

**TO_BASE64(*str*)**

문자열을 base-64 암호화 형식으로 변환하여 결과를 반환한다. 입력 인자가 문자열이 아니면 변환이 발생하기 전에 문자열로 변환된다. 입력 인자가 NULL이면 NULL을 반환한다. Base-64로 암호화된 문자열은 `FROM_BASE64()` 함수로 복호화될 수 있다.

Parameters:
- str: 입력 문자열

Return type:	STRING

``` sql
SELECT TO_BASE64('abcd'), FROM_BASE64(TO_BASE64('abcd'));
```
```
  to_base64('abcd')   from_base64( to_base64('abcd'))   
========================================================
  'YWJjZA=='          'abcd'                            
```
다음은 `TO_BASE64()` 함수와 `FROM_BASE64()` 함수에서 사용되는 암호화 및 복호화 규칙이다.

-   알파벳 값 62에 대한 암호화는 '+'이다.
-   알파벳 값 63에 대한 암호화는 '/'이다.
-   암호화된 결과는 4개의 출력 가능한 문자 그룹으로 구성되어 있다. 입력 데이터의 세 바이트는 네 개의 문자로 암호화된다. 마지막 그룹이 네 개의 문자로 채워지지 않으면 '=' 문자를 덧붙여(padding) 네 개 문자의 길이를 만든다.
-   긴 출력을 여러 개의 라인으로 나누기 위해 76개의 암호화된 출력 문자마다 뉴라인(newline)이 추가된다.
-   복호화는 뉴 라인(newline), 캐리지 리턴(carriage return), 탭, 공백 문자를 인식하고 이들을 무시한다.

TRANSLATE
---------

**TRANSLATE(*string*, *from_substring*, *to_substring*)**

TRANSLATE 함수는 지정된 문자열 string 내에 문자열 from_substring 에 지정된 문자가 존재한다면, 이를 to_substring 에 지정된 문자로 대체한다. 이때, from_substring 과 to_substring 에 지정되는 문자의 순서에 따라 대응 관계를 가지며, to_substring 과 1:1 대응되지 않는 나머지 from_substring 문자는 문자열 string 내에서 모두 제거된다. `REPLACE()` 함수와 유사하게 동작하나, TRANSLATE 함수에서는 to_substring 인자를 생략할 수 없다.

Parameters:
- string: 입력 문자열. 최대 길이는 33,554,432이며, 이를 초과하면 NULL 을 반환한다
- from_substring: 검색할 문자열을 지정한다. 값이 NULL 이면 결과로 NULL 이 반환된다.
- to_substring: from_substring 에 지정된 문자열을 대체할 문자열을 지정하며, 생략할 수 없다. 값이 NULL 이면 결과로 NULL 이 반환된다.

Return type:	STRING

``` sql
--it returns NULL when an argument is specified with NULL value
SELECT TRANSLATE('12345abcdeabcde','abcde', NULL);
```
```
  translate('12345abcdeabcde', 'abcde', null)   
================================================
  NULL                                          
```
``` sql
--it translates 'a','b','c','d','e' into '1', '2', '3', '4', '5' respectively
SELECT TRANSLATE('12345abcdeabcde', 'abcde', '12345');
```
```
  translate('12345abcdeabcde', 'abcde', '12345')   
===================================================
  '123451234512345'                                
```
``` sql
--it translates 'a','b','c' into '1', '2', '3' respectively and removes 'd's and 'e's
SELECT TRANSLATE('12345abcdeabcde','abcde', '123');
```
```
  translate('12345abcdeabcde', 'abcde', '123')   
=================================================
  '12345123123'                                  
```
``` sql
--it removes 'a's,'b's,'c's,'d's, and 'e's in the string
SELECT TRANSLATE('12345abcdeabcde','abcde', '');
```
```
  translate('12345abcdeabcde', 'abcde', '')   
==============================================
  '12345'                                     
```
``` sql
--it only translates 'a','b','c' into '3', '4', '5' respectively
SELECT TRANSLATE('12345abcdeabcde','ABabc', '12345');
```
```
  translate('12345abcdeabcde', 'ABabc', '12345')   
===================================================
  '12345345de345de'                                
```

TRIM
----

**TRIM( [ [ LEADING | TRAILING | BOTH ] [ *trim_string* ] FROM ] *string* )**

TRIM 함수는 문자열의 앞, 뒤 또는 앞뒤에 위치한 특정 문자들을 제거한다.

Parameters:
- trim_string: 대상 문자열의 앞, 뒤 또는 앞뒤에서 제거하고자 하는 특정 문자열을 지정할 수 있으며, 이를 지정하지 않으면 공백 문자(' ')가 자동으로 지정되어 대상 문자열의 앞, 뒤 또는 앞뒤에 위치한 공백이 제거된다.
- string: 트리밍할 문자열 또는 문자열 타입의 칼럼을 입력하며, 이 값이 NULL 이면 NULL 이 반환된다.

Return type:	STRING

-   **\[LEADING|TRAILING|BOTH\]** : 대상 문자열의 어느 위치에서 지정된 문자열을 트리밍할 것인지를 옵션으로 명시할 수 있다. **LEADING** 은 문자열의 앞 부분에서 트리밍을 수행하고, **TRAILING** 은 문자열의 뒷 부분에서 트리밍을 수행하며, **BOTH** 는 앞뒤에서 지정된 문자열을 트리밍한다. 옵션을 명시하지 않으면 기본값은 **BOTH** 이다.
-   *trim\_string* 과 *string* 의 문자열은 같은 문자셋을 가져야 한다.

``` sql
--trimming NULL returns NULL
SELECT TRIM (NULL);
```
```
  trim(both  from null)   
==========================
  NULL                    
```
``` sql
--trimming spaces on both leading and trailing parts
SELECT TRIM ('     Olympic     ');
```
```
  trim(both  from '     Olympic     ')   
=========================================
  'Olympic'                              
```
``` sql
--trimming specific strings on both leading and trailing parts
SELECT TRIM ('i' FROM 'iiiiiOlympiciiiii');
```
```
  trim(both 'i' from 'iiiiiOlympiciiiii')   
============================================
  'Olympic'                                 
```
``` sql
--trimming specific strings on the leading part
SELECT TRIM (LEADING 'i' FROM 'iiiiiOlympiciiiii');
```
```
  trim(leading 'i' from 'iiiiiOlympiciiiii')   
===============================================
  'Olympiciiiii'                               
```
``` sql
--trimming specific strings on the trailing part
SELECT TRIM (TRAILING 'i' FROM 'iiiiiOlympiciiiii');
```
```
  trim(trailing 'i' from 'iiiiiOlympiciiiii')   
================================================
  'iiiiiOlympic'                                
```

UCASE, UPPER
------------

**UCASE(*string*)**

**UPPER(*string*)**

UCASE 함수와 UPPER 함수는 동일하며, 문자열에 포함된 소문자를 대문자로 변환한다.

Parameters:
- string: 대문자로 변환할 문자열을 지정한다. 값이 NULL 이면 결과는 NULL 이 반환된다.

Return type:	STRING

``` sql
SELECT UPPER('');
```
```
  upper('')   
==============
  ''          
```
``` sql
SELECT UPPER(NULL);
```
```
  upper(null)   
================
  NULL          
```
``` sql
SELECT UPPER('Rye');
```
```
  upper('Rye')   
=================
  'RYE'          
```
``` sql
SELECT UPPER('ă');
```
```
  upper('ă')   
===============
  'ă'          
```
