테이블
======

CREATE TABLE
------------

### 테이블 정의

**CREATE TABLE** 문을 사용하여 새로운 테이블을 생성한다.

    CREATE {GLOBAL | SHARD} TABLE table_name
    [(<column_definition>, ... [, <table_constraint>, ...])] 
    [SHARD BY column_name] ;

        <column_definition> ::= 
            column_name <data_type> [<column_constraint>] [<default>]

            <data_type> ::= <column_type> [<collation_modifier_clause>]

                <collation_modifier_clause> ::= COLLATE {<char_string_literal>|<identifier>}

            <default> ::= DEFAULT <value_specification> 

            <column_constraint> ::= [CONSTRAINT constraint_name] {NOT NULL | UNIQUE | PRIMARY KEY}

        <table_constraint> ::=
            [CONSTRAINT [constraint_name]] 
            { 
                UNIQUE [KEY|INDEX](column_name, ...) |
                {KEY|INDEX} [constraint_name](column_name, ...) |
                PRIMARY KEY (column_name, ...)
            }

-   *table_name*: 생성할 테이블의 이름을 지정한다(최대 254바이트).
-   *column_name*: 생성할 칼럼의 이름을 지정한다(최대 254바이트).
-   *column_type*: 칼럼의 데이터 타입을 지정한다.
-   **DEFAULT** *value*: 칼럼의 초기값을 지정한다.
-   &lt;*column_constraint*&gt;: 칼럼의 제약 조건을 지정하며 제약 조건의 종류에는 **NOT NULL**, **UNIQUE**, **PRIMARY KEY** 가 있다. 자세한 내용은 제약조건 정의를 참고한다.


**테이블 스키마의 CHECK 제약 조건**

테이블 스키마에 정의된 CHECK 제약 조건은 파싱되지만, 실제 동작은 무시된다. 

``` sql
CREATE GLOBAL TABLE tbl (
    id INT PRIMARY KEY,
    CHECK (id > 0)
)
```

### 테이블 타입

Rye는 **GLOBAL**, **SHARD** 두 가지 타입의 테이블을 지원한다.
**GLOBAL** 테이블은 모든 샤드 노드에서 동일한 데이터를 가지는 테이블로서 코드 테이블과 같이 **SHARD** 테이블과 조인하는 용도로 주로 사용된다.
**SHARD** 테이블은 shard-key 컬럼을 기준으로 수평 분산된 데이터를 저장하는 테이블로서 대용량의 데이터를 저장하기 위한 용도로 사용된다. **SHARD** 테이블을 생성하는 경우 *SHARD BY &lt;column_name&gt;* 절을 통해 shard-key컬럼을 반드시 지정해야 한다.

테이블 생성시 유의사항
- 모든 테이블은 PRIMARY KEY를 가져야 한다.
- SHARD 테이블의 shard-key 컬럼은 VARCHAR타입만 지원된다.
- SHARD 테이블의 PRIMARY KEY 첫번째 컬럼은 shard-key 컬럼이 지정되어야 한다.

``` sql
CREATE GLOBAL TABLE code_tbl
(
  id INT PRIMARY KEY,
  code_name VARCHAR(20) NOT NULL
);
CREATE SHARD TABLE log_tbl
(
  user_id VARCHAR(50),
  log_date DATETIME,
  code_id  INT,
  PRIMARY KEY(user_id, log_date)
) SHARD BY user_id;
```

### 칼럼 정의

칼럼은 테이블에서 각 열에 해당하는 항목이며, 칼럼은 칼럼 이름과 데이터 타입을 명시하여 정의한다.

        <column_definition> ::= 
            column_name <data_type> [<column_constraint>] [<default>]

            <data_type> ::= <column_type> [<collation_modifier_clause>]

                <collation_modifier_clause> ::= COLLATE {<char_string_literal>|<identifier>}

            <default> ::= DEFAULT <value_specification> 

            <column_constraint> ::= [CONSTRAINT constraint_name] {NOT NULL | UNIQUE | PRIMARY KEY}

#### 칼럼 이름

칼럼 이름 작성 원칙은 [식별자](../identifier.md) 절을 참고한다. 생성한 칼럼의 이름은 **ALTER TABLE** 문의 rename-column을 사용하여 변경할 수 있다.

다음은 *id*, *full_name*, *age* 3개의 칼럼을 가지는 *manager2* 테이블을 생성하는 예제이다.

``` sql
CREATE GLOBAL TABLE manager2 (id INT PRIMARY KEY, full_name VARCHAR(40), age INT );
```

-   칼럼 이름의 첫 글자는 반드시 알파벳이어야 한다.
-   칼럼 이름은 테이블 내에서 고유해야 한다.

#### 칼럼의 초기 값 설정(DEFAULT)

테이블의 칼럼의 초기값을 **DEFAULT** 값을 통해 정의할 수 있다.

-   **DEFAULT**: 새로운 행을 삽입할 때 칼럼 값을 지정하지 않으면 **DEFAULT** 속성으로 설정한 값이 저장된다.

**DEFAULT**의 값으로 허용되는 의사 칼럼(pseudocolumn)과 함수는 다음과 같다.

|DEFAULT 값|데이터 타입|
|--|--|
|SYS_DATETIME|	DATETIME|
|SYS_DATE	 |DATE|
|SYS_TIME	 |TIME|
|USER, USER()|	STRING|

``` sql
CREATE GLOBAL TABLE colval_tbl (id INT PRIMARY KEY, phone VARCHAR DEFAULT '000-0000');
INSERT INTO colval_tbl (id) VALUES (1), (2);
INSERT INTO colval_tbl (id, phone) VALUES (3, '123-4567');
SELECT * FROM colval_tbl;
```
```
  id   phone        
====================
  1    '000-0000'   
  2    '000-0000'   
  3    '123-4567'   
```

하나 이상의 칼럼에 의사 칼럼의 **DEFAULT** 값 지정이 가능하다.

``` sql
CREATE GLOBAL TABLE tbl0 (id int PRIMARY KEY, date1 DATE DEFAULT SYSDATE, date2 DATETIME DEFAULT SYSDATETIME);
```

### 제약 조건 정의

제약 조건으로 **NOT NULL**, **UNIQUE**, **PRIMARY KEY**를 정의할 수 있다. 또한 제약 조건은 아니지만 **INDEX** 또는 **KEY** 를 사용하여 인덱스를 생성할 수도 있다.

        <column_constraint> ::= [CONSTRAINT constraint_name] {NOT NULL | UNIQUE | PRIMARY KEY}

        <table_constraint> ::=
            [CONSTRAINT [constraint_name]] 
            { 
                UNIQUE [KEY|INDEX](column_name, ...) |
                {KEY|INDEX} [constraint_name](column_name, ...) |
                PRIMARY KEY (column_name, ...)
            }
#### NOT NULL 제약

**NOT NULL** 제약 조건이 정의된 칼럼은 반드시 **NULL** 이 아닌 값을 가져야 한다. 모든 칼럼에 대해 **NOT NULL** 제약 조건을 정의할 수 있다. **INSERT**, **UPDATE** 구문을 통해 **NOT NULL** 속성 칼럼에 **NULL** 값을 입력하거나 갱신하면 에러가 발생한다.

아래 예에서 *phone* 칼럼은 NULL 값을 가질 수 없으므로, INSERT 문에서 *id* 칼럼에 NULL을 입력하면 오류가 발생한다.

``` sql
CREATE GLOBAL TABLE const_tbl1(id INT PRIMARY KEY, phone VARCHAR NOT NULL);
INSERT INTO const_tbl1 VALUES (1, NULL);
```
```
ERROR: SQL statement violated NOT NULL constraint.
```

#### UNIQUE 제약

**UNIQUE** 제약 조건은 정의된 칼럼이 고유한 값을 갖도록 하는 제약 조건이다. 기존 레코드와 동일한 칼럼 값을 갖는 레코드가 추가되면 에러가 발생한다.

**UNIQUE** 제약 조건은 단일 칼럼뿐만 아니라 하나 이상의 다중 칼럼에 대해서도 정의가 가능하다. **UNIQUE** 제약 조건이 다중 칼럼에 대해 정의되면 각 칼럼 값에 대해 고유성이 보장되는 것이 아니라, 다중 칼럼 값의 조합에 대해 고유성이 보장된다.

SHARD 테이블의 **UNIQUE** 제약은 shard-key 칼럼을 반드시 포함해야 한다.

``` sql
CREATE GLOBAL TABLE const_tbl2(id INT PRIMARY KEY, phone VARCHAR UNIQUE);
INSERT INTO const_tbl2 VALUES (1, NULL);
INSERT INTO const_tbl2 VALUES (2, '123-4567');
INSERT INTO const_tbl2 VALUES (3, NULL);
```
``` sql
SELECT * FROM const_tbl2;
```
```
  id   phone        
====================
  1    NULL         
  2    '123-4567'   
  3    NULL         
```

아래 예에서 **UNIQUE** 제약 조건이 다중 칼럼에 대해 정의되면 칼럼 전체 값의 조합에 대해 고유성이 보장된다.

``` sql
CREATE GLOBAL  TABLE const_tbl3(id INT PRIMARY KEY, name VARCHAR, phone VARCHAR, CONSTRAINT UNIQUE (id, phone));
INSERT INTO const_tbl3 VALUES (1, NULL, NULL), (2, NULL, NULL);
INSERT INTO const_tbl3 VALUES (3, 'BBB', '000-0000'), (4, 'BBB', '111-1111');
SELECT * FROM const_tbl3;
```
```
  id   name    phone        
============================
  1    NULL    NULL         
  2    NULL    NULL         
  3    'BBB'   '000-0000'   
  4    'BBB'   '111-1111'   
```

#### PRIMARY KEY 제약

테이블에서 키(key)란 각 행을 고유하게 식별할 수 있는 하나 이상의 칼럼들의 집합을 말한다. 후보키(candidate key)는 테이블 내의 각 행을 고유하게 식별하는 칼럼들의 집합을 의미하며, 사용자는 이러한 후보 키 중 하나를 기본키(primary key)로 정의할 수 있다. 즉, 기본키로 정의된 칼럼 값은 각 행에서 고유하게 식별된다.

기본키를 정의하여 생성되는 인덱스는 기본적으로 오름차순으로 생성되며, 칼럼 뒤에 **ASC** 또는 **DESC** 키워드를 명시하여 키의 순서를 지정할 수 있다.

``` sql
CREATE GLOBAL TABLE pk_tbl (a INT, b INT, PRIMARY KEY (a, b DESC));

CREATE GLOBAL TABLE const_tbl4 (
    id INT,
    phone VARCHAR,
    CONSTRAINT pk_id PRIMARY KEY (id)
);

CREATE GLOBAL TABLE const_tbl5 (
    id INT PRIMARY KEY,
    phone VARCHAR
);

CREATE GLOBAL TABLE const_tbl6 (
    host_year    INT,
    event_code   INT,
    athlete_code INT,
    medal        CHAR (1),
    score        VARCHAR (20),
    unit         VARCHAR (5),
    PRIMARY KEY (host_year, event_code, athlete_code, medal)
);
```

#### KEY 또는 INDEX

**KEY** 와 **INDEX** 는 동일하며, 해당 칼럼을 키로 하는 인덱스를 생성한다.

``` sql
CREATE GLOBAL TABLE const_tbl7(id INT PRIMARY KEY, phone VARCHAR, KEY i_key(phone ASC, id DESC));
```

### 칼럼 옵션

특정 칼럼에 **UNIQUE** 또는 **INDEX** 를 정의할 때, 해당 칼럼 이름 뒤에 **ASC** 또는 **DESC** 옵션을 명시할 수 있다. 이 키워드는 오름차순 또는 내림차순 인덱스 값 저장을 위해 명시된다. :

``` sql
CREATE GLOBAL TABLE const_tbl8(
    id VARCHAR PRIMARY KEY,
    name VARCHAR,
    CONSTRAINT UNIQUE INDEX(name DESC)
);
```

#### 콜레이션

해당 칼럼에 적용할 콜레이션을 **CREATE TABLE** 문에 명시할 수 있다. 이에 관한 자세한 내용은 :ref: collation-charset-string 절을 참조하면 된다.

### CREATE TABLE LIKE

**CREATE TABLE ... LIKE** 문을 사용하면, 이미 존재하는 테이블의 스키마와 동일한 스키마를 갖는 테이블을 생성할 수 있다. 기존 테이블에서 정의된 칼럼 속성, 테이블 제약 조건, 인덱스도 그대로 복제된다. 원본 테이블에서 자동 생성된 인덱스의 이름은 새로 생성된 테이블의 이름에 맞게 새로 생성되지만, 사용자에 의해 지어진 인덱스 이름은 그대로 복제된다. 그러므로 인덱스 힌트 구문(:ref: index-hint-syntax 참고)으로 특정 인덱스를 사용하도록 작성된 질의문이 있다면 주의해야 한다.

원본 테이블과 새로 생성되는 테이블은 같은 타입이어야 한다. **SHARD** 테이블을 **GLOBAL** 테이블로 생성할 수 없으며, **GLOBAL** 테이블을 **SHARD** 테이블로 생성하는 경우도 허용하지 않는다.

    CREATE {GLOBAL | SHARD} TABLE <new_table_name> LIKE <source_table_name>;

-   *new_table_name*: 새로 생성할 테이블 이름이다.
-   *source_table_name*: 데이터베이스에 이미 존재하는 원본 테이블 이름이다.

``` sql
CREATE GLOBAL TABLE a_tbl (
  id INT NOT NULL DEFAULT 0 PRIMARY KEY,
  phone VARCHAR(10)
);
INSERT INTO a_tbl VALUES (1,'111-1111'), (2,'222-2222'), (3, '333-3333');

-- creating an empty table with the same schema as a_tbl
CREATE GLOBAL TABLE new_tbl LIKE a_tbl;
SELECT * FROM new_tbl;
```
```
  id   phone   
===============
```

ALTER TABLE
-----------

**ALTER** 구문을 이용하여 테이블의 구조를 변경할 수 있다. 대상 테이블에 칼럼 추가/삭제, 인덱스 생성/삭제, 테이블 이름 변경, 칼럼 이름 변경 등을 수행하거나 테이블 제약 조건을 변경한다.

    ALTER {TABLE | VIEW} <table_name> <alter_clause> ;

        <alter_clause> ::=
            ADD <alter_add>  |
            ADD {KEY | INDEX} <index_name> (<index_col_name>) |
            ALTER [COLUMN] column_name SET DEFAULT <value_specification> |
            DROP <alter_drop> |
            DROP {KEY | INDEX} index_name |
            RENAME <alter_rename> |
            CHANGE <alter_change>

            <alter_add> ::= 
                [COLUMN] [(]<table_element>, ...[)] [FIRST|AFTER old_column_name] |
                CONSTRAINT <constraint_name> <column_constraint> (column_name) |
                QUERY <select_statement>

                <table_element> ::= <column_definition> | <table_constraint>

                <column_constraint> ::= UNIQUE [KEY] | PRIMARY KEY | FOREIGN KEY

            <alter_drop> ::= 
                [COLUMN]
                {
                    column_name, ... |
                    QUERY [<unsigned_integer_literal>] |
                    CONSTRAINT constraint_name
                }

            <alter_rename> ::= 
                [COLUMN]
                {
                    old_column_name AS new_column_name |
                }

            <alter_change> ::= 
                QUERY [<unsigned_integer_literal>] <select_statement> |
                <column_name> DEFAULT <value_specification>

            <index_col_name> ::= column_name [ASC | DESC]

테이블의 소유자, **DBA** 만이 테이블 스키마를 변경할 수 있으며, 그 밖의 사용자는 소유자나 **DBA** 로부터 이름을 변경할 수 있는 권한을 받아야 한다(권한 관련 사항은 :ref: granting-authorization 참조)

### ADD COLUMN 절

**ADD COLUMN** 절을 사용하여 새로운 칼럼을 추가할 수 있다. **FIRST** 또는 **AFTER** 키워드를 사용하여 새로 추가할 칼럼의 위치를 지정할 수 있다.

    ALTER {TABLE | VIEW} table_name
    ADD [COLUMN] [(] <column_definition> [)] [FIRST | AFTER old_column_name] ;

        <column_definition> ::= 
            column_name <data_type> [[<default>] | [<column_constraint>]]

            <data_type> ::= <column_type> [<collation_modifier_clause>]

                <collation_modifier_clause> ::= COLLATE {<char_string_literal>|<identifier>}

            <default> ::= DEFAULT <value_specification>

            <column_constraint> ::= [CONSTRAINT constraint_name] {NOT NULL | UNIQUE}

-   *table_name*: 칼럼을 추가할 테이블의 이름을 지정한다.
-   &lt;*column_definition*&gt;: 새로 추가할 칼럼의 이름(최대 254 바이트), 데이터 타입, 제약 조건을 정의한다.
-   **AFTER** *old_column_name*: 새로 추가할 칼럼 앞에 위치하는 기존 칼럼 이름을 명시한다.

``` sql
CREATE GLOBAL TABLE b_tbl (id int primary key);
ALTER TABLE b_tbl ADD COLUMN age INT DEFAULT 0 NOT NULL;
ALTER TABLE b_tbl ADD COLUMN name VARCHAR FIRST;
INSERT INTO b_tbl(id) VALUES(1);

ALTER TABLE b_tbl ADD COLUMN phone VARCHAR(13) DEFAULT '000-0000-0000' AFTER name;

SELECT * FROM b_tbl;
```
```
  name   phone             id   age   
======================================
  NULL   '000-0000-0000'   1    0     
```

새로 추가되는 칼럼에 어떤 제약 조건이 오느냐에 따라 다른 결과를 보여준다.

-   새로 추가되는 칼럼에 **DEFAULT** 제약 조건이 있으면 **DEFAULT** 값이 입력된다.
-   새로 추가되는 칼럼에 **DEFAULT** 제약 조건이 없고 **NOT NULL** 제약 조건이 있는 경우 에러를 반환한다.

-   테이블에 데이터가 있고 새로 추가되는 칼럼에 UNIQUE 제약 조건을 지정하는 경우, DEFAULT 제약 조건이 있으면 고유 키 위반 에러를 반환한다.

### CHANGE/MODIFY 절

**CHANGE** 절은 칼럼의 이름, 크기 및 속성을 변경한다. 

**MODIFY** 절은 칼럼의 크기 및 속성을 변경할 수 있으며, 칼럼의 이름은 변경할 수 없다.

**CHANGE**, **MODIFY** 절을 이용해서 컬럼 타입을 변경할 수 없으며, **NOT NULL** 제약을 추가 할 수 없다.

    ALTER TABLE tbl_name <table_options> ;

        <table_options> ::=
            <table_option>[, <table_option>, ...]

            <table_option> ::=
                CHANGE [COLUMN | CLASS ATTRIBUTE] old_col_name new_col_name column_definition
                         [FIRST | AFTER col_name]
              | MODIFY [COLUMN | CLASS ATTRIBUTE] col_name column_definition
                         [FIRST | AFTER col_name]

-   *tbl_name*: 변경할 칼럼이 속한 테이블의 이름을 지정한다.
-   *old_col_name*: 기존 칼럼의 이름을 지정한다.
-   *new_col_name*: 변경할 칼럼의 이름을 지정한다.
-   *column_definition*: 변경할 칼럼의 타입, 크기 및 속성을 지정한다.
-   *col_name*: 변경할 칼럼이 어느 칼럼 뒤에 위치할지를 지정한다.

``` sql
CREATE GLOBAL TABLE t1 (id INT PRIMARY KEY, a INTEGER, b VARCHAR(10));

-- changing column name
ALTER TABLE t1 CHANGE a a1 INTEGER;
```
``` sql
-- changing column size
ALTER TABLE t1 MODIFY b VARCHAR(20);
```
``` sql
-- changing column size and position
ALTER TABLE t1 CHANGE b b VARCHAR(30) FIRST;  
```
### RENAME COLUMN 절

**RENAME COLUMN** 절을 사용하여 칼럼의 이름을 변경할 수 있다. :

    ALTER [TABLE | VIEW] table_name
    RENAME [COLUMN] old_column_name { AS | TO } new_column_name

-   *table_name*: 이름을 변경할 칼럼의 테이블 이름을 지정한다.
-   *old_column_name*: 현재의 칼럼 이름을 지정한다.
-   *new_column_name*: 새로운 칼럼 이름을 **AS** 키워드 뒤에 명시한다(최대 254 바이트).

``` sql
CREATE GLOBAL TABLE t2 (id INT PRIMARY KEY, name VARCHAR(50), age INT);
ALTER TABLE t2 RENAME COLUMN name AS name1;
```

### DROP COLUMN 절

**DROP COLUMN** 절을 사용하여 테이블에 존재하는 칼럼을 삭제할 수 있다. 삭제하고자 하는 칼럼들을 쉼표(,)로 구분하여 여러 개의 칼럼을 한 번에 삭제할 수 있다. :

    ALTER [TABLE | CLASS | VCLASS | VIEW] table_name
    DROP [COLUMN | ATTRIBUTE] column_name, ... ;

-   *table_name*: 삭제할 칼럼의 테이블 이름을 명시한다.
-   *column_ name*: 삭제할 칼럼의 이름을 명시한다. 쉼표로 구분하여 여러 개의 칼럼을 지정할 수 있다.

``` sql
ALTER TABLE t2 DROP COLUMN name1, age;
```

### DROP INDEX 절

**DROP INDEX** 절을 사용하여 인덱스를 삭제할 수 있다. 고유 인덱스는 **DROP CONSTRAINT** 절로도 삭제할 수 있다.

    ALTER [TABLE | CLASS] table_name DROP INDEX index_name ;

-   *table_name*: 제약 조건을 삭제할 테이블의 이름을 지정한다.
-   *index_name*: 삭제할 인덱스의 이름을 지정한다.

``` SQL
ALTER TABLE a_tbl DROP INDEX i_a_tbl_age;
```

DROP TABLE
----------

**DROP** 구문을 이용하여 기존의 테이블을 삭제할 수 있다. 하나의 **DROP** 구문으로 여러 개의 테이블을 삭제할 수 있으며 테이블이 삭제되면 포함된 행도 모두 삭제된다. **IF EXISTS** 절을 함께 사용하면 해당 테이블이 존재하지 않더라도 에러가 발생하지 않는다.

    DROP [TABLE] [IF EXISTS] table_name[, ...];

-   *table_name*: 삭제할 테이블의 이름을 지정한다. 쉼표로 구분하여 여러 개의 테이블을 한 번에 삭제할 수 있다.

``` sql
DROP TABLE t1;
DROP TABLE IF EXISTS t1, t2;
```

RENAME TABLE
------------

**RENAME TABLE** 구문을 사용하여 테이블 이름을 변경할 수 있다.

    RENAME  [TABLE] old_table_name {AS | TO} new_table_name;

-   *old_table_name*: 변경할 테이블의 이름을 지정한다.
-   *new_table_name*: 새로운 테이블 이름을 지정한다(최대 254 바이트).

``` sql
RENAME TABLE a_tbl AS aa_tbl;
```

테이블의 소유자, **DBA**만이 테이블의 이름을 변경할 수 있으며, 그 밖의 사용자는 소유자나 **DBA** 로부터 이름을 변경할 수 있는 권한을 받아야 한다(권한 관련 사항은 :ref:granting-authorization 참조).


