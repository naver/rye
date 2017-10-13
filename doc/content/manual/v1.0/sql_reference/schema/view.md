뷰
===

CREATE VIEW
-----------

뷰(가상 테이블)는 물리적으로 존재하지 않는 가상의 테이블이며, 기존의 테이블이나 뷰에 대한 질의문을 이용하여 뷰를 생성할 수 있다.

**CREATE VIEW** 문을 이용하여 뷰를 생성한다. 뷰 이름 작성 원칙은 [식별자](../identifier.md#식별자) 를 참고한다.

    CREATE [OR REPLACE] VIEW view_name
    (view_column_name_type, ...)
    AS <select_statement>;

-   **OR REPLACE**: **CREATE** 뒤에 **OR REPLACE** 키워드가 명시되면, *view\_name* 이 기존의 뷰와 이름이 중복되더라도 에러를 출력하지 않고 기존의 뷰를 새로운 뷰로 대체한다.
-   *view\_name*: 생성하려는 뷰의 이름을 지정한다. 뷰의 이름은 데이터베이스 내에서 고유해야 한다.
-   *view\_column\_name\_type*: 생성하려는 뷰의 칼럼 이름과 타입을 지정한다.
-   **AS** <*select\_statement*>: 유효한 **SELECT** 문이 명시되어야 한다. 이를 기반으로 뷰가 생성된다.

``` sql
CREATE GLOBAL TABLE a_tbl(
id INT PRIMARY KEY,
phone VARCHAR(10));
INSERT INTO a_tbl VALUES(1,'111-1111'), (2,'222-2222'), (3, '333-3333'), (4, NULL), (5, NULL);


--creating a new view based on AS select_statement from a_tbl
CREATE VIEW b_view (id INT, phone VARCHAR(10)) AS SELECT * FROM a_tbl WHERE phone IS NOT NULL;
SELECT * FROM b_view;
```
```
    id  phone
 ===================================  
 1 '111-1111'
 2 '222-2222'
 3 '333-3333'
```
``` sql
--creating view which name is as same as existing view name
CREATE OR REPLACE VIEW b_view (id INT, phone VARCHAR(10)) AS SELECT * FROM a_tbl ORDER BY id DESC;

--the existing view has been replaced as a new view by OR REPLACE keyword
SELECT * FROM b_view;
```
```
    id  phone
 ===================================  
 5 NULL
 4 NULL
 3 '333-3333'
 2 '222-2222'
 1 '111-1111'

```

DROP VIEW
---------

뷰는 **DROP VIEW** 문을 이용하여 삭제할 수 있다. 뷰를 삭제하는 방법은 일반 테이블을 삭제하는 방법과 동일하다. IF EXISTS 절을 함께 사용하면 해당 뷰가 존재하지 않더라도 에러가 발생하지 않는다. :

    DROP [VIEW] [IF EXISTS] view_name [{ ,view_name , ... }] ;

-   *view\_name* : 삭제하려는 뷰의 이름을 지정한다.

``` sql
DROP VIEW b_view;
```

RENAME VIEW
-----------

뷰의 이름은 **RENAME VIEW** 문을 사용하여 변경할 수 있다. :

    RENAME [VIEW] old_view_name {AS | TO} new_view_name[, old_view_name {AS | TO} new_view_name, ...] ;

-   *old\_view\_name* : 변경할 뷰의 이름을 지정한다.
-   *new\_view\_name* : 뷰의 새로운 이름을 지정한다.

다음은 *game\_2004* 뷰의 이름을 *info\_2004* 로 변경하는 예제이다.

``` sql
RENAME VIEW game_2004 AS info_2004;
```
