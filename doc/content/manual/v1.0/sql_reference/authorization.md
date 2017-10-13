데이터베이스 사용자 권한
========================

데이터베이스 사용자
-------------------

사용자 이름 작성 원칙은 [식별자](../identifier.md) 절을 참고한다.

Rye는 기본적으로 **DBA**와 **PUBLIC** 두 종류의 사용자를 제공한다. 처음 제품을 설치했을 때에는 비밀번호가 설정되어 있지 않다.

-   **DBA** 는 데이터베이스 관리자를 위한 권한을 소유한다. **DBA**는 모든 테이블에 대한 접근 권한을 갖는다. 따라서 **DBA**에 명시적으로 권한을 부여할 필요는 없다. 데이터베이스 사용자는 고유한 이름을 갖는다. 
-   **PUBLIC** 은 일반 계정이다. DBA 가 생성할 수 있는 일반적인 계정과 동일하며, DB 생성시 기본적으로 생성되는 계정이다. 

사용자 관리
-----------

**DBA**는 SQL 문을 사용하여 사용자를 생성, 변경, 삭제할 수 있다.

    CREATE USER user_name
    [ PASSWORD password ] ;

    DROP USER user_name;

    ALTER USER user_name PASSWORD password;

-   *user\_name*: 생성, 삭제, 변경할 사용자 이름을 지정한다.
-   *password*: 생성 혹은 변경할 사용자의 비밀번호를 지정한다.

다음은 사용자 *Fred*를 생성하고 비밀번호를 변경한 후에 *Fred*를 삭제하는 예제이다.

``` sql
CREATE USER Fred;
ALTER USER Fred PASSWORD '1234';
DROP USER Fred;
```

권한 부여
---------

Rye에서 권한 부여의 최소 단위는 테이블이다. 자신이 만든 테이블에 다른 사용자(그룹)의 접근을 허용하려면 해당 사용자(그룹)에게 적절한 권한을 부여해야 한다. **GRANT** 문을 사용하여 사용자에게 접근 권한을 부여할 수 있다.

    GRANT operation [ { ,operation } ... ] ON table_name [ { ,table_name } ... ]
    TO user [ { ,user } ... ] ; 

-   *operation*: 권한을 부여할 때 사용 가능한 연산을 나타낸다.
    -   **SELECT**: 테이블 정의 내용을 읽을 수 있고 레코드 조회가 가능. 가장 일반적인 유형의 권한.
    -   **INSERT**: 테이블에 레코드를 삽입할 수 있는 권한.
    -   **UPDATE**: 테이블에 이미 존재하는 레코드를 수정할 수 있는 권한.
    -   **DELETE**: 테이블의 레코드를 삭제할 수 있는 권한.
    -   **ALTER**: 테이블의 정의를 수정할 수 있고, 테이블의 이름을 변경하거나 삭제할 수 있는 권한.
    -   **ALL PRIVILEGES**: 앞서 설명한 5가지 권한을 모두 포함.
-   *table\_name*: 권한을 부여할 테이블 혹은 뷰의 이름을 지정한다.
-   *user*: 권한을 부여할 사용자의 이름을 지정한다. 데이터베이스 사용자의 로그인 이름을 입력한다.

다음은 *smith*에게 *olympic* 테이블의 검색 권한을 부여한 예제이다.

``` sql
GRANT SELECT ON olympic TO smith;
```

다음은 *brown* 와 *jones* 에게 *nation*과 *athlete* 테이블에 대해 **SELECT**, **INSERT**, **UPDATE**, **DELETE** 권한을 부여한 예제이다.

``` sql
GRANT SELECT, INSERT, UPDATE, DELETE ON nation, athlete TO brown, jones;
```

다음은 *smith* 와 *nation*과 *athlete* 테이블에 대해 모든 권한을 부여한 예제이다.

``` sql
GRANT ALL PRIVILEGES ON nation, athlete TO smith;
```

* 권한을 부여하는 사용자는 권한 부여 전에 나열된 모든 테이블과 뷰의 소유자여야 한다.
* **DBA** 사용자는 자동적으로 모든 테이블에 대한 모든 권한을 가진다.
* **TRUNCATE** 문을 수행하려면 **ALTER**, **INDEX**, **DELETE** 권한이 필요하다.

권한 해지
---------

**REVOKE** 문을 사용하여 권한을 해지할 수 있다. 사용자에게 부여된 권한은 언제든지 해지가 가능하다. 한 사용자에게 두 종류 이상의 권한을 부여했다면 권한 중 일부 또는 전부를 해지할 수 있다. 또한 하나의 **GRANT** 문으로 여러 사용자에게 여러 테이블에 대한 권한을 부여한 경우라도 일부 사용자와 일부 테이블에 대해 선택적인 권한 해지가 가능하다.

    REVOKE operation [ { , operation } ... ] ON table_name [ { , class_name } ... ]
    FROM user [ { , user } ... ] ;

-   *operation*: 권한을 부여할 때 부여할 수 있는 연산의 종류이다(자세한 내용은 [권한 부여](#권한 부여) 참조).
-   *table\_name*: 권한을 부여할 테이블 혹은 뷰의 이름을 지정한다.
-   *user*: 권한을 부여할 사용자나 그룹의 이름을 지정한다.

다음은 *smith*, *jones* 사용자에게 *nation*, *athlete* 두 테이블에 대해 **SELECT**, **INSERT**, **UPDATE**, **DELETE** 권한을 부여하는 예제이다.

``` sql
GRANT SELECT, INSERT, UPDATE, DELETE ON nation, athlete TO smith, jones;
```

다음은 *jones*에게 조회 권한만을 부여하기 위해 **REVOKE** 문장을 수행하는 예제이다.

``` sql
REVOKE INSERT, UPDATE, DELETE ON nation, athlete FROM jones;
```

다음은 *smith*에게 부여한 모든 권한을 해지하기 위해 **REVOKE** 문을 수행하는 예제이다. 이 문장이 수행되면 *smith*는 *nation*, *athlete* 테이블에 대한 어떠한 연산도 허용되지 않는다.

``` sql
REVOKE ALL PRIVILEGES ON nation, athlete FROM smith;
```

소유자 변경
-----------

데이터베이스 관리자(**DBA**)는 다음의 질의를 통해 테이블, 뷰의 소유자를 변경할 수 있다. 

    ALTER [TABLE | VIEW ] name OWNER TO user_id;

-   *name*: 소유자를 변경할 스키마 객체의 이름
-   *user\_id*: 사용자 ID

``` sql
ALTER TABLE test_tbl OWNER TO public;
ALTER VIEW test_view OWNER TO public;
```
