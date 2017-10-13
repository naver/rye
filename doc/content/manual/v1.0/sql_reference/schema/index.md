인덱스
======

CREATE INDEX
------------

**CREATE INDEX** 구문을 이용하여 지정한 테이블에 인덱스를 생성한다. 인덱스 이름 작성 원칙은 [식별자](../identifier.md) 를 참고한다.

인덱스 힌트 구문, 내림차순 인덱스, 커버링 인덱스, 인덱스 스킵 스캔, **ORDER BY** 최적화, **GROUP BY** 최적화 등 인덱스를 이용하는 방법과 필터링된 인덱스, 함수 인덱스를 생성하는 방법에 대해서는 [질의최적화](../tuning.md) 을 참고한다.

    CREATE [UNIQUE] INDEX index_name ON table_name <index_col_desc> ;

        <index_col_desc> ::=
            ( column_name [ASC | DESC] [ {, column_name [ASC | DESC]} ...] )

-   **UNIQUE**: 유일한 값을 갖는 고유 인덱스를 생성한다.
-   *index\_name*: 생성하려는 인덱스의 이름을 명시한다. 인덱스 이름은 테이블 안에서 고유한 값이어야 한다.
-   *table\_name*: 인덱스를 생성할 테이블의 이름을 명시한다.
-   *column\_name*: 인덱스를 적용할 칼럼의 이름을 명시한다. 다중 칼럼 인덱스를 생성할 경우 둘 이상의 칼럼 이름을 명시한다.
-   **ASC** | **DESC**: 칼럼의 정렬 방향을 설정한다.

다음은 내림차순으로 정렬된 인덱스를 생성하는 예제이다.

``` sql
CREATE INDEX gold_index ON participant(gold DESC);
```

다음은 다중 칼럼 인덱스를 생성하는 예제이다.

``` sql
CREATE INDEX name_nation_idx ON athlete(name, nation_code);
```

ALTER INDEX
-----------

**ALTER INDEX** 문을 사용하여 인덱스를 재생성한다. 즉, 기존 인덱스를 삭제하고 다시 생성한다.만약, **ON** 절 뒤에 테이블 이름과 칼럼 이름을 명시하면 해당 정보를 사용하여 인덱스를 재생성한다.

    ALTER [UNIQUE] INDEX index_name
    [ON table_name [<index_col_desc>]] REBUILD

    <index_col_desc> ::=
        ( column_name [ASC | DESC] [ {, column_name [ASC | DESC]} ...] )

-   **UNIQUE**: 재생성하려는 인덱스가 고유 인덱스임을 지정한다.
-   *index\_name*: 재생성하려는 인덱스의 이름을 명시한다. 인덱스 이름은 테이블 안에서 고유한 값이어야 한다.
-   *table\_name*: 인덱스를 재생성할 테이블의 이름을 명시한다.
-   *column\_name*: 인덱스를 적용할 칼럼의 이름을 명시한다. 다중 칼럼 인덱스를 생성할 경우 둘 이상의 칼럼 이름을 명시한다.
-   **ASC** | **DESC**: 칼럼의 정렬 방향을 설정한다.

다음은 인덱스를 재생성하는 여러 가지 방법을 보여주는 예제이다.

``` sql
CREATE INDEX i_game_medal ON game(medal);
ALTER INDEX i_game_medal ON game REBUILD;
```

DROP INDEX
----------

**DROP INDEX** 문을 사용하여 인덱스를 삭제할 수 있다.

    DROP [UNIQUE] INDEX index_name [ON table_name] ;

-   **UNIQUE**: 삭제하려는 인덱스가 고유 인덱스임을 지정한다. 고유 인덱스는 **DROP CONSTRAINT** 절로도 삭제할 수 있다.
-   *index\_name*: 삭제할 인덱스의 이름을 지정한다.
-   *table\_name*: 삭제할 인덱스가 지정된 테이블 이름을 지정한다.

다음은 인덱스를 삭제하는 예제이다.

``` sql
DROP INDEX i_game_medal ON game;
```
