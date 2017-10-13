# windows 환경 설정

[깃 설치(Install Git for windows)](http://spinse.tistory.com/36)

[하루패드 설치](http://recoveryman.tistory.com/323)
에디터 테마는 monokai, 글자크기 14를 추천한다.

- - -

# 편집 규약

제목1
================
제목2
----------------

**강조**

*이텔릭*

| 테이블 | 설명 | 비고 |
|--------|--------|--------|
| **UNION**   | 합집합  | **UNION ALL** 이면        |
| **DIFFERENCE**   | 차집합  | **EXCEPT** 연산자와 동일        |

```
[외부링크](http_url)

[내부링크](filename.md#id)
```
id 는 소문자로 표기해야 함

FIND\_IN\_SET
-------------

**FIND_IN_SET (*str*,*strlist*)**

FIND_IN_SET 함수는 여러 개의 문자열을 쉼표(,)로 연결하여 구성한 문자열 리스트 strlist 에서 특정 문자열 str 이 존재하면 str 의 위치를 반환한다. strlist 에 str 이 존재하지 않거나 strlist 가 빈 문자열이면 0을 반환한다. 둘 중 하나의 인자가 NULL 이면 NULL 을 반환한다. str 이 쉼표를 포함하면 제대로 동작하지 않는다.

Parameters:
- str: 검색 대상 문자열
- strlist: 쉼표로 구분한 문자열의 집합

Return type: INT

``` sql
SELECT FIND_IN_SET('b','a,b,c,d');
```
```
===================================
2
```

- - -

멀티 마크다운 사용 방법
====================

~~취소선~~

주석달기: 지원 안되므로 사용하지 말 것
마크다운 문법이다.[^1]
변환이 가능하다.[^2]

[^1]: 마구 활용하고 싶어지는 규칙입니다.
[^2]: [위키백과 마크다운 문서](http://ko.wikipedia.org/wiki/마크다운)

표 그리기

좋은 음식 | 나쁜 음식 | 몰라
--- | --- | ---
과일 | 사탕 | 과일사탕
당근 | 감자튀김 | 당근감자튀김

사전
================

AS_IS | TO_BE | Desc
--- | --- | ---
클래스 | 테이블|
가상 클래스 | 뷰 |
속성 | 칼럼 |
인스턴스 | 레코드 |
경로 표현식 | | 제거할 것
BIT | VARBINARY |


