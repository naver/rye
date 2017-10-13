HA 카탈로그
===============

HA 카탈로그 테이블
----------------------

### db_log_writer

**rye_repl** 유틸리티가 복제 로그를 로칼 디스크로 복사를 진행할때 그 상태를 저장하기 위한 테이블이다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
host_ip | VARCHAR(20) | 복제 로그에 대한 원본 DB의 host ip
last_received_pageid | BIGINT | 마지막으로 받은 복제 로그의 페이지 번호
last_received_time | DATETIME | 복제 로그를 마지막으로 받은 시간
eof_lsa | BIGINT | 복제 로그에 대한 원본 DB의 마지막 로그 레코드의 lsa


### db_log_analyzer

**rye_repl** 유틸리티가 복제 로그를 분석 할때 그 진행 상태를 저장하기 위한 테이블이다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
host_ip | VARCHAR(20) | 복제 로그에 대한 원본 DB의 host ip
current_lsa | BIGINT | 현재 분석 중인 복제 로그 레코드의 lsa
required_lsa | BIGINT | 복제 로그를 반영할때 필요한 가장 작은 lsa. 이 값보다 작은 로그 레코드들이 저장되어 있는 복제 보관 로그만 삭제 가능함.
start_time | DATETIME | 분석을 시작한 시간.(슬레이브 DB에 접속한 시간)
last_access_time | DATETIME | 트랜젝션을 분석한 마지막 시간
creation_time | DATETIME | 반영하는 로그에 대한 원본 DB의 생성 시간
queue_full | BIGINT | 복제 반영 큐가 가득찬 횟수(누적 값)

### db_log_applier

**rye_repl** 유틸리티가 복제 로그를 반영 할때 그 진행 상태를 저장하기 위한 테이블이다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
host_ip | VARCHAR(20) | 복제 로그에 대한 원본 DB의 host ip
id | INTEGER | 복제 로그를 반영하는 applier의 id
committed_lsa | BIGINT | 마지막으로 반영한 트랜젝션의 커밋 로그 레코드의 lsa
master_last_commit_time | DATETIME | 마지막으로 반영한 트랜잭션의 마스터에서 커밋 된 시간.
repl_delay | BIGINT | 마지막으로 반영된 트랜잭션의 복제 지연 시간
insert_count | BIGINT | applier가 insert한 횟수 (누적)
update_count | BIGINT | applier가 update한 횟수 (누적)
delete_count | BIGINT | applier가 delete한 횟수 (누적)
schema_count | BIGINT | applier가 schema를 변경한 횟수 (누적)
commit_count | BIGINT | applier가 commit한 횟수 (누적)
fail_count | BIGINT | applier가 반영을 실패한 횟수 (누적), ha_ignore_error_list에 등록된 에러들만 실패시에도 반영을 계속 진행 한다.
