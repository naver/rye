SHARD 카탈로그
===============

SHARD 카탈로그 테이블
----------------------

### shard_db

샤딩에 대한 요약 정보

컬럼명 | 데이터 타입 | 설명
--- | --- | ---
id               | INTEGER | 
dbname           | STRING | GLOBAL DB이름
groupid_count    | INTEGER | shard-key에 의해 해싱되는 GROUPID 개수
groupid_last_ver | BIGINT | GROUPID 의 마지막 버전번호
node_last_ver    | BIGINT | NODEID의 마지막 버전번호
mig_req_count    | INTEGER | 수행중인 마이그레이션 프로세스 개수. 마이그레이션이 수행중일때 DDL과 GC는 수행되지 않음
ddl_req_count    | INTEGER | 수행중인 DDL 요청 개수. DDL이 수행중일때 마이그레이션은 수행되지 않음
gc_req_count     | INTEGER | 수행중인 GC 프로세스 개수. GC가 수행중일때 마이그레이션은 수행되지 않음
node_status      | INTEGER | 노드 상태 <br> 0: 일반적인 상태 <br> 1: 노드 추가 진행중
created_at       | BIGINT | 샤드가 초기화된 시간

### shard_node

샤드 노드로 운영되는 인스턴스에 대한 정보

컬럼명 | 데이터 타입 | 설명
--- | --- | ---
nodeid  | INTEGER  | NODEID
dbname  | STRING | 로컬 DB이름
host    | STRING | 호스트 IP주소
port    | INTEGER  | 브로커 포트
status  | INTEGER  | 노드 상태  <br> 0: 노드 추가가 완료된 상태 <br> 1: 추가되는 노드에 스키마 복사가 완료된 상태  <br> 2: 노드 추가요청이 시작된 상태
version | BIGINT  | NODEID의 버전 번호

### shard_groupid

GROUPID, NODEID 매핑 테이블

컬럼명 | 데이터 타입 | 설명
--- | --- | ---
groupid  | INTEGER | GROUPID
nodeid   | INTEGER | GROUPID 대한 데이터가 저장되는 NODEID
version  | BIGINT | GROUPID version. 마이그레이션에 의해 관리되는 NODEID가 달라지는 경우 version이 바뀐다.


### shard_migration

마이그레이션 진행상황에 대한 정보 테이블

컬럼명 | 데이터 타입 | 설명
--- | --- | ---
groupid       | INTEGER | 마이그레이션 대상 GROUPID
mgmt_host     | STRING | rye_migrator 프로세스를 실행하도록 요청한 shard management server 의 호스트
mgmt_pid      | INTEGER | rye_migrator 프로세스를 실행하도록 요청한 shard management server의 process id
src_nodeid    | INTEGER | 마이그레이션 대상 소스 NODEID
dest_nodeid   | INTEGER | 마이그레이션 대상 타겟 NODEID
status        | INTEGER | 마이그레이션 상태. <br> 1: 마이그레이션 대기상태 <br> 2: 마이그레이션 시작됨.  <br> 3:  rye_migrator 프로세스가 구동된 상태. <br> 4:  GROUPID 마이그레이션 성공 <br> 5: GROUPID 마이그레이션 실패
mig_order     | INTEGER | 마이그레이션 대기 순서
modified_at   | BIGINT | 마이그레이션 상태 변경 시간
run_time      | INTEGER | 마이그레이션이 완료된 경우 수행 시간
shard_keys    | INTEGER | 마이그레이션이 완료된 경우 이동한 shard-key 개수
