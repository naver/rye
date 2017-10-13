시스템 카탈로그
===============

시스템 카탈로그 테이블
----------------------

카탈로그 테이블들은 데이터베이스 안의 모든 테이블, 칼럼에 대한 정보를 표현한다.

### db\_table

테이블에 대한 정보를 표현하며 table\_name에 대한 인덱스가 생성되어 있다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
table_of | object | 테이블 객체로서 시스템 내부에 저장된 테이블에 대한 메타 정보 객체를 의미한다.
table_name | VARCHAR(255) | 테이블명
type_type | INTEGER | 테이블이면 0, 뷰이면 1
is_system_table | INTEGER | 사용자가 정의한 테이블이면 0, 시스템 테이블이면 1
owner | db_user | 테이블 소유자
owner_name | VARCHAR(255) | 테이블 소유자명
num_col | INTEGER | 칼럼의 개수
collation_id | INTEGER | 콜레이션 ID
cols | SEQUENCE OF db_column | 칼럼 속성
query_specs | SEQUENCE OF db_query_spec | 뷰인 경우 그 SQL 정의문
indexes | SEQUENCE OF db_index | 테이블에 생성된 인덱스

### db\_column

칼럼에 대한 정보를 표현하며 table\_name, col\_name에 대한 인덱스가 생성되어 있다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
table_of | db_table | 칼럼이 속한 테이블
table_name | VARCHAR(255) | 테이블명
col_name | VARCHAR(255) | 칼럼명
def_order | INTEGER | 칼럼이 테이블에 정의된 순서로 0부터 시작한다.
data_type | INTEGER| 칼럼의 데이터 타입. 아래의 'Rye가 지원하는 데이터 타입' 표에서 명시하는 value 중 하나이다.
default_value | VARCHAR (255) | 기본값. 데이터 타입에 관계없이 모두 문자열로 저장된다. 기본값이 없으면 **NULL** , 기본값이 **NULL** 이면 'NULL'로 표현된다.
domains | SEQUENCE OF db_domain | 데이터 타입에 대한 도메인 정보.
is_nullable | INTEGER | not null 제약이 설정되어 있으면 0, 그렇지 않으면 1이 설정된다.
is_shard_key | INTEGER | shard 칼럼이면 1, 그렇지 않으면 0이 설정된다.

**Rye가 지원하는 데이터 타입**

값 | 의미
--- | ---
1 | INTEGER
3 | DOUBLE
4 | VARCHAR
5 | OBJECT
8 | SEQUENCE
10 | TIME
12 | DATE
22 | NUMERIC
24 | VARBINARY
31 | BIGINT
32 | DATETIME

다음 예제에서는 사용자 테이블(is\_system\_table = 0)인 것을 검색한다.

``` sql
SELECT c.table_name, c.col_name
FROM db_table t INNER JOIN db_column c ON t.table_name = c.table_name
WHERE t.is_system_table = 0
ORDER BY 1, c.def_order;
```
```
  table_name          col_name             
===========================================
  'athlete'           'code'               
  'athlete'           'name'               
  'athlete'           'gender'             
  'athlete'           'nation_code'        
  'athlete'           'event'              
  'code'              's_name'             
  'code'              'f_name'             
  'event'             'code'               
  'event'             'sports'             
  'event'             'name'               
  'event'             'gender'             
  'event'             'players'            
  'game'              'host_year'          
  'game'              'event_code'         
  'game'              'athlete_code'       
  'game'              'stadium_code'       
  'game'              'nation_code'        
  'game'              'medal'              
  'game'              'game_date'          
  'history'           'event_code'         
  'history'           'athlete'            
  'history'           'host_year'          
  'history'           'score'              
  'history'           'unit'               
  'nation'            'code'               
  'nation'            'name'               
  'nation'            'continent'          
  'nation'            'capital'            
  'olympic'           'host_year'          
  'olympic'           'host_nation'        
  'olympic'           'host_city'          
  'olympic'           'opening_date'       
  'olympic'           'closing_date'       
  'olympic'           'mascot'             
  'olympic'           'slogan'             
  'olympic'           'introduction'       
  'participant'       'host_year'          
  'participant'       'nation_code'        
  'participant'       'gold'               
  'participant'       'silver'             
  'participant'       'bronze'             
  'record'            'host_year'          
  'record'            'event_code'         
  'record'            'athlete_code'       
  'record'            'medal'              
  'record'            'score'              
  'record'            'unit'               
  'shard_db'          'id'                 
  'shard_db'          'dbname'             
  'shard_db'          'groupid_count'      
  'shard_db'          'groupid_last_ver'   
  'shard_db'          'node_last_ver'      
  'shard_db'          'mig_req_count'      
  'shard_db'          'ddl_req_count'      
  'shard_db'          'gc_req_count'       
  'shard_db'          'node_status'        
  'shard_db'          'created_at'         
  'shard_groupid'     'groupid'            
  'shard_groupid'     'nodeid'             
  'shard_groupid'     'version'            
  'shard_migration'   'groupid'            
  'shard_migration'   'mgmt_host'          
  'shard_migration'   'mgmt_pid'           
  'shard_migration'   'src_nodeid'         
  'shard_migration'   'dest_nodeid'        
  'shard_migration'   'status'             
  'shard_migration'   'mig_order'          
  'shard_migration'   'modified_at'        
  'shard_migration'   'run_time'           
  'shard_migration'   'shard_keys'         
  'shard_node'        'nodeid'             
  'shard_node'        'dbname'             
  'shard_node'        'host'               
  'shard_node'        'port'               
  'shard_node'        'status'             
  'shard_node'        'version'            
  'stadium'           'code'               
  'stadium'           'nation_code'        
  'stadium'           'name'               
  'stadium'           'area'               
  'stadium'           'seats'              
  'stadium'           'address'            
```

### db\_domain

도메인에 대한 정보이며 table\_name, col\_name에 대한 인덱스가 생성되어 있다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
object_of | object | 도메인을 참조하는 칼럼
table_name | VARCHAR(255) | 테이블명
col_name | VARCHAR(255) | 칼럼명
data_type | INTEGER | 도메인의 데이터 타입(db_column의 'Rye가 지원하는 데이터 타입' 표의 '값' 중 하나)
prec | INTEGER | 데이터 타입에 대한 전체 자릿수(precision). 전체 자릿수가 명시되지 않은 경우 0이 설정됨
scale | INTEGER | 데이터 타입에 대한 소수점 이하의 자릿수(scale). 소수점 이하의 자릿수가 명시되지 않은 경우 0이 설정됨
table_of | db_table | 데이터 타입이 객체 타입인 경우 그 도메인 테이블. 객체 타입이 아닌 경우 **NULL** 이 설정됨.
domain_table_name | VARCHAR(255) | 도메인 테이블명
code_set | INTEGER | 문자열 타입인 경우, 문자셋(db_column의 'Rye가 지원하는 문자셋' 표의 '값' 중 하나). 문자 스트링 타입이 아닌 경우 0.
collation_id | INTEGER | 콜레이션 ID
set_domains | SEQUENCE OF db_domain | 컬렉션 타입인 경우, 그 집합을 구성하는 원소의 데이터 타입에 대한 도메인 정보. 컬렉션 타입이 아닌 경우 **NULL** 이 설정됨

### db\_query\_spec

뷰의 SQL 정의문이며 table\_name에 대한 인덱스가 생성되어 있다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
table_of | db_table | 테이블 객체로서 시스템 내부에 저장된 뷰에 대한 메타 정보 객체를 의미한다.
table_name | VARCHAR(255) | 테이블명
spec | VARCHAR(4096) | 뷰에 대한 SQL 정의문

### db\_index

인덱스에 대한 정보이며 table\_name, index\_name에 대한 인덱스가 생성되어 있다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
table_of | db_table | 인덱스가 속한 테이블
table_name | VARCHAR(255) | 테이블명
index_name | VARCHAR(255) | 인덱스명
is_unique | INTEGER | 고유 인덱스(unique index)이면 1, 그렇지 않으면 0
key_count | INTEGER| 키를 구성하는 칼럼의 개수
key_cols | SEQUENCE OF db_index_key | 키를 구성하는 칼럼들
is_primary_key | INTEGER | 기본 키이면 1, 그렇지 않으면 0
status | INTEGER | 정상이면 2

다음 예제에서는 각 테이블에 속하는 인덱스명을 검색한다.

``` sql
SELECT i.table_name, i.index_name
FROM db_index i
ORDER BY 1;
```
```
  table_name                    index_name                                            
======================================================================================
  'athlete'                     'pk_athlete_code'                                     
  'db_auth'                     'i_db_auth_grantee_name_table_name'                   
  'db_column'                   'i_db_column_table_name_col_name'                     
  'db_domain'                   'i_db_domain_table_name_col_name'                     
  'db_index'                    'i_db_index_table_name_index_name'                    
  'db_index_key'                'i_db_index_key_table_name_index_name'                
  'db_index_stats'              'pk_db_index_stats_table_name_index_name'             
  'db_index_stats'              'i_db_index_stats_index_name'                         
  'db_log_analyzer'             'pk_db_log_analyzer_host_ip'                          
  'db_log_applier'              'pk_db_log_applier_host_ip_id'                        
  'db_log_writer'               'pk_db_log_writer_host_ip'                            
  'db_query_spec'               'i_db_query_spec_table_name'                          
  'db_shard_gid_removed_info'   'pk_db_shard_gid_removed_info_gid'                    
  'db_shard_gid_skey_info'      'pk_db_shard_gid_skey_info_gid_skey'                  
  'db_table'                    'i_db_table_table_name'                               
  'db_user'                     'i_db_user_name'                                      
  'event'                       'pk_event_code'                                       
  'game'                        'pk_game_host_year_event_code_athlete_code'           
  'history'                     'pk_history_event_code_athlete'                       
  'nation'                      'pk_nation_code'                                      
  'olympic'                     'pk_olympic_host_year'                                
  'participant'                 'pk_participant_host_year_nation_code'                
  'record'                      'pk_record_host_year_event_code_athlete_code_medal'   
  'shard_db'                    'pk_shard_db_id'                                      
  'shard_groupid'               'pk_shard_groupid_groupid'                            
  'shard_migration'             'pk_shard_migration_groupid'                          
  'shard_migration'             'shard_migration_idx1'                                
  'shard_node'                  'pk_shard_node_nodeid_dbname_host'                    
  'stadium'                     'pk_stadium_code'                                     
```

### db\_index\_key

인덱스에 대한 키 정보이며 table\_name, index\_name에 대한 인덱스가 생성되어 있다.
칼럼명 | 데이터 타입 | 설명
--- | --- | ---
index_of | db_index | 키 칼럼이 속한 인덱스
table_name | VARCHAR(255) | 테이블명
index_name | VARCHAR(255) | 인덱스명
key_col_name | VARCHAR(255) | 키를 구성하는 칼럼명
key_order | INTEGER | 키에서 칼럼이 위치한 순서로 0부터 시작함
asc_desc | INTEGER | 칼럼 값의 순서가 내림차순이면 1, 그렇지 않으면 0

### db\_auth

테이블에 대한 사용자 권한 정보를 나타내며, grantee\_name, table\_name에 인덱스가 생성되어 있다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
grantee_name | VARCHAR(255) | 권한 피부여자명
table_name | VARCHAR(255) | 테이블명
select_priv | INTEGER | SELECT 권한이 있으면 1, 그렇지 않으면 0
insert_priv | INTEGER | INSERT 권한이 있으면 1, 그렇지 않으면 0
update_priv | INTEGER | UPDATE 권한이 있으면 1, 그렇지 않으면 0
delete_priv | INTEGER | DELETE 권한이 있으면 1, 그렇지 않으면 0
alter_priv  | INTEGER | ALTER 권한이 있으면 1, 그렇지 않으면 0

### db\_data\_type

Rye가 지원하는 데이터 타입(db_column 의 'Rye가 지원하는 데이터 타입' 표 참조)을 나타낸다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
type_id | INTEGER | 데이터 타입 식별자. 'Rye가 지원하는 데이터 타입' 표의 '값'에 해당함
type_name | VARCHAR(9) | 데이터 타입 이름. 'Rye가 지원하는 데이터 타입' 표의 '의미'에 해당함

### db\_collation

콜레이션에 대한 정보이다.

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
coll_id | INTEGER| 콜레이션 ID
coll_name | VARCHAR(32) | 콜레이션 이름
charset_id | INTEGER | 문자셋 ID
built_in | INTEGER | 제품 설치 시 콜레이션 포함 여부
expansions | INTEGER | 확장 지원 여부 (0: 지원 안 함, 1: 지원)
contractions | INTEGER | 축약 지원 여부 (0: 지원 안 함, 1: 지원)
uca_strength | INTEGER |가중치 세기(weight strength)
checksum | VARCHAR(32) |콜레이션 파일의 체크섬

### db\_user

칼럼명 | 데이터 타입 | 설명
--- | --- | ---
name | VARCHAR(1073741823) | 사용자명
id | INTEGER | 사용자 식별자
password | VARCHAR(1073741823) | 사용자 패스워드로 암호화되어 보여진다.

### db\_log\_applier

(작업 중)
**applylogdb** 유틸리티가 복제 로그를 반영할 때마다 그 진행 상태를 저장하기 위한 테이블이다. 이 테이블은 **applylogdb** 유틸리티가 커밋하는 시점마다 갱신되며, *\_counter* 칼럼에는 수행 연산의 누적 카운트 값이 저장된다. 각 칼럼의 의미는 다음과 같다.

<table>
<colgroup>
<col width="19%" />
<col width="13%" />
<col width="66%" />
</colgroup>
<thead>
<tr class="header">
<th>칼럼명</th>
<th>칼럼 타입</th>
<th>의미</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>db_name</td>
<td>VARCHAR(255)</td>
<td>로그에 저장된 DB 이름</td>
</tr>
<tr class="even">
<td>db_creation_time</td>
<td>DATETIME</td>
<td>반영하는 로그에 대한 원본 DB의 생성 시각</td>
</tr>
<tr class="odd">
<td>copied_log_path</td>
<td>VARCHAR(4096)</td>
<td>반영하는 로그 파일의 경로</td>
</tr>
<tr class="even">
<td>committed_lsa_pageid</td>
<td>BIGINT</td>
<td>마지막에 반영한 commit log lsa의 page id applylogdb가 재시작해도 last_committed_lsa 이전 로그는 재반영하지 않음</td>
</tr>
<tr class="odd">
<td>committed_lsa_offset</td>
<td>INTEGER</td>
<td>마지막에 반영한 commit log lsa의 offset applylogdb가 재시작해도 last_committed_lsa 이전 로그는 재반영하지 않음</td>
</tr>
<tr class="even">
<td>committed_rep_pageid</td>
<td>BIGINT</td>
<td>마지막 반영한 복제 로그 lsa의 pageid 복제 반영 지연 여부 확인</td>
</tr>
<tr class="odd">
<td>committed_rep_offset</td>
<td>INTEGER</td>
<td>마지막 반영한 복제 로그 lsa의 offset. 복제 반영 지연 여부 확인</td>
</tr>
<tr class="even">
<td>append_lsa_page_id</td>
<td>BIGINT</td>
<td>마지막 반영 당시 복제 로그 마지막 lsa의 page id 복제 반영 당시, applylogdb에서 처리 중인 복제 로그 헤더의 append_lsa를 저장 복제 로그 반영 당시의 지연 여부를 확인</td>
</tr>
<tr class="odd">
<td>append_lsa_offset</td>
<td>INTEGER</td>
<td>마지막 반영 당시 복제 로그 마지막 lsa의 offset 복제 반영 당시, applylogdb에서 처리 중인 복제 로그 헤더의 append_lsa를 저장 복제 로그 반영 당시의 지연 여부를 확인</td>
</tr>
<tr class="even">
<td>eof_lsa_page_id</td>
<td>BIGINT</td>
<td>마지막 반영 당시 복제 로그 EOF lsa의 page id 복제 반영 당시, applylogdb에서 처리 중인 복제 로그 헤더의 eof_lsa를 저장 복제 로그 반영 당시의 지연 여부를 확인</td>
</tr>
<tr class="odd">
<td>eof_lsa_offset</td>
<td>INTEGER</td>
<td>마지막 반영 당시 복제 로그 EOF lsa의 offset 복제 반영 당시, applylogdb에서 처리 중인 복제 로그 헤더의 eof_lsa를 저장 복제 로그 반영 당시의 지연 여부를 확인</td>
</tr>
<tr class="even">
<td>final_lsa_pageid</td>
<td>BIGINT</td>
<td>applylogdb에서 마지막으로 처리한 로그 lsa의 pageid 복제 반영 지연 여부 확인</td>
</tr>
<tr class="odd">
<td>final_lsa_offset</td>
<td>INTEGER</td>
<td>applylogdb에서 마지막으로 처리한 로그 lsa의 offset 복제 반영 지연 여부 확인</td>
</tr>
<tr class="even">
<td>required_page_id</td>
<td>BIGINT</td>
<td>log_max_archives 파라미터에 의해 삭제되지 않아야 할 가장 작은 log page id, 복제 반영 시작할 로그 페이지 번호</td>
</tr>
<tr class="odd">
<td>required_page_offset</td>
<td>INTEGER</td>
<td>복제 반영 시작할 로그 페이지 offset</td>
</tr>
<tr class="even">
<td>log_record_time</td>
<td>DATETIME</td>
<td>슬레이브 DB에 커밋된 복제 로그에 포함된 timestamp, 즉 해당 로그 레코드 생성 시간</td>
</tr>
<tr class="odd">
<td>log_commit_time</td>
<td>DATETIME</td>
<td>마지막 commit log의 반영 시간</td>
</tr>
<tr class="even">
<td>last_access_time</td>
<td>DATETIME</td>
<td>db_ha_apply_info 카탈로그의 최종 갱신 시간</td>
</tr>
<tr class="odd">
<td>status</td>
<td>INTEGER</td>
<td>반영 진행 상태(0: IDLE, 1: BUSY)</td>
</tr>
<tr class="even">
<td>insert_counter</td>
<td>BIGINT</td>
<td>applylogdb가 insert한 횟수</td>
</tr>
<tr class="odd">
<td>update_counter</td>
<td>BIGINT</td>
<td>applylogdb가 update한 횟수</td>
</tr>
<tr class="even">
<td>delete_counter</td>
<td>BIGINT</td>
<td>applylogdb가 delete한 횟수</td>
</tr>
<tr class="odd">
<td>schema_counter</td>
<td>BIGINT</td>
<td>applylogdb가 schema를 변경한 횟수</td>
</tr>
<tr class="even">
<td>commit_counter</td>
<td>BIGINT</td>
<td>applylogdb가 commit한 횟수</td>
</tr>
<tr class="odd">
<td>fail_counter</td>
<td>BIGINT</td>
<td>applylogdb가 insert/update/delete/commit/schema 변경 중 실패 횟수</td>
</tr>
<tr class="even">
<td>start_time</td>
<td>DATETIME</td>
<td>applylogdb 프로세스가 슬레이브 DB에 접속한 시간</td>
</tr>
</tbody>
</table>

### db\_log\_analyzer

(작업 중)

### db\_log\_writer

(작업 중)

카탈로그 테이블 사용 권한
-------------------------------------

카탈로그 테이블들은 **dba** 소유로 생성된다. 그러나, **dba** 가 **SELECT** 연산만 수행할 수 있을 뿐이며, **UPDATE** / **DELETE** 등의 연산을 수행할 경우에는 authorization failure 에러가 발생한다. 일반 사용자는 시스템 카탈로그 테이블에 대해서 SELECT 질의를 수행할 수 있다.

카탈로그 테이블에 대한 갱신은 사용자가 테이블/칼럼/인덱스/사용자/권한을 생성/변경/삭제하는 DDL 문을 수행할 경우 시스템에 의해 자동으로 수행된다.

카탈로그에 대한 질의
--------------------

테이블, 뷰, 칼럼, 인덱스 이름 등과 같은 식별자(identifier)는 모두 소문자로 변경되어 시스템 카탈로그에 저장된다. 반면, DB 사용자 이름은 대문자로 변경되어 db\_user 시스템 카탈로그 테이블에 저장된다.
