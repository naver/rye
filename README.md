### RYE: Native Sharding RDBMS
Scalability problems in relational databases are perceived as a major disadvantage.
Sharding is one of the solutions to overcome the scalability problems,
but the lack of functionality supported by DBMS makes it difficult to apply for the application.
To solve it, we are developing RYE which is RDBMS supporting sharding functionality.

### Quickstart

#### Build on Linux
RYE builds successfully on 64-bit Linux (CentOS) environments. Other Linux distributions have not been tested.

**Compile**

To build RYE from the git-cloned source code,

```bash
$ ./autogen.sh
$ mkdir Release64
$ cd Release64
$ ../configure --prefix=$HOME/RYE
$ make
$ make install
```

#### Install

```bash
$ tar xvfz RYE.1.0.0.0610.tar.gz
$ mv RYE $HOME
$ export RYE=$HOME/RYE
$ export RYE_DATABASES=$HOME/db
$ export PATH=$RYE/bin:$PATH
$ export LD_LIBRARY_PATH=$RYE/lib:$LD_LIBRARY_PATH
$ export CLASSPATH=$RYE/jdbc/rye_jdbc.jar:$CLASS_PATH
$ rye --version
rye (Rye utilities)
Rye 1.0 (1.0.0.0610) (64bit release build for linux_gnu) (Oct 16 2017 16:20:53)
```

#### Run
To create and start demodb database, with single shard node
* HOST_IP=1.1.1.1
* SHARD_MGMT_PORT=40000

Execute service command at 1.1.1.1
```bash
$ rye service start
```

Execute shard init on a machine where RYE jdbc driver is installed
```bash
$ java rye.jdbc.admin.Shard init demodb 1.1.1.1 40000 --num-groups=10000
```

Execute service command at 1.1.1.1
```bash
$ rye service status
```

Connect to demodb database using jdbc connection url
(Can use a 3rd party SQL browser, such as the SQuirrel SQL Client)

```
jdbc:rye://1.1.1.1:40000/demodb:dba:/rw?charset=utf-8&queryTimeout=6000
```

Insert sample data

```SQL
CREATE SHARD TABLE mail (
   	uid  	VARCHAR,
   	mid  	INT,
   	title 	VARCHAR,
   	PRIMARY KEY (uid, mid)
) SHARD BY uid;

INSERT INTO mail VALUES ('id1', 1, 'hello user1');
INSERT INTO mail VALUES ('id2', 2, 'hello user2');
INSERT INTO mail VALUES ('id3', 3, 'hello user3');
INSERT INTO mail VALUES ('id4', 4, 'hello user4');
INSERT INTO mail VALUES ('id5', 5, 'hello user5');
```

Retrieve the data

```SQL
-- get shard node info
SELECT shard_nodeid(), count(*) FROM mail;
   shard_nodeid()   count(*)   
 ==============================
   1                5          

-- query with shardkey condition
SELECT title FROM mail WHERE uid = 'id1';
   title           
 ==================
   'hello user1'   

-- query with non-shardkey condition
SELECT title FROM mail WHERE mid < 3;
   title           
 ==================
   'hello user1'   
   'hello user2'
```

### RYE Contributors
RYE is forked from CUBRID 9.3.

### License

### Patents

### mailing group
oss_rye ( dl_oss_rye@navercorp.com )


-----------


### RYE: Native Sharding RDBMS

* SQL 을 분석하여 자동으로 샤드 노드에 분산합니다.
* 서비스 운영 중에 샤드 노드를 추가하고 데이타를 재분배합니다.
* 레코드 단위로 샤딩 정합성을 관리합니다.
* Shardkey 기반의 동시성 제어 및 병렬 복제, Online DDL, 노드 관리 자동화 기능을 제공합니다.

#### RYE 사용성
* 사용자에게 일정한 응답성을 보장하면서 데이타를 분산하고 스키마를 변경한다.
* 개발자에게 친숙한 표준화 된 프로그래밍 방식을 제공한다.
* SNS, 메일 같은 대용량의 개인화 서비스 저장소로 사용하는데 적합하다.
* Complete platform for sharding an RDB
 
#### 인터넷 서비스의 특성
* 사용자가 폭발적으로 늘어날 수 있고 사용자 요구에 따른 기능 변경이 꾸준히 발생한다.
* 위를 포함한 어떠한 상황(OS 패닉, H/W 장애, 네트워크 단절 등)에서도 사용자에게 일정한 응답성을 보장해야 한다.

#### 데이타 저장소에서 발생하는 이슈
* 부하 분산을 위한 데이타 저장소의 수평적 확장이 용이해야 함
* 스키마 변경에 유연하게 대처할 수 있어야 함
* 데이타에 대한 효율적인 검색 및 정합성을 보장해야 함

#### RYE 에 적용된 기술
* 분산된 노드 관리 및 SQL 자동 분산
* 응답성과 리소스 사용량을 해치지 않는 online 인덱스 생성
* 새로운 잠금(lock) 방식 도입
* HA, 병렬(parallel) 복제
* 고성능 write-intensive workload 처리
* 인덱스 최적화

#### RYE 특징
* 응답성
	* 유연한 부하 분산 및 스키마 변경		
	* 고성능, 고가용성
* 개발 및 운영 편의성
	* 관계형 스키마, SQL 지원
	* 표준 프로그래밍 인터페이스 제공 (JDBC)
	* SQL 자동 분산, 트랜젝션 지원

#### RYE Roadmap
* 2016.10 Beta 릴리스
	* 응용과 동일한 DB 스키마 유지
	* 온라인 수평 확장
	* 온라인 스키마 변경 1단계
	* 병렬 복제
* 2016.12
	* 온라인 운영 편의성 강화
	* 온라인 스키마 변경 확장
	* trouble shooting 기능 강화
	* 고성능 DML 처리

#### RYE Vision
* Relational paradigm, Distributed processing, Commodity hardware, Elasticity and Cloud/hybrid flexibility
* Technical Direction
	* High-speed, In-memory computing, SSD
	* Automated data distribution, Scale-out architecture
	* Fixed stable schema, Online schema change
	* SQL and ACID support
	* High availability and Reliability
	* Open-source software

#### RYE Dev Items
* native sharding, SQL-level routing
* native HA
* docker 환경에 맞게 conf, db vol 수정
* 노드 구성 자동화 커맨드 제공
* shard key locking 방식, deadlock free
* remote backup 지원
* docker 배포 방식 지원
* SHA-512 지원
* online migration
* migrator 수행 위치를 master/slave 모두 지원
* migration 이후 delayed GC 지원
* oodb 특성 제거, 코드 간소화, 고성능
* 각 프로세스의 상태 정보를 shared memory 로 유지, 모니터링 도구로 shared memory 에 접속하는 방식 (npot 등)
* parallel 복제
* DDL 복제로 인한 복제 지연 제거 (index 생성과 DML 동시 반영)
* 인덱스 구조 개선, 안정화; non-unique index 와 unique index 가 동일한 구조 가지도록, overflow-OIDs, overflow-key 방식 제거

### Reference

