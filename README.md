[![Build Status](https://travis-ci.org/naver/rye.svg?branch=master)](https://travis-ci.org/naver/rye)
[![Coverity Scan Status](https://scan.coverity.com/projects/14194/badge.svg)](https://scan.coverity.com/projects/naver-rye)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b176ed8900494f0d836030798aa47699)](https://www.codacy.com/app/kyungsik.seo/rye?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=naver/rye&amp;utm_campaign=Badge_Grade)

[![Coverage Status](https://coveralls.io/repos/github/naver/rye/badge.svg?branch=master)](https://coveralls.io/github/naver/rye?branch=master)

### RYE: Native Sharding RDBMS
Scalability problems in relational databases are perceived as a major disadvantage.
Sharding is one of the solutions to overcome the scalability problems,
but the lack of functionality supported by DBMS makes it difficult to apply for the application.
To solve it, we are developing RYE which is RDBMS supporting sharding functionality.

### RYE Features

* Analyze SQL and automatically route it to shard nodes
* Dynamically add shard nodes and redistribute data
* Ensure efficient search and record-level shard data integrity
* Provide a standardized programming API for developers
* Support online DDL that does not interfere with response and resource usage
* Use shardkey-based deadlock-free concurrency control and parallel replication
* Native HA
* Keep various monitoring information for each process into shared memory
* Provide remote backup
* Docker deployment


### Quickstart

* [Build and Installation](doc/quick_start.md)
* [Using docker](doc/quick_start_docker.md)


### RYE Contributors
RYE is forked from CUBRID 9.3.

### License

### Patents

### mailing group
oss_rye ( dl_oss_rye@navercorp.com )


-----------


### Reference

* [DEVIEW 2017 Day2, 2-3. RYE, 샤딩을 지원하는 오픈소스 관계형 DBMS](https://deview.kr/2017/schedule/185)
* [SlideShare link](https://www.slideshare.net/deview/223rye-dbms)
* [NAVER TV link](http://tv.naver.com/v/2302946)
