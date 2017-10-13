관리자 안내서
=============

관리자 안내서는 데이터베이스 관리자(**DBA**)가 Rye 시스템을 사용하는데 필요한 작업 방법을 설명한다.

-   Rye 서버, 브로커 및 매니저 서버 등의 다양한 프로세스들을 구동하고 정지하는 방법을 설명한다.
-   데이터베이스 생성 및 삭제, 볼륨 추가와 같은 데이터베이스 관리 작업, 데이터베이스를 다른 곳으로 이동하거나 시스템 버전에 맞춰서 변경하는 마이그레이션 작업, 장애 대비를 위한 데이터베이스의 백업 및 복구 작업 등에 대한 내용을 포함한다.
-   시스템 설정 방법에 대해 설명한다.
-   트러블슈팅 방법에 대해 설명한다.

**rye** 유틸리티는 Rye 서비스를 통합 관리할 수 있는 기능을 제공하며, Rye 서비스 프로세스를 관리하는 서비스 관리 유틸리티와 데이터베이스를 관리하는 데이터베이스 관리 유틸리티로 구분된다.

서비스 관리 유틸리티는 다음과 같다.

-   서비스 유틸리티: 마스터 프로세스를 구동 및 관리한다.
    -   rye service
-   서버 유틸리티: 서버 프로세스를 구동 및 관리한다.
    -   rye server
-   브로커 유틸리티: 브로커 프로세스 및 응용서버(CAS) 프로세스를 구동 및 관리한다.
    -   rye broker
-   HA 유틸리티: HA 관련 프로세스를 구동 및 관리한다.
    -   rye heartbeat

자세한 설명은 control-rye-processes 절을 참조한다.

데이터베이스 관리 유틸리티는 다음과 같다.

-   데이터베이스 생성, 볼륨 추가, 삭제
    -   rye createdb
    -   rye addvoldb
    -   rye deletedb
-   데이터베이스 이름 변경
    -   rye renamedb
-   데이터베이스 백업
    -   rye backupdb
-   데이터베이스 복구
    -   rye restoredb
-   데이터베이스 공간 확인
    -   rye spacedb
-   통계 정보 갱신, 질의 계획 확인
    -   rye plandump
    -   rye statdump
-   잠금 확인, 트랜잭션 확인, 트랜잭션 제거
    -   rye lockdb
    -   rye tranlist
    -   rye killtran
-   데이터베이스 진단/파라미터 출력
    -   rye diagdb
    -   rye paramdump
-   HA 모드 변경,로그 복제/반영
    -   rye changemode
    -   rye applyinfo

자세한 설명은 rye-utilities 를 참조한다.
