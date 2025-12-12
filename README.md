# ⛅Global Weather Live: Real-time Data Pipeline & AI Agents

> **Kafka & Spark Structured Streaming 기반의 대용량 기상 데이터 실시간 처리 및 AI 협업 파이프라인**

Docker 기반의 Airflow 환경을 직접 구축하여 전 세계 30개 주요 도시의 기상 데이터를 수집하는 파이프라인이며, Google Gemini API 기반의 AI Agent를 활용한 자동 PR 생성 및 자동 코드 리뷰 시스템을 포함합니다.

## 📖 프로젝트 개요 (Project Overview)
이 프로젝트는 단순한 배치 처리를 넘어, 대용량 트래픽 상황을 가정한 실시간 데이터 엔지니어링 역량을 확보하기 위해 시작되었습니다. Kafka를 통해 급격한 트래픽 버스트를 안정적으로 처리하고, Spark Structured Streaming의 Two-Stream Architecture를 도입하여 '실시간 분석'과 '데이터 유실 방지'라는 두 마리 토끼를 모두 잡고자 합니다.

또한, 1인 개발의 한계를 극복하기 위해 AI Agent **CodeRabbit**을 도입, 코드 리뷰까지 자동화된 협업 환경을 구축했습니다.

### 🎯 주요 목표
- Real-time Processing: Kafka와 Spark Structured Streaming을 활용해 데이터 생성부터 시각화까지의 지연 시간을 최소화 (Micro-batch)
- Reliability & Replayability: 원본 데이터(Raw)를 Data Lake에 우선 적재하여, 로직 오류 발생 시 언제든 재처리(Replay) 가능한 아키텍처 구현
- Scalability: 초당 수천 건의 메시지가 유입되어도 버틸 수 있는 내결함성(Fault Tolerance) 확보
- Docker 인프라 운영: 로컬 환경에서 컨테이너 기반의 데이터 플랫폼 직접 구성 및 운영
- AI-Driven Workflow: LLM(CodeRabbit)을 활용한 자동 코드 리뷰 및 문서화로 코드 품질 유지

## 🏗️ 아키텍처 (Architecture)
Two-Stream Strategy: Spark에서 데이터를 받아 [Data Lake 저장]과 [DW 실시간 집계]를 병렬로 처리합니다.

### 🔄 데이터 흐름 (Data Flow)
1. Ingestion (Producer): 전 세계 50개 도시의 OpenWeatherMap API를 1분 주기로 비동기 호출 → Kafka Topic으로 전송 (Throughput 확보)
2. Buffering: Kafka가 트래픽 버퍼 역할을 수행하며 시스템 부하 조절
3. Processing (Spark Structured Streaming):
   - Stream A (Bronze Layer): 원본 JSON 데이터를 변형 없이 **MinIO(S3)**에 Parquet 포맷으로 적재 (Partitioning 적용)
   - Stream B (Gold Layer): Window Function을 사용해 10분/1시간 단위 평균 기온, 급격한 변화 감지 등을 수행 후 **PostgreSQL(DW)**에 Upsert
4. Serving: Superset 통해 실시간 기상 변화 모니터링


## 🛠️ 기술 스택 (Tech Stack)
| 구분 | 기술 | 설명 |
| :--- | :--- | :--- |
| Language | Python 3.9+ | 데이터 수집 및 Spark Streaming 로직 구현 |
| Ingestion | Apache Kafka | 비동기 API 호출 데이터 버퍼링 및 메시지 큐잉 |
| Processing | Spark Structured Streaming | 실시간 데이터 변환, Window Aggregation, Two-Stream 분기 처리 |
| Data Lake | MinIO | S3 호환 Object Storage. Raw 데이터를 Parquet 포맷으로 영구 보존 |
| Data Warehouse | PostgreSQL | 가공된 집계 데이터 저장 및 시각화 도구 연동 |
| Orchestration | Apache Airflow 2.x | Producer 스케줄링 및 컨테이너/파이프라인 상태 모니터링 |
| Infra | Docker Compose | 로컬 환경에서의 인프라 구축 및 네트워크 관리 |
| AI Agent | CodeRabbit (LLM) | 코드 리뷰 자동화 및 AI 기반 코드 품질 관리 |


## 🤖 AI Agent Workflow
1. Issue 생성: 작업할 내용(Task)을 Issue로 정의.
2. 코드 작성 & Push: 기능 구현 후 Git Push.
3. Auto PR (Agent): CodeRabbit이 변경 사항(git diff)을 분석하여 PR 제목과 본문을 자동으로 작성하고 PR 생성.
4. Auto Review (Agent): 생성된 PR에 대해 CodeRabbit이 코드 리뷰 코멘트 작성.
5. Merge: AI의 피드백 반영 후 Main 브랜치 병합.
