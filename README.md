# ⛅Weather Data Pipeline with Airflow & AI Agents

Docker 기반의 Airflow 환경을 직접 구축하여 영국 날씨 데이터를 수집하는 ELT 파이프라인이며, Google Gemini API 기반의 AI Agent를 활용한 자동 PR 생성 및 자동 코드 리뷰 시스템을 포함합니다.

## 📖 프로젝트 개요 (Project Overview)
이 프로젝트는 단순한 데이터 수집을 넘어, 인프라 구축부터 데이터 웨어하우스 적재까지(A to Z) 데이터 엔지니어링의 전체 수명주기를 학습하기 위해 시작되었습니다. 특히 1인 프로젝트의 한계를 극복하기 위해 **AI Agent**를 동료 개발자처럼 활용하여 코드 품질을 관리합니다.


### 🎯 주요 목표
- Airflow 2.x 심화 학습: 단순 파이프라인 작성을 넘어 아키텍처, Executor, Dataset 기능을 활용한 이벤트 기반 스케줄링 구현
- ELT 아키텍처 구현: Data Lake(MinIO)와 Data Warehouse(PostgreSQL)를 분리하여 원본 보존 및 가공 로직의 유연성 확보
- AI-Driven Development: GitHub Actions와 LLM을 연동하여 자동 PR 생성 및 AI 코드 리뷰 워크플로우 구축
- Docker 인프라 운영: 로컬 환경에서 컨테이너 기반의 데이터 플랫폼 직접 구성 및 운영

## 🏗️ 아키텍처 (Architecture)
데이터는 OpenWeatherMap API에서 수집되어 MinIO(DL)를 거쳐 PostgreSQL(DW)로 흐릅니다

### 🔄 데이터 흐름 (Data Flow)
1. Ingestion(Extract): Airflow가 매 시간 런던 등 영국 주요 도시의 날씨 API를 호출
2. Data Lake: 응답받은 데이터를 파싱 없이 MinIO 버킷에 적재 (Idempotency 보장)
3. Transformation: Airflow Dataset 기능을 통해?? 적재 완료 이벤트를 감지, 데이터를 가공하여 PostgreSQL에 적재


## 🛠️ 기술 스택 (Tech Stack)
| 구분 | 기술 |
| :--- | :--- |
| Orchestration | Apache Airflow 2.10+ |
| Infrastructure | Docker & Compose |
| Data Lake (DL) | MinIO |
| Data Warehouse (DW) | PostgreSQL |
| AI Agent (Brain) | ... |
| CI/CD | GitHub Actions |
| Language | Python 3.9+ |


## 🤖 AI Agent Workflow
1. Issue 생성: 작업할 내용(Task)을 Issue로 정의.
2. 코드 작성 & Push: 기능 구현 후 Git Push.
3. Auto PR (Agent): Gemini가 변경 사항(git diff)을 분석하여 PR 제목과 본문을 자동으로 작성하고 PR 생성.
4. Auto Review (Agent): 생성된 PR에 대해 Gemini가 Airflow Best Practice 관점에서 코드 리뷰 코멘트 작성.
5. Merge: AI의 피드백 반영 후 Main 브랜치 병합.
