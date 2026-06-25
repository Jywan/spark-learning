# Spark Weblog Analytics API

FastAPI + PySpark를 사용해서 웹로그 데이터를 분석하는 학습용 프로젝트입니다.
Spark의 핵심 기능을 단계별로 학습하면서 만들었습니다.

## 기술 스택

- **Python 3.14**
- **FastAPI** — HTTP API 서버
- **PySpark 4.1** — 데이터 처리 및 머신러닝
- **uvicorn** — ASGI 서버

## 프로젝트 구조

```
spark-webapp/
  app/
    main.py               # FastAPI 엔드포인트 + 요청 로그 미들웨어
    spark/
      session.py          # SparkSession 생성
      transform.py        # DataFrame 변환/집계 함수
      transform_sql.py    # Spark SQL 버전 집계 함수
    services/
      analytic.py         # 분석 서비스 함수
  data/
    web_logs.csv          # 샘플 웹로그 데이터
    request_logs.csv      # FastAPI 실제 요청 로그 (자동 생성)
    generate_logs.py      # 더미 데이터 생성 스크립트 (10만 행)
```

## 실행 방법

```bash
# 의존성 설치
pip install -r requirements.txt

# 서버 실행
python -m uvicorn app.main:app --reload
```

## API 엔드포인트

### 기본

| 엔드포인트 | 설명 |
|---|---|
| `GET /` | 서버 상태 확인 |

### 웹로그 분석

| 엔드포인트 | 설명 | 학습 주제 |
|---|---|---|
| `GET /analytics/web-logs` | DataFrame API 기본 집계 | DataFrame API |
| `GET /analytics/request-logs` | 실제 요청 로그 분석 | 미들웨어 + Spark |
| `GET /analytics/web-logs-sql` | Spark SQL 집계 | Spark SQL |
| `GET /analytics/web-logs-window` | 응답시간 순위, 이동 평균 | 윈도우 함수 |
| `GET /analytics/web-logs-cached` | 캐싱 적용 집계 | 캐싱 최적화 |
| `GET /analytics/web-logs-udf` | 응답시간 등급 분류 | UDF |
| `GET /analytics/web-logs-partition` | 파티셔닝 읽기 비교 | 파티셔닝 |

### 머신러닝

| 엔드포인트 | 설명 | 학습 주제 |
|---|---|---|
| `GET /analytics/web-logs-kmeans` | 요청 클러스터링 | K-Means |
| `GET /analytics/web-logs-lr` | 에러 여부 예측 | 로지스틱 회귀 |
| `GET /analytics/web-logs-lr-weighted` | 클래스 가중치 적용 | Weighted LR |
| `GET /analytics/web-logs-gbt` | 앙상블 분류 | GBT |

## 학습 단계

1. FastAPI + PySpark MVP
2. 실제 요청 로그 수집
3. Spark SQL
4. 윈도우 함수
5. 캐싱
6. UDF
7. 파티셔닝
8. K-Means 클러스터링
9. 로지스틱 회귀 + 모델 비교

## 블로그

[Spark 학습 프로젝트 시리즈]([https://j-y-w.tistory.com/category/spark%20%ED%95%99%EC%8A%B5%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8])
