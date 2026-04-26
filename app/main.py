import csv
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request

from app.services.analytic import get_web_log_analytics, get_request_log_analytics, get_web_log_analytics_sql

REQUEST_LOG_CSV_PATH = Path("data/request_logs.csv")
REQUEST_LOG_FIELDNAMES = ["timestamp", "method", "path", "status_code", "response_time_ms"]

app = FastAPI()


def write_request_log(row: dict) -> None:
    file_exists = REQUEST_LOG_CSV_PATH.exists()

    with REQUEST_LOG_CSV_PATH.open("a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=REQUEST_LOG_FIELDNAMES)

        if not file_exists:
            writer.writeheader()
        
        writer.writerow(row)

# 미들웨어 구성
@app.middleware("http")
async def log_request(request: Request, call_next):
    started_at = time.perf_counter()

    response = await call_next(request)

    response_time_ms = round((time.perf_counter() - started_at) * 1000)

    write_request_log({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "method": request.method,
        "path": request.url.path,
        "status_code": response.status_code,
        "response_time_ms": response_time_ms,
    })

    return response


@app.get("/")
def read_root():
    return {"message": "SPARK API 체크"}


@app.get("/analytics/web-logs")
def read_web_log_analytics() -> dict:
    return get_web_log_analytics()


@app.get("/analytics/request-logs")
def read_request_log_analytics() -> dict:
    return get_request_log_analytics()


@app.get("/analytics/web-logs-sql")
def read_web_log_analytics_sql() -> dict:
    return get_web_log_analytics_sql()