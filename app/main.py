from fastapi import FastAPI

from app.services.analytic import get_web_log_analytics

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "SPARK API 체크"}

@app.get("/analytics/web-logs")
def read_web_log_analytics() -> dict:
    return get_web_log_analytics()