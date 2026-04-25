from app.spark.session import get_spark_session
from app.spark.transform import (
    calculate_average_response_time,
    calculate_error_rate,
    count_requests_by_path,
    count_requests_by_status_code,
    prepare_web_logs,
)

WEB_LOGS_CSV_PATH = "data/web_logs.csv"

def get_web_log_analytics() -> dict:
    spark = get_spark_session()

    raw_df = (
        spark.read
            .option("header", "true")
            .csv(WEB_LOGS_CSV_PATH)
    )

    logs_df = prepare_web_logs(raw_df)

    requests_by_path = [
        row.asDict()
        for row in count_requests_by_path(logs_df).collect()
    ]

    requests_by_status_code = [
        row.asDict()
        for row in count_requests_by_status_code(logs_df).collect()
    ]

    average_response_time = (
        calculate_average_response_time(logs_df)
            .first()
            .asDict()
    )

    error_rate = (
        calculate_error_rate(logs_df)
            .first()
            .asDict()
    )

    return {
        "requests_by_path": requests_by_path,
        "requests_by_status_code": requests_by_status_code,
        "average_response_time": average_response_time["average_response_time_ms"],
        "error_rate": error_rate["error_rate"],
    }