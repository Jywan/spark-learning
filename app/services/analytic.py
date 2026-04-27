from app.spark.session import get_spark_session
from app.spark.transform import (
    calculate_average_response_time,
    calculate_error_rate,
    count_requests_by_path,
    count_requests_by_status_code,
    count_requests_by_method,
    prepare_web_logs,
    rank_paths_by_response_time,
    moving_average_response_time,
)
from app.spark.transform_sql import (
    calculate_average_response_time_sql,
    calculate_error_rate_sql,
    count_requests_by_path_sql,
    count_requests_by_status_code_sql,
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

REQUEST_LOGS_CSV_PATH = "data/request_logs.csv"

def get_request_log_analytics() -> dict:
    spark = get_spark_session()

    raw_df = (
        spark.read
            .option("header", "true")
            .csv(REQUEST_LOGS_CSV_PATH)
    )

    logs_df = prepare_web_logs(raw_df)

    request_by_method = [
        row.asDict()
        for row in count_requests_by_method(logs_df).collect()
    ]

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
        "request_by_method": request_by_method,
        "requests_by_path": requests_by_path,
        "requests_by_status_code": requests_by_status_code,
        "average_response_time": average_response_time["average_response_time_ms"],
        "error_rate": error_rate["error_rate"],
    }

def get_web_log_analytics_sql() -> dict:
    spark = get_spark_session()

    raw_df = (
        spark.read
            .option("header", "true")
            .csv(WEB_LOGS_CSV_PATH)
    )

    logs_df = prepare_web_logs(raw_df)

    requests_by_path = [
        row.asDict()
        for row in count_requests_by_path_sql(logs_df).collect()
    ]

    requests_by_status_code = [
        row.asDict()
        for row in count_requests_by_status_code_sql(logs_df).collect()
    ]

    average_responese_time = (
        calculate_average_response_time_sql(logs_df)
            .first()
            .asDict()
    )

    error_rate = (
        calculate_error_rate_sql(logs_df)
            .first()
            .asDict()
    )

    return {
        "requests_by_path": requests_by_path,
        "requests_by_status_code": requests_by_status_code,
        "average_response_time": average_responese_time["average_response_time_ms"],
        "error_rate": error_rate["error_rate"],
    }

def get_web_log_window_analytics() -> dict:
    spark = get_spark_session()

    raw_df = (
        spark.read
            .option("header", "true")
            .csv(WEB_LOGS_CSV_PATH)
    )

    logs_df = prepare_web_logs(raw_df)

    path_response_time_rank = [
        row.asDict()
        for row in rank_paths_by_response_time(logs_df).collect()
    ]

    moving_avg = [
        row.asDict()
        for row in moving_average_response_time(logs_df).collect()
    ]

    return {
        "path_response_time_rank": path_response_time_rank,
        "moving_average_response_time": moving_avg,
    }
