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
    add_response_time_grade,
    count_requests_by_grade,
    save_partitioned,
    read_partitioned,
)
from app.spark.transform_sql import (
    calculate_average_response_time_sql,
    calculate_error_rate_sql,
    count_requests_by_path_sql,
    count_requests_by_status_code_sql,
)

WEB_LOGS_CSV_PATH = "data/web_logs.csv"
PARTITION_PATH = "data/web_logs_partitioned"

def get_web_log_analytics() -> dict:
    import time
    started_at = time.perf_counter()

    spark = get_spark_session()
    raw_df = spark.read.option("header", True).csv(WEB_LOGS_CSV_PATH)
    logs_df = prepare_web_logs(raw_df)

    requests_by_path = [row.asDict() for row in count_requests_by_path(logs_df).collect()]
    requests_by_status_code = [row.asDict() for row in count_requests_by_status_code(logs_df).collect()]
    average_response_time = calculate_average_response_time(logs_df).first().asDict()
    error_rate = calculate_error_rate(logs_df).first().asDict()

    elapsed_ms = round((time.perf_counter() - started_at) * 1000)

    return {
        "elapsed_ms": elapsed_ms,
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

def get_web_log_analytics_cached() -> dict:
    import time
    started_at = time.perf_counter()

    spark = get_spark_session()
    raw_df = spark.read.option("header", "true").csv(WEB_LOGS_CSV_PATH)
    logs_df = prepare_web_logs(raw_df)
    logs_df.cache()

    requests_by_path = [
        row.asDict()
        for row in count_requests_by_path(logs_df).collect()
    ]

    requests_by_status_code = [
        row.asDict()
        for row in count_requests_by_status_code(logs_df).collect()
    ]

    average_response_time = calculate_average_response_time(logs_df).first().asDict()

    error_rate = calculate_error_rate(logs_df).first().asDict()

    logs_df.unpersist()

    elapsed_ms = round((time.perf_counter() - started_at) * 1000)

    return {
        "elapsed_ms": elapsed_ms,
        "requests_by_path": requests_by_path,
        "requests_by_status_code": requests_by_status_code,
        "average_response_time": average_response_time["average_response_time_ms"],
        "error_rate": error_rate["error_rate"],
    }

def get_web_log_udf_analytics() -> dict:
    spark = get_spark_session()

    raw_df = (
        spark.read
            .option("header", "true")
            .csv(WEB_LOGS_CSV_PATH)
    )

    logs_df = prepare_web_logs(raw_df)
    graded_df = add_response_time_grade(logs_df)

    requests_by_grade = [
        row.asDict()
        for row in count_requests_by_grade(graded_df).collect()
    ]

    return {
        "requests_by_grade": requests_by_grade,
    }

def get_web_log_partition_analytics() -> dict:
    import time
    spark = get_spark_session()

    raw_df = spark.read.option("header", "true").csv(WEB_LOGS_CSV_PATH)
    logs_df = prepare_web_logs(raw_df)

    save_partitioned(logs_df, PARTITION_PATH, "status_code")

    started_full = time.perf_counter()
    full_df = spark.read.parquet(PARTITION_PATH)
    full_count = full_df.count()
    elapsed_full_ms = round((time.perf_counter() - started_full) * 1000)

    started_partition = time.perf_counter()
    partition_df = read_partitioned(spark, PARTITION_PATH, "status_code", 200)
    partition_count = partition_df.count()
    elapsed_partition_ms = round((time.perf_counter() - started_partition) * 1000)

    return {
        "full_read": {"count": full_count, "elapsed_ms": elapsed_full_ms},
        "partition_read": {"status_code": 200, "count": partition_count, "elapsed_ms": elapsed_partition_ms},
    }