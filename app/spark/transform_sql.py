from pyspark.sql import DataFrame


def create_temp_view(df: DataFrame, view_name: str = "web_logs") -> None:
    df.createOrReplaceTempView(view_name)


def count_requests_by_path_sql(df: DataFrame) -> DataFrame:
    create_temp_view(df)
    return df.sparkSession.sql("""
        SELECT path, COUNT(*) AS count
        FROM web_logs
        GROUP BY path
        ORDER BY count DESC
    """)


def count_requests_by_status_code_sql(df: DataFrame) -> DataFrame:
    create_temp_view(df)
    return df.sparkSession.sql("""
        SELECT status_code, COUNT(*) AS count
        FROM web_logs
        GROUP BY status_code
        ORDER BY count DESC
    """)


def calculate_average_response_time_sql(df: DataFrame) -> DataFrame:
    create_temp_view(df)
    return df.sparkSession.sql("""
        SELECT AVG(response_time_ms) AS average_response_time_ms
        FROM web_logs
    """)


def calculate_error_rate_sql(df: DataFrame) -> DataFrame:
    create_temp_view(df)
    return df.sparkSession.sql("""
        SELECT SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*) AS error_rate
        FROM web_logs
    """)