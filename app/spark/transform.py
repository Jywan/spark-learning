from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def prepare_web_logs(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("status_code", F.col("status_code").cast("int"))
            .withColumn("response_time_ms", F.col("response_time_ms").cast("int"))
    )

def count_requests_by_path(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("path")
            .count()
            .orderBy(F.desc("count"))
    )

def count_requests_by_status_code(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("status_code")
            .count()
            .orderBy(F.desc("count"))
    )

def calculate_average_response_time(df: DataFrame) -> DataFrame:
    return df.select(
        F.avg("response_time_ms").alias("average_response_time_ms")
    )

def calculate_error_rate(df: DataFrame) -> DataFrame:
    return df.select(
        (
            F.sum(F.when(F.col("status_code") >= 400, 1).otherwise(0))
            / F.count("*")
        ).alias("error_rate")
    )

def count_requests_by_method(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("method")
            .count()
            .orderBy(F.desc("count"))
    )