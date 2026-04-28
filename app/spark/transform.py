from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

def rank_paths_by_response_time(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("path").orderBy(F.desc("response_time_ms"))
    return (
        df.withColumn("rank", F.rank().over(window_spec))
        .select("path", "response_time_ms", "rank")
        .orderBy("path", "rank")
    )

def moving_average_response_time(df: DataFrame) -> DataFrame:
    window_spec = Window.orderBy("timestamp").rowsBetween(-2, 0)
    return (
        df.withColumn(
            "moving_avg_response_time_ms",
            F.avg("response_time_ms").over(window_spec)
        )
        .select("timestamp", "path", "response_time_ms", "moving_avg_response_time_ms")
        .orderBy("timestamp")
    )

def classify_response_time(response_time_ms):
    if response_time_ms <= 50:
        return "빠름"
    elif response_time_ms <= 150:
        return "보통"
    else:
        return "느림"
    
classify_response_time_udf = F.udf(classify_response_time, StringType())

def add_response_time_grade(df: DataFrame) -> DataFrame:
    return df.withColumn("grade", classify_response_time_udf(F.col("response_time_ms")))

def count_requests_by_grade(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("grade")
            .count()
            .orderBy(F.desc("count"))
    )

def save_partitioned(df: DataFrame, output_path: str, partition_by: str) -> None:
    (
        df.write
            .partitionBy(partition_by)
            .mode("overwrite")
            .parquet(output_path)
    )

def read_partitioned(spark: SparkSession, input_path: str, filter_col: str, filter_val) -> DataFrame:
    return (
        spark.read
            .parquet(input_path)
            .filter(F.col(filter_col) == filter_val)
    )