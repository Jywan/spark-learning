from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

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


def run_kmeans_clustering(df: DataFrame, k: int) -> DataFrame:
    indexer = StringIndexer(inputCol="path", outputCol="path_index")

    assembler = VectorAssembler(
        inputCols=["path_index", "status_code", "response_time_ms"],
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42)

    pipeline = Pipeline(stages=[indexer, assembler, scaler, kmeans])

    model = pipeline.fit(df)

    return model.transform(df).select("path", "status_code", "response_time_ms", "cluster")


def summarize_clusters(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("cluster")
            .agg(
                F.count("*").alias("count"),
                F.avg("response_time_ms").alias("avg_response_time_ms"),
                F.max("response_time_ms").alias("max_response_time_ms"),
                F.min("response_time_ms").alias("min_response_time_ms"),
            )
            .orderBy("cluster")
    )


def prepare_classification_features(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "label",
        F.when(F.col("status_code") >= 400, 1.0).otherwise(0.0)
    )


def run_logistic_regression(df: DataFrame) -> dict:
    df_labeled = prepare_classification_features(df)

    train_df, test_df = df_labeled.randomSplit([0.8, 0.2], seed=42)

    indexer = StringIndexer(inputCol="path", outputCol="path_index")

    assembler = VectorAssembler(
        inputCols=["path_index", "response_time_ms"],
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    lr = LogisticRegression(featuresCol="features", labelCol="label")
    
    pipeline = Pipeline(stages=[indexer, assembler, scaler, lr])

    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    samples = [
        row.asDict()
        for row in predictions.select("path", "status_code", "response_time_ms", "label", "prediction")
        .limit(10)
        .collect()
    ]

    return {
        "auc": round(auc, 4),
        "samples": samples,
    }


def run_logistic_regression_improved(df: DataFrame) -> dict:
    df_labeled = prepare_classification_features(df)

    # 오분류 개선작업 - 시간대별 패턴을 반영하기 위해 'hour' 피처 추가
    df_with_hour = df_labeled.withColumn("hour", F.hour("timestamp"))

    train_df, test_df = df_with_hour.randomSplit([0.8, 0.2], seed=42)

    indexer = StringIndexer(inputCol="path", outputCol="path_index")

    assembler = VectorAssembler(
        # inputCols=["path_index", "response_time_ms"],
        # 시간대 패턴 반영
        inputCols=["path_index", "response_time_ms", "hour"],
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    # lr = LogisticRegression(featuresCol="features", labelCol="label")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        threshold=0.3,  # 민감도 조정 - 오류 예측을 더 적극적으로 수행
    )
    
    pipeline = Pipeline(stages=[indexer, assembler, scaler, lr])

    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    total = predictions.count()
    correct = predictions.filter(F.col("label") == F.col("prediction")).count()
    false_negatives = predictions.filter(
        (F.col("label") == 1.0) & (F.col("prediction") == 0.0)
    ).count()

    samples = [
        row.asDict()
        for row in predictions.select("path", "status_code", "response_time_ms", "label", "prediction")
        .limit(10)
        .collect()
    ]

    return {
        # "auc": round(auc, 4),
        # "samples": samples,
        "auc": round(auc, 4),
        "accuracy": round(correct / total, 4),
        "false_negatives": false_negatives,
        "samples": samples,
    }


def run_logistic_regression_weighted(df: DataFrame) -> dict:
    df_labeled = prepare_classification_features(df)

    error_count = df_labeled.filter(F.col("label") == 1.0).count()
    normal_count = df_labeled.filter(F.col("label") == 0.0).count()
    total = error_count + normal_count

    df_weighted = df_labeled.withColumn(
        "weight",
        F.when(F.col("label") == 1.0, total / (2 * error_count))
        .otherwise(total / (2 * normal_count))
    )

    train_df, test_df = df_weighted.randomSplit([0.8, 0.2], seed=42)

    indexer = StringIndexer(inputCol="path", outputCol="path_index")

    assembler = VectorAssembler(
        inputCols=["path_index", "response_time_ms"],
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="weight"
    )

    pipeline = Pipeline(stages=[indexer, assembler, scaler, lr])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    total_test = predictions.count()
    correct = predictions.filter(F.col("label") == F.col("prediction")).count()
    false_negatives = predictions.filter(
        (F.col("label") == 1.0) & (F.col("prediction") == 0.0)
    ).count()

    samples = [
        row.asDict()
        for row in predictions.select("path", "status_code", "response_time_ms", "label", "prediction")
        .limit(10)
        .collect()
    ]

    return {
        "auc": round(auc, 4),
        "accuracy": round(correct / total_test, 4),
        "false_negatives": false_negatives,
        "samples": samples,
    }


def run_gbt(df: DataFrame) -> dict:
    df_labeled = prepare_classification_features(df)

    train_df, test_df = df_labeled.randomSplit([0.8, 0.2], seed=42)

    indexer = StringIndexer(inputCol="path", outputCol="path_index")

    assembler = VectorAssembler(
        inputCols=["path_index", "response_time_ms"],
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        seed=42
    )

    pipeline = Pipeline(stages=[indexer, assembler, scaler, gbt])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    total_test = predictions.count()
    correct = predictions.filter(F.col("label") == F.col("prediction")).count()
    false_negatives = predictions.filter(
        (F.col("label") == 1.0) & (F.col("prediction") == 0.0)
    ).count()

    samples = [
        row.asDict()
        for row in predictions.select("path", "status_code", "response_time_ms", "label", "prediction")
        .limit(10)
        .collect()
    ]

    return {
        "auc": round(auc, 4),
        "accuracy": round(correct / total_test, 4),
        "false_negatives": false_negatives,
        "samples": samples,
    }