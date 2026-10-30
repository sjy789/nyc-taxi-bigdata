import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 初始化 Spark
spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Stage2-Cleaning")
    .config("spark.sql.session.timeZone", "America/New_York")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

RAW_PATH = (
    "file:///opt/spark-data/raw/"
    "yellow_tripdata_2024-01.parquet"
)

CLEAN_PATH = (
    "file:///opt/spark-data/processed/"
    "yellow_2024_01_clean"
)

REPORT_PATH = (
    "/opt/spark-data/reports/"
    "quality_2024_01.json"
)

# 2. 读取原始数据
raw = spark.read.parquet(RAW_PATH)

# 3. 计算行程时长（分钟）
df = raw.withColumn(
    "trip_duration_min",
    (
        F.unix_timestamp(
            F.col("tpep_dropoff_datetime").cast("timestamp")
        )
        - F.unix_timestamp(
            F.col("tpep_pickup_datetime").cast("timestamp")
        )
    ) / 60.0
)

pickup = F.col("tpep_pickup_datetime")
dropoff = F.col("tpep_dropoff_datetime")

start_time = F.to_timestamp(
    F.lit("2024-01-01 00:00:00")
)

end_time = F.to_timestamp(
    F.lit("2024-02-01 00:00:00")
)

# 4. 定义数据质量规则
rules = {
    "time": (
        pickup.isNotNull()
        & dropoff.isNotNull()
        & (pickup >= start_time)
        & (pickup < end_time)
    ),
    "duration": (
        F.col("trip_duration_min").between(1.0, 180.0)
    ),
    "location": (
        F.col("PULocationID").between(1, 263)
        & F.col("DOLocationID").between(1, 263)
    ),
    "distance": (
        F.col("trip_distance").between(0.1, 100.0)
    ),
    "amount": (
        (F.col("total_amount") > 0)
        & (F.col("total_amount") <= 500)
    )
}

# 5. 统计原始数量和各类异常记录
quality_expressions = [
    F.count("*").alias("raw_rows")
]

for name, condition in rules.items():
    invalid = ~F.coalesce(condition, F.lit(False))
    quality_expressions.append(
        F.sum(
            F.when(invalid, 1).otherwise(0)
        ).alias("invalid_" + name)
    )

quality = df.agg(*quality_expressions).first().asDict()

raw_count = int(quality["raw_rows"])

# 6. 合并所有规则，过滤无效订单
valid_condition = F.lit(True)

for condition in rules.values():
    valid_condition = valid_condition & condition

cleaned = df.filter(valid_condition)

# 7. 增加时间分析特征
cleaned = (
    cleaned
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_weekday",
        F.dayofweek("tpep_pickup_datetime")
    )
)

# 8. 保留后续分析需要的字段
cleaned = cleaned.select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "trip_duration_min",
    "pickup_date",
    "pickup_hour",
    "pickup_weekday"
)

# 9. 计算清洗后的数据量
clean_count = cleaned.count()

assert raw_count > 0, "Raw dataset is empty"
assert clean_count > 0, "All records were filtered out"
assert clean_count <= raw_count

removed_count = raw_count - clean_count

retention_rate = round(
    clean_count / raw_count * 100, 2
)

# 10. 将清洗结果保存为 Parquet
print("\n========== SAVING CLEAN DATA ==========")

(
    cleaned
    .repartition(2)
    .write
    .mode("overwrite")
    .parquet(CLEAN_PATH)
)

# 11. 重新读取保存结果，验证行数
saved = spark.read.parquet(CLEAN_PATH)
saved_count = saved.count()

assert saved_count == clean_count, (
    "Saved data row count does not match"
)

# 12. 输出数据质量报告
report = {
    "dataset": "NYC Yellow Taxi 2024-01",
    "raw_rows": raw_count,
    "clean_rows": clean_count,
    "removed_rows": removed_count,
    "retention_rate_percent": retention_rate,
    "saved_rows_verified": saved_count,
    "invalid_counts_overlapping": {
        name: int(quality["invalid_" + name])
        for name in rules
    },
    "cleaned_columns": cleaned.columns,
    "cleaned_path": CLEAN_PATH
}

with open(REPORT_PATH, "w", encoding="utf-8") as f:
    json.dump(
        report,
        f,
        ensure_ascii=False,
        indent=2
    )

print("\n========== CLEANING REPORT ==========")
print(json.dumps(report, indent=2, ensure_ascii=False))

# 13. 展示清洗后的样本
print("\n========== CLEANED DATA SAMPLE ==========")

saved.select(
    "tpep_pickup_datetime",
    "PULocationID",
    "trip_distance",
    "trip_duration_min",
    "total_amount",
    "pickup_hour"
).show(5, truncate=False)

print("\nSTAGE2_CLEANING_SUCCESS")
print("SPARK_MASTER =", spark.sparkContext.master)

spark.stop()
