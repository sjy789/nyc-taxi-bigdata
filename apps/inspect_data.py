from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 创建 SparkSession
spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Stage2-Inspection")
    .config("spark.sql.session.timeZone", "America/New_York")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. 读取出租车原始数据
data_path = (
    "file:///opt/spark-data/raw/"
    "yellow_tripdata_2024-01.parquet"
)

df = spark.read.parquet(data_path)

# 3. 查看数据结构
print("\n========== DATA SCHEMA ==========")
df.printSchema()

# 4. 统计原始记录数量
print("\n========== DATA COUNT ==========")
print("RAW_ROWS =", df.count())
print("COLUMN_COUNT =", len(df.columns))

# 5. 查看部分原始数据
print("\n========== SAMPLE DATA ==========")
df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount"
).show(5, truncate=False)

# 6. 统计关键字段的空值数量
print("\n========== NULL VALUE CHECK ==========")

important_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount",
    "passenger_count"
]

null_expressions = [
    F.sum(
        F.when(F.col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in important_columns
]

df.agg(*null_expressions).show(truncate=False)

# 7. 显示当前 Spark Master
print("\nSPARK_MASTER =", spark.sparkContext.master)

spark.stop()
