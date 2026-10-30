from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 创建 SparkSession
spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Stage1-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. 生成 100 万条测试数据
df = spark.range(0, 1000000)

# 3. 创建分组字段
df = df.withColumn("bucket", F.col("id") % 10)

# 4. 注册临时 SQL 视图
df.createOrReplaceTempView("numbers")

# 5. 使用 Spark SQL 统计数据
result = spark.sql("""
    SELECT
        bucket,
        COUNT(*) AS cnt
    FROM numbers
    GROUP BY bucket
    ORDER BY bucket
""")

# 6. 显示统计结果
print("========== SPARK SQL RESULT ==========")
result.show(10, truncate=False)

# 7. 检查数据总量
print("TOTAL_ROWS =", df.count())

# 8. 检查 Spark 连接地址
print("SPARK_MASTER =", spark.sparkContext.master)

# 9. 释放资源
spark.stop()
