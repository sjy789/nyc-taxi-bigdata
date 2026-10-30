import json
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = "/opt/spark-data"

spark = (SparkSession.builder
    .appName("NYC-Taxi-Stage3-SQL-Analytics")
    .config("spark.sql.session.timeZone", "America/New_York")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

trips = spark.read.parquet(f"file://{ROOT}/processed/yellow_2024_01_clean")
trips.createOrReplaceTempView("trips")
zones = (spark.read.option("header", "true")
         .option("inferSchema", "true")
         .csv(f"file://{ROOT}/raw/taxi_zone_lookup.csv"))
zones.createOrReplaceTempView("zones")

kpi = spark.sql("""
SELECT COUNT(*) AS trip_count,
       COUNT(DISTINCT PULocationID) AS pickup_zone_count,
       ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
       ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
       ROUND(AVG(total_amount), 2) AS avg_order_amount_usd,
       ROUND(SUM(total_amount), 2) AS total_order_amount_usd
FROM trips
""").first().asDict()

report_file = Path(f"{ROOT}/reports/quality_2024_01.json")
with report_file.open(encoding="utf-8") as f:
    stage2 = json.load(f)
assert kpi["trip_count"] == stage2["clean_rows"], "Stage 2 data count mismatch"

queries = {
"daily_trips": """
    SELECT CAST(pickup_date AS STRING) AS date,
           COUNT(*) AS trip_count,
           ROUND(AVG(total_amount),2) AS avg_amount_usd
    FROM trips GROUP BY pickup_date ORDER BY pickup_date
""",
"hourly_trips": """
    SELECT pickup_hour AS hour, COUNT(*) AS trip_count
    FROM trips GROUP BY pickup_hour ORDER BY pickup_hour
""",
"weekday_trips": """
    SELECT pickup_weekday AS weekday, COUNT(*) AS trip_count
    FROM trips GROUP BY pickup_weekday ORDER BY pickup_weekday
""",
"top_pickup_zones": """
    SELECT t.PULocationID AS location_id,
           COALESCE(z.Borough, 'Unknown') AS borough,
           COALESCE(z.Zone, 'Unknown') AS zone,
           COUNT(*) AS trip_count
    FROM trips t LEFT JOIN zones z ON t.PULocationID=z.LocationID
    GROUP BY t.PULocationID,z.Borough,z.Zone
    ORDER BY trip_count DESC LIMIT 15
""",
"top_routes": """
    SELECT t.PULocationID AS pickup_id,
           COALESCE(p.Zone, 'Unknown') AS pickup_zone,
           t.DOLocationID AS dropoff_id,
           COALESCE(d.Zone, 'Unknown') AS dropoff_zone,
           COUNT(*) AS trip_count
    FROM trips t
    LEFT JOIN zones p ON t.PULocationID=p.LocationID
    LEFT JOIN zones d ON t.DOLocationID=d.LocationID
    GROUP BY t.PULocationID,p.Zone,t.DOLocationID,d.Zone
    ORDER BY trip_count DESC LIMIT 20
""",
"distance_distribution": """
    WITH binned AS (
      SELECT CASE
       WHEN trip_distance < 1 THEN '0.1-1'
       WHEN trip_distance < 3 THEN '1-3'
       WHEN trip_distance < 5 THEN '3-5'
       WHEN trip_distance < 10 THEN '5-10'
       WHEN trip_distance < 20 THEN '10-20'
       WHEN trip_distance < 50 THEN '20-50'
       ELSE '50-100' END AS distance_bin,
       CASE
       WHEN trip_distance < 1 THEN 1 WHEN trip_distance < 3 THEN 2
       WHEN trip_distance < 5 THEN 3 WHEN trip_distance < 10 THEN 4
       WHEN trip_distance < 20 THEN 5 WHEN trip_distance < 50 THEN 6
       ELSE 7 END AS sort_id
      FROM trips
    )
    SELECT distance_bin, COUNT(*) AS trip_count
    FROM binned GROUP BY distance_bin,sort_id ORDER BY sort_id
""",
"amount_distribution": """
    WITH binned AS (
      SELECT CASE
       WHEN total_amount < 10 THEN '0-10'
       WHEN total_amount < 20 THEN '10-20'
       WHEN total_amount < 30 THEN '20-30'
       WHEN total_amount < 50 THEN '30-50'
       WHEN total_amount < 100 THEN '50-100'
       WHEN total_amount < 200 THEN '100-200'
       ELSE '200-500' END AS amount_bin,
       CASE
       WHEN total_amount < 10 THEN 1 WHEN total_amount < 20 THEN 2
       WHEN total_amount < 30 THEN 3 WHEN total_amount < 50 THEN 4
       WHEN total_amount < 100 THEN 5 WHEN total_amount < 200 THEN 6
       ELSE 7 END AS sort_id
      FROM trips
    )
    SELECT amount_bin, COUNT(*) AS trip_count
    FROM binned GROUP BY amount_bin,sort_id ORDER BY sort_id
""",
"zone_hourly_trips": """
    SELECT PULocationID AS location_id, pickup_hour AS hour,
           COUNT(*) AS trip_count
    FROM trips GROUP BY PULocationID,pickup_hour
    ORDER BY location_id,hour
""",
"zone_date_hourly_trips": """
    SELECT CAST(pickup_date AS STRING) AS date,
           pickup_hour AS hour, PULocationID AS location_id,
           COUNT(*) AS trip_count
    FROM trips GROUP BY pickup_date,pickup_hour,PULocationID
    ORDER BY date,hour,location_id
"""
}

for name, sql in queries.items():
    frame = spark.sql(sql)
    frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        f"file://{ROOT}/marts/{name}")
    print(f"\n========== {name.upper()} ==========")
    frame.show(5, truncate=False)

# Core counts must remain consistent for complete distributions.
for name in ("daily_trips", "hourly_trips", "weekday_trips", "distance_distribution", "amount_distribution"):
    df = (spark.read.option("header", "true")
          .option("inferSchema", "true")
          .csv(f"file://{ROOT}/marts/{name}"))
    actual = sum(int(row["trip_count"]) for row in df.select("trip_count").collect())
    assert actual == kpi["trip_count"], f"{name}: {actual} != {kpi['trip_count']}"

summary = {
    "month": "2024-01",
    "kpi": kpi,
    "output_tables": list(queries),
    "checked_total": kpi["trip_count"],
    "status": "SUCCESS"
}
with open(f"{ROOT}/reports/stage3_summary.json", "w", encoding="utf-8") as file:
    json.dump(summary, file, ensure_ascii=False, indent=2)
print("\n========== STAGE 3 SUMMARY ==========")
print(json.dumps(summary, ensure_ascii=False, indent=2))
print("\nSTAGE3_ANALYSIS_SUCCESS")
print("SPARK_MASTER =", spark.sparkContext.master)
spark.stop()
