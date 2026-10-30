"""Stage 4A: apply stage-2 cleaning policy to 2024-01..06 and aggregate hourly pickups.
Runs in Spark 3.5.8 master container; all paths point to shared container mount.
"""
import json
import os
from pyspark.sql import SparkSession, functions as F

BASE = '/opt/spark-data'
spark = (SparkSession.builder.appName('NYC-Taxi-Stage4-Prepare')
         .config('spark.sql.session.timeZone', 'America/New_York')
         .config('spark.sql.shuffle.partitions', '4').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
summary = {'months': [], 'cleaning_policy': 'same as stage 2 (pickup month, 1-180 min, IDs 1-263, 0.1-100 mi, 0-500 USD)', 'status': 'RUNNING'}

for month in range(1, 7):
    month_tag = f'2024-{month:02d}'
    raw_path = f'file://{BASE}/raw/yellow_tripdata_{month_tag}.parquet'
    output_path = f'file://{BASE}/ml/monthly_aggregates/m_{month_tag.replace("-", "_")}'
    print(f'\n========== PREPARING {month_tag} ==========', flush=True)
    raw = spark.read.parquet(raw_path)
    p = F.col('tpep_pickup_datetime')
    d = F.col('tpep_dropoff_datetime')
    duration = (F.unix_timestamp(d.cast('timestamp')) - F.unix_timestamp(p.cast('timestamp'))) / 60.0
    df = raw.select('tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID',
                    'DOLocationID','trip_distance','total_amount').withColumn('trip_duration_min', duration)
    start = F.to_timestamp(F.lit(f'{month_tag}-01 00:00:00'))
    if month == 6:
        next_month = '2024-07-01 00:00:00'
    else:
        next_month = f'2024-{month + 1:02d}-01 00:00:00'
    end = F.to_timestamp(F.lit(next_month))
    good = (p.isNotNull() & d.isNotNull() & (p >= start) & (p < end)
            & F.col('trip_duration_min').between(1.0, 180.0)
            & F.col('PULocationID').between(1, 263)
            & F.col('DOLocationID').between(1, 263)
            & F.col('trip_distance').between(0.1, 100.0)
            & (F.col('total_amount') > 0) & (F.col('total_amount') <= 500))
    filtered = df.where(good)
    hourly = (filtered.groupBy(F.col('PULocationID').alias('zone_id'),
                               F.date_format('tpep_pickup_datetime','yyyy-MM-dd HH').alias('hour_key'))
              .agg(F.count('*').alias('trip_count')))
    (hourly.repartition(2).write.mode('overwrite').parquet(output_path))
    # Count only after write: saved counts are small; avoid re-running scan unnecessarily.
    valid_rows = int(spark.read.parquet(output_path).agg(F.sum('trip_count').alias('n')).first()['n'])
    print(f'MONTH={month_tag} VALID_ROWS={valid_rows}', flush=True)
    if month == 1:
        with open(f'{BASE}/reports/quality_2024_01.json', 'r', encoding='utf-8') as f:
            january_expected = json.load(f)['clean_rows']
        if valid_rows != january_expected:
            raise RuntimeError(f'January does not match stage 2: {valid_rows} != {january_expected}')
    summary['months'].append({'month': month_tag, 'valid_rows': valid_rows})

summary['total_valid_rows'] = sum(item['valid_rows'] for item in summary['months'])
summary['status'] = 'SUCCESS'
with open(f'{BASE}/reports/stage4_prepare_summary.json','w',encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print('\n========== STAGE 4 PREPARE SUMMARY ==========', flush=True)
print(json.dumps(summary,ensure_ascii=False,indent=2),flush=True)
print('STAGE4_PREPARE_SUCCESS',flush=True)
spark.stop()
