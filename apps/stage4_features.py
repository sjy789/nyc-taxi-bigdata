"""Stage 4B: 30 training-only popular zones, complete zone-hour grid, strictly past-only features."""
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, Window

BASE = '/opt/spark-data'
spark = (SparkSession.builder.appName('NYC-Taxi-Stage4-Features')
         .config('spark.sql.session.timeZone','America/New_York')
         .config('spark.sql.shuffle.partitions','4').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
paths = [f'file://{BASE}/ml/monthly_aggregates/m_2024_{m:02d}' for m in range(1,7)]
monthly = spark.read.parquet(*paths)
# Only Jan-Apr determine the zones; May/June are strictly validation/test periods.
train_month = monthly.where(F.col('hour_key') < '2024-05-01 00')
chosen = (train_month.groupBy('zone_id').agg(F.sum('trip_count').alias('train_orders'))
          .orderBy(F.desc('train_orders'),F.asc('zone_id')).limit(30).collect())
zone_ids = [int(row['zone_id']) for row in chosen]
assert len(zone_ids) == 30 and len(set(zone_ids)) == 30
print('TOP30_ZONE_IDS =', zone_ids, flush=True)
zones_df = spark.createDataFrame([(z,) for z in zone_ids], ['zone_id'])
start = datetime(2024,1,1)
end = datetime(2024,7,1)
rows = []
i = 0
cur = start
while cur < end:
    rows.append((cur.strftime('%Y-%m-%d %H'), cur.strftime('%Y-%m-%d'),
                 int(cur.hour), int(cur.weekday()), int(cur.month),
                 int(cur.weekday() >= 5), i))
    cur += timedelta(hours=1)
    i += 1
hour_df = spark.createDataFrame(rows, ['hour_key','date','hour','day_of_week','month','is_weekend','time_index'])
grid = zones_df.crossJoin(F.broadcast(hour_df))
agg = (monthly.where(F.col('zone_id').isin(zone_ids))
       .groupBy('zone_id','hour_key').agg(F.sum('trip_count').alias('target')))
joined = grid.join(agg, ['zone_id','hour_key'], 'left').fillna({'target':0})
w = Window.partitionBy('zone_id').orderBy('time_index')
features = (joined
    .withColumn('lag_1',F.lag('target',1).over(w))
    .withColumn('lag_24',F.lag('target',24).over(w))
    .withColumn('lag_168',F.lag('target',168).over(w))
    .withColumn('rolling_mean_24',F.avg('target').over(w.rowsBetween(-24,-1)))
    .withColumn('rolling_mean_168',F.avg('target').over(w.rowsBetween(-168,-1)))
    .where(F.col('lag_168').isNotNull())
    .withColumn('zone_id_str',F.col('zone_id').cast('string'))
    .select('zone_id','zone_id_str','hour_key','date','hour','day_of_week','month',
            'is_weekend','time_index','lag_1','lag_24','lag_168',
            'rolling_mean_24','rolling_mean_168',F.col('target').cast('double').alias('label')))
# Persist and verify counts. Each hour from Jan 8 onwards for each zone appears once.
out = f'file://{BASE}/ml/features_2024_h1'
(features.repartition(2).write.mode('overwrite').parquet(out))
verified = spark.read.parquet(out)
split_counts = {r['split']:int(r['count']) for r in
    verified.withColumn('split',F.when(F.col('date') < '2024-05-01','train')
        .when(F.col('date') < '2024-06-01','validation').otherwise('test'))
    .groupBy('split').count().collect()}
expected = {'train':82080, 'validation':22320, 'test':21600}
if split_counts != expected:
    raise RuntimeError(f'Feature split row counts unexpected {split_counts}; expected {expected}')
meta = {'status':'SUCCESS','zones':zone_ids,'zone_count':30,'hour_count_before_lag':i,
        'counts':split_counts,'split':{'train':'2024-01-08 .. 2024-04-30',
        'validation':'2024-05-01 .. 2024-05-31','test':'2024-06-01 .. 2024-06-30'},
        'prediction_definition':'one-hour ahead given previously observed hourly order counts',
        'feature_lags_hours':[1,24,168], 'zero_order_hours':'included'}
with open(f'{BASE}/reports/stage4_features_summary.json','w',encoding='utf-8') as f:
    json.dump(meta,f,ensure_ascii=False,indent=2)
print('\n========== STAGE 4 FEATURE SUMMARY ==========',flush=True)
print(json.dumps(meta,indent=2,ensure_ascii=False),flush=True)
print('STAGE4_FEATURES_SUCCESS',flush=True)
spark.stop()
