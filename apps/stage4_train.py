"""Stage 4C: fit a historical baseline, Spark Random Forest, Spark GBT; time-respecting evaluation."""
import csv
import json
import os
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor

BASE='/opt/spark-data'
MODEL_DIR=f'{BASE}/ml/models'
EXPORT_DIR=f'{BASE}/ml/exports'
os.makedirs(EXPORT_DIR,exist_ok=True)
spark=(SparkSession.builder.appName('NYC-Taxi-Stage4-MLlib-Training')
       .config('spark.sql.session.timeZone','America/New_York')
       .config('spark.sql.shuffle.partitions','4').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
source=spark.read.parquet(f'file://{BASE}/ml/features_2024_h1')
train_raw=source.where(F.col('date')<'2024-05-01')
idx_model=StringIndexer(inputCol='zone_id_str',outputCol='zone_index',handleInvalid='error').fit(train_raw)
feature_cols=['zone_index','hour','day_of_week','month','is_weekend',
              'lag_1','lag_24','lag_168','rolling_mean_24','rolling_mean_168']
assembler=VectorAssembler(inputCols=feature_cols,outputCol='features')
all_data=assembler.transform(idx_model.transform(source))
train=all_data.where(F.col('date')<'2024-05-01')
validation=all_data.where((F.col('date')>='2024-05-01') & (F.col('date')<'2024-06-01'))
test=all_data.where(F.col('date')>='2024-06-01')

estimators={
 'rf':RandomForestRegressor(featuresCol='features',labelCol='label',predictionCol='prediction',
       numTrees=20,maxDepth=6,maxBins=64,minInstancesPerNode=10,maxMemoryInMB=64,
       subsamplingRate=0.8,seed=42),
 'gbt':GBTRegressor(featuresCol='features',labelCol='label',predictionCol='prediction',
       maxIter=12,maxDepth=4,maxBins=64,maxMemoryInMB=64,
       minInstancesPerNode=20,stepSize=0.15,seed=42)
}
models={}
times={}
for name, estimator in estimators.items():
    print(f'\n========== TRAINING {name.upper()} ==========',flush=True)
    start=time.perf_counter()
    models[name]=estimator.fit(train)
    times[name]=round(time.perf_counter()-start,2)
    models[name].write().overwrite().save(f'file://{MODEL_DIR}/{name}')
    print(f'TRAINED {name.upper()} SECONDS={times[name]}',flush=True)
idx_model.write().overwrite().save(f'file://{MODEL_DIR}/zone_indexer')

keys=['zone_id','hour_key']
def make_predictions(part):
    base=part.select('zone_id','hour_key','date','hour','label',
                     F.col('lag_168').alias('pred_baseline'))
    rf=(models['rf'].transform(part).select(*keys,F.greatest(F.lit(0.0),F.col('prediction')).alias('pred_rf')))
    gbt=(models['gbt'].transform(part).select(*keys,F.greatest(F.lit(0.0),F.col('prediction')).alias('pred_gbt')))
    return base.join(rf,keys).join(gbt,keys)

print('\n========== WRITING VALIDATION PREDICTIONS ==========',flush=True)
(make_predictions(validation).repartition(2).write.mode('overwrite')
 .parquet(f'file://{BASE}/ml/predictions_validation'))
print('\n========== WRITING TEST PREDICTIONS ==========',flush=True)
(make_predictions(test).repartition(2).write.mode('overwrite')
 .parquet(f'file://{BASE}/ml/predictions_test'))

v=spark.read.parquet(f'file://{BASE}/ml/predictions_validation')
t=spark.read.parquet(f'file://{BASE}/ml/predictions_test')

def metrics(df):
    names=['baseline','rf','gbt']
    expr=[F.count('*').alias('n'),F.sum('label').alias('actual_total')]
    for name in names:
        diff=F.col(f'pred_{name}')-F.col('label')
        expr.extend([F.avg(F.abs(diff)).alias(name+'_mae'),
                     F.sqrt(F.avg(F.pow(diff,2.0))).alias(name+'_rmse'),
                     F.sum(F.abs(diff)).alias(name+'_total_absolute_error')])
    d=df.agg(*expr).first().asDict()
    if d['actual_total'] <= 0:
        raise RuntimeError('No real trips found in evaluation period')
    result={}
    for name in names:
        result[name]={'MAE':round(float(d[name+'_mae']),3),
                      'RMSE':round(float(d[name+'_rmse']),3),
                      'WAPE_percent':round(100*float(d[name+'_total_absolute_error'])/float(d['actual_total']),3)}
    return {'rows':int(d['n']),'actual_total':int(d['actual_total']),'models':result}

val_metrics=metrics(v)
test_metrics=metrics(t)
best=min(['baseline','rf','gbt'],key=lambda n:val_metrics['models'][n]['MAE'])

print('\n========== VALIDATION METRICS (MAY) ==========',flush=True)
print(json.dumps(val_metrics,indent=2),flush=True)
print('\n========== TEST METRICS (JUNE) ==========',flush=True)
print(json.dumps(test_metrics,indent=2),flush=True)
print(f'BEST_MODEL_SELECTED_ON_MAY = {best}',flush=True)

# Export tiny CSV aggregates for an offline HTML report, not raw trip data.
cols=['date','actual','pred_baseline','pred_rf','pred_gbt']
daily=(t.groupBy('date').agg(F.sum('label').alias('actual'),
        F.sum('pred_baseline').alias('pred_baseline'),F.sum('pred_rf').alias('pred_rf'),
        F.sum('pred_gbt').alias('pred_gbt')).orderBy('date').collect())
with open(f'{EXPORT_DIR}/daily_june.csv','w',newline='',encoding='utf-8') as f:
    w=csv.DictWriter(f,fieldnames=cols)
    w.writeheader()
    for row in daily:
        w.writerow({c:row[c] for c in cols})

with open(f'{BASE}/reports/stage4_features_summary.json','r',encoding='utf-8') as f:
    feature_meta=json.load(f)
example_zone=int(feature_meta['zones'][0])
example=(t.where((F.col('zone_id')==example_zone) &
        (F.col('date')>='2024-06-10') & (F.col('date')<='2024-06-16'))
        .orderBy('hour_key').collect())
with open(f'{EXPORT_DIR}/sample_zone_week.csv','w',newline='',encoding='utf-8') as f:
    w=csv.DictWriter(f,fieldnames=['hour_key','zone_id','actual','pred_baseline','pred_rf','pred_gbt'])
    w.writeheader()
    for row in example:
        w.writerow({'hour_key':row['hour_key'],'zone_id':row['zone_id'],
                    'actual':row['label'], 'pred_baseline':row['pred_baseline'],
                    'pred_rf':row['pred_rf'],'pred_gbt':row['pred_gbt']})
summary={'status':'SUCCESS','train_rows':82080,'validation_rows':val_metrics['rows'],
         'test_rows':test_metrics['rows'],'best_model_by_validation_mae':best,
         'training_seconds':times,'validation':val_metrics,'test':test_metrics,
         'example_zone_id':example_zone,'models':['baseline','rf','gbt'],
         'interpretation':'June test uses only older observed hours as lag features, one-hour-ahead setting; model chosen on May only',
         'parameters':{'rf':{'numTrees':20,'maxDepth':6},'gbt':{'maxIter':12,'maxDepth':4}}}
if summary['validation_rows']!=22320 or summary['test_rows']!=21600 or len(daily)!=30 or len(example)!=168:
    raise RuntimeError('Evaluation export row counts incorrect')
with open(f'{BASE}/reports/stage4_metrics.json','w',encoding='utf-8') as f:
    json.dump(summary,f,indent=2,ensure_ascii=False)
print('\n========== STAGE 4 SUMMARY ==========',flush=True)
print(json.dumps(summary,indent=2,ensure_ascii=False),flush=True)
print('STAGE4_TRAIN_SUCCESS',flush=True)
print('SPARK_MASTER =',spark.sparkContext.master,flush=True)
spark.stop()
