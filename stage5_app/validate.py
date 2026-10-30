"""Run inside the dashboard container. Read-only checks on real stage 3/4 outputs."""
import json
from pathlib import Path
import pandas as pd

root = Path('/data')
required = ['daily_trips','hourly_trips','weekday_trips','top_pickup_zones',
            'top_routes','distance_distribution','amount_distribution',
            'zone_hourly_trips','zone_date_hourly_trips']
for name in required:
    directory=root/'marts'/name
    paths=list(directory.glob('part-*.csv'))
    if not paths:
        raise RuntimeError(f'MISSING_STAGE3_TABLE: {name}: {directory}')
    if not (directory/'_SUCCESS').exists():
        raise RuntimeError(f'MISSING_STAGE3_SUCCESS: {name}')
for filename in ('stage3_summary.json','stage4_features_summary.json','stage4_metrics.json'):
    path=root/'reports'/filename
    if not path.exists():
        raise RuntimeError(f'MISSING_REPORT: {path}')
    content=json.loads(path.read_text(encoding='utf-8'))
    if content.get('status')!='SUCCESS':
        raise RuntimeError(f'BAD_REPORT_STATUS: {filename}')
geo=root/'marts'/'taxi_zones_normalized.geojson'
if not geo.exists():
    raise RuntimeError('MISSING_GEOJSON: run prepare_geo.py')
geodata=json.loads(geo.read_text(encoding='utf-8'))
if len(geodata.get('features',[]))<200:
    raise RuntimeError('INCOMPLETE_GEOJSON')
metrics=json.loads((root/'reports'/'stage4_metrics.json').read_text(encoding='utf-8'))
if metrics['test_rows']!=21600:
    raise RuntimeError('UNEXPECTED_TEST_ROWS')
if metrics['best_model_by_validation_mae'] not in ('rf','gbt','baseline'):
    raise RuntimeError('BAD_SELECTED_MODEL')
predict=pd.read_parquet(root/'ml'/'predictions_test',
    columns=['zone_id','hour_key','date','hour','label','pred_baseline','pred_rf','pred_gbt'])
if len(predict)!=21600 or predict['zone_id'].nunique()!=30:
    raise RuntimeError('BAD_TEST_PREDICTIONS')
if int(predict['label'].sum())!=int(metrics['test']['actual_total']):
    raise RuntimeError('TEST_TOTAL_MISMATCH')
for model in ('baseline','rf','gbt'):
    diff=predict[f'pred_{model}']-predict['label']
    mae=diff.abs().mean()
    if abs(mae-float(metrics['test']['models'][model]['MAE'])) > 0.02:
        raise RuntimeError(f'METRIC_MISMATCH: {model}')
print('STAGE5_DATA_VALIDATION_SUCCESS')
print('STAGE3_TABLES =',len(required))
print('STAGE3_JAN_ORDERS =',json.loads((root/'reports'/'stage3_summary.json').read_text())['kpi']['trip_count'])
print('STAGE4_JUNE_TEST_ROWS =',len(predict))
print('GEO_POLYGONS =',len(geodata['features']))
print('SELECTED_MODEL =',metrics['best_model_by_validation_mae'])
