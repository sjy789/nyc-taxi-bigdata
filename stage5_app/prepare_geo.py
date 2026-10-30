"""Check and normalize official NYC Taxi Zones GeoJSON; no third-party libraries."""
import json
from pathlib import Path
import os

root = Path(os.environ.get('NYC_TAXI_DATA_DIR', str(Path.home()/'nyc-taxi-bigdata'/'data')))
source = root/'raw'/'taxi_zones.geojson'
target = root/'marts'/'taxi_zones_normalized.geojson'
if not source.exists():
    raise SystemExit('GEO_ERROR: missing /data/raw/taxi_zones.geojson; download official NYC dataset first')
try:
    data = json.loads(source.read_text(encoding='utf-8'))
except Exception as exc:
    raise SystemExit(f'GEO_ERROR: downloaded file is not valid JSON: {exc}')
if not isinstance(data, dict) or data.get('type') != 'FeatureCollection':
    raise SystemExit('GEO_ERROR: expected a GeoJSON FeatureCollection')
features = []
for item in data.get('features', []):
    props = item.get('properties') or {}
    lower = {str(k).lower().replace('_', ''): v for k, v in props.items()}
    identifier = lower.get('locationid') or lower.get('zoneid')
    if identifier is None:
        continue
    try:
        zone_id = int(float(identifier))
    except (ValueError, TypeError):
        continue
    geometry = item.get('geometry') or {}
    if geometry.get('type') not in ('Polygon', 'MultiPolygon'):
        continue
    features.append({'type':'Feature','geometry':geometry,'properties':{
        'zone_id':zone_id,
        'zone':str(lower.get('zone') or f'Zone {zone_id}'),
        'borough':str(lower.get('borough') or 'Unknown')
    }})
if len(features) < 200 or len({f['properties']['zone_id'] for f in features}) < 200:
    raise SystemExit(f'GEO_ERROR: fewer than 200 valid polygons/zones detected (features={len(features)})')
# Official Socrata GeoJSON is WGS84 longitude/latitude. Save unchanged geometry.
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text(json.dumps({'type':'FeatureCollection','features':features}, separators=(',',':'), ensure_ascii=False), encoding='utf-8')
print('STAGE5_GEO_SUCCESS')
print('POLYGON_COUNT =', len(features))
print('ZONE_COUNT =',len({f['properties']['zone_id'] for f in features}))
print('OUTPUT =',target)
