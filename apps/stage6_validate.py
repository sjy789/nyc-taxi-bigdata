"""Stage 6 output checker, designed to run on Ubuntu host (stdlib only)."""
from __future__ import annotations

import csv
import json
from pathlib import Path

BASE=Path.home()/'nyc-taxi-bigdata'/'data'/'reports'
EXPECTED=[2831806,9109073,19461117]

def check(name):
    jsonfile=BASE/f'stage6_{name}.json'
    csvfile=BASE/f'stage6_{name}.csv'
    obj=json.loads(jsonfile.read_text(encoding='utf-8'))
    assert obj['mode']==name and obj['status']=='SUCCESS'
    assert obj['spark_master']=='spark://spark-master:7077'
    assert len(obj['cases'])==3 and obj['repeats']>=2
    if name=='scale':
        assert [c['months'] for c in obj['cases']]==[1,3,6]
        assert [c['rows_expected'] for c in obj['cases']]==EXPECTED
        assert len(set(c['partitions'] for c in obj['cases']))==1
    else:
        assert [c['months'] for c in obj['cases']]==[1]*3
        assert [c['partitions'] for c in obj['cases']]==[2,4,8]
        assert [c['rows_expected'] for c in obj['cases']]==[EXPECTED[0]]*3
    for case in obj['cases']:
        assert len(case['runs'])==obj['repeats']
        assert all(r['rows']==case['rows_expected'] and r['seconds']>0 for r in case['runs'])
        assert case['median_seconds']>0 and case['median_rows_per_second']>0
    with open(csvfile,encoding='utf-8-sig',newline='') as f:
        assert len(list(csv.DictReader(f)))==3
    print(f'STAGE6_{name.upper()}_VALIDATED: 3 cases, {obj["repeats"]} trials per case')


def main():
    check('scale')
    check('partition')
    assert (BASE/'stage6_dashboard.html').stat().st_size>5000
    for kind in ('scale','partition','throughput'):
        svg=BASE/f'stage6_{kind}.svg'
        assert svg.stat().st_size>500
        assert '<svg' in svg.read_text(encoding='utf-8')
    print('STAGE6_VALIDATION_SUCCESS')

if __name__=='__main__':
    main()
