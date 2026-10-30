NYC TAXI STAGE 4 (Spark 3.5.8, single Master / single Worker)

Files:
  download_stage4.sh - downloads Feb..Jun 2024 TLC yellow taxi files to data/raw
  stage4_prepare.py  - cleans each of six months using stage-2 rules; outputs hourly counts
  stage4_features.py - selects top 30 areas using train months; creates lagged hourly panel
  stage4_train.py    - trains Random Forest and GBT, compares weekly baseline, exports metrics
  stage4_report.py   - writes a standalone HTML and SVG charts (run on Ubuntu host)

Run spark-submit with the same options from prior stages. Both containers must mount ./data
at /opt/spark-data, and the Master must mount ./apps at /opt/spark-apps.

WARNING: Do not train stage 4 from January only. All six Parquet files are required.
This project models one-step-ahead hourly orders conditional on recently observed hours.

This code has passed Python syntax/static checks but has not been executed on the
user's Spark deployment. Verify each stage's actual output and report any error.
