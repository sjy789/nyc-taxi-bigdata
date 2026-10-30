NYC Taxi Stage 6 — Spark SQL distributed performance experiment

Deploy files to the EXISTING project apps/ folder. No Dockerfile or Compose edits.

Environment: existing Spark 3.5.8 standalone Master / one Worker and mounted ./data.
Source: 2024-01..06 NYC Yellow Taxi Parquet in data/raw. All stage 2-5 results untouched.
Workload: read original Parquet; filter with same cleaning policy as Stage4;
repartition N; group by pickup zone and hour; count, distance and amount sums;
collect small result; verify row sum against stage-4 monthly counts.

Run mode A: scale 1/3/6 months at fixed 4 shuffle input partitions, 3 repeats.
Run mode B: partition 2/4/8 partitions on January, 3 repeats.
Measurements include scan, filtering, repartition, aggregation, collect and
scheduling/IO costs (wall time); no caching. Reports use medians.

Use Spark submit command shown in ChatGPT stage6 guide; two independent invocations:
  -- /opt/spark-apps/stage6_benchmark.py --mode scale --repeats 3
  -- /opt/spark-apps/stage6_benchmark.py --mode partition --repeats 3

After both finish, on the Ubuntu host:
  python3 apps/stage6_report.py
  python3 apps/stage6_validate.py

Output in data/reports/:
 stage6_scale.json/.csv, stage6_partition.json/.csv,
 stage6_scale.svg, stage6_partition.svg, stage6_throughput.svg,
 stage6_dashboard.html

No invented speedup: all timings come from actual experiments. Results may be
nonmonotonic and are specific to the user's single physical host / VM.

Tested locally for Python syntax and stdlib report/validation on fixture JSON;
Spark executor runtime must be confirmed on the actual Ubuntu environment.
