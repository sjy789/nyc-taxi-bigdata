"""Stage 6: repeatable Spark SQL large-data benchmarks.

Run on the existing standalone Spark master container. This script never changes
source parquet or stage 2-5 files. All output is in data/reports/stage6_*.json/csv.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

DATA_ROOT = "/opt/spark-data"
REPORT_ROOT = f"{DATA_ROOT}/reports"
MONTH_VALID_ROWS = [2831806, 2868609, 3408658, 3382231, 3578910, 3390903]


def parse_args():
    parser = argparse.ArgumentParser(description="NYC Taxi Stage 6 Spark SQL benchmarks")
    parser.add_argument("--mode", choices=("scale", "partition"), required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--scale-partitions", type=int, default=4)
    args = parser.parse_args()
    if not 2 <= args.repeats <= 10:
        parser.error("--repeats must be 2..10")
    if not 1 <= args.scale_partitions <= 32:
        parser.error("--scale-partitions must be 1..32")
    return args


def input_frame(spark, months):
    frames = []
    for month in range(1, months + 1):
        tag = f"2024-{month:02d}"
        input_path = f"file://{DATA_ROOT}/raw/yellow_tripdata_{tag}.parquet"
        raw = spark.read.parquet(input_path).select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
            "DOLocationID", "trip_distance", "total_amount"
        )
        pickup = F.col("tpep_pickup_datetime")
        dropoff = F.col("tpep_dropoff_datetime")
        duration = (
            F.unix_timestamp(dropoff.cast("timestamp"))
            - F.unix_timestamp(pickup.cast("timestamp"))
        ) / F.lit(60.0)
        next_tag = f"2024-{month + 1:02d}-01 00:00:00"
        good = (
            pickup.isNotNull() & dropoff.isNotNull()
            & (pickup >= F.to_timestamp(F.lit(f"{tag}-01 00:00:00")))
            & (pickup < F.to_timestamp(F.lit(next_tag)))
            & duration.between(1.0, 180.0)
            & F.col("PULocationID").between(1, 263)
            & F.col("DOLocationID").between(1, 263)
            & F.col("trip_distance").between(0.1, 100.0)
            & (F.col("total_amount") > 0)
            & (F.col("total_amount") <= 500)
        )
        frames.append(
            raw.where(good).select(
                "PULocationID",
                F.hour("tpep_pickup_datetime").alias("pickup_hour"),
                "trip_distance", "total_amount"
            )
        )
    output = frames[0]
    for next_frame in frames[1:]:
        output = output.unionByName(next_frame)
    return output


def run_one(spark, months, partitions, expected):
    # Includes Spark planning, source scans, clean filter, repartition/shuffle,
    # aggregation and collect; excludes saving results and graph generation.
    start = time.perf_counter()
    df = input_frame(spark, months)
    result = (df.repartition(partitions)
              .groupBy("PULocationID", "pickup_hour")
              .agg(F.count("*").alias("n"),
                   F.sum("trip_distance").alias("distance_sum"),
                   F.sum("total_amount").alias("amount_sum"))
              .collect())
    elapsed = round(time.perf_counter() - start, 3)
    count = sum(int(row["n"]) for row in result)
    if count != expected:
        raise RuntimeError(
            f"ROWS_CHECK_FAILED: months={months}, partitions={partitions}, "
            f"actual={count}, expected={expected}; do not publish benchmark."
        )
    return {"seconds": elapsed, "rows": count,
            "group_count": len(result),
            "rows_per_second": round(count / max(elapsed, .0001), 1)}


def atomic_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def write_csv(path, obj):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["experiment", "months", "partitions", "rows",
                         "input_mib", "repeats", "median_seconds", "min_seconds",
                         "max_seconds", "median_rows_per_second"])
        for item in obj["cases"]:
            writer.writerow([
                obj["mode"], item["months"], item["partitions"],
                item["rows_expected"], item["input_mib"], len(item["runs"]),
                item["median_seconds"], item["min_seconds"],
                item["max_seconds"], item["median_rows_per_second"]
            ])


def main():
    args = parse_args()
    spark = (SparkSession.builder
             .appName(f"NYC-Taxi-Stage6-{args.mode.title()}-Benchmark")
             .config("spark.sql.session.timeZone", "America/New_York")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.speculation", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    if spark.sparkContext.master != "spark://spark-master:7077":
        raise RuntimeError(f"Not running on expected Spark cluster: {spark.sparkContext.master}")
    if args.mode == "scale":
        cases = [(1, args.scale_partitions), (3, args.scale_partitions),
                 (6, args.scale_partitions)]
    else:
        cases = [(1, 2), (1, 4), (1, 8)]
    os.makedirs(REPORT_ROOT, exist_ok=True)
    output = {
        "status": "RUNNING", "mode": args.mode, "spark_version": spark.version,
        "spark_master": spark.sparkContext.master,
        "machine_note": "Single Ubuntu VM, Spark standalone, one worker (2 cores / 2 GiB advertised)",
        "measurement_note": (
            "Elapsed wall time for reading original monthly Parquet, applying stage-4 "
            "cleaning, repartitioning, grouping by pickup zone and hour, and collecting "
            "small aggregate; includes I/O and scheduling; all runs recompute from disk."
        ),
        "repeats": args.repeats, "aqe": False,
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "cases": []
    }
    try:
        for case_idx, (months, partitions) in enumerate(cases, 1):
            expected = sum(MONTH_VALID_ROWS[:months])
            total_size = sum(os.stat(
                f"{DATA_ROOT}/raw/yellow_tripdata_2024-{m:02d}.parquet"
            ).st_size for m in range(1, months + 1))
            info = {"months": months, "partitions": partitions,
                    "input_mib": round(total_size / 1048576, 2),
                    "rows_expected": expected, "runs": []}
            print(f"\n========== {args.mode.upper()} CASE {case_idx}/{len(cases)}: "
                  f"MONTHS={months}, PARTITIONS={partitions}, "
                  f"EXPECTED_ROWS={expected} ==========", flush=True)
            for run_idx in range(1, args.repeats + 1):
                print(f"RUN {run_idx}/{args.repeats} START", flush=True)
                stats = run_one(spark, months, partitions, expected)
                info["runs"].append(stats)
                print(f"RUN {run_idx}/{args.repeats}: SECONDS={stats['seconds']} "
                      f"ROWS={stats['rows']} THROUGHPUT_ROWS_PER_SEC="
                      f"{stats['rows_per_second']}", flush=True)
            durations = [v["seconds"] for v in info["runs"]]
            info["median_seconds"] = round(statistics.median(durations), 3)
            info["min_seconds"] = min(durations)
            info["max_seconds"] = max(durations)
            info["median_rows_per_second"] = round(
                expected / max(info["median_seconds"], .0001), 1)
            output["cases"].append(info)
            print(f"CASE_COMPLETED: MONTHS={months} PARTITIONS={partitions} "
                  f"MEDIAN_SECONDS={info['median_seconds']}", flush=True)
        output["status"] = "SUCCESS"
        out_path = f"{REPORT_ROOT}/stage6_{args.mode}.json"
        csv_path = f"{REPORT_ROOT}/stage6_{args.mode}.csv"
        atomic_json(out_path, output)
        write_csv(csv_path, output)
        print("\n========== STAGE 6 BENCHMARK SUMMARY ==========", flush=True)
        for item in output["cases"]:
            print(f"MONTHS={item['months']} PARTITIONS={item['partitions']} "
                  f"ROWS={item['rows_expected']} "
                  f"MEDIAN_SECONDS={item['median_seconds']} "
                  f"ROWS_PER_SECOND={item['median_rows_per_second']}", flush=True)
        print(f"STAGE6_{args.mode.upper()}_SUCCESS", flush=True)
        print(f"OUTPUT_JSON = {out_path}", flush=True)
        print(f"OUTPUT_CSV = {csv_path}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
