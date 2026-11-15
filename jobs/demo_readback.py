#!/usr/bin/env python3
"""Read-only live demonstration of the verified DE 2019 Parquet dataset."""
from pyspark.sql import SparkSession, functions as F


def main():
    spark = (SparkSession.builder.appName("OPSD_ReadOnly_Demo").master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        path = "hdfs://namenode:8020/bigdata/opsd/processed/de_2019"
        print("READ-ONLY DEMO: PARQUET ON HDFS")
        print("Source:", path)
        df = spark.read.parquet(path)
        df.printSchema()
        df.orderBy("timestamp_utc").show(5, truncate=False)
        metrics = df.agg(F.count("*").alias("rows"),
                         F.min("timestamp_utc").alias("min_utc"),
                         F.max("timestamp_utc").alias("max_utc"),
                         F.min("load_mw").alias("min_mw"),
                         F.max("load_mw").alias("max_mw"),
                         F.round(F.avg("load_mw"), 2).alias("avg_mw"))
        metrics.show(truncate=False)
        actual = metrics.first()
        if actual.rows != 8760 or actual.min_mw != 33295.0 or actual.max_mw != 76311.0:
            raise ValueError("Readback does not match the verified DE 2019 snapshot")
        print("READBACK CHECK PASSED. For exact source reconciliation run validate_processed.py.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
