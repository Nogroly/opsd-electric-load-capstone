#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jobs/etl.py
===========
PySpark Batch ETL: Trích xuất và chuẩn hóa phụ tải thực tế CHLB Đức năm 2019.

Yêu cầu nghiêm ngặt:
  1. Phân loại nguồn thành ba nhóm MECE (không chồng lấn, tổng bằng tổng dòng nguồn):
     - Nhóm 1: Timestamp trống hoặc không parse được (timestamp_invalid_or_empty).
     - Nhóm 2: Timestamp hợp lệ ngoài năm 2019 (timestamp_valid_out_of_scope).
     - Nhóm 3: Timestamp hợp lệ trong năm 2019 (timestamp_valid_in_scope).
     Dừng và báo lỗi nếu có timestamp nguồn không xác định được (Nhóm 1 > 0).
     Không gọi timestamp lỗi là ngoài phạm vi.
  2. Kiểm tra phụ tải load_mw trước khi ghi HDFS: không null, không NaN,
     không +Infinity/-Infinity và phải lớn hơn 0 (> 0).
  3. Giới hạn overwrite: Chỉ cho phép đích duy nhất:
     hdfs://namenode:8020/bigdata/opsd/processed/de_2019
     Từ chối mọi đường dẫn khác trước khi ghi.
  4. Schema đầu ra:
     - timestamp_utc (TimestampType)
     - country = "DE" (StringType)
     - load_mw (DoubleType)
     - year (IntegerType)
     - month (IntegerType)
     - source_file (StringType)
  5. Xuất etl_summary.json ghi nhận số dòng chuẩn bị ghi và metadata thực thi.
  6. Ghi Parquet nén Snappy, partition theo year.
"""

import os
import sys
import json
import argparse
import hashlib
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

ALLOWED_TARGET_HDFS = "hdfs://namenode:8020/bigdata/opsd/processed/de_2019"


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="PySpark Batch ETL - OPSD DE 2019 Load")
    parser.add_argument(
        "--input",
        type=str,
        default="/workspace/data/raw/time_series_60min_singleindex.csv",
        help="Đường dẫn file CSV nguồn",
    )
    parser.add_argument(
        "--output-hdfs",
        type=str,
        default=ALLOWED_TARGET_HDFS,
        help=f"Đường dẫn đích lưu Parquet trên HDFS (chỉ chấp nhận {ALLOWED_TARGET_HDFS})",
    )
    parser.add_argument(
        "--summary-out",
        type=str,
        default="/workspace/outputs/quality/etl_summary.json",
        help="Đường dẫn xuất file tóm tắt ETL",
    )
    return parser.parse_args(argv)


def validate_target_path(target_path):
    normalized = target_path.rstrip("/")
    if normalized != ALLOWED_TARGET_HDFS:
        raise ValueError(
            f"Từ chối đường dẫn output-hdfs: '{target_path}'.\n"
            f"Quy tắc an toàn chỉ cho phép ghi đè đúng thư mục đích: '{ALLOWED_TARGET_HDFS}'"
        )
    return normalized


def source_sha256(path):
    """Nhận diện đúng file nguồn của lần chạy, không đọc toàn bộ vào RAM."""
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_summary(path, summary):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as output:
        json.dump(summary, output, ensure_ascii=False, indent=2, allow_nan=False)
    os.replace(temporary, path)


def run_etl(args, spark, summary=None):
    """Chạy ETL; lỗi chất lượng được báo trước khi ghi đè dữ liệu đích."""
    if summary is None:
        summary = {}
    start_time = datetime.utcnow()

    print("=" * 70)
    print("PYSPARK BATCH ETL – OPSD ĐỨC NĂM 2019 (UTC)")
    print(f"  Source Input CSV : {args.input}")
    print(f"  Target HDFS Path : {args.output_hdfs}")
    print("=" * 70)

    # 1. Kiểm tra giới hạn đường dẫn đích trước khi thực hiện bất kỳ thao tác nào
    normalized_target_hdfs = validate_target_path(args.output_hdfs)

    # 3. Đọc CSV nguồn (không sửa file nguồn)
    print("\n[Bước 1] Đọc tệp CSV nguồn...")
    df_raw = spark.read.option("header", "true").csv(args.input)
    total_source_rows = df_raw.count()
    source_filename = os.path.basename(args.input)
    print(f"  ✔ Tổng số dòng tệp nguồn: {total_source_rows:,} dòng")

    target_ts_col = "utc_timestamp"
    target_load_col = "DE_load_actual_entsoe_transparency"
    if target_ts_col not in df_raw.columns:
        raise ValueError(f"Thiếu cột bắt buộc '{target_ts_col}' trong file nguồn!")
    if target_load_col not in df_raw.columns:
        raise ValueError(f"Thiếu cột bắt buộc '{target_load_col}' trong file nguồn!")

    # 4. Phân loại nguồn thành ba nhóm riêng biệt không chồng lấn (MECE)
    print("\n[Bước 2] Phân loại nguồn thành 3 nhóm MECE...")
    df_typed = df_raw.select(
        F.col(target_ts_col).alias("raw_ts"),
        F.col(target_load_col).alias("raw_load"),
        F.to_timestamp(F.col(target_ts_col)).alias("ts_parsed"),
        F.col(target_load_col).cast(DoubleType()).alias("load_parsed")
    )

    # Điều kiện phân loại
    cond_ts_invalid_or_empty = (
        F.col("raw_ts").isNull()
        | (F.trim(F.col("raw_ts")) == "")
        | F.col("ts_parsed").isNull()
    )

    cond_ts_valid_in_scope_2019 = (
        (~cond_ts_invalid_or_empty)
        & (F.col("ts_parsed") >= F.to_timestamp(F.lit("2019-01-01 00:00:00")))
        & (F.col("ts_parsed") < F.to_timestamp(F.lit("2020-01-01 00:00:00")))
    )

    cond_ts_valid_out_of_scope = (
        (~cond_ts_invalid_or_empty)
        & (~cond_ts_valid_in_scope_2019)
    )

    # 3 nhóm DataFrame
    df_group1_invalid_ts = df_typed.filter(cond_ts_invalid_or_empty)
    df_group2_out_of_scope = df_typed.filter(cond_ts_valid_out_of_scope)
    df_group3_in_scope = df_typed.filter(cond_ts_valid_in_scope_2019)

    count_group1 = df_group1_invalid_ts.count()
    count_group2 = df_group2_out_of_scope.count()
    count_group3 = df_group3_in_scope.count()
    summary.update({
        "total_source_rows": total_source_rows,
        "group_1_invalid_or_empty_ts_count": count_group1,
        "group_2_valid_out_of_scope_ts_count": count_group2,
        "group_3_valid_in_scope_ts_count": count_group3,
    })

    print(f"  - Nhóm 1 (Timestamp trống hoặc lỗi parse)    : {count_group1:,} dòng")
    print(f"  - Nhóm 2 (Timestamp hợp lệ ngoài năm 2019)   : {count_group2:,} dòng")
    print(f"  - Nhóm 3 (Timestamp hợp lệ trong năm 2019)   : {count_group3:,} dòng")

    # Kiểm tra bảo toàn số lượng dòng: Tổng 3 nhóm phải bằng tổng dòng nguồn
    sum_three_groups = count_group1 + count_group2 + count_group3
    if sum_three_groups != total_source_rows:
        print(f"  ❌ LỖI LOGIC: Tổng 3 nhóm ({sum_three_groups:,}) khác tổng dòng nguồn ({total_source_rows:,})!")
        raise ValueError("Tổng ba nhóm timestamp không khớp tổng nguồn")
    print(f"  ✔ Kiểm tra MECE thành công: {count_group1} + {count_group2:,} + {count_group3:,} == {total_source_rows:,}")

    # Quy tắc: Dừng và báo lỗi nếu có timestamp nguồn không xác định được
    if count_group1 > 0:
        print(f"  ❌ LỖI: Phát hiện {count_group1} dòng có timestamp nguồn không xác định được!")
        print("  Theo quy tắc: Không gọi timestamp lỗi là ngoài phạm vi. Dừng pipeline để kiểm tra.")
        raise ValueError("Timestamp nguồn không xác định được")

    # 5. Kiểm tra chất lượng dữ liệu nhóm 3 (năm 2019) trước khi ghi
    print("\n[Bước 3] Kiểm tra chất lượng load_mw của năm 2019...")
    EXPECTED_HOURS = 8760
    if count_group3 != EXPECTED_HOURS:
        print(f"  ❌ LỖI: Số dòng trong phạm vi năm 2019 ({count_group3}) khác {EXPECTED_HOURS} giờ theo lịch!")
        raise ValueError(f"Số dòng năm 2019 phải bằng {EXPECTED_HOURS}")

    # Kiểm tra load_mw: không null, không NaN, không +Inf/-Inf, và phải > 0
    df_load_invalid = df_group3_in_scope.filter(
        F.col("load_parsed").isNull()
        | F.isnan(F.col("load_parsed"))
        | (F.col("load_parsed") == float("inf"))
        | (F.col("load_parsed") == float("-inf"))
        | (F.col("load_parsed") <= 0)
    )

    invalid_load_count = df_load_invalid.count()
    if invalid_load_count > 0:
        print(f"  ❌ LỖI: Phát hiện {invalid_load_count} dòng phụ tải không hợp lệ trong năm 2019 (null, NaN, Inf hoặc <= 0)!")
        print("  Theo quy tắc: Không thay thế bằng 0 hay nội suy. Dừng pipeline.")
        raise ValueError("Phụ tải không hợp lệ: null, NaN, Inf hoặc <= 0")

    # Kiểm tra khóa logic trùng lặp (country = 'DE', timestamp_utc)
    df_with_country = df_group3_in_scope.withColumn("country", F.lit("DE"))
    dup_keys_count = (
        df_with_country
        .groupBy("country", "ts_parsed")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_keys_count > 0:
        print(f"  ❌ LỖI: Phát hiện {dup_keys_count} khóa logic bị trùng trong năm 2019!")
        raise ValueError("Khóa logic bị trùng trong năm 2019")

    expected_timeline = spark.sql("""
        SELECT explode(sequence(
            TIMESTAMP '2019-01-01 00:00:00', TIMESTAMP '2019-12-31 23:00:00',
            INTERVAL 1 HOUR)) AS ts_parsed
    """)
    # Đủ 8.760 dòng vẫn có thể sai nếu một mốc bị lệch phút/giây.
    if df_group3_in_scope.select("ts_parsed").exceptAll(expected_timeline).limit(1).count():
        raise ValueError("Chuỗi timestamp không khớp đủ 8.760 mốc giờ UTC năm 2019")

    print("  ✔ Toàn bộ 8,760 dòng năm 2019 đạt kiểm định chất lượng: load_mw hữu hạn > 0, không trùng lặp.")

    # 6. Chuẩn hóa Schema đầu ra
    print("\n[Bước 4] Chuẩn hóa schema đầu ra...")
    df_transformed = df_with_country.select(
        F.col("ts_parsed").alias("timestamp_utc"),
        F.col("country"),
        F.col("load_parsed").alias("load_mw"),
        F.year(F.col("ts_parsed")).cast(IntegerType()).alias("year"),
        F.month(F.col("ts_parsed")).cast(IntegerType()).alias("month"),
        F.lit(source_filename).cast(StringType()).alias("source_file"),
    )

    records_prepared_to_write = df_transformed.count()
    df_transformed.printSchema()

    # 7. Lưu số dòng chuẩn bị ghi; main xuất summary cả khi chạy thất bại.
    summary.update({
        "execution_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "source_csv_path": args.input,
        "source_filename": source_filename,
        "output_hdfs_path": normalized_target_hdfs,
        "total_source_rows": total_source_rows,
        "group_1_invalid_or_empty_ts_count": count_group1,
        "group_2_valid_out_of_scope_ts_count": count_group2,
        "group_3_valid_in_scope_ts_count": count_group3,
        "records_prepared_to_write": records_prepared_to_write,
        "status": "READY_FOR_WRITE"
    })

    # 8. Ghi Parquet Snappy lên HDFS (partitionBy year)
    print(f"\n[Bước 5] Ghi Parquet Snappy lên HDFS: {normalized_target_hdfs} ...")
    (
        df_transformed
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("year")
        .parquet(normalized_target_hdfs)
    )

    summary["status"] = "SUCCESS"
    print(f"  ✔ Ghi thành công {records_prepared_to_write:,} dòng lên HDFS tại {normalized_target_hdfs}.")

    duration = (datetime.utcnow() - start_time).total_seconds()
    print("\n" + "=" * 70)
    print(f"HOÀN THÀNH ETL TRONG {duration:.2f} GIÂY!")
    print("=" * 70)
    return summary


def main(argv=None):
    args = parse_arguments(argv)
    # Từ chối đích sai trước khi khởi động Spark hoặc sửa báo cáo hiện có.
    args.output_hdfs = validate_target_path(args.output_hdfs)
    summary = {
        "run_id": str(uuid.uuid4()),
        "execution_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "source_csv_path": args.input,
        "source_filename": os.path.basename(args.input),
        "output_hdfs_path": args.output_hdfs,
        "status": "RUNNING",
    }
    write_summary(args.summary_out, summary)
    spark = None
    exit_code = 1
    try:
        summary["source_sha256"] = source_sha256(args.input)
        spark = (
            SparkSession.builder.appName("OPSD_Batch_ETL_DE_2019")
            .master("local[2]").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.hadoop.dfs.replication", "1")
            .config("spark.sql.parquet.compression.codec", "snappy").getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        run_etl(args, spark, summary)
        exit_code = 0
    except Exception as error:
        summary.update(status="FAILED", error=str(error))
        print(f"ETL FAILED: {error}")
    finally:
        summary["finished_timestamp_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        write_summary(args.summary_out, summary)
        if spark is not None:
            spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
