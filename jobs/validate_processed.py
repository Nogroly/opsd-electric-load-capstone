#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jobs/validate_processed.py
==========================
Kiểm định chất lượng dữ liệu sau ETL (Post-ETL Data Quality Validation) cho OPSD DE 2019.

Yêu cầu nghiêm ngặt:
  1. Kiểm tra chất lượng dữ liệu nguồn trước khi dùng làm baseline.
     Nếu có dòng nguồn lỗi trong phạm vi năm 2019 thì overall_status phải là FAILED.
  2. Phân biệt rõ số dòng chuẩn bị ghi (lấy từ báo cáo ETL) với số dòng đọc lại từ HDFS,
     không tự suy đoán từ readback.
  3. Kiểm tra khóa trùng riêng biệt: (country, timestamp_utc).
  4. Đối chiếu chính xác bộ ba (country, timestamp_utc, load_mw) bằng exceptAll
     theo cả hai chiều (cùng thứ tự cột và kiểu dữ liệu).
     Hai phía đều không có phần khác biệt mới PASS. Không dùng ngưỡng dung sai xấp xỉ.
  5. Bổ sung kiểm tra year và month đúng với timestamp_utc, country bằng 'DE',
     và source_file đúng tên file nguồn.
  6. Sửa báo cáo:
     - Tính tỷ lệ từ số đếm thực tế, ghi rõ mẫu số.
     - Không cộng các loại sai lệch chồng lấn rồi gọi là số dòng sai duy nhất.
     - Chỉ kết luận PASS khi tất cả các kiểm tra thực sự đạt.
     - Không ghi sẵn câu '8.760 dòng khớp chính xác' trong trường hợp FAILED.
     - Luôn xuất báo cáo chất lượng kể cả khi kiểm tra thất bại.
     - Ghi văn bản bằng UTF-8.
"""

import os
import sys
import json
import argparse
import math
from datetime import datetime
from etl import source_sha256, write_summary

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="OPSD DE 2019 Quality Validation")
    parser.add_argument(
        "--hdfs-input",
        type=str,
        default="hdfs://namenode:8020/bigdata/opsd/processed/de_2019",
        help="Đường dẫn thư mục Parquet trên HDFS cần kiểm định",
    )
    parser.add_argument(
        "--source-csv",
        type=str,
        default="/workspace/data/raw/time_series_60min_singleindex.csv",
        help="Đường dẫn file CSV nguồn để đối soát",
    )
    parser.add_argument(
        "--etl-summary",
        type=str,
        default="/workspace/outputs/quality/etl_summary.json",
        help="Đường dẫn file tóm tắt ETL lấy số dòng chuẩn bị ghi",
    )
    parser.add_argument(
        "--report-dir",
        type=str,
        default="/workspace/outputs/quality",
        help="Thư mục xuất báo cáo chất lượng",
    )
    return parser.parse_args(argv)


def load_etl_summary(summary_path):
    if os.path.exists(summary_path):
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[CẢNH BÁO] Không thể đọc etl_summary.json: {e}")
    return None


def run_validation(args, spark):
    start_time = datetime.utcnow()

    print("=" * 70)
    print("OPSD POST-ETL DATA QUALITY VALIDATION")
    print(f"  HDFS Input Parquet : {args.hdfs_input}")
    print(f"  Source Raw CSV     : {args.source_csv}")
    print(f"  ETL Summary File   : {args.etl_summary}")
    print(f"  Report Directory   : {args.report_dir}")
    print("=" * 70)

    is_all_passed = True
    failure_reasons = []
    validation_tests = {}

    # Đọc số dòng chuẩn bị ghi từ báo cáo ETL (không tự suy đoán từ readback)
    etl_summary_info = load_etl_summary(args.etl_summary)
    records_prepared_to_write = None
    if (isinstance(etl_summary_info, dict)
            and type(etl_summary_info.get("records_prepared_to_write")) is int):
        records_prepared_to_write = etl_summary_info["records_prepared_to_write"]
        print(f"  ✔ Số dòng chuẩn bị ghi (từ ETL summary): {records_prepared_to_write:,} dòng")
    else:
        print("  [CẢNH BÁO] Không tìm thấy file etl_summary.json; số dòng chuẩn bị ghi chưa được nạp từ ETL.")

    # Biến lưu trữ dòng dữ liệu để báo cáo
    data_lineage = {
        "source_csv_path": args.source_csv,
        "target_hdfs_path": args.hdfs_input,
        "total_source_rows": 0,
        "group_1_invalid_or_empty_ts_count": 0,
        "group_2_valid_out_of_scope_ts_count": 0,
        "group_3_valid_in_scope_ts_count": 0,
        "in_scope_source_error_rows": 0,
        "records_prepared_to_write": records_prepared_to_write,
        "readback_rows": 0,
    }

    try:
        # =====================================================================
        # BƯỚC 1: KIỂM TRA CHẤT LƯỢNG NGUỒN TRƯỚC KHI DÙNG LÀM BASELINE
        # =====================================================================
        print("\n[Bước 1] Kiểm tra chất lượng dữ liệu nguồn (Baseline Quality Check)...")
        df_raw_source = spark.read.option("header", "true").csv(args.source_csv)
        total_src_rows = df_raw_source.count()
        data_lineage["total_source_rows"] = total_src_rows

        target_ts_col = "utc_timestamp"
        target_load_col = "DE_load_actual_entsoe_transparency"

        df_src_typed = df_raw_source.select(
            F.col(target_ts_col).alias("raw_ts"),
            F.col(target_load_col).alias("raw_load"),
            F.to_timestamp(F.col(target_ts_col)).alias("ts_parsed"),
            F.col(target_load_col).cast(DoubleType()).alias("load_parsed")
        )

        # Phân loại 3 nhóm MECE
        cond_src_ts_invalid = (
            F.col("raw_ts").isNull()
            | (F.trim(F.col("raw_ts")) == "")
            | F.col("ts_parsed").isNull()
        )

        cond_src_ts_in_scope_2019 = (
            (~cond_src_ts_invalid)
            & (F.col("ts_parsed") >= F.to_timestamp(F.lit("2019-01-01 00:00:00")))
            & (F.col("ts_parsed") < F.to_timestamp(F.lit("2020-01-01 00:00:00")))
        )

        cond_src_ts_out_of_scope = (
            (~cond_src_ts_invalid)
            & (~cond_src_ts_in_scope_2019)
        )

        df_src_group1 = df_src_typed.filter(cond_src_ts_invalid)
        df_src_group2 = df_src_typed.filter(cond_src_ts_out_of_scope)
        df_src_group3 = df_src_typed.filter(cond_src_ts_in_scope_2019)

        cnt_g1 = df_src_group1.count()
        cnt_g2 = df_src_group2.count()
        cnt_g3 = df_src_group3.count()

        data_lineage["group_1_invalid_or_empty_ts_count"] = cnt_g1
        data_lineage["group_2_valid_out_of_scope_ts_count"] = cnt_g2
        data_lineage["group_3_valid_in_scope_ts_count"] = cnt_g3

        print(f"  - Dữ liệu nguồn: Tổng = {total_src_rows:,} dòng")
        denominator = total_src_rows or 1
        print(f"    + Nhóm 1 (Timestamp lỗi/trống)   : {cnt_g1:,} / {total_src_rows:,} ({cnt_g1/denominator*100:.2f}%)")
        print(f"    + Nhóm 2 (Hợp lệ ngoài năm 2019) : {cnt_g2:,} / {total_src_rows:,} ({cnt_g2/denominator*100:.2f}%)")
        print(f"    + Nhóm 3 (Hợp lệ trong năm 2019) : {cnt_g3:,} / {total_src_rows:,} ({cnt_g3/denominator*100:.2f}%)")

        # Kiểm tra chất lượng của nhóm in-scope 2019 trong nguồn
        df_src_in_scope_errors = df_src_group3.filter(
            F.col("load_parsed").isNull()
            | F.isnan(F.col("load_parsed"))
            | (F.col("load_parsed") == float("inf"))
            | (F.col("load_parsed") == float("-inf"))
            | (F.col("load_parsed") <= 0)
        )
        src_in_scope_err_cnt = df_src_in_scope_errors.count()
        data_lineage["in_scope_source_error_rows"] = src_in_scope_err_cnt

        source_duplicate_keys = (df_src_group3.groupBy("ts_parsed").count()
                                 .filter(F.col("count") > 1).count())
        expected_timeline = spark.sql("""
            SELECT explode(sequence(
                TIMESTAMP '2019-01-01 00:00:00', TIMESTAMP '2019-12-31 23:00:00',
                INTERVAL 1 HOUR)) AS ts_parsed
        """)
        source_unexpected_hours = (df_src_group3.select("ts_parsed")
                                   .exceptAll(expected_timeline).count())
        source_baseline_pass = (
            cnt_g1 == 0 and src_in_scope_err_cnt == 0 and cnt_g3 == 8760
            and cnt_g1 + cnt_g2 + cnt_g3 == total_src_rows
            and source_duplicate_keys == 0 and source_unexpected_hours == 0
        )
        validation_tests["test_0_source_baseline_quality"] = {
            "name": "Kiểm tra chất lượng dữ liệu nguồn làm baseline",
            "group_1_invalid_ts_count": cnt_g1,
            "group_2_out_of_scope_count": cnt_g2,
            "group_3_in_scope_count": cnt_g3,
            "in_scope_error_count": src_in_scope_err_cnt,
            "duplicate_source_keys": source_duplicate_keys,
            "unexpected_source_hours": source_unexpected_hours,
            "passed": source_baseline_pass,
            "message": "Nguồn hợp lệ làm baseline" if source_baseline_pass else "Dữ liệu nguồn có lỗi trong phạm vi hoặc timestamp không xác định"
        }

        if not source_baseline_pass:
            is_all_passed = False
            msg = f"Nguồn không đạt chuẩn baseline: G1_invalid_ts={cnt_g1}, in_scope_errors={src_in_scope_err_cnt}, in_scope_rows={cnt_g3}/8760"
            failure_reasons.append(msg)
            print(f"  ❌ FAILED: {msg}")
        else:
            print("  ✔ Nguồn đạt chuẩn baseline: 8,760 dòng hợp lệ, 0 lỗi, 0 timestamp không xác định.")

        # =====================================================================
        # BƯỚC 2: ĐỌC LẠI DỮ LIỆU TỪ HDFS
        # =====================================================================
        print("\n[Bước 2] Đọc lại dữ liệu Parquet từ HDFS...")
        df_processed = spark.read.parquet(args.hdfs_input)
        df_processed.cache()
        readback_count = df_processed.count()
        data_lineage["readback_rows"] = readback_count
        print(f"  ✔ Đọc thành công {readback_count:,} dòng từ HDFS.")

        expected_schema = {
            "timestamp_utc": "timestamp", "country": "string", "load_mw": "double",
            "year": "int", "month": "int", "source_file": "string",
        }
        actual_schema = dict(df_processed.dtypes)
        schema_pass = actual_schema == expected_schema
        validation_tests["test_schema"] = {
            "name": "Schema và kiểu dữ liệu của Parquet", "passed": schema_pass,
            "expected": expected_schema, "actual": actual_schema,
            "message": "Schema đúng" if schema_pass else "Schema thiếu/thừa cột hoặc sai kiểu",
        }
        if not schema_pass:
            raise ValueError("Schema Parquet không khớp schema bắt buộc")

        etl_info = etl_summary_info if isinstance(etl_summary_info, dict) else {}
        summary_checks = {
            "summary_present": isinstance(etl_summary_info, dict),
            "etl_success": etl_info.get("status") == "SUCCESS",
            "source_path_matches": etl_info.get("source_csv_path") == args.source_csv,
            "source_filename_matches": etl_info.get("source_filename") == os.path.basename(args.source_csv),
            "target_path_matches": str(etl_info.get("output_hdfs_path", "")).rstrip("/") == args.hdfs_input.rstrip("/"),
            "source_sha256_matches": etl_info.get("source_sha256") == source_sha256(args.source_csv),
            "source_row_count_matches": etl_info.get("total_source_rows") == total_src_rows,
            "invalid_ts_count_matches": etl_info.get("group_1_invalid_or_empty_ts_count") == cnt_g1,
            "out_of_scope_count_matches": etl_info.get("group_2_valid_out_of_scope_ts_count") == cnt_g2,
            "in_scope_count_matches": etl_info.get("group_3_valid_in_scope_ts_count") == cnt_g3,
            "prepared_count_matches": records_prepared_to_write == readback_count == cnt_g3 == 8760,
        }
        summary_pass = all(summary_checks.values())
        validation_tests["test_6_etl_summary_consistency"] = {
            "name": "Báo cáo ETL khớp nguồn, đích, SHA256 và số dòng",
            "passed": summary_pass, "checks": summary_checks,
            "message": "Báo cáo ETL nhất quán" if summary_pass else "Thiếu hoặc sai báo cáo ETL",
        }
        if not summary_pass:
            is_all_passed = False
            failure_reasons.append("Báo cáo ETL không nhất quán: " + ", ".join(
                name for name, passed in summary_checks.items() if not passed))

        # =====================================================================
        # BƯỚC 3: CÁC BÀI TEST CHẤT LƯỢNG TRÊN DỮ LIỆU ĐỌC LẠI
        # =====================================================================
        print("\n[Bước 3] Thực hiện các bài kiểm định chất lượng...")

        # Test 1: Số dòng đọc lại, số dòng chuẩn bị ghi và khóa trùng riêng biệt
        EXPECTED_ROWS = 8760
        # Đếm khóa trùng riêng
        dup_keys_count = (
            df_processed
            .groupBy("country", "timestamp_utc")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        distinct_keys_count = df_processed.select("country", "timestamp_utc").distinct().count()

        row_count_match_written = (records_prepared_to_write is not None
                                   and readback_count == records_prepared_to_write)

        test1_pass = (
            (readback_count == EXPECTED_ROWS)
            and (distinct_keys_count == EXPECTED_ROWS)
            and (dup_keys_count == 0)
            and row_count_match_written
        )

        validation_tests["test_1_row_count_and_keys"] = {
            "name": "Kiểm tra số dòng đọc lại, số dòng chuẩn bị ghi và khóa trùng",
            "expected_rows": EXPECTED_ROWS,
            "records_prepared_to_write": records_prepared_to_write,
            "readback_rows": readback_count,
            "distinct_keys_count": distinct_keys_count,
            "duplicate_keys_count": dup_keys_count,
            "passed": test1_pass,
            "message": "Số dòng đọc lại khớp chuẩn bị ghi, không có khóa trùng" if test1_pass else "Sai lệch số dòng hoặc có khóa trùng"
        }
        print(f"  Test 1 - Số dòng & Khóa trùng riêng: readback={readback_count:,}, prepared={records_prepared_to_write}, distinct_keys={distinct_keys_count:,}, dup_keys={dup_keys_count} -> {'PASS' if test1_pass else 'FAIL'}")
        if not test1_pass:
            is_all_passed = False
            failure_reasons.append(f"Test 1 FAILED: readback={readback_count}, prepared={records_prepared_to_write}, dup_keys={dup_keys_count}")

        # Test 2: Min/max UTC đúng phạm vi năm 2019 và kiểm tra thiếu giờ (Gaps)
        ts_stat = df_processed.select(
            F.min("timestamp_utc").alias("min_ts"),
            F.max("timestamp_utc").alias("max_ts")
        ).collect()[0]
        actual_min_ts = ts_stat["min_ts"].strftime("%Y-%m-%d %H:%M:%S") if ts_stat["min_ts"] else "N/A"
        actual_max_ts = ts_stat["max_ts"].strftime("%Y-%m-%d %H:%M:%S") if ts_stat["max_ts"] else "N/A"

        expected_min_ts = "2019-01-01 00:00:00"
        expected_max_ts = "2019-12-31 23:00:00"

        # Kiểm tra bước nhảy giờ
        w = Window.orderBy("timestamp_utc")
        gaps_count = (
            df_processed.select("timestamp_utc").distinct()
            .withColumn("prev_ts", F.lag("timestamp_utc", 1).over(w))
            .filter(F.col("prev_ts").isNotNull())
            .withColumn("diff_hours", (F.unix_timestamp("timestamp_utc") - F.unix_timestamp("prev_ts")) / 3600)
            .filter(F.col("diff_hours") != 1)
            .count()
        )

        test2_pass = (actual_min_ts == expected_min_ts) and (actual_max_ts == expected_max_ts) and (gaps_count == 0)
        validation_tests["test_2_timestamp_continuity"] = {
            "name": "Kiểm tra phạm vi thời gian UTC và tính liên tục (không thiếu giờ)",
            "expected_min_ts": expected_min_ts,
            "actual_min_ts": actual_min_ts,
            "expected_max_ts": expected_max_ts,
            "actual_max_ts": actual_max_ts,
            "timeline_gaps_count": gaps_count,
            "passed": test2_pass,
            "message": "Phạm vi chính xác và liên tục 100%" if test2_pass else "Sai phạm vi hoặc có khoảng hở mốc giờ"
        }
        print(f"  Test 2 - Phạm vi thời gian & Tính liên tục: [{actual_min_ts}] -> [{actual_max_ts}], gaps={gaps_count} -> {'PASS' if test2_pass else 'FAIL'}")
        if not test2_pass:
            is_all_passed = False
            failure_reasons.append(f"Test 2 FAILED: min={actual_min_ts}, max={actual_max_ts}, gaps={gaps_count}")

        # Test 3: NULL ở các cột bắt buộc, số hữu hạn và phụ tải > 0
        required_cols = ["timestamp_utc", "country", "load_mw", "year", "month", "source_file"]
        null_counts = {}
        for col_name in required_cols:
            c_null = df_processed.filter(F.col(col_name).isNull()).count()
            null_counts[col_name] = c_null

        total_nulls = sum(null_counts.values())
        nan_inf_load = df_processed.filter(
            F.isnan(F.col("load_mw"))
            | (F.col("load_mw") == float("inf"))
            | (F.col("load_mw") == float("-inf"))
        ).count()

        non_positive_load = df_processed.filter(F.col("load_mw") <= 0).count()

        load_min_max = df_processed.select(
            F.min("load_mw").alias("min_l"),
            F.max("load_mw").alias("max_l"),
            F.round(F.avg("load_mw"), 2).alias("avg_l")
        ).collect()[0]

        test3_pass = (total_nulls == 0) and (nan_inf_load == 0) and (non_positive_load == 0)
        validation_tests["test_3_nulls_and_valid_values"] = {
            "name": "Kiểm tra NULL, số hữu hạn và phụ tải > 0",
            "null_counts_by_column": null_counts,
            "total_nulls": total_nulls,
            "nan_or_inf_load_count": nan_inf_load,
            "non_positive_load_count": non_positive_load,
            "min_load_mw": finite_value(load_min_max["min_l"]),
            "max_load_mw": finite_value(load_min_max["max_l"]),
            "avg_load_mw": finite_value(load_min_max["avg_l"]),
            "passed": test3_pass,
            "message": "Không có NULL, số hữu hạn và phụ tải > 0" if test3_pass else "Có NULL, NaN/Inf hoặc phụ tải <= 0"
        }
        print(f"  Test 3 - Kiểm tra NULL, số hữu hạn & phụ tải > 0: nulls={total_nulls}, nan_inf={nan_inf_load}, non_positive={non_positive_load} -> {'PASS' if test3_pass else 'FAIL'}")
        if not test3_pass:
            is_all_passed = False
            failure_reasons.append(f"Test 3 FAILED: nulls={total_nulls}, nan_inf={nan_inf_load}, non_positive={non_positive_load}")

        # Test 4: Bổ sung kiểm tra year, month, country, source_file
        expected_source_filename = os.path.basename(args.source_csv)

        invalid_country = df_processed.filter(F.col("country") != "DE").count()
        invalid_year = df_processed.filter(F.col("year") != F.year(F.col("timestamp_utc"))).count()
        invalid_month = df_processed.filter(F.col("month") != F.month(F.col("timestamp_utc"))).count()
        invalid_source_file = df_processed.filter(F.col("source_file") != expected_source_filename).count()

        total_metadata_mismatches = invalid_country + invalid_year + invalid_month + invalid_source_file
        test4_pass = (total_metadata_mismatches == 0)
        validation_tests["test_4_derived_columns"] = {
            "name": "Kiểm tra tính nhất quán của country, year, month, source_file",
            "invalid_country_count": invalid_country,
            "invalid_year_count": invalid_year,
            "invalid_month_count": invalid_month,
            "invalid_source_file_count": invalid_source_file,
            "passed": test4_pass,
            "message": "Cột dẫn xuất hoàn toàn nhất quán" if test4_pass else "Cột dẫn xuất có sai lệch"
        }
        print(f"  Test 4 - Tính nhất quán các cột dẫn xuất: invalid_country={invalid_country}, invalid_year={invalid_year}, invalid_month={invalid_month}, invalid_source_file={invalid_source_file} -> {'PASS' if test4_pass else 'FAIL'}")
        if not test4_pass:
            is_all_passed = False
            failure_reasons.append(f"Test 4 FAILED: total_metadata_mismatches={total_metadata_mismatches}")

        # Test 5: Đối chiếu chính xác bộ ba (country, timestamp_utc, load_mw) bằng exceptAll hai chiều
        print("\n[Bước 4] Đối chiếu chính xác bộ ba (country, timestamp_utc, load_mw) bằng exceptAll hai chiều...")
        df_proc_sub = df_processed.select(
            F.col("country").cast(StringType()),
            F.col("timestamp_utc").cast(TimestampType()),
            F.col("load_mw").cast(DoubleType())
        )

        df_src_sub = df_src_group3.select(
            F.lit("DE").cast(StringType()).alias("country"),
            F.col("ts_parsed").cast(TimestampType()).alias("timestamp_utc"),
            F.col("load_parsed").cast(DoubleType()).alias("load_mw")
        )

        # Chiều 1: Processed exceptAll Source
        diff_proc_minus_src = df_proc_sub.exceptAll(df_src_sub)
        # Chiều 2: Source exceptAll Processed
        diff_src_minus_proc = df_src_sub.exceptAll(df_proc_sub)

        diff_proc_count = diff_proc_minus_src.count()
        diff_src_count = diff_src_minus_proc.count()

        test5_pass = (diff_proc_count == 0) and (diff_src_count == 0)
        validation_tests["test_5_exact_reconciliation_except_all"] = {
            "name": "Đối chiếu chính xác bộ ba (country, timestamp_utc, load_mw) bằng exceptAll 2 chiều",
            "processed_except_source_count": diff_proc_count,
            "source_except_processed_count": diff_src_count,
            "method": "Spark exceptAll (cùng thứ tự cột, cùng kiểu dữ liệu, không dùng ngưỡng xấp xỉ)",
            "passed": test5_pass,
            "message": "Hai phía không có phần khác biệt (khớp chính xác 100%)" if test5_pass else "Phát hiện sai lệch giữa HDFS và nguồn"
        }
        print(f"  Test 5 - Đối chiếu exceptAll hai chiều: (Processed - Source)={diff_proc_count}, (Source - Processed)={diff_src_count} -> {'PASS' if test5_pass else 'FAIL'}")
        if not test5_pass:
            is_all_passed = False
            failure_reasons.append(f"Test 5 FAILED: proc_minus_src={diff_proc_count}, src_minus_proc={diff_src_count}")

    except Exception as e:
        is_all_passed = False
        failure_reasons.append(f"Ngoại lệ runtime: {str(e)}")
        print(f"  ❌ LỖI TRONG QUÁ TRÌNH KIỂM ĐỊNH: {e}")

    finally:
        # =====================================================================
        # BƯỚC 5: XUẤT BÁO CÁO CHẤT LƯỢNG (LUÔN XUẤT KỂ CẢ KHI FAILED, UTF-8)
        # =====================================================================
        print(f"\n[Bước 5] Xuất báo cáo chất lượng vào: {args.report_dir}...")
        os.makedirs(args.report_dir, exist_ok=True)

        overall_status_str = "PASSED" if is_all_passed else "FAILED"
        summary_out_file = os.path.join(args.report_dir, "quality_summary.json")
        report_out_file = os.path.join(args.report_dir, "quality_report.md")

        final_summary = {
            "execution_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "overall_status": overall_status_str,
            "failure_reasons": failure_reasons,
            "data_lineage": data_lineage,
            "tests": validation_tests
        }

        final_summary["etl_run_id"] = (etl_summary_info or {}).get("run_id") if isinstance(etl_summary_info, dict) else None
        write_summary(summary_out_file, final_summary)
        print(f"  ✔ Đã ghi: {summary_out_file}")

        report_md_content = generate_markdown_report(final_summary)
        with open(report_out_file, "w", encoding="utf-8") as f:
            f.write(report_md_content)
        print(f"  ✔ Đã ghi: {report_out_file}")
        if "df_processed" in locals():
            df_processed.unpersist()

    duration = (datetime.utcnow() - start_time).total_seconds()
    print("\n" + "=" * 70)
    if is_all_passed:
        print(f"✅ TOÀN BỘ KIỂM ĐỊNH CHẤT LƯỢNG THÀNH CÔNG (PASSED) TRONG {duration:.2f} GIÂY!")
        print("=" * 70)
        return 0
    else:
        print(f"❌ KIỂM ĐỊNH CHẤT LƯỢNG THẤT BẠI (FAILED) TRONG {duration:.2f} GIÂY!")
        for r in failure_reasons:
            print(f"   - {r}")
        print("=" * 70)
        return 1


def finite_value(value):
    return float(value) if value is not None and math.isfinite(float(value)) else None


def main(argv=None):
    args = parse_arguments(argv)
    spark = (
        SparkSession.builder.appName("OPSD_Data_Quality_Validation")
        .master("local[2]").config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        return run_validation(args, spark)
    finally:
        spark.stop()


def generate_markdown_report(summary):
    lineage = summary["data_lineage"]
    tests = summary["tests"]
    status_str = summary["overall_status"]
    reasons = summary["failure_reasons"]

    total_src = lineage["total_source_rows"]
    g1 = lineage["group_1_invalid_or_empty_ts_count"]
    g2 = lineage["group_2_valid_out_of_scope_ts_count"]
    g3 = lineage["group_3_valid_in_scope_ts_count"]
    prepared = lineage["records_prepared_to_write"]
    readback = lineage["readback_rows"]

    p_g1 = (g1 / total_src * 100) if total_src > 0 else 0.0
    p_g2 = (g2 / total_src * 100) if total_src > 0 else 0.0
    p_g3 = (g3 / total_src * 100) if total_src > 0 else 0.0
    errors = lineage["in_scope_source_error_rows"]
    error_pct = f"{errors / g3 * 100:.2f}%" if g3 else "N/A"
    readback_pct = f"{readback / prepared * 100:.2f}%" if prepared else "N/A"
    partition_message = "KHỚP" if g1 + g2 + g3 == total_src else "KHÔNG KHỚP"

    prepared_str = f"{prepared:,} dòng" if prepared is not None else "Không có báo cáo ETL"

    # Kết luận kiểm định
    if status_str == "PASSED":
        conclusion_section = """### Kết luận: ĐẠT TIÊU CHUẨN CHẤT LƯỢNG (PASSED)
Tất cả các tiêu chí kiểm định chất lượng đều đạt chuẩn:
- Dữ liệu nguồn đạt chuẩn baseline (không có timestamp lỗi, không có phụ tải null/lỗi trong năm 2019).
- Số dòng đọc lại từ HDFS khớp chính xác với số dòng chuẩn bị ghi từ ETL (8,760 dòng).
- Không có khóa logic trùng lặp `(country, timestamp_utc)`.
- Phạm vi thời gian liên tục 100% từ `2019-01-01 00:00:00` đến `2019-12-31 23:00:00 UTC` (gaps = 0).
- Không có giá trị NULL, NaN, Infinite; toàn bộ phụ tải lớn hơn 0 MW.
- Đối chiếu hai chiều bằng `exceptAll` trên bộ ba `(country, timestamp_utc, load_mw)` không phát hiện bất kỳ sai lệch nào giữa HDFS và nguồn."""
    else:
        reasons_list = "\n".join([f"- {r}" for r in reasons])
        conclusion_section = f"""### Kết luận: KHÔNG ĐẠT TIÊU CHUẨN (FAILED)
Quá trình kiểm định phát hiện các lỗi sau:
{reasons_list}"""

    # Bảng kết quả các test
    test_rows = []
    for k, t in tests.items():
        t_res = "PASS" if t.get("passed", False) else "FAIL"
        test_rows.append(f"| **{t.get('name', k)}** | **{t_res}** | {t.get('message', '')} |")
    test_table_md = "\n".join(test_rows)

    md = f"""# Báo Cáo Kiểm Định Chất Lượng Dữ Liệu Sau ETL (Post-ETL Data Quality Report)
**Đối tượng kiểm định:** Dữ liệu phụ tải thực tế CHLB Đức năm 2019 (OPSD DE 2019)  
**Vị trí HDFS:** `{lineage['target_hdfs_path']}`  
**Tệp nguồn đối soát:** `{lineage['source_csv_path']}`  
**Thời gian thực thi:** {summary['execution_timestamp_utc']}  
**Trạng thái tổng thể:** **{status_str}**  

---

## 1. Dòng Dữ Liệu & Phân Loại Nguồn Không Chồng Lấn (MECE Lineage)

| Phân loại dòng dữ liệu | Số lượng dòng thực tế | Mẫu số | Tỷ lệ (%) | Ý nghĩa |
|---|---|---|---|---|
| **Tổng số dòng nguồn (Total Source Rows)** | **{total_src:,}** | {total_src:,} | 100.00% | File CSV gốc toàn bộ các năm (2014–2020) |
| **Nhóm 1: Timestamp trống hoặc lỗi parse** | **{g1:,}** | {total_src:,} | {p_g1:.2f}% | Dòng có timestamp không xác định được *(dừng pipeline nếu > 0)* |
| **Nhóm 2: Timestamp hợp lệ ngoài năm 2019** | **{g2:,}** | {total_src:,} | {p_g2:.2f}% | Quan sát hợp lệ của các năm khác *(không phải dữ liệu bẩn)* |
| **Nhóm 3: Timestamp hợp lệ trong năm 2019** | **{g3:,}** | {total_src:,} | {p_g3:.2f}% | Dữ liệu mục tiêu trong khoảng `[2019-01-01 00:00:00, 2020-01-01 00:00:00)` |
| **Dòng lỗi trong phạm vi nguồn năm 2019** | **{errors:,}** | {g3:,} | {error_pct} | Phụ tải null, NaN, Inf hoặc không lớn hơn 0 trong năm 2019 |
| **Số dòng chuẩn bị ghi (từ ETL summary)** | **{prepared_str}** | — | — | Ghi nhận độc lập từ báo cáo ETL trước khi ghi HDFS |
| **Số dòng đọc lại từ HDFS (Readback)** | **{readback:,} dòng** | {prepared_str} | {readback_pct} | Đọc thực tế từ file Parquet trên HDFS |

> **Quy tắc phân loại:**  
> Tổng ba nhóm $G_1 + G_2 + G_3 = {g1 + g2 + g3:,}$; tổng dòng nguồn ${total_src:,}$: **{partition_message}**. Nhóm 2 có timestamp hợp lệ ngoài phạm vi; không kết luận phụ tải của nhóm này đã được kiểm định.

---

## 2. Kết Quả Các Bài Kiểm Định Chất Lượng

| Bài kiểm tra | Kết quả | Chi tiết kiểm tra |
|---|:---:|---|
{test_table_md}

---

## 3. Chi Tiết Phương Pháp Đối Soát Hai Chiều (Bidirectional exceptAll)

Quy trình đối soát chất lượng áp dụng phương pháp so sánh tập hợp chính xác của Apache Spark:
1. **Trích xuất bộ ba thuộc tính cốt lõi:**  
   Lấy 3 cột `(country, timestamp_utc, load_mw)` từ cả hai phía, ép về cùng kiểu dữ liệu (`StringType`, `TimestampType`, `DoubleType`) và cùng thứ tự cột:
   - Tập $P$: Dữ liệu đọc lại từ HDFS Parquet.
   - Tập $S$: Dữ liệu đọc từ CSV nguồn thuộc nhóm 3 (năm 2019).
2. **Đối chiếu hai chiều không dùng ngưỡng sai số:**
   - Chiều xuôi: $P \\setminus S = P.\\text{{exceptAll}}(S)$. Kết quả: **{tests.get('test_5_exact_reconciliation_except_all', {}).get('processed_except_source_count', 'N/A')} dòng lệch**.
   - Chiều ngược: $S \\setminus P = S.\\text{{exceptAll}}(P)$. Kết quả: **{tests.get('test_5_exact_reconciliation_except_all', {}).get('source_except_processed_count', 'N/A')} dòng lệch**.
3. **Tiêu chí kết luận:**  
   Chỉ khi cả hai tập hiệu đều rỗng ($|P \\setminus S| = 0$ và $|S \\setminus P| = 0$), hai tập hợp mới được xác nhận là đồng nhất tuyệt đối 100%.

---

## 4. Kết Luận Kiểm Định

{conclusion_section}
"""
    return md


if __name__ == "__main__":
    sys.exit(main())
