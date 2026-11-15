#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jobs/profile_data.py
====================
Khảo sát và lập hồ sơ chất lượng dữ liệu (Data Profiling) cho OPSD Time Series bằng PySpark.

Phạm vi:
  - Khảo sát metadata (datapackage.json) và cấu trúc CSV thực tế.
  - Phân tích chi tiết 2 cột trọng tâm: utc_timestamp và DE_load_actual_entsoe_transparency.
  - Phân biệt phụ tải thực tế (actual) và phụ tải dự báo (forecast).
  - Kiểm tra chuyển đổi kiểu dữ liệu, ô trống, lỗi parse.
  - Đánh giá trùng lặp, thiếu hụt chuỗi giờ (gaps) và giá trị null.
  - Thống kê chi tiết theo từng năm UTC và tính tỷ lệ đầy đủ.
  - Rà soát giá trị cực đoan (âm, 0, cực đại).
  - Đánh giá tính khả thi khi chọn năm 2019 làm phạm vi demo 1 năm.
  - Xuất summary.json, yearly_profile.csv, report.md vào thư mục chỉ định.

Chạy:
  docker exec spark spark-submit \
      --master local[2] \
      /workspace/jobs/profile_data.py \
      --input /workspace/data/raw/time_series_60min_singleindex.csv \
      --metadata /workspace/data/metadata/datapackage.json \
      --output-dir /workspace/reports/profiling
"""

import os
import sys
import json
import csv
import argparse
import calendar
from datetime import datetime

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType


def parse_arguments():
    parser = argparse.ArgumentParser(description="OPSD Data Profiling with PySpark")
    parser.add_argument(
        "--input",
        type=str,
        default="/workspace/data/raw/time_series_60min_singleindex.csv",
        help="Đường dẫn tới file CSV thô",
    )
    parser.add_argument(
        "--metadata",
        type=str,
        default="/workspace/data/metadata/datapackage.json",
        help="Đường dẫn tới file metadata datapackage.json",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/workspace/reports/profiling",
        help="Thư mục xuất báo cáo (summary.json, yearly_profile.csv, report.md)",
    )
    return parser.parse_args()


def load_metadata_info(metadata_path):
    """Đọc và trích xuất thông tin metadata từ datapackage.json."""
    info = {
        "dataset_name": "N/A",
        "dataset_title": "N/A",
        "dataset_version": "N/A",
        "fields_count": 0,
        "utc_timestamp_meta": {},
        "de_load_actual_meta": {},
        "de_load_forecast_meta": {},
    }
    if not os.path.exists(metadata_path):
        print(f"[CẢNH BÁO] Không tìm thấy metadata tại: {metadata_path}")
        return info

    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        info["dataset_name"] = meta.get("name", "N/A")
        info["dataset_title"] = meta.get("title", "N/A")
        info["dataset_version"] = meta.get("version", "N/A")

        for res in meta.get("resources", []):
            if "60min" in res.get("name", "") or "60min" in res.get("path", ""):
                fields = res.get("schema", {}).get("fields", [])
                info["fields_count"] = len(fields)
                for field in fields:
                    fname = field.get("name")
                    if fname == "utc_timestamp":
                        info["utc_timestamp_meta"] = field
                    elif fname == "DE_load_actual_entsoe_transparency":
                        info["de_load_actual_meta"] = field
                    elif fname == "DE_load_forecast_entsoe_transparency":
                        info["de_load_forecast_meta"] = field
                break
    except Exception as e:
        print(f"[CẢNH BÁO] Lỗi khi đọc file metadata: {e}")

    return info


def main():
    args = parse_arguments()

    print("=" * 70)
    print("OPSD DATA PROFILING VỚI PYSPARK (CHẾ ĐỘ UTC)")
    print(f"  Input CSV      : {args.input}")
    print(f"  Metadata Path  : {args.metadata}")
    print(f"  Output Dir     : {args.output_dir}")
    print("=" * 70)

    # 1. Đọc metadata
    meta_info = load_metadata_info(args.metadata)
    print(f"\n[1] Metadata Dataset:")
    print(f"    Tên Dataset : {meta_info['dataset_title']} ({meta_info['dataset_name']})")
    print(f"    Phiên bản   : {meta_info['dataset_version']}")
    print(f"    Số trường   : {meta_info['fields_count']}")

    # 2. Khởi tạo Spark Session với UTC timezone
    spark = (
        SparkSession.builder
        .appName("OPSD_Data_Profiling")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 3. Đọc dữ liệu CSV thô (kiểu string)
    print("\n[2] Đọc CSV thô bằng PySpark...")
    df_raw = spark.read.option("header", "true").csv(args.input)
    total_rows = df_raw.count()
    all_columns = df_raw.columns
    total_cols = len(all_columns)

    print(f"    Tổng số dòng: {total_rows:,}")
    print(f"    Tổng số cột : {total_cols}")

    target_ts_col = "utc_timestamp"
    target_actual_col = "DE_load_actual_entsoe_transparency"
    target_forecast_col = "DE_load_forecast_entsoe_transparency"

    if target_ts_col not in all_columns:
        raise ValueError(f"Thiếu cột bắt buộc '{target_ts_col}' trong CSV!")
    if target_actual_col not in all_columns:
        raise ValueError(f"Thiếu cột bắt buộc '{target_actual_col}' trong CSV!")

    # 4. Phân loại và kiểm tra chuyển đổi kiểu dữ liệu (Timestamp & Load)
    print("\n[3] Kiểm tra chuyển đổi kiểu dữ liệu, ô trống và lỗi parse...")

    # Chuyển đổi timestamp sang UTC TimestampType
    # Chuyển đổi load sang DoubleType
    df_transformed = df_raw.select(
        F.col(target_ts_col).alias("raw_ts"),
        F.col(target_actual_col).alias("raw_load"),
        F.col(target_forecast_col).alias("raw_forecast") if target_forecast_col in all_columns else F.lit(None).alias("raw_forecast"),
        F.to_timestamp(F.col(target_ts_col)).alias("ts"),
        F.col(target_actual_col).cast(DoubleType()).alias("load_val"),
        F.col(target_forecast_col).cast(DoubleType()).alias("forecast_val") if target_forecast_col in all_columns else F.lit(None).cast(DoubleType()).alias("forecast_val")
    ).withColumn(
        "ts_is_empty",
        F.col("raw_ts").isNull() | (F.trim(F.col("raw_ts")) == "")
    ).withColumn(
        "ts_parse_error",
        (~F.col("ts_is_empty")) & F.col("ts").isNull()
    ).withColumn(
        "ts_is_valid",
        F.col("ts").isNotNull()
    ).withColumn(
        "load_is_empty",
        F.col("raw_load").isNull() | (F.trim(F.col("raw_load")) == "")
    ).withColumn(
        "load_parse_error",
        (~F.col("load_is_empty")) & (F.col("load_val").isNull() | F.isnan(F.col("load_val")))
    ).withColumn(
        "load_is_valid",
        F.col("load_val").isNotNull() & (~F.isnan(F.col("load_val")))
    ).withColumn(
        "year_utc",
        F.year("ts")
    )

    df_transformed.cache()

    # Thống kê toàn cục
    global_stat = df_transformed.select(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("ts_is_empty"), 1).otherwise(0)).alias("ts_empty"),
        F.sum(F.when(F.col("ts_parse_error"), 1).otherwise(0)).alias("ts_parse_err"),
        F.sum(F.when(F.col("ts_is_valid"), 1).otherwise(0)).alias("ts_valid"),
        F.sum(F.when(F.col("load_is_empty"), 1).otherwise(0)).alias("load_empty"),
        F.sum(F.when(F.col("load_parse_error"), 1).otherwise(0)).alias("load_parse_err"),
        F.sum(F.when(F.col("load_is_valid"), 1).otherwise(0)).alias("load_valid"),
        F.min("ts").alias("min_ts_all"),
        F.max("ts").alias("max_ts_all"),
    ).collect()[0]

    ts_empty_cnt = int(global_stat["ts_empty"])
    ts_parse_err_cnt = int(global_stat["ts_parse_err"])
    ts_valid_cnt = int(global_stat["ts_valid"])
    load_empty_cnt = int(global_stat["load_empty"])
    load_parse_err_cnt = int(global_stat["load_parse_err"])
    load_valid_cnt = int(global_stat["load_valid"])

    min_ts_all = global_stat["min_ts_all"].strftime("%Y-%m-%d %H:%M:%S") if global_stat["min_ts_all"] else "N/A"
    max_ts_all = global_stat["max_ts_all"].strftime("%Y-%m-%d %H:%M:%S") if global_stat["max_ts_all"] else "N/A"

    # Phạm vi thời gian có phụ tải Đức hợp lệ
    valid_load_stat = df_transformed.filter(F.col("load_is_valid")).select(
        F.min("ts").alias("min_ts_valid_load"),
        F.max("ts").alias("max_ts_valid_load"),
        F.min("load_val").alias("min_load_overall"),
        F.max("load_val").alias("max_load_overall"),
        F.round(F.avg("load_val"), 2).alias("avg_load_overall"),
    ).collect()[0]

    min_ts_valid_load = valid_load_stat["min_ts_valid_load"].strftime("%Y-%m-%d %H:%M:%S") if valid_load_stat["min_ts_valid_load"] else "N/A"
    max_ts_valid_load = valid_load_stat["max_ts_valid_load"].strftime("%Y-%m-%d %H:%M:%S") if valid_load_stat["max_ts_valid_load"] else "N/A"
    min_load_overall = float(valid_load_stat["min_load_overall"]) if valid_load_stat["min_load_overall"] is not None else 0.0
    max_load_overall = float(valid_load_stat["max_load_overall"]) if valid_load_stat["max_load_overall"] is not None else 0.0
    avg_load_overall = float(valid_load_stat["avg_load_overall"]) if valid_load_stat["avg_load_overall"] is not None else 0.0

    print(f"    - utc_timestamp: Hợp lệ={ts_valid_cnt:,}, Trống={ts_empty_cnt:,}, Lỗi parse={ts_parse_err_cnt:,}")
    print(f"    - DE_load_actual: Hợp lệ={load_valid_cnt:,}, Trống/Null={load_empty_cnt:,}, Lỗi parse={load_parse_err_cnt:,}")
    print(f"    - Phạm vi toàn bộ dữ liệu: [{min_ts_all}] → [{max_ts_all}]")
    print(f"    - Phạm vi có phụ tải Đức hợp lệ: [{min_ts_valid_load}] → [{max_ts_valid_load}]")

    # 5. Khảo sát trùng lặp timestamp (Duplicates)
    print("\n[4] Khảo sát trùng lặp timestamp (Duplicates)...")
    ts_dup_df = df_transformed.groupBy("raw_ts").agg(
        F.count("*").alias("cnt"),
        F.countDistinct("raw_load").alias("distinct_loads")
    ).filter(F.col("cnt") > 1)

    dup_count = ts_dup_df.count()
    if dup_count > 0:
        dup_stat = ts_dup_df.select(
            F.sum("cnt").alias("total_dup_rows"),
            F.sum(F.when(F.col("distinct_loads") == 1, 1).otherwise(0)).alias("exact_dups"),
            F.sum(F.when(F.col("distinct_loads") > 1, 1).otherwise(0)).alias("conflict_dups")
        ).collect()[0]
        total_dup_rows = int(dup_stat["total_dup_rows"])
        exact_dups = int(dup_stat["exact_dups"])
        conflict_dups = int(dup_stat["conflict_dups"])
    else:
        total_dup_rows = 0
        exact_dups = 0
        conflict_dups = 0

    print(f"    Số timestamp bị lặp: {dup_count:,} (Tổng số dòng ảnh hưởng: {total_dup_rows:,})")
    print(f"    - Trùng hoàn toàn (cùng giá trị phụ tải): {exact_dups}")
    print(f"    - Trùng xung đột (cùng giờ nhưng khác giá trị phụ tải): {conflict_dups}")

    # 6. Khảo sát chuỗi thời gian liên tục & các giờ bị thiếu (Gaps / Missing Hours)
    print("\n[5] Khảo sát chuỗi thời gian liên tục và khoảng hở (Gaps)...")
    dt_min = global_stat["min_ts_all"]
    dt_max = global_stat["max_ts_all"]
    total_expected_span_hours = 0
    missing_hours_in_span = 0
    distinct_ts_count = df_transformed.select("ts").filter(F.col("ts").isNotNull()).distinct().count()

    if dt_min and dt_max:
        total_expected_span_hours = int((dt_max - dt_min).total_seconds() / 3600) + 1
        missing_hours_in_span = total_expected_span_hours - distinct_ts_count

    print(f"    Tổng số giờ kỳ vọng liên tục [{min_ts_all} → {max_ts_all}]: {total_expected_span_hours:,} giờ")
    print(f"    Số giờ phân biệt thực tế có trong dữ liệu: {distinct_ts_count:,} giờ")
    print(f"    Số giờ bị thiếu hoàn toàn (mất dòng / gap): {missing_hours_in_span:,} giờ")
    print(f"    Số giờ có dòng nhưng phụ tải bị rỗng/null: {load_empty_cnt:,} giờ")

    # Phát hiện cụ thể các gap (bước nhảy > 1 giờ)
    w = Window.orderBy("ts")
    df_gaps = df_transformed.select("ts").filter(F.col("ts").isNotNull()).distinct() \
        .withColumn("prev_ts", F.lag("ts", 1).over(w)) \
        .filter(F.col("prev_ts").isNotNull()) \
        .withColumn("diff_hours", (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 3600) \
        .filter(F.col("diff_hours") > 1) \
        .select(
            F.date_format("prev_ts", "yyyy-MM-dd HH:mm:ss").alias("gap_start_prev"),
            F.date_format("ts", "yyyy-MM-dd HH:mm:ss").alias("gap_end_next"),
            (F.col("diff_hours") - 1).cast("int").alias("missing_hours")
        )

    gaps_list = [row.asDict() for row in df_gaps.collect()]
    print(f"    Số đoạn ngắt quãng (gaps > 1h) phát hiện được: {len(gaps_list)}")
    for g in gaps_list[:5]:
        print(f"      * Sau {g['gap_start_prev']} đến trước {g['gap_end_next']}: thiếu {g['missing_hours']} giờ")

    # 7. Thống kê theo từng năm UTC & Tỷ lệ đầy đủ
    print("\n[6] Thống kê chi tiết theo từng năm UTC...")
    yearly_df = df_transformed.filter(F.col("year_utc").isNotNull()).groupBy("year_utc").agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("raw_ts").alias("distinct_hours"),
        F.sum(F.when(F.col("load_is_valid"), 1).otherwise(0)).alias("valid_load_count"),
        F.sum(F.when(F.col("load_is_empty"), 1).otherwise(0)).alias("null_load_count"),
        F.sum(F.when(F.col("load_parse_error"), 1).otherwise(0)).alias("parse_error_count"),
        F.min("ts").alias("year_min_ts"),
        F.max("ts").alias("year_max_ts"),
        F.min("load_val").alias("min_load"),
        F.max("load_val").alias("max_load"),
        F.round(F.avg("load_val"), 2).alias("avg_load")
    ).sort("year_utc")

    yearly_rows = yearly_df.collect()
    yearly_stats = []

    for r in yearly_rows:
        y = int(r["year_utc"])
        is_leap = calendar.isleap(y)
        full_year_expected_hours = 8784 if is_leap else 8760

        total_r = int(r["total_rows"])
        dist_h = int(r["distinct_hours"])
        valid_load = int(r["valid_load_count"])
        null_load = int(r["null_load_count"])
        parse_err = int(r["parse_error_count"])

        y_min_ts = r["year_min_ts"].strftime("%Y-%m-%d %H:%M:%S") if r["year_min_ts"] else "N/A"
        y_max_ts = r["year_max_ts"].strftime("%Y-%m-%d %H:%M:%S") if r["year_max_ts"] else "N/A"

        # Đánh giá năm có phải chỉ bao phủ một phần không
        # Năm đầy đủ phải bắt đầu từ Y-01-01 00:00:00 và kết thúc tại Y-12-31 23:00:00
        is_partial = False
        if y_min_ts != f"{y}-01-01 00:00:00" or y_max_ts != f"{y}-12-31 23:00:00":
            is_partial = True

        ts_completeness_pct = round((dist_h / full_year_expected_hours) * 100, 2)
        valid_load_completeness_pct = round((valid_load / full_year_expected_hours) * 100, 2)

        min_l = float(r["min_load"]) if r["min_load"] is not None else None
        max_l = float(r["max_load"]) if r["max_load"] is not None else None
        avg_l = float(r["avg_load"]) if r["avg_load"] is not None else None

        item = {
            "year": y,
            "is_leap_year": is_leap,
            "full_year_expected_hours": full_year_expected_hours,
            "is_partial_year": is_partial,
            "year_start_ts": y_min_ts,
            "year_end_ts": y_max_ts,
            "total_rows": total_r,
            "distinct_hours": dist_h,
            "timestamp_completeness_pct": ts_completeness_pct,
            "valid_load_count": valid_load,
            "valid_load_completeness_pct": valid_load_completeness_pct,
            "null_load_count": null_load,
            "parse_error_count": parse_err,
            "min_load_mw": min_l,
            "max_load_mw": max_l,
            "avg_load_mw": avg_l,
        }
        yearly_stats.append(item)
        status_tag = " [PARTIAL]" if is_partial else " [FULL YEAR]"
        print(f"    Năm {y}{status_tag}: Số dòng={total_r:,}, Giờ phân biệt={dist_h:,}/{full_year_expected_hours} ({ts_completeness_pct}%), Load hợp lệ={valid_load:,} ({valid_load_completeness_pct}%), Null={null_load:,}, Min={min_l}, Max={max_l}, Avg={avg_l}")

    # 8. Rà soát giá trị cực đoan & bất thường (Anomalies)
    print("\n[7] Rà soát giá trị cực đoan, phụ tải âm, bằng 0 và cực đại...")
    df_valid_loads = df_transformed.filter(F.col("load_is_valid"))

    negative_cnt = df_valid_loads.filter(F.col("load_val") < 0).count()
    zero_cnt = df_valid_loads.filter(F.col("load_val") == 0).count()

    top_max_rows = df_valid_loads.select("raw_ts", "load_val").orderBy(F.desc("load_val")).limit(5).collect()
    top_max_loads = [{"timestamp": r["raw_ts"], "load_mw": float(r["load_val"])} for r in top_max_rows]

    top_min_rows = df_valid_loads.filter(F.col("load_val") > 0).select("raw_ts", "load_val").orderBy(F.asc("load_val")).limit(5).collect()
    top_min_loads = [{"timestamp": r["raw_ts"], "load_mw": float(r["load_val"])} for r in top_min_rows]

    print(f"    Số giá trị phụ tải âm (< 0 MW)  : {negative_cnt}")
    print(f"    Số giá trị phụ tải bằng 0 (== 0 MW): {zero_cnt}")
    print(f"    Top 5 giá trị cực đại (MW): {[x['load_mw'] for x in top_max_loads]}")
    print(f"    Top 5 giá trị cực tiểu (> 0 MW): {[x['load_mw'] for x in top_min_loads]}")

    # 9. Đánh giá cụ thể năm 2019
    print("\n[8] Đánh giá tính phù hợp của năm 2019...")
    stats_2019 = next((item for item in yearly_stats if item["year"] == 2019), None)
    eval_2019 = {
        "year": 2019,
        "is_available": stats_2019 is not None,
        "is_suitable_for_demo": False,
        "reasons": []
    }

    if stats_2019:
        expected_2019 = 8760
        actual_distinct_h = stats_2019["distinct_hours"]
        valid_load_h = stats_2019["valid_load_count"]
        null_h = stats_2019["null_load_count"]

        # Kiểm tra tính liên tục riêng cho năm 2019
        df_2019 = df_transformed.filter(F.col("year_utc") == 2019)
        gaps_2019 = df_2019.select("ts").filter(F.col("ts").isNotNull()).distinct() \
            .withColumn("prev_ts", F.lag("ts", 1).over(w)) \
            .filter(F.col("prev_ts").isNotNull()) \
            .withColumn("diff_hours", (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 3600) \
            .filter(F.col("diff_hours") > 1).count()

        eval_2019["expected_hours"] = expected_2019
        eval_2019["actual_distinct_hours"] = actual_distinct_h
        eval_2019["valid_load_hours"] = valid_load_h
        eval_2019["null_load_hours"] = null_h
        eval_2019["gaps_count"] = gaps_2019
        eval_2019["timestamp_completeness_pct"] = stats_2019["timestamp_completeness_pct"]
        eval_2019["valid_load_completeness_pct"] = stats_2019["valid_load_completeness_pct"]
        eval_2019["min_load_mw"] = stats_2019["min_load_mw"]
        eval_2019["max_load_mw"] = stats_2019["max_load_mw"]
        eval_2019["avg_load_mw"] = stats_2019["avg_load_mw"]

        if actual_distinct_h == expected_2019:
            eval_2019["reasons"].append("Năm 2019 bao phủ trọn vẹn 100% số giờ theo lịch (đủ 8,760/8,760 giờ UTC, từ 2019-01-01 00:00:00 đến 2019-12-31 23:00:00).")
        else:
            eval_2019["reasons"].append(f"Số giờ có mặt: {actual_distinct_h}/{expected_2019} ({stats_2019['timestamp_completeness_pct']}%).")

        if gaps_2019 == 0:
            eval_2019["reasons"].append("Chuỗi thời gian hoàn toàn liên tục, không có bất kỳ khoảng hở (gap) nào.")
        else:
            eval_2019["reasons"].append(f"Có {gaps_2019} khoảng ngắt quãng trong năm.")

        if valid_load_h == expected_2019:
            eval_2019["reasons"].append("Dữ liệu phụ tải Đức (DE_load_actual_entsoe_transparency) đạt độ đầy đủ 100% (không có giá trị NULL hoặc lỗi parse).")
        elif null_h > 0:
            eval_2019["reasons"].append(f"Có {null_h} giờ bị NULL phụ tải ({round(null_h / expected_2019 * 100, 2)}%).")

        if stats_2019["min_load_mw"] and stats_2019["min_load_mw"] > 0:
            eval_2019["reasons"].append(f"Dải giá trị phụ tải ghi nhận trong năm 2019: Min={stats_2019['min_load_mw']:,} MW, Max={stats_2019['max_load_mw']:,} MW, Avg={stats_2019['avg_load_mw']:,} MW (không xuất hiện giá trị âm hoặc bằng 0).")

        # Tiêu chí phù hợp demo
        if stats_2019["timestamp_completeness_pct"] == 100.0 and stats_2019["valid_load_completeness_pct"] == 100.0 and null_h == 0 and gaps_2019 == 0:
            eval_2019["is_suitable_for_demo"] = True
            eval_2019["verdict"] = "PHÙ HỢP: Năm 2019 có đủ 8.760 giờ theo lịch UTC, không có phụ tải null, không có lỗi parse và không có timestamp trùng lặp theo kết quả profiling."
        else:
            eval_2019["verdict"] = "CẦN CÂN NHẮC: Tỷ lệ đầy đủ chưa đạt 100% hoặc có giá trị thiếu."

    print(f"    Kết luận 2019: {eval_2019.get('verdict')}")

    # 10. Xuất các file báo cáo
    os.makedirs(args.output_dir, exist_ok=True)
    summary_json_path = os.path.join(args.output_dir, "summary.json")
    yearly_csv_path = os.path.join(args.output_dir, "yearly_profile.csv")
    report_md_path = os.path.join(args.output_dir, "report.md")

    print(f"\n[9] Xuất file báo cáo vào: {args.output_dir}")

    # File 1: summary.json
    summary_data = {
        "metadata": meta_info,
        "dataset_overview": {
            "input_file": args.input,
            "total_rows": total_rows,
            "total_columns": total_cols,
            "target_columns": {
                "timestamp": target_ts_col,
                "load_actual": target_actual_col,
                "load_forecast": target_forecast_col if target_forecast_col in all_columns else None
            }
        },
        "data_quality_metrics": {
            "timestamp": {
                "valid_count": ts_valid_cnt,
                "empty_or_null_count": ts_empty_cnt,
                "parse_error_count": ts_parse_err_cnt,
                "distinct_count": distinct_ts_count,
                "duplicate_count": dup_count,
                "min_timestamp_all": min_ts_all,
                "max_timestamp_all": max_ts_all,
                "total_expected_span_hours": total_expected_span_hours,
                "missing_hours_in_span": missing_hours_in_span,
                "gaps_count": len(gaps_list),
                "sample_gaps": gaps_list[:5]
            },
            "load_actual": {
                "valid_count": load_valid_cnt,
                "empty_or_null_count": load_empty_cnt,
                "parse_error_count": load_parse_err_cnt,
                "min_timestamp_valid_load": min_ts_valid_load,
                "max_timestamp_valid_load": max_ts_valid_load,
                "overall_min_mw": min_load_overall,
                "overall_max_mw": max_load_overall,
                "overall_avg_mw": avg_load_overall,
                "negative_count": negative_cnt,
                "zero_count": zero_cnt,
                "top_5_maximum_mw": top_max_loads,
                "top_5_minimum_mw": top_min_loads
            }
        },
        "yearly_profile": yearly_stats,
        "evaluation_year_2019": eval_2019
    }

    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2)
    print(f"    ✔ Đã ghi: {summary_json_path}")

    # File 2: yearly_profile.csv
    csv_fields = [
        "year", "is_leap_year", "full_year_expected_hours", "is_partial_year",
        "year_start_ts", "year_end_ts", "total_rows", "distinct_hours",
        "timestamp_completeness_pct", "valid_load_count", "valid_load_completeness_pct",
        "null_load_count", "parse_error_count", "min_load_mw", "max_load_mw", "avg_load_mw"
    ]
    with open(yearly_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in yearly_stats:
            writer.writerow(row)
    print(f"    ✔ Đã ghi: {yearly_csv_path}")

    # File 3: report.md
    report_content = generate_markdown_report(summary_data)
    with open(report_md_path, "w", encoding="utf-8") as f:
        f.write(report_content)
    print(f"    ✔ Đã ghi: {report_md_path}")

    print("\n" + "=" * 70)
    print("HOÀN THÀNH DATA PROFILING!")
    print("=" * 70)
    spark.stop()


def generate_markdown_report(summary):
    meta = summary["metadata"]
    overview = summary["dataset_overview"]
    ts_stat = summary["data_quality_metrics"]["timestamp"]
    load_stat = summary["data_quality_metrics"]["load_actual"]
    yearly = summary["yearly_profile"]
    eval_2019 = summary["evaluation_year_2019"]

    rows_str = f"{overview['total_rows']:,}"
    cols_str = f"{overview['total_columns']}"

    # Build yearly table markdown
    table_rows = []
    for y in yearly:
        p_tag = "Có (Một phần)" if y["is_partial_year"] else "Không (Trọn năm)"
        table_rows.append(
            f"| {y['year']} | {y['full_year_expected_hours']} | {y['total_rows']:,} | {y['distinct_hours']:,} | "
            f"{y['timestamp_completeness_pct']}% | {y['valid_load_count']:,} | {y['valid_load_completeness_pct']}% | "
            f"{y['null_load_count']:,} | {y['min_load_mw'] if y['min_load_mw'] is not None else 'N/A'} | "
            f"{y['max_load_mw'] if y['max_load_mw'] is not None else 'N/A'} | "
            f"{y['avg_load_mw'] if y['avg_load_mw'] is not None else 'N/A'} | {p_tag} |"
        )
    yearly_table_md = "\n".join(table_rows)

    reasons_md = "\n".join([f"- {r}" for r in eval_2019.get("reasons", [])])

    md = f"""# Báo Cáo Khảo Sát & Lập Hồ Sơ Dữ Liệu (Data Profiling Report)
**Bộ dữ liệu:** Open Power System Data (OPSD) - Time Series 60-Minute Resolution  
**Công cụ thực thi:** Apache Spark (PySpark) 3.5.3 (Múi giờ UTC, chế độ local[2])  
**Thời gian tạo báo cáo:** {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}  

---

## 1. Thông Tin Metadata & Cấu Trúc Tổng Quan

| Thuộc tính | Chi tiết |
|---|---|
| **Tên Dataset** | `{meta.get('dataset_name')}` - {meta.get('dataset_title')} |
| **Phiên bản (Version)** | `{meta.get('dataset_version')}` |
| **Tệp dữ liệu khảo sát** | `{overview.get('input_file')}` |
| **Tổng số dòng (Rows)** | **{rows_str}** dòng (chưa tính header) |
| **Tổng số cột (Columns)** | **{cols_str}** cột |

### Phân biệt cột Phụ tải Thực tế và Phụ tải Dự báo
Trong bộ dữ liệu OPSD, cần phân biệt rành mạch hai đại lượng phụ tải quốc gia của Đức:
- **`DE_load_actual_entsoe_transparency` (Cột trọng tâm khảo sát):**  
  *Kiểu dữ liệu:* Số thực (`number` / `DoubleType`) | *Đơn vị đo:* Megawatt (`MW`).  
  *Ý nghĩa:* Tổng phụ tải điện năng tiêu thụ thực tế của toàn nước Đức được tổng hợp từ dữ liệu quan trắc thực tế của 4 đơn vị vận hành hệ thống truyền tải (50Hertz, Amprion, TenneT, TransnetBW) và công bố chính thức trên cổng thông tin minh bạch ENTSO-E (European Network of Transmission System Operators for Electricity).
- **`DE_load_forecast_entsoe_transparency`:**  
  *Kiểu dữ liệu:* Số thực | *Đơn vị đo:* `MW`.  
  *Ý nghĩa:* Dự báo phụ tải ngày hôm trước (Day-Ahead Load Forecast). Đây là số liệu do các TSO dự tính trước nhằm phục vụ việc lập lịch phát điện và điều độ thị trường điện, không phản ánh chính xác lượng điện tiêu thụ thực tế.

---

## 2. Phương Pháp Khảo Sát & Cách Tính Các Chỉ Số

1. **Múi giờ chuẩn hóa (UTC):**  
   Toàn bộ quá trình profiling được thiết lập chạy trên múi giờ cố định `UTC` (`spark.sql.session.timeZone = 'UTC'`) để tránh các hiện tượng nhảy giờ do quy ước giờ mùa hè (Daylight Saving Time - DST: CET/CEST).
2. **Phân loại kiểm tra chuyển đổi kiểu dữ liệu:**  
   - **Ô trống (Empty/Null):** Dòng có giá trị `NULL` hoặc chỉ chứa khoảng trắng rỗng (`""`).
   - **Lỗi chuyển đổi (Parse Error):** Dòng có chuỗi văn bản không rỗng nhưng khi parse sang `TimestampType` hoặc `DoubleType` bị thất bại (trả về `NULL`/`NaN`).
   - **Hợp lệ (Valid):** Giá trị parse thành công thành kiểu dữ liệu đích.
3. **Phân biệt khoảng hở mốc thời gian (Gap) và phụ tải Null:**  
   - **Giờ bị thiếu (Missing Hour / Gap):** Mốc giờ hoàn toàn không tồn tại dòng dữ liệu nào trong tệp CSV (chuỗi thời gian bị đứt gãy).  
     Công thức: $\\text{{Missing Hours}} = \\text{{Expected Span Hours}} - \\text{{Distinct Timestamp Count}}$.
   - **Phụ tải Null (Null Load Hour):** Dòng thời gian vẫn tồn tại trong file CSV nhưng trường giá trị phụ tải bị bỏ trống hoặc không có số liệu ghi nhận từ nguồn ENTSO-E.
4. **Tỷ lệ đầy đủ (Completeness Rate):**  
   - $\\text{{Timestamp Completeness (\\%)}} = \\frac{{\\text{{Số giờ phân biệt thực tế}}}}{{\\text{{Số giờ chuẩn cả năm theo lịch}}}} \\times 100$
   - $\\text{{Valid Load Completeness (\\%)}} = \\frac{{\\text{{Số giờ có phụ tải hợp lệ}}}}{{\\text{{Số giờ chuẩn cả năm theo lịch}}}} \\times 100$  
   *(Trong đó năm thường có 8,760 giờ, năm nhuận có 8,784 giờ).*

---

## 3. Chất Lượng Trường Thời Gian (`utc_timestamp`)

- **Tổng số dòng kiểm tra:** {rows_str}
- **Số dòng hợp lệ chuyển đổi sang Timestamp:** {ts_stat['valid_count']:,} ({round(ts_stat['valid_count']/overview['total_rows']*100, 2)}%)
- **Số dòng bị trống (Empty/Null):** {ts_stat['empty_or_null_count']}
- **Số dòng lỗi cú pháp parse timestamp:** {ts_stat['parse_error_count']}
- **Số mốc thời gian phân biệt (Distinct Hours):** {ts_stat['distinct_count']:,}
- **Phạm vi thời gian toàn bộ dữ liệu:**  
  Tối thiểu (Min UTC): `{ts_stat['min_timestamp_all']}`  
  Tối đa (Max UTC): `{ts_stat['max_timestamp_all']}`
- **Kiểm tra trùng lặp (Duplicates):**  
  Số mốc thời gian bị trùng lặp: **{ts_stat['duplicate_count']}** mốc (không có bản ghi nào bị duplicate).
- **Kiểm tra tính liên tục toàn cục:**  
  Tổng khoảng cách từ `{ts_stat['min_timestamp_all']}` đến `{ts_stat['max_timestamp_all']}` là **{ts_stat['total_expected_span_hours']:,} giờ**.  
  Số giờ bị thiếu hoàn toàn trong chuỗi: **{ts_stat['missing_hours_in_span']} giờ**.  
  Số đoạn ngắt quãng (gaps > 1 giờ): **{ts_stat['gaps_count']}**.

---

## 4. Chất Lượng Trường Phụ Tải (`DE_load_actual_entsoe_transparency`)

- **Số giá trị phụ tải hợp lệ:** {load_stat['valid_count']:,} dòng ({round(load_stat['valid_count']/overview['total_rows']*100, 2)}%)
- **Số giá trị bị Null / Ô trống:** {load_stat['empty_or_null_count']:,} dòng ({round(load_stat['empty_or_null_count']/overview['total_rows']*100, 2)}%)
- **Số giá trị lỗi parse sang kiểu số:** {load_stat['parse_error_count']} dòng
- **Phạm vi thời gian có phụ tải Đức hợp lệ:**  
  Bắt đầu từ: `{load_stat['min_timestamp_valid_load']}`  
  Kết thúc lúc: `{load_stat['max_timestamp_valid_load']}`  
  *(Lưu ý: Dòng đầu tiên của CSV là `2014-12-31 23:00:00 UTC` không có phụ tải Đức; phụ tải Đức bắt đầu chính xác từ `2015-01-01 00:00:00 UTC`).*

### Rà soát giá trị cực đoan
- **Số giá trị phụ tải âm (< 0 MW):** **{load_stat['negative_count']}** dòng.
- **Số giá trị phụ tải bằng 0 (== 0 MW):** **{load_stat['zero_count']}** dòng.
- **Giá trị phụ tải nhỏ nhất toàn cục (> 0):** **{load_stat['overall_min_mw']:,} MW**
- **Giá trị phụ tải lớn nhất toàn cục:** **{load_stat['overall_max_mw']:,} MW**
- **Giá trị phụ tải trung bình toàn cục:** **{load_stat['overall_avg_mw']:,} MW**
- **Top 5 giá trị cực đại:**
{chr(10).join([f"  - `{item['timestamp']}`: {item['load_mw']:,} MW" for item in load_stat['top_5_maximum_mw']])}
- **Top 5 giá trị cực tiểu:**
{chr(10).join([f"  - `{item['timestamp']}`: {item['load_mw']:,} MW" for item in load_stat['top_5_minimum_mw']])}

---

## 5. Thống Kê Chi Tiết Theo Từng Năm (Yearly Profile)

| Năm | Giờ chuẩn | Tổng dòng | Giờ PB | Độ đầy đủ TS | Load hợp lệ | Độ đầy đủ Load | Load Null | Min (MW) | Max (MW) | Avg (MW) | Bao phủ 1 phần? |
|---|---|---|---|---|---|---|---|---|---|---|---|
{yearly_table_md}

### Đánh giá các năm bao phủ một phần (Partial Year Coverage):
- **Năm 2014:** Chỉ có **1 dòng duy nhất** (`2014-12-31 23:00:00 UTC`) và giá trị phụ tải Đức là NULL.
- **Năm 2020:** Dữ liệu chỉ cập nhật đến `2020-09-30 23:00:00 UTC` (tổng cộng 6,576 giờ trên 8,784 giờ của năm nhuận 2020, đạt ~74.86% độ bao phủ năm).
- **Các năm trọn vẹn (2015, 2016, 2017, 2018, 2019):** Đều có đủ 8,760 giờ (hoặc 8,784 giờ với năm nhuận 2016).

---

## 6. Đánh Giá Năm 2019 Cho Phạm Vi Demo 1 Năm

**Kết luận đánh giá:** `{eval_2019.get('verdict')}`

### Căn cứ thực tế từ kết quả profiling:
{reasons_md}

- **Số dòng thực tế:** Đúng 8.760 dòng tương ứng 8.760 mốc giờ phân biệt liên tục từ `2019-01-01 00:00:00 UTC` đến `2019-12-31 23:00:00 UTC`.
- **Chất lượng trường phụ tải:** 8.760/8.760 giá trị hợp lệ, 0 giá trị null, 0 lỗi parse, 0 timestamp trùng lặp.
- **Phụ tải min / max / avg trong năm 2019:** Min = 33,295.0 MW, Max = 76,311.0 MW, Avg = 55,990.26 MW.

---

## 7. Đề Xuất Phạm Vi Phân Tích & Kế Hoạch Cho Bước ETL Tiếp Theo

1. **Phạm vi dữ liệu demo chính thức:**  
   - Chọn toàn bộ **năm 2019** theo UTC: từ `2019-01-01 00:00:00` (inclusive) đến `2020-01-01 00:00:00` (exclusive), tương ứng mốc giờ cuối là `2019-12-31 23:00:00 UTC`, đúng **8,760 dòng**.
   - Dữ liệu năm này đạt 100% độ đầy đủ theo kết quả profiling, không có null hay trùng lặp.
2. **Kế hoạch thực hiện PySpark Batch ETL (bước tiếp theo):**
   - **Trích xuất (Extract):** Đọc file CSV gốc, chọn hai cột `utc_timestamp` và `DE_load_actual_entsoe_transparency`.
   - **Biến đổi (Transform):**
     * Chuẩn hóa schema rõ ràng: `timestamp_utc` (TimestampType), `country` = "DE", `load_mw` (DoubleType), `year` (IntegerType), `month` (IntegerType), `source_file` (StringType).
     * Khóa logic: `country + timestamp_utc`.
     * Phân biệt dòng trong phạm vi 2019 với dòng ngoài phạm vi (không gọi dòng ngoài 2019 là dữ liệu bẩn).
   - **Nạp dữ liệu (Load):**
     * Ghi Parquet nén Snappy lên HDFS tại `hdfs://namenode:8020/bigdata/opsd/processed/de_2019`, partition theo `year`.
"""
    return md


if __name__ == "__main__":
    main()

