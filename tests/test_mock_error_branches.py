#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Full-year regression fixtures; production HDFS writes are blocked in error tests."""
import contextlib
import csv
import json
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))
import etl
import validate_processed as validator


def main():
    root = Path(tempfile.mkdtemp(prefix="bigdata-regression-"))
    evidence = Path(__file__).resolve().parents[1] / "outputs" / "quality"
    evidence.mkdir(parents=True, exist_ok=True)
    results = []
    spark = (SparkSession.builder.master("local[2]").appName("OPSD_Regression_Tests")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    log = open(evidence / "error_branch_tests.log", "w", encoding="utf-8")

    def check(name, action):
        try:
            with contextlib.redirect_stdout(log):
                print("\nCASE:", name, flush=True)
                action()
            result = {"name": name, "passed": True}
        except Exception as error:
            result = {"name": name, "passed": False, "error": str(error)}
        results.append(result)
        print(f"{'PASS' if result['passed'] else 'FAIL'}: {name}", flush=True)

    rows = [((datetime(2019, 1, 1) + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "50000.0") for i in range(8760)]

    def source_file(name, changed=None):
        path = root / name / "fixture.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["utc_timestamp", "DE_load_actual_entsoe_transparency"])
            for i, row in enumerate(rows):
                writer.writerow(changed if i == 1 and changed is not None else row)
        return str(path)

    good_source = source_file("good")
    good_df = (spark.sql("""SELECT explode(sequence(
        TIMESTAMP '2019-01-01 00:00:00', TIMESTAMP '2019-12-31 23:00:00',
        INTERVAL 1 HOUR)) AS timestamp_utc""")
        .withColumn("country", F.lit("DE"))
        .withColumn("load_mw", F.lit(50000.0))
        .withColumn("year", F.year("timestamp_utc"))
        .withColumn("month", F.month("timestamp_utc"))
        .withColumn("source_file", F.lit("fixture.csv")))
    good_parquet = str(root / "good_parquet")
    good_df.write.parquet(good_parquet)

    def reject_etl(source, reason):
        args = SimpleNamespace(input=source, output_hdfs=etl.ALLOWED_TARGET_HDFS,
                               summary_out=str(root / "unused_summary.json"))
        # Even a regression must not write synthetic data into the real target.
        with patch("pyspark.sql.readwriter.DataFrameWriter.parquet",
                   side_effect=AssertionError("Unexpected Parquet write")) as writer:
            try:
                etl.run_etl(args, spark)
            except ValueError as error:
                assert reason in str(error), str(error)
            else:
                raise AssertionError("ETL incorrectly accepted invalid data")
            writer.assert_not_called()

    def validate_case(name, parquet=good_parquet, source=good_source,
                      summary_changes=None, missing_summary=False):
        summary_path = root / (name + "_etl.json")
        summary = {
            "run_id": name, "status": "SUCCESS", "source_csv_path": source,
            "source_filename": "fixture.csv", "output_hdfs_path": parquet,
            "source_sha256": etl.source_sha256(source), "total_source_rows": 8760,
            "group_1_invalid_or_empty_ts_count": 0,
            "group_2_valid_out_of_scope_ts_count": 0,
            "group_3_valid_in_scope_ts_count": 8760,
            "records_prepared_to_write": 8760,
        }
        summary.update(summary_changes or {})
        if not missing_summary:
            etl.write_summary(str(summary_path), summary)
        report_dir = root / (name + "_report")
        args = SimpleNamespace(hdfs_input=parquet, source_csv=source,
                               etl_summary=str(summary_path), report_dir=str(report_dir))
        code = validator.run_validation(args, spark)
        def reject_constant(value):
            raise AssertionError("Non-standard JSON value: " + value)
        report = json.loads((report_dir / "quality_summary.json").read_text(encoding="utf-8"),
                            parse_constant=reject_constant)
        assert (code == 0) == (report["overall_status"] == "PASSED")
        return code, report, (report_dir / "quality_report.md").read_text(encoding="utf-8")

    def expect_fail(name, expected_test, **kwargs):
        code, report, markdown = validate_case(name, **kwargs)
        assert code == 1, report
        assert report["tests"][expected_test]["passed"] is False, report
        assert "Kết luận: KHÔNG ĐẠT" in markdown
        return report, markdown

    try:
        def check_paths():
            assert etl.validate_target_path(etl.ALLOWED_TARGET_HDFS + "/") == etl.ALLOWED_TARGET_HDFS
            for path in ["hdfs://namenode:8020/", "hdfs://namenode:8020/bigdata/opsd/processed",
                         etl.ALLOWED_TARGET_HDFS + "/../", "file:///tmp/output"]:
                try:
                    etl.validate_target_path(path)
                except ValueError:
                    continue
                raise AssertionError("Unsafe overwrite path accepted: " + path)
        check("ETL refuses other overwrite targets", check_paths)
        for name, changed, reason in [
            ("invalid_timestamp", ("not-a-timestamp", "50000"), "Timestamp nguồn"),
            ("empty_timestamp", ("", "50000"), "Timestamp nguồn"),
            ("null_load", (rows[1][0], ""), "Phụ tải không hợp lệ"),
            ("nan_load", (rows[1][0], "NaN"), "Phụ tải không hợp lệ"),
            ("infinite_load", (rows[1][0], "Infinity"), "Phụ tải không hợp lệ"),
            ("negative_load", (rows[1][0], "-1"), "Phụ tải không hợp lệ"),
            ("zero_load", (rows[1][0], "0"), "Phụ tải không hợp lệ"),
            ("duplicate_key", (rows[0][0], "50000"), "Khóa logic bị trùng"),
            ("shifted_hour", ("2019-01-01T01:30:00Z", "50000"), "mốc giờ UTC"),
        ]:
            source = source_file(name, changed)
            check("ETL rejects " + name + " on 8760-row fixture",
                  lambda s=source, r=reason: reject_etl(s, r))

        def valid_control():
            code, report, _ = validate_case("valid_control")
            assert code == 0, report
            assert len(report["tests"]) == 8, report
        check("Validation accepts valid full-year control", valid_control)
        check("Validation rejects missing ETL summary", lambda: expect_fail(
            "missing_summary", "test_6_etl_summary_consistency", missing_summary=True))

        def stale_summary():
            report, _ = expect_fail("stale_summary", "test_6_etl_summary_consistency",
                summary_changes={"status": "FAILED", "source_sha256": "wrong",
                                 "output_hdfs_path": "hdfs://wrong/path"})
            checks = report["tests"]["test_6_etl_summary_consistency"]["checks"]
            assert not checks["etl_success"] and not checks["source_sha256_matches"]
            assert not checks["target_path_matches"]
        check("Validation rejects failed/stale/wrong-target summary", stale_summary)

        bad_source = source_file("source_null", (rows[1][0], ""))
        def bad_baseline():
            report, markdown = expect_fail("source_null", "test_0_source_baseline_quality",
                                            source=bad_source)
            assert report["data_lineage"]["in_scope_source_error_rows"] == 1
            assert "0.01%" in markdown
        check("Validation rejects source null and reports actual rate", bad_baseline)

        first = F.col("timestamp_utc") == F.lit("2019-01-01 00:00:00").cast("timestamp")
        tampered_path = str(root / "tampered_parquet")
        (good_df.withColumn("load_mw", F.when(first, F.lit(50000.00001)).otherwise(F.col("load_mw")))
         .withColumn("country", F.when(first, F.lit("FR")).otherwise(F.col("country")))
         .withColumn("month", F.when(first, F.lit(2)).otherwise(F.col("month")))
         .write.parquet(tampered_path))
        def tampered():
            report, _ = expect_fail("tampered", "test_5_exact_reconciliation_except_all",
                                     parquet=tampered_path)
            assert not report["tests"]["test_4_derived_columns"]["passed"]
            assert report["tests"]["test_5_exact_reconciliation_except_all"]["processed_except_source_count"] == 1
        check("Validation detects small load change and metadata mismatch", tampered)

        missing_path = str(root / "missing_hour_parquet")
        good_df.filter(~first).write.parquet(missing_path)
        def missing_hour():
            _, markdown = expect_fail("missing_hour", "test_1_row_count_and_keys", parquet=missing_path)
            assert "99.99%" in markdown
        check("Validation detects missing hour and reports readback rate", missing_hour)

        infinite_path = str(root / "infinite_parquet")
        (good_df.withColumn("load_mw", F.when(first, F.lit(float("inf"))).otherwise(F.col("load_mw")))
         .write.parquet(infinite_path))
        check("Validation rejects Infinity and writes valid JSON", lambda: expect_fail(
            "infinite", "test_3_nulls_and_valid_values", parquet=infinite_path))
        schema_path = str(root / "bad_schema_parquet")
        good_df.drop("month").write.parquet(schema_path)
        check("Validation reports schema failure", lambda: expect_fail(
            "bad_schema", "test_schema", parquet=schema_path))

        def failed_run_summary():
            summary_path = str(root / "failure_status.json")
            etl.write_summary(summary_path, {"status": "SUCCESS", "old": True})
            with patch("pyspark.sql.readwriter.DataFrameWriter.parquet",
                       side_effect=AssertionError("Unexpected production write")) as writer:
                code = etl.main(["--input", source_file("failed_run", (rows[1][0], "Infinity")),
                                 "--summary-out", summary_path])
                writer.assert_not_called()
            report = json.loads(Path(summary_path).read_text(encoding="utf-8"))
            assert code == 1 and report["status"] == "FAILED", report
            assert "Phụ tải không hợp lệ" in report["error"] and "old" not in report
        # main owns/stops the Spark session, so this case must run last.
        check("Failed ETL replaces stale SUCCESS summary", failed_run_summary)
    finally:
        spark.stop()
        log.close()
        report = {"overall_status": "PASSED" if all(r["passed"] for r in results) else "FAILED",
                  "case_count": len(results), "fixture_directory": str(root), "cases": results}
        etl.write_summary(str(evidence / "error_branch_tests.json"), report)
    return 0 if report["overall_status"] == "PASSED" else 1


if __name__ == "__main__":
    sys.exit(main())
