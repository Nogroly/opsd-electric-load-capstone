"""
jobs/smoke_test.py
==================
Smoke test kiểm tra toàn bộ pipeline:
  1. Tạo SparkSession (local[2])
  2. Tạo DataFrame 3 dòng
  3. Ghi Parquet lên HDFS
  4. Đọc lại từ HDFS
  5. Kiểm tra số dòng và giá trị – in kết quả PASS / FAIL

Chạy trong container spark:
  docker exec spark spark-submit \
      --master local[2] \
      /workspace/jobs/smoke_test.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row

# ── Cấu hình ────────────────────────────────────────────────
HDFS_URI   = "hdfs://namenode:8020"
OUTPUT_DIR = f"{HDFS_URI}/smoke_test/output"

# ── Khởi tạo SparkSession ────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SmokeTest")
    .master("local[2]")
    # Trỏ Spark tới HDFS NameNode
    .config("spark.hadoop.fs.defaultFS", HDFS_URI)
    # Tắt speculation để tránh cảnh báo không cần thiết trên môi trường single-node
    .config("spark.speculation", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SMOKE TEST – BigData Pipeline")
print(f"  Spark version : {spark.version}")
print(f"  HDFS target   : {OUTPUT_DIR}")
print("=" * 60)

# ── 1. Tạo DataFrame 3 dòng ──────────────────────────────────
EXPECTED_ROWS = 3
data = [
    Row(id=1, city="Hanoi",      value=42.0),
    Row(id=2, city="Ho Chi Minh", value=99.5),
    Row(id=3, city="Da Nang",    value=7.77),
]
df = spark.createDataFrame(data)

print("\n[1] DataFrame gốc:")
df.show()
df.printSchema()

# ── 2. Ghi Parquet lên HDFS ──────────────────────────────────
print(f"[2] Đang ghi Parquet → {OUTPUT_DIR} ...")
df.write.mode("overwrite").parquet(OUTPUT_DIR)
print("    Ghi thành công.")

# ── 3. Đọc lại từ HDFS ──────────────────────────────────────
print(f"[3] Đang đọc Parquet ← {OUTPUT_DIR} ...")
df_read = spark.read.parquet(OUTPUT_DIR)
print("    Đọc thành công.")
df_read.show()

# ── 4. Kiểm tra ──────────────────────────────────────────────
actual_rows   = df_read.count()
actual_cities = set(row.city for row in df_read.collect())
expected_cities = {"Hanoi", "Ho Chi Minh", "Da Nang"}

row_ok  = actual_rows == EXPECTED_ROWS
city_ok = actual_cities == expected_cities

print("\n[4] Kết quả kiểm tra:")
print(f"    Số dòng  : {actual_rows} (mong đợi {EXPECTED_ROWS}) → {'OK' if row_ok else 'FAIL'}")
print(f"    Giá trị  : {actual_cities} → {'OK' if city_ok else 'FAIL'}")

if row_ok and city_ok:
    print("\n✅  SMOKE TEST PASSED – Pipeline hoạt động bình thường.")
    spark.stop()
    sys.exit(0)
else:
    print("\n❌  SMOKE TEST FAILED – Kiểm tra lại cấu hình.")
    spark.stop()
    sys.exit(1)

