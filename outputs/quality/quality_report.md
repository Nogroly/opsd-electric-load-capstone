# Báo Cáo Kiểm Định Chất Lượng Dữ Liệu Sau ETL (Post-ETL Data Quality Report)
**Đối tượng kiểm định:** Dữ liệu phụ tải thực tế CHLB Đức năm 2019 (OPSD DE 2019)  
**Vị trí HDFS:** `hdfs://namenode:8020/bigdata/opsd/processed/de_2019`  
**Tệp nguồn đối soát:** `/workspace/data/raw/time_series_60min_singleindex.csv`  
**Thời gian thực thi:** 2026-11-14 10:10:44 UTC  
**Trạng thái tổng thể:** **PASSED**  

---

## 1. Dòng Dữ Liệu & Phân Loại Nguồn Không Chồng Lấn (MECE Lineage)

| Phân loại dòng dữ liệu | Số lượng dòng thực tế | Mẫu số | Tỷ lệ (%) | Ý nghĩa |
|---|---|---|---|---|
| **Tổng số dòng nguồn (Total Source Rows)** | **50,401** | 50,401 | 100.00% | File CSV gốc toàn bộ các năm (2014–2020) |
| **Nhóm 1: Timestamp trống hoặc lỗi parse** | **0** | 50,401 | 0.00% | Dòng có timestamp không xác định được *(dừng pipeline nếu > 0)* |
| **Nhóm 2: Timestamp hợp lệ ngoài năm 2019** | **41,641** | 50,401 | 82.62% | Quan sát hợp lệ của các năm khác *(không phải dữ liệu bẩn)* |
| **Nhóm 3: Timestamp hợp lệ trong năm 2019** | **8,760** | 50,401 | 17.38% | Dữ liệu mục tiêu trong khoảng `[2019-01-01 00:00:00, 2020-01-01 00:00:00)` |
| **Dòng lỗi trong phạm vi nguồn năm 2019** | **0** | 8,760 | 0.00% | Phụ tải null, NaN, Inf hoặc không lớn hơn 0 trong năm 2019 |
| **Số dòng chuẩn bị ghi (từ ETL summary)** | **8,760 dòng** | — | — | Ghi nhận độc lập từ báo cáo ETL trước khi ghi HDFS |
| **Số dòng đọc lại từ HDFS (Readback)** | **8,760 dòng** | 8,760 dòng | 100.00% | Đọc thực tế từ file Parquet trên HDFS |

> **Quy tắc phân loại:**  
> Tổng ba nhóm $G_1 + G_2 + G_3 = 50,401$; tổng dòng nguồn $50,401$: **KHỚP**. Nhóm 2 có timestamp hợp lệ ngoài phạm vi; không kết luận phụ tải của nhóm này đã được kiểm định.

---

## 2. Kết Quả Các Bài Kiểm Định Chất Lượng

| Bài kiểm tra | Kết quả | Chi tiết kiểm tra |
|---|:---:|---|
| **Kiểm tra chất lượng dữ liệu nguồn làm baseline** | **PASS** | Nguồn hợp lệ làm baseline |
| **Schema và kiểu dữ liệu của Parquet** | **PASS** | Schema đúng |
| **Báo cáo ETL khớp nguồn, đích, SHA256 và số dòng** | **PASS** | Báo cáo ETL nhất quán |
| **Kiểm tra số dòng đọc lại, số dòng chuẩn bị ghi và khóa trùng** | **PASS** | Số dòng đọc lại khớp chuẩn bị ghi, không có khóa trùng |
| **Kiểm tra phạm vi thời gian UTC và tính liên tục (không thiếu giờ)** | **PASS** | Phạm vi chính xác và liên tục 100% |
| **Kiểm tra NULL, số hữu hạn và phụ tải > 0** | **PASS** | Không có NULL, số hữu hạn và phụ tải > 0 |
| **Kiểm tra tính nhất quán của country, year, month, source_file** | **PASS** | Cột dẫn xuất hoàn toàn nhất quán |
| **Đối chiếu chính xác bộ ba (country, timestamp_utc, load_mw) bằng exceptAll 2 chiều** | **PASS** | Hai phía không có phần khác biệt (khớp chính xác 100%) |

---

## 3. Chi Tiết Phương Pháp Đối Soát Hai Chiều (Bidirectional exceptAll)

Quy trình đối soát chất lượng áp dụng phương pháp so sánh tập hợp chính xác của Apache Spark:
1. **Trích xuất bộ ba thuộc tính cốt lõi:**  
   Lấy 3 cột `(country, timestamp_utc, load_mw)` từ cả hai phía, ép về cùng kiểu dữ liệu (`StringType`, `TimestampType`, `DoubleType`) và cùng thứ tự cột:
   - Tập $P$: Dữ liệu đọc lại từ HDFS Parquet.
   - Tập $S$: Dữ liệu đọc từ CSV nguồn thuộc nhóm 3 (năm 2019).
2. **Đối chiếu hai chiều không dùng ngưỡng sai số:**
   - Chiều xuôi: $P \setminus S = P.\text{exceptAll}(S)$. Kết quả: **0 dòng lệch**.
   - Chiều ngược: $S \setminus P = S.\text{exceptAll}(P)$. Kết quả: **0 dòng lệch**.
3. **Tiêu chí kết luận:**  
   Chỉ khi cả hai tập hiệu đều rỗng ($|P \setminus S| = 0$ và $|S \setminus P| = 0$), hai tập hợp mới được xác nhận là đồng nhất tuyệt đối 100%.

---

## 4. Kết Luận Kiểm Định

### Kết luận: ĐẠT TIÊU CHUẨN CHẤT LƯỢNG (PASSED)
Tất cả các tiêu chí kiểm định chất lượng đều đạt chuẩn:
- Dữ liệu nguồn đạt chuẩn baseline (không có timestamp lỗi, không có phụ tải null/lỗi trong năm 2019).
- Số dòng đọc lại từ HDFS khớp chính xác với số dòng chuẩn bị ghi từ ETL (8,760 dòng).
- Không có khóa logic trùng lặp `(country, timestamp_utc)`.
- Phạm vi thời gian liên tục 100% từ `2019-01-01 00:00:00` đến `2019-12-31 23:00:00 UTC` (gaps = 0).
- Không có giá trị NULL, NaN, Infinite; toàn bộ phụ tải lớn hơn 0 MW.
- Đối chiếu hai chiều bằng `exceptAll` trên bộ ba `(country, timestamp_utc, load_mw)` không phát hiện bất kỳ sai lệch nào giữa HDFS và nguồn.
