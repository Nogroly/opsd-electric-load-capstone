# Project 4 — Phân tích và dự báo phụ tải điện Đức

**Đọc trong 3 phút:** Đề tài dùng [OPSD Time series 2020-10-06](https://data.open-power-system-data.org/time_series/2020-10-06/) để dựng một pipeline có thể kiểm tra lại: 50.401 dòng CSV nguồn → chọn phụ tải thực tế Đức năm 2019 → **8.760 giờ** trên HDFS/Parquet → phân tích xu hướng và dự báo trước 1 giờ → phát lại 24 giờ lịch sử bằng Spark Structured Streaming. Đơn vị phụ tải là **MW**. Đây là capstone trên **một máy**, không phải hệ thống nhận số đo điện trực tiếp.

```text
OPSD CSV (utc_timestamp, DE_load_actual_entsoe_transparency)
  └─ PySpark ETL → HDFS/Parquet (Đức 2019, 8.760 giờ) → 8 nhóm kiểm định
       ├─ Plan A: Spark SQL → bảng ngày/tháng/giờ → dashboard
       ├─ Plan B: MA7/MA30 + nhiệt đồ → dashboard xu hướng
       ├─ Plan B: Spark ML Random Forest + 2 baseline → MAE/RMSE → dashboard
       └─ Plan C: JSON replay 24 giờ → Structured Streaming → HDFS/checkpoint → dashboard
```

## Kết quả đã kiểm chứng (lượt chạy 14/11/2026)

| Phần | Bằng chứng | Kết quả |
|---|---|---|
| Plan A | `outputs/quality/quality_summary.json`, `outputs/analytics/verification.json` | 8.760 giờ liên tục; 8/8 nhóm kiểm định đạt; CSV–Parquet không lệch hàng. |
| Plan B xu hướng | `outputs/trends/verification.json` | MA30 cao 62.917,75 MW ngày 07/02, thấp 51.446,43 MW ngày 25/08. |
| Plan B dự báo | `outputs/ml_forecast/metrics.csv`, `demo_verify_mae.ps1` | Random Forest MAE test 1.135,29 MW trên 2.208 giờ; baseline giờ trước 1.977,92 MW; giảm 42,60%. |
| Plan C | `outputs/streaming/checks.json` | 25 JSON đọc, 24 giờ duy nhất; restart và file trùng đều +0 hàng. |

Các nhánh hiện cùng ETL run ID; Plan C dùng đúng ML run của lần chạy này. `./demo_preflight.ps1` xác nhận trạng thái, lineage và Docker. Bản tóm tắt để thầy nhận xét ở [docs/01_bao_cao_gui_thay.md](docs/01_bao_cao_gui_thay.md); bản giải thích toàn bộ kỹ thuật ở [docs/02_ky_thuat_cho_vinh.md](docs/02_ky_thuat_cho_vinh.md).

## Xem kết quả nhanh, không cần chạy lại

Mở bốn file HTML có sẵn: `outputs/analytics/index.html`, `outputs/trends/index.html`, `outputs/ml_forecast/index.html`, `outputs/streaming/index.html`. Đọc [docs/03_demo_7_phut.md](docs/03_demo_7_phut.md) để biết phải chỉ vào đâu và nói gì. [Slide Canva chỉnh sửa](https://www.canva.com/d/38WkXqBjUBGzYt6) gồm 11 trang; slide 5 là sơ đồ khối, slide 6 là xu hướng, 8–9 là dự báo, 10 là streaming. [Hướng dẫn rà từng slide](docs/05_slide_content_review.md) dành cho bạn nhóm.

[Trang mở slide cho thầy và nhóm](slides/README.md) có cả link xem và link chỉnh sửa; Canva cần cấp quyền riêng.

## Chạy lại từ đầu trên Windows

Yêu cầu: Docker Desktop đang chạy ở Linux containers, Docker Compose, PowerShell, internet để tải CSV và image. Từ thư mục repo:

```powershell
.\download_opsd.ps1
.\run_plan_a.ps1 -NoOpen
.\run_forecast.ps1 -NoOpen
.\run_plan_b.ps1 -NoOpen
.\run_plan_c.ps1 -NoOpen
.\demo_preflight.ps1
.\demo_verify_mae.ps1
```

**Đợi mỗi lệnh có `COMPLETED` rồi mới sang lệnh sau.** Tệp CSV 130.339.665 byte không nằm trong GitHub; `download_opsd.ps1` lấy từ OPSD và đối chiếu SHA-256 với metadata. Docker Compose tạo NameNode, DataNode, Spark; dữ liệu HDFS nằm trong Docker volume của máy chạy, nên bản clone phải chạy Plan A trước khi đọc HDFS. `outputs/` trong repo là **snapshot minh chứng** để đọc ngay, không tự chứng minh container trên máy khác đã chạy.

## Giới hạn và lý do chọn công nghệ

Spark thống nhất ETL, SQL, ML và Structured Streaming; HDFS/Parquet cho tập đã chuẩn hóa có schema, đọc lại được. Docker Compose giúp dựng môi trường ba dịch vụ trên một máy. Tập 8.760 giờ riêng Đức vẫn có thể xử lý bằng pandas; lựa chọn Spark/HDFS nhằm học và minh họa kiến trúc, **không khẳng định tốc độ hoặc chịu lỗi nhiều máy**. JSON replay đủ để kiểm tra microbatch/checkpoint; chưa cần Kafka khi chưa có nguồn sự kiện thật. Random Forest là machine learning hồi quy, không phải LLM. Mô hình giả định số đo giờ trước đã sẵn có, chưa tính độ trễ công bố; chưa có thời tiết, ngày lễ hay dữ liệu nhiều năm để kết luận tổng quát.

## Bản đồ repo và cách cộng tác

| Thư mục/file | Ý nghĩa |
|---|---|
| `jobs/`, `sql/`, `tests/` | ETL, phân tích, dự báo, streaming và kiểm tra độc lập. |
| `run_plan_*.ps1`, `run_forecast.ps1` | Lệnh điều phối; các nhãn `[A…]`, `[1/6]`, `[1/8]` giúp đọc terminal. |
| `outputs/` | Kết quả snapshot đã kiểm định và dashboard HTML. |
| `data/metadata/` | Metadata nguồn; `data/raw/` được tải riêng. |
| `docs/` | Báo cáo gửi thầy, bản học kỹ thuật, kịch bản demo, nội dung slide, lịch tập. |
| `assets/slide/` | Hình biểu đồ và minh họa để chỉnh Canva. |

**Bạn nhóm chỉnh slide và Docs:** ưu tiên độ đọc rõ, trật tự kể chuyện, caption và logo công nghệ. Có thể đề xuất sửa câu chữ trong `docs/05_slide_content_review.md`. Trước khi đổi số, đối chiếu `outputs/` và ghi nguồn ngay trên slide. Giữ rõ ba mức: kết quả thực đã làm; giới hạn; ý tưởng sau này. Không gọi replay lịch sử là dữ liệu trực tiếp hay gọi MA30 là AI dự báo.

Repo dự kiến **Private**. Xem [quyền truy cập và bàn giao](docs/00_access_and_handoff.md): thầy và bạn nhóm cần được mời vào GitHub, còn slide Canva cần cấp quyền riêng.

**Khi cần góp ý:** tạo issue/nhận xét trong repo với số slide, câu đang có, câu đề xuất và lý do. Đừng sửa kết quả kiểm định hoặc chỉ số mô hình chỉ để slide đẹp hơn.
