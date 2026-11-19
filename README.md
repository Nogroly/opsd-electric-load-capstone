# Project 4 Phân tích và dự báo phụ tải điện Đức

Demo chạy tuần tự **Plan A → Plan B → Plan C** trong PowerShell. Mỗi lệnh tạo kết quả, kiểm định rồi mở dashboard để trình bày trước khi chuyển sang phần tiếp theo.

- **Plan A** xử lý CSV, ghi và kiểm định Parquet trên HDFS, phân tích phụ tải theo giờ, tháng, nhóm ngày, đối chiếu trung bình ngày với MA7/MA30 để thấy tác dụng làm mượt, và nhiệt đồ.
- **Plan B** dự báo trước một giờ bằng Random Forest và so sánh với hai baseline trên cùng tập test.
- **Plan C** phát lại 24 giờ lịch sử thành JSON theo đợt, dùng mô hình Plan B để dự báo và kiểm tra checkpoint, dữ liệu trùng.

## Cập nhật sau góp ý buổi báo cáo 19/11/2026

Biểu đồ xu hướng Plan A đã có trung bình ngày (xám), MA7 (xanh) và MA30 (cam), cùng trục MW và ngày UTC để thấy tác dụng làm mượt. Không thay số liệu nguồn hoặc công thức cửa sổ. Đối chiếu 403 nhóm thống kê và 365 giá trị ngày/cửa sổ trượt đều đạt. Tháng 01/2019 có 744 giờ, trung bình 61.597,139785 MW từ CSV, khớp bảng SQL.

- GROUP BY gom các giờ cùng tháng; AVG(load_mw) tính trung bình; ORDER BY month_utc mặc định tăng dần (ASC).
- Hai baseline và RF được đánh giá trên cùng 2.208 giờ test. MAE RF 1.135,29 MW; giảm MAE 42,60% so với giờ gần nhất, không phải độ chính xác 42,60%.
- [Tóm tắt workflow cho bạn nhóm](reporting/2026-11-19/01_TOM_TAT_WORKFLOW_VA_HUONG_DAN_BAO_CAO.md) và [góp ý đã xử lý, việc biên tập tiếp theo](reporting/2026-11-19/02_GOP_Y_THAY_VA_KET_QUA_CAP_NHAT.md).
- [Xem nhanh biểu đồ ba đường mới](outputs/trends/dashboard-preview.png). Dashboard tương tác mở từ HTML sau khi tải repo; GitHub không chạy HTML trực tiếp.
- Docs báo cáo và slide do nhóm tiếp tục chỉnh. Các tài liệu theo ngày trong reporting/ và docs/ giữ bối cảnh từng lần chuẩn bị; không tự coi bản Word cũ trong repo là bản biên tập cuối.

## Chuẩn bị trước buổi demo

Cần Docker Desktop đang chạy Linux containers, Docker Compose và PowerShell. Spark và Python chạy trong container. Docker Compose gắn thư mục dự án vào /workspace.

Mở PowerShell tại thư mục chứa README này và kiểm tra môi trường.

```powershell
docker version
docker compose version
docker compose up -d
docker compose ps
```

Ba dịch vụ của dự án là namenode, datanode và spark. Chờ NameNode báo healthy.

Tải dữ liệu trước giờ demo nếu máy chưa có CSV.

```powershell
.\download_opsd.ps1
```

Script tải CSV OPSD phiên bản 06/10/2020 và đối chiếu kích thước cùng SHA-256 với metadata. File nguồn 130.339.665 byte được lưu ở data/raw/time_series_60min_singleindex.csv. Máy đã có CSV hợp lệ chỉ cần kiểm tra lại bằng lệnh trên. [Nguồn OPSD](https://data.open-power-system-data.org/time_series/2020-10-06/)

## 1 Chạy Plan A và trình bày dữ liệu cùng phân tích

```powershell
.\run_plan_a.ps1
```

Đợi PLAN A COMPLETED rồi xem dashboard tự mở.

| Nhãn terminal | Công việc và kết quả |
|---|---|
| A0/6 | Khởi động Docker Compose. |
| A1/6 | Chọn hai cột giờ UTC và phụ tải thực tế Đức, lọc năm 2019, ghi Parquet lên HDFS. |
| A2/6 | Đọc lại HDFS, kiểm định dữ liệu và tính các bảng Spark SQL. |
| A3/6 | Đối chiếu các bảng tổng hợp với CSV nguồn. |
| A4/6 | Tính MA7/MA30, nhiệt đồ theo thứ và giờ, tháng và giờ. |
| A5/6 | Kiểm tra cửa sổ trượt và các nhóm tổng hợp. |
| A6/6 | Dựng dashboard HTML từ kết quả đã kiểm định. |

Mở [dashboard Plan A](outputs/trends/index.html). Trình bày trung bình theo giờ, tháng, ngày thường/cuối tuần và MA7/MA30. Có nút nhảy đến từng biểu đồ ở đầu trang. Các bảng SQL và biểu đồ riêng nằm ở [outputs/analytics](outputs/analytics/).

Kết quả đối chiếu gồm 8.760 giờ, 8/8 nhóm kiểm định đạt và 0 dòng khác biệt CSV–Parquet theo cả hai chiều. Trung bình cuối tuần thấp hơn 18,14% so với ngày thường trong mẫu 2019. Biểu đồ tháng dùng UTC, còn giờ và nhóm ngày dùng Europe/Berlin. Đơn vị phụ tải là MW.

## 2 Chạy Plan B và so sánh dự báo

```powershell
.\run_plan_b.ps1
```

Plan B đọc Parquet của Plan A. Lệnh tự tạo và kiểm tra hai baseline trước khi huấn luyện Random Forest.

| Nhãn terminal | Công việc và kết quả |
|---|---|
| B0/5 | Kiểm tra môi trường Docker. |
| B1/5 | Tạo hai dự báo baseline từ giờ gần nhất và đúng 168 giờ trước. |
| B2/5 | Đối chiếu mốc giờ, dự báo và sai số baseline với CSV nguồn. |
| B3/5 | Huấn luyện Random Forest và so sánh ba phương pháp trên cùng giờ mục tiêu. |
| B4/5 | Kiểm tra độc lập các dự báo và chỉ số. |
| B5/5 | Dựng dashboard so sánh ba phương pháp. |

Đợi PLAN B COMPLETED rồi xem [dashboard Plan B](outputs/ml_forecast/index.html).

| Phương pháp | MAE test (MW) |
|---|---:|
| Random Forest | 1.135,29 |
| Giờ gần nhất | 1.977,92 |
| Cùng giờ 168 giờ trước | 2.897,08 |

Huấn luyện dùng tháng 1–8, chọn cấu hình bằng tháng 9, test tháng 10–12. Tập test có 2.208 giờ. MAE là trung bình trị tuyệt đối sai số, cùng đơn vị MW. Random Forest giảm MAE 42,60% so với baseline giờ gần nhất trên tập test này. Mô hình giả định số đo giờ trước đã sẵn có.

Có thể tính lại MAE ngay khi trình bày.

```powershell
.\demo_verify_mae.ps1
```

## 3 Chạy Plan C và quan sát dữ liệu đến theo đợt

```powershell
.\run_plan_c.ps1
```

Plan C lấy 24 giờ ngày 01/10/2019 từ dữ liệu Plan A và nạp mô hình vừa tạo ở Plan B. Mỗi lượt có ID, thư mục JSON, HDFS đầu ra và checkpoint riêng.

| Nhãn terminal | Công việc và kết quả |
|---|---|
| C0/8 | Kiểm tra môi trường Docker. |
| C1/8 | Chuẩn bị 24 giờ lịch sử để phát lại. |
| C2/8 → C4/8 | Phát và xử lý lần lượt 8 → 16 → 24 giờ. Dashboard cập nhật sau từng đợt. |
| C5/8 | Khởi động lại query với cùng checkpoint và kiểm tra số hàng. |
| C6/8 | Gửi thêm một file chứa mốc giờ trùng, xử lý và kiểm tra đầu ra. |
| C7/8 | Đối chiếu nguồn, HDFS và dự báo qua các giai đoạn. |
| C8/8 | Dựng dashboard cuối cùng và báo cáo. |

[Dashboard Plan C](outputs/streaming/index.html) tự mở sau đợt đầu có 8 giờ. Giữ trang mở để quan sát cập nhật. Đợi PLAN C COMPLETED để kết luận toàn bộ phép thử đã đạt.

Kết quả kỳ vọng là 25 JSON được đọc, 24 giờ duy nhất trên HDFS, chạy lại thêm 0 hàng và file trùng thêm 0 hàng. Đây là replay dữ liệu lịch sử. Mô hình được huấn luyện trong Plan B rồi nạp vào luồng để dự báo.

## 4 Kiểm tra sau cả ba phần

```powershell
.\demo_preflight.ps1
```

EVIDENCE PREFLIGHT PASSED xác nhận trạng thái kiểm định và quan hệ giữa các lượt ETL, baseline, ML và replay. Nếu chạy lại Plan A, tiếp tục chạy B rồi C để tạo lại đầy đủ các đầu ra phụ thuộc. Nếu chỉ chạy lại B, chạy tiếp C để dùng mô hình mới.

Dừng ở lỗi đầu tiên và đọc nhãn A/B/C cùng thông báo lỗi. Các dòng INFO, Stage và Task thể hiện Spark đang làm việc. Dòng COMPLETED chỉ xuất hiện sau khi phần tương ứng đã chạy xong.

## Tùy chọn và thời gian demo

- Mỗi script hỗ trợ -NoOpen để tự mở dashboard khi cần.
- Plan A hỗ trợ -SkipEtl khi muốn dùng lại tập Parquet đã có. Khi demo toàn tuyến từ CSV, dùng lệnh mặc định.
- Script run_forecast.ps1 vẫn có thể tạo báo cáo baseline riêng khi cần xem chi tiết. Luồng demo dùng run_plan_b.ps1 để thực hiện baseline và Random Forest trong một phần.
- demo_preflight.ps1 -CoreOnly kiểm tra A/B khi C chưa chạy. Sau C, dùng demo_preflight.ps1 đầy đủ.

Một lượt kiểm thử đã đo khoảng 3 phút cho A, 2,5 phút cho B và 5 phút cho C. Thời gian phụ thuộc máy và trạng thái Docker. Khi tập demo trực tiếp, bấm giờ cả thời gian chạy lẫn phần nói.

## Tài liệu và nơi xem kết quả

- [Kịch bản Google Docs để luyện thuyết trình](https://docs.google.com/document/d/1W0B1vqy_RHZutBpG6PW8aYRtPcKkjLEr97Jh2CLzP5c/edit).
- [Kịch bản thuyết trình slide và demo trong PowerShell của Antigravity](DEMO_SLIDE_POWERSHELL.md).
- [Kịch bản chạy demo A → B → C](DEMO_A_B_C.md).
- [Hướng dẫn sửa slide và chụp minh chứng](reporting/2026-11-17/HUONG_DAN_SUA_SLIDE_VA_CHUP_MINH_CHUNG.md).
- [Báo cáo Google Docs](https://docs.google.com/document/d/1JPeIrLZtFNuXXrQxdubIThnghMXupaitLNkaSWNu2bE/edit).
- jobs/ chứa ETL, kiểm định, SQL, ML, replay và dựng dashboard.
- sql/ chứa truy vấn tổng hợp, tests/ chứa đối chiếu độc lập.
- outputs/quality, analytics, trends, forecast, ml_forecast và streaming chứa kết quả từng phần.
- reporting/ lưu tài liệu chuẩn bị theo ngày. Dùng README và DEMO_A_B_C.md cho thứ tự chạy hiện tại.

## Môi trường kỹ thuật

| Thành phần | Cấu hình |
|---|---|
| Spark | apache/spark:3.5.3-java17-python3, local[2] |
| HDFS | apache/hadoop:3.3.6, một NameNode và một DataNode |
| Lưu trữ | Parquet trên HDFS, replication 1 |
| Mạng và dữ liệu | Docker Compose, thư mục dự án gắn vào /workspace, HDFS lưu trong Docker volumes |

Giao diện HDFS ở http://localhost:9870. Spark UI ở http://localhost:4040 trong lúc job đang chạy.

Dừng dịch vụ sau khi demo bằng lệnh dưới. Docker volumes giữ dữ liệu HDFS để dùng lại.

```powershell
docker compose stop
```
