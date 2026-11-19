# Thẻ nói demo 7 phút — Project 4

Tài liệu này lưu kịch bản từ lượt tập trước. Luồng hiện tại chạy trực tiếp A → B → C. Dùng [kịch bản slide và PowerShell](../DEMO_SLIDE_POWERSHELL.md) và [README](../README.md) cho buổi bảo vệ.

**Trước giờ trình bày:** mở Docker Desktop và PowerShell tại thư mục dự án. Chạy `./demo_preflight.ps1`; nếu ba container chưa chạy, gõ `docker compose up -d`, chờ rồi `docker compose ps`. Mở sẵn bốn dashboard HTML trong `outputs/analytics`, `outputs/trends`, `outputs/ml_forecast`, `outputs/streaming`. Dùng kết quả đã kiểm định để giữ demo gọn; chỉ chạy lại job nặng khi thầy muốn xem quá trình.

| Lúc | Màn hình/lệnh | Nói với thầy |
|---|---|---|
| 0:00–0:45 | Slide 5 sơ đồ | “Bài toán là hiểu và ước lượng phụ tải điện Đức theo giờ, đơn vị MW. Nguồn OPSD CSV → Spark ETL → HDFS/Parquet đã kiểm định. Từ đó có ba phần: phân tích, ML dự báo và replay streaming.” |
| 0:45–1:40 | `docker compose ps`; `docker exec namenode hdfs dfs -ls -R /bigdata/opsd/processed/de_2019` | “Ba container là môi trường trên một máy. Lệnh thứ hai chỉ liệt kê Parquet **thật trên HDFS**. Spark chọn 2 cột nguồn và lọc 2019 UTC thành 8.760 giờ.” |
| 1:40–2:45 | Slide 6; dashboard `outputs/analytics/index.html` rồi `outputs/trends/index.html` | “Spark SQL cho trung bình; MA7/MA30 làm mượt nhịp năm. Nhiệt đồ cho mẫu theo thứ/giờ, không chứng minh nguyên nhân. MA30 đỉnh đầu tháng 2, thấp cuối tháng 8.” |
| 2:45–3:15 | Slide 7; `Get-Content .\outputs\quality\quality_summary.json -Raw` | “Biểu đồ dùng tập đã đọc lại từ HDFS và kiểm 8 nhóm: cấu trúc, giờ thiếu/trùng, null, phụ tải và đối soát từng hàng CSV–Parquet. Kết quả PASSED.” |
| 3:15–4:35 | Slide 8–9; dashboard `outputs/ml_forecast/index.html`; `./demo_verify_mae.ps1` | “Mô hình nhận phụ tải 1/24/168 giờ trước và lịch UTC, dự báo giờ kế. Train tháng 1–8, chọn bằng tháng 9, test tháng 10–12. MAE là trung bình trị tuyệt đối sai số. Script tính lại từ 2.208 hàng: RF 1.135 MW, giờ trước 1.978 MW, giảm 42,60% trên test.” |
| 4:35–5:55 | Slide 10; dashboard `outputs/streaming/index.html`; `Get-Content .\outputs\streaming\checks.json -Raw` | “Plan C phát lại 24 giờ lịch sử thành JSON 8→16→24. Structured Streaming đọc file mới, nạp mô hình offline, ghi HDFS và checkpoint. Chạy lại và thêm file trùng vẫn 24 giờ duy nhất; 25 file đã đọc.” |
| 5:55–6:35 | Slide 11 | “Giới hạn: một máy/1 DataNode, một năm Đức, chưa có nguồn trực tiếp hay độ trễ phát hành số đo. Hướng tiếp: thêm nhiều năm, thời tiết/ngày lễ và đánh giá qua nhiều giai đoạn.” |
| 6:35–7:00 | Hỏi đáp | “Em có thể mở từng file kiểm định hoặc giải thích bất kỳ bước nào trên terminal.” |

**Nếu thầy muốn xem chạy thật:** `./run_plan_c.ps1` tạo run ID mới; trang streaming tự cập nhật khi xử lý 8, 16, 24 giờ. Spark có thể mất vài phút khởi động. Không chạy lại Plan A/B giữa buổi demo ngắn vì chúng ghi lại đầu ra và đổi run ID. Nếu vẫn muốn chạy từ đầu, nói rõ đó là một lượt mới và đợi dòng `PLAN ... COMPLETED`.

**Câu “terminal đang làm gì?”:** đọc dòng nhãn `[A…]`, `[1/6]`, `[1/8]` và đối chiếu với mục 7 của [bản học kỹ thuật](02_ky_thuat_cho_vinh.md). Dòng `INFO/Stage/Task` chỉ là tiến trình Spark; bằng chứng kết thúc là `PASSED` và `COMPLETED`, không phải nhiều chữ chạy trên màn hình.

**Dự phòng:** nếu Docker chưa lên, vẫn trình bày dashboard và tệp kiểm định đã tạo, nói rõ đang xem kết quả đã lưu. Bản chạy ngày 14/11 đã đồng bộ Plan A/B/C cùng ETL run; nếu sau này chạy lẻ một phần, kiểm tra lại preflight trước khi tuyên bố mọi kết quả cùng một lượt.
