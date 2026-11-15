# Rà 11 slide — nội dung đã chốt, việc còn chỉnh bằng Canva

| Slide | Vai trò khi nói | Điều cần giữ | Gợi ý chỉnh hình thức |
|---:|---|---|---|
| 1 | Tên đề tài, nhóm | Phân tích phụ tải Đức + dự báo 1 giờ | Giữ title ngắn, chữ lớn; hình năng lượng không watermark. |
| 2 | Lộ trình | Plan A/B/C và demo | Rút text nếu xem trên màn chiếu bị nhỏ. |
| 3 | Câu hỏi/mục tiêu | Biến động giờ–tuần–mùa; ML so 2 baseline | Chỉ hai câu hỏi; không hứa giải thích nguyên nhân vật lý. |
| 4 | Dữ liệu | OPSD CSV; 2 cột dùng; 8.760 giờ Đức 2019 UTC, MW | Ghi rõ `DE_load_actual_entsoe_transparency`; không nói đã làm sạch 300 cột. |
| 5 | Sơ đồ khối | CSV → Spark ETL → HDFS/Parquet → QA → SQL/trend, ML, replay streaming | Giữ logo Spark/Hadoop/Parquet; nêu đầu vào–đầu ra mỗi khối. Plan C ở dòng cuối, không phải nguồn trực tiếp. |
| 6 | Kết quả xu hướng | Đường ngày, MA7, MA30 và hai cực trị MA30 | Phóng biểu đồ khi nói; MA30 là trung bình trượt, không phải dự báo. |
| 7 | Bằng chứng Plan A | 8/8 nhóm QA; Parquet đọc lại 8.760 giờ; `exceptAll` hai chiều 0 | Xem như slide chứng minh sau biểu đồ slide 6. |
| 8 | Cách dự báo | Random Forest dùng lag 1/24/168 giờ và lịch UTC | Nếu còn chỗ, ghi rõ target là giờ kế tiếp, không dùng giá trị giờ cần dự báo. |
| 9 | Kết quả dự báo | MAE test RF 1.135,29 MW; baseline tốt nhất 1.977,92 MW; giảm 42,60% trên 2.208 giờ | Phóng số MAE và đặt chú thích “cùng tập test” sát ba cột. |
| 10 | Plan C | 8→16→24 JSON lịch sử; 25 đọc, 24 giờ duy nhất; restart/trùng +0 | Logo Spark ở đọc luồng, Hadoop ở HDFS/checkpoint. |
| 11 | Chốt | Ba plan đã chạy; giới hạn một máy, một năm, replay hữu hạn | Kết thúc bằng 1–2 câu, không thêm kết luận nhân quả. |

**Số liệu gốc:** `outputs/quality/`, `outputs/trends/`, `outputs/ml_forecast/`, `outputs/streaming/`. Slide 6 dùng biểu đồ tạo từ `outputs/trends/daily_trend.csv`; slide 9 dùng `metrics.csv`. Mọi chỉnh sửa đồ họa nên giữ nguyên các số này. Nếu thay ảnh, dùng `assets/slide/` hoặc ảnh có quyền sử dụng; tránh watermark.

**Mạch nói 30 giây cho sơ đồ slide 5:** “Em chỉ lấy timestamp UTC và phụ tải thực tế Đức từ CSV OPSD. Spark lọc 2019 thành 8.760 giờ; HDFS/Parquet giữ tập đã chuẩn hóa; tám nhóm kiểm định đọc lại và đối soát với nguồn. Từ cùng tập này, SQL/trend giải thích biến động lịch sử, ML dự báo giờ kế tiếp, còn streaming phát lại 24 giờ lịch sử để kiểm tra luồng và checkpoint.”
