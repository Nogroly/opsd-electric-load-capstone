# Nội dung ngắn cho bạn nhóm chỉnh Canva và luyện nói

**Một câu:** Project 4 phân tích phụ tải điện thực tế Đức theo giờ (MW) từ OPSD 2019; dựng ETL/kiểm định, phân tích xu hướng, dự báo ML 1 giờ và demo phát lại dữ liệu theo đợt.

**Sơ đồ slide:** `OPSD CSV (2 cột sử dụng)` → `PySpark ETL (lọc Đức 2019)` → `HDFS/Parquet (8.760 giờ)` → `Spark QA (8/8 nhóm)` → ba nhánh: `Spark SQL + MA7/MA30/nhiệt đồ`, `Spark ML Random Forest vs 2 baseline`, `JSON replay → Spark Structured Streaming → HDFS/checkpoint → dashboard`. Dùng logo Spark cho ETL, SQL, ML và streaming; logo Hadoop/HDFS và Parquet cho lưu trữ. Docker Compose là lớp môi trường, không phải nguồn dữ liệu.

| Phần | Số cần nhớ | Cách đọc đúng |
|---|---:|---|
| Nguồn CSV | 50.401 dòng, 300 cột | Đồ án chỉ dùng timestamp UTC và actual load Đức; không xử lý cả 300 cột. |
| Tập nghiên cứu | 8.760 giờ năm 2019 | Đủ liên tục theo UTC, đơn vị MW. |
| QA | 8/8 nhóm đạt | Đối chiếu cả CSV đã chọn và Parquet đọc lại. |
| Xu hướng | MA30 cao 62.917,75; thấp 51.446,43 MW | Chỉ là biến động mẫu 2019, không giải thích nguyên nhân. |
| Random Forest | MAE test 1.135,29 MW | 2.208 giờ test tháng 10–12; so baseline giờ trước 1.977,92 MW, giảm 42,60%. |
| Streaming | 8→16→24 giờ; 25 JSON, 24 giờ duy nhất | Replay lịch sử, checkpoint + bỏ trùng; không phải dữ liệu trực tiếp. |

**Tên ba kế hoạch:** Plan A = ETL, lưu, QA, Spark SQL; Plan B = xu hướng và ML; Plan C = Structured Streaming replay. Random Forest thuộc machine learning, không phải LLM. MA7/MA30 là thống kê làm mượt, không phải AI dự báo. Dashboard HTML là cách trình bày kết quả đã tính, không tự đọc điện kế.

**Những câu cần sửa nếu thấy trên slide:** “chưa có streaming” → đã có replay hữu hạn; “dự báo cơ sở là kết quả cuối” → đã có Random Forest; “dữ liệu thời gian thực” → “phát lại dữ liệu lịch sử theo đợt”; “hệ thống phân tán chịu lỗi” → “demo trên một máy với 1 DataNode”.

**Vai trò khi thuyết trình:** Vinh giải thích dữ liệu, lệnh, chất lượng, MAE và giới hạn; bạn nhóm dẫn mạch bài toán, chỉ biểu đồ và hỗ trợ chuyển slide. Cùng luyện một lần: người xem phải trả lời được “khối này nhận gì, làm gì, trả gì?”.
