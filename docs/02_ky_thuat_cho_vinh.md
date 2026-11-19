# Project 4 — bản học kỹ thuật để tự giải thích và kiểm chứng

Lưu ý về thứ tự demo hiện tại. Plan A gồm dữ liệu, Spark SQL và phân tích xu hướng. Plan B gồm hai baseline cùng Random Forest. Plan C chạy replay sau B. Dùng [kịch bản slide và PowerShell](../DEMO_SLIDE_POWERSHELL.md) để thực hành. Các tên giai đoạn dưới đây giữ theo lượt chuẩn bị 14/11.

Đọc trước khi tập demo. Phạm vi đã làm: **Plan A** (CSV → PySpark ETL → HDFS/Parquet → Spark SQL), **Plan B** (phân tích xu hướng + Random Forest dự báo trước 1 giờ), **Plan C** (phát lại 24 giờ lịch sử bằng Spark Structured Streaming). Số liệu dưới đây lấy từ các tệp trong `outputs/` ngày 14/11; không diễn giải dashboard cũ thành dữ liệu thời gian thực.

## 1. Câu chuyện nghiệp vụ trong 45 giây

Phụ tải điện là mức công suất hệ thống đang cần tại một thời điểm, đơn vị **MW**. Khi nhu cầu thay đổi theo giờ, người vận hành cần biết mức phụ tải, nhịp trong ngày/tuần, giai đoạn tăng giảm và một ước lượng cho giờ kế tiếp. Đề tài dùng dữ liệu công khai của Open Power System Data (OPSD) để dựng một chuỗi xử lý có thể kiểm tra lại. Tập nghiên cứu chỉ là **phụ tải thực tế của Đức năm 2019**, nên các phát hiện là mô tả lịch sử và backtest, chưa phải hệ điều hành điện thực tế.

## 2. Mỗi khối nhận gì, trả gì, dùng công nghệ nào?

| Khối | Dữ liệu vào | Công nghệ và thao tác | Dữ liệu ra / bằng chứng |
|---|---|---|---|
| Nguồn | `time_series_60min_singleindex.csv`, 50.401 dòng, 300 cột | OPSD, metadata + SHA-256 | Chỉ đọc `utc_timestamp`, `DE_load_actual_entsoe_transparency` |
| ETL | Hai cột trên, nhiều năm | PySpark đọc CSV, lọc UTC 2019 và tải hợp lệ, tạo cột truy vết | 8.760 giờ Đức, `timestamp_utc`, `load_mw` cùng cột phụ |
| Lưu | DataFrame đã chọn | HDFS giữ file; Parquet giữ schema, nén Snappy | `/bigdata/opsd/processed/de_2019`; đọc lại được |
| Chất lượng | CSV và Parquet đọc lại | Spark đối soát schema, giờ, null, khóa, từng hàng | 8/8 nhóm đạt trong `outputs/quality/quality_report.md` |
| Plan A phân tích | Parquet 8.760 giờ | Spark SQL tổng hợp theo ngày/tháng/giờ | `outputs/analytics/index.html` và `summary.json` |
| Plan B xu hướng | Parquet 8.760 giờ | Spark tính MA7, MA30, nhóm thứ–giờ, tháng–giờ | `outputs/trends/index.html`; bảng kiểm 365 ngày/168/288 ô |
| Plan B dự báo | Parquet và phụ tải quá khứ | Spark ML RandomForestRegressor, so hai baseline | `outputs/ml_forecast/index.html`, dự báo và MAE |
| Plan C replay | 24 giờ lịch sử trích từ Parquet rồi viết JSON | Spark Structured Streaming đọc file mới, nạp mô hình ML, checkpoint | HDFS Parquet, dashboard 8→16→24, `checks.json` |

HDFS là **nơi lưu**, Spark là **nơi xử lý**. Docker Compose là cách dựng ba dịch vụ `namenode`, `datanode`, `spark` trên một máy; Spark dùng `local[2]`. Một DataNode với replication 1 không chứng minh khả năng chịu lỗi hoặc mở rộng của cụm nhiều máy.

## 3. Plan A: hiểu con số 8.760 và 8 nhóm kiểm định

2019 không nhuận: `365 × 24 = 8.760` mốc giờ UTC. Nguồn có 41.641 mốc hợp lệ ngoài năm 2019 và 0 mốc thời gian hỏng trong phân loại nguồn. Hai cột đầu vào được đổi tên thành `timestamp_utc` và `load_mw`; thêm `country`, `year`, `month`, `source_file` để truy vết. Không tuyên bố đã làm sạch tất cả 300 cột.

1. Nguồn chấp nhận được làm mốc đối chiếu.
2. Parquet có cột và kiểu đúng.
3. Summary ETL khớp nguồn, hash và số dòng.
4. Đọc lại 8.760 dòng; không trùng khóa `(country, timestamp_utc)`.
5. Khoảng UTC đủ, liên tục từng giờ.
6. Giá trị tải không NULL/NaN/Inf và dương.
7. Các cột truy vết `country/year/month/source_file` nhất quán.
8. Đối soát `exceptAll` hai chiều giữa CSV đã chọn và Parquet, không khác hàng.

Nói ngắn khi demo: “Em không chỉ ghi file. Em đọc lại từ HDFS và đối chiếu từng giờ với nguồn, để các biểu đồ và mô hình dùng cùng tập đã xác nhận.”

## 4. Plan B: phân tích xu hướng là gì?

Spark SQL trả trung bình ngày/tháng/giờ. MA7 là trung bình của 7 mức tải trung bình ngày liên tiếp; MA30 tương tự với 30 ngày. Chúng **làm mượt** dao động, không dự báo. Trong 2019, MA30 cao nhất 62.917,75 MW ngày 07/02 và thấp nhất 51.446,43 MW ngày 25/08. Hai nhiệt đồ cho nhịp theo thứ–giờ Đức (168 ô) và tháng–giờ (288 ô). Khóa luôn là UTC; nhãn giờ địa phương Europe/Berlin có thể lặp/thiếu do đổi giờ mùa hè. Các giờ lệch tham chiếu chỉ là điểm cần xem thêm, không phải lỗi hoặc quan hệ nhân quả đã chứng minh.

## 5. Plan B: dự báo, công thức MAE và cách tự kiểm chứng

Đích là `y(t)` = phụ tải giờ `t`. Đặc trưng chỉ gồm `y(t−1h)`, `y(t−24h)`, `y(t−168h)`, giờ UTC và thứ UTC của `t`. Ba giá trị tải được nối bằng timestamp chính xác. 7 ngày đầu năm không có lag 168 giờ nên không thành mục tiêu. Chia theo thời gian: **train 5.664 giờ** (08/01–31/08), **validation 720 giờ** (tháng 9), **test 2.208 giờ** (tháng 10–12). Tháng 9 chọn trong hai cấu hình Random Forest: 20 cây/sâu 5 và 50 cây/sâu 7; cấu hình sau có MAE validation thấp hơn. Không dùng test để chọn cấu hình.

Hai mốc so sánh: `last_hour = y(t−1h)` và `last_week_168h = y(t−168h)`. Tất cả chấm trên cùng timestamp test. **MAE** là trung bình sai số tuyệt đối:

`MAE = (|ŷ1−y1| + |ŷ2−y2| + ... + |ŷn−yn|) / n`.

Ví dụ học công thức: thực tế `[100, 120, 140] MW`, dự báo `[110, 100, 145] MW` → lỗi tuyệt đối `[10, 20, 5] MW` → `MAE = 35/3 = 11,67 MW`. Dấu cộng/trừ không triệt tiêu vì lấy trị tuyệt đối. **RMSE** lấy căn của trung bình bình phương lỗi nên nhạy hơn với lỗi rất lớn. Cả hai tính bằng MW và càng thấp càng tốt. MAE 1.135 MW **không có nghĩa** mỗi giờ đều sai đúng 1.135 MW.

Kết quả test: Random Forest **1.135,29 MW**; giờ trước **1.977,92 MW**; 168 giờ trước **2.897,08 MW**. So baseline tốt nhất trong test: `(1977,9180−1135,2855)/1977,9180 = 42,60%`. Đây là mức giảm trên tập test 2019, không phải cam kết tương lai. Chạy `./demo_verify_mae.ps1` trong PowerShell để xem một hàng ví dụ và tự tính lại MAE từ **2.208 hàng dự báo**, đối chiếu với `metrics.csv`; script đã chạy và báo `MAE CROSS-CHECK PASSED`.

**Giả định quan trọng:** tại lúc dự báo `t`, số đo của `t−1h` đã sẵn có. Độ trễ công bố hoặc sửa số đo chưa được mô phỏng. Mô hình dự báo nối tiếp từng giờ với tải thực tế quá khứ, chưa phải dự báo đồng thời 24 giờ.

## 6. Plan C: đọc đúng chữ “streaming”

Script lấy đúng 24 quan sát của 01/10/2019 từ Parquet và phát thành file JSON theo ba đợt: 8, 16, 24. Structured Streaming đọc file mới theo microbatch hữu hạn (`availableNow`), không nhận điện kế trực tiếp. Khi nhận thực tế giờ `s`, mô hình **đã huấn luyện offline** dự báo giờ `s+1h`; nó không học lại ở mỗi đợt. HDFS giữ Parquet kết quả và checkpoint. Thử khởi động lại cùng checkpoint, rồi thêm JSON trùng timestamp: 25 file đọc, nhưng 24 timestamp duy nhất, số hàng mới sau restart và trùng đều bằng 0. Dashboard có 23 cặp đã biết thực tế để tính sai số, vì dự báo từ event cuối hướng tới giờ thứ 25 chưa phát.

Không tuyên bố “real-time production”, “Kafka”, “exactly once cho mọi hệ thống” hoặc “AI/LLM phân tích xu hướng”. Random Forest ở đây là **machine learning hồi quy** trong Spark ML. LLM không cần cho mục tiêu hiện tại; nếu thêm sau phải định nghĩa câu hỏi, dữ liệu vào, độ tin cậy và cách chấm độc lập.

## 7. Đọc terminal theo mục đích, không đọc từng log

Lệnh PowerShell gọi Docker; `docker exec spark ... spark-submit` chạy Python job **bên trong** container Spark. Các dòng `INFO`, `Stage`, `Task` chủ yếu là tiến trình Spark; khi thuyết trình nhìn **dòng nhãn bước** và dòng **PASSED/COMPLETED**. `WARN NativeCodeLoader ... using builtin-java classes` chỉ báo Spark dùng thư viện Java thay thế trên môi trường này; trong lần chạy đã kiểm tra, job tiếp tục và kết thúc `PASSED`. Nếu lệnh không thoát hoặc chưa có `COMPLETED`, đừng nói đã xong.

| Nhãn | Việc đang làm | Câu nói đủ ý |
|---|---|---|
| `[A0/4]` | Dựng NameNode, DataNode, Spark | “Em dựng môi trường lưu và xử lý.” |
| `[A1/4]` | Đọc CSV, chọn Đức 2019, ghi Parquet | “Đây là ETL; đầu ra 8.760 giờ.” |
| `[A2/4]` | Đọc lại, kiểm định HDFS rồi Spark SQL tổng hợp | “Em xác nhận tập đầu vào trước khi tính bảng số.” |
| `[A3/4]` | Kiểm tra độc lập | “Em đối chiếu để không lấy nhầm kết quả.” |
| `[A4/4]` | Sinh dashboard HTML | “Đây là cách trình bày kết quả đã tính.” |
| `[B0/6]`, `[C0/8]` | Dựng lại dịch vụ cho Plan B/C | “Em xác nhận HDFS và Spark sẵn sàng trước job.” |
| `[1/6]–[3/6]` của Plan B | Tính, kiểm, vẽ xu hướng | “MA7/MA30 và nhịp giờ/tuần được kiểm lại.” |
| `[4/6]–[6/6]` của Plan B | Train, test ML, kiểm, vẽ | “So RF với baseline trên cùng giờ test.” |
| `[1/8]` Plan C | Chuẩn bị 24 giờ replay | “Dữ liệu lịch sử được đóng thành sự kiện JSON.” |
| `[2/8]–[4/8]` Plan C | Xử lý 8→16→24 giờ | “File mới đi qua streaming, kết quả lên HDFS.” |
| `[5/8]–[8/8]` Plan C | Restart, thêm trùng, kiểm, vẽ | “Checkpoint và kiểm tra trùng giữ 24 giờ duy nhất.” |

## 8. Năm câu thầy có thể hỏi

1. **Vì sao Spark thay vì pandas?** 8.760 giờ riêng Đức xử lý được bằng pandas, nhưng Spark cho một API thống nhất qua ETL, SQL, ML, Structured Streaming và phù hợp mục tiêu học kiến trúc. Không gán lợi thế tốc độ trên mẫu nhỏ.
2. **Vì sao HDFS/Parquet?** HDFS minh họa lưu tập đã chuẩn hóa và checkpoint; Parquet có schema, nén và thuận đọc cột. Một DataNode là demo kiến trúc.
3. **Vì sao không VM?** Compose định nghĩa ba dịch vụ, mạng và volume trong một file, dễ khởi động/khôi phục cho demo một máy. VM vẫn dùng được, nhưng tốn phần cấu hình hệ điều hành riêng.
4. **Tại sao MAE chứ không chỉ nhìn biểu đồ?** Biểu đồ cho hình dạng; MAE đo độ lệch trung bình trên cùng 2.208 giờ test. RMSE bổ sung góc nhìn lỗi lớn.
5. **Vì sao không Kafka/LLM?** JSON replay đủ để kiểm tra đường đi và checkpoint ở phạm vi 24 giờ. Kafka chỉ hợp lý khi cần broker sự kiện thật; LLM không thay thế phép đo và backtest dự báo số.

## 9. Điểm phải nói thật khi trình bày

Ngày 14/11 đã chạy đồng bộ lại theo thứ tự Plan A → baseline → Plan B → Plan C. `demo_preflight.ps1` xác nhận **cùng ETL run ID** cho Plan A/B, cùng SHA-256 CSV nguồn và Plan C nạp đúng ML run vừa tạo; cả ba container đang `Up`. Sau khi khởi động lại máy, cần mở Docker Desktop và `docker compose up -d` trước live demo. Vẫn phải nói đúng giới hạn một máy, một năm và replay lịch sử.
