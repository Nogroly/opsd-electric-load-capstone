# Project 4 – Tóm tắt cho Phúc viết báo cáo

Snapshot ngày 19/11/2026. Đọc phần 1–4 để hiểu nghiệp vụ và workflow; phần 5–7 hướng dẫn lấy minh chứng và viết báo cáo. Gói này tập trung Plan A và B; Plan C được giải thích để hiểu hệ thống hoàn chỉnh.

## 1. Dự án giải quyết việc gì?

Nhóm phân tích phụ tải điện Đức năm 2019 và thử dự báo trước một giờ. Phụ tải là công suất hệ thống cần đáp ứng tại từng thời điểm, đơn vị MW. Kết quả giúp nhận diện nhịp phụ tải theo giờ, tháng và nhóm ngày; phần dự báo đo xem học máy cải thiện sai số bao nhiêu so với các cách đơn giản. Đây là thử nghiệm trên dữ liệu lịch sử, chưa phải hệ thống điều độ điện đang vận hành.

Dữ liệu: OPSD Time series, bản 06/10/2020. CSV nguồn có 50.401 dòng, 300 cột. Nhóm chọn utc_timestamp và DE_load_actual_entsoe_transparency, lọc đủ 8.760 giờ năm 2019 theo UTC. 41.641 dòng ngoài năm được loại theo phạm vi, không mặc định là dữ liệu lỗi. MW là công suất, MWh là năng lượng; không gọi biểu đồ trung bình MW là tổng điện năng hoặc tiền điện.

## 2. Workflow của toàn hệ thống

CSV OPSD → PySpark ETL → Parquet trên HDFS → kiểm định → ba nhánh A/B/C → dashboard HTML.

| Khối | Dữ liệu vào → công việc → đầu ra |
|---|---|
| Nguồn OPSD | Tải CSV và metadata; đối chiếu dung lượng, SHA-256 để kiểm tra tệp tải về đúng bản nguồn. |
| PySpark ETL | Chọn hai cột, lọc năm 2019 UTC, chuẩn hóa kiểu thời gian và phụ tải → tập dữ liệu chuẩn hóa. |
| HDFS + Parquet | Lưu tập chuẩn hóa có schema, nén Snappy → các bước sau đọc lại cùng dữ liệu. |
| Kiểm định | Đọc lại HDFS; kiểm tra 8 nhóm và đối soát với CSV → 8.760 giờ, không thiếu giờ/trùng khóa, 0 dòng lệch ở cả hai chiều. |
| Plan A | Spark SQL và PySpark tính trung bình theo giờ/tháng/nhóm ngày, MA7/MA30, nhiệt đồ và độ lệch so với tham chiếu → bảng CSV, SVG và dashboard. |
| Plan B | Tạo hai baseline, huấn luyện/chọn cấu hình RF, đánh giá trên cùng giờ test → dự báo CSV, bảng sai số, mô hình đã lưu và dashboard. |
| Plan C | Phát lại 24 giờ lịch sử thành JSON theo đợt 8→16→24; Structured Streaming dùng mô hình B → Parquet và checkpoint trên HDFS, dashboard replay. Đây là mô phỏng luồng, chưa phải nguồn live và chưa học trực tuyến. |

Docker Compose dựng NameNode, DataNode và Spark trên một máy; Spark chạy local[2]. Docker giúp tái lập môi trường. Spark dùng chung cho ETL, SQL, ML và streaming. Parquet giữ kiểu dữ liệu và phù hợp đọc theo cột. Với quy mô hiện tại vẫn có thể dùng Pandas; chưa có benchmark để khẳng định Spark nhanh hơn hoặc đã chạy cụm nhiều máy.

Tám nhóm kiểm định: chất lượng nguồn; schema; thông tin lượt ETL; khóa và số dòng; tính liên tục theo giờ; giá trị; cột dẫn xuất; đối soát nguồn–đầu ra. Hash đúng chỉ chứng minh tệp tải về khớp metadata; đối soát từng bước mới giúp kiểm tra kết quả xử lý.

## 3. Plan A: hiểu kết quả và cách diễn giải

Mở Plan_A/index.html. Biểu đồ giờ, thứ và nhóm ngày dùng Europe/Berlin; tháng, ngày và trung bình trượt dùng UTC. Không đổi cách gọi múi giờ khi viết chú thích.

• Theo giờ: 11:00 giờ Đức có phụ tải trung bình cao nhất, 64.119,93 MW. Đây là trung bình các quan sát cùng giờ trong mẫu, không phải ngày nào 11:00 cũng đạt giá trị đó.
• Theo tháng: tháng 01 trung bình 61.597,14 MW; tháng 08 là 52.171,91 MW. Mỗi cột là trung bình các số đo giờ thuộc tháng. Kiểm tra độc lập từ CSV cho tháng 01: 744 giờ, trung bình 61.597,139785 MW.
• Ngày thường/cuối tuần: lần lượt 59.042,57 và 48.330,13 MW. Cuối tuần thấp hơn 18,14% so với ngày thường; phân nhóm theo thứ, chưa tách ngày lễ. Không kết luận nguyên nhân nhân quả chỉ từ biểu đồ.
• MA7/MA30: lấy trung bình phụ tải ngày trong cửa sổ 7/30 ngày đã quan sát rồi dịch cửa sổ từng ngày. Mục đích là làm mượt dao động để nhìn xu hướng; MA30 thường mượt hơn và phản ứng chậm hơn MA7. Đây là mô tả lịch sử, chưa phải dự báo. Đầu chuỗi chưa đủ 7/30 ngày sẽ chưa có MA tương ứng.
• Nhiệt đồ: mỗi ô là trung bình của một nhóm thứ–giờ; màu biểu thị mức phụ tải MW. Cần đọc kèm thang màu và múi giờ.
• Mục 20 thời điểm lệch: so các giờ tháng 9–12 với trung bình cùng thứ, cùng giờ địa phương của tháng 1–8. Gọi là độ lệch so với mức tham chiếu; chưa có nhãn để khẳng định đó là sự cố hoặc bất thường vận hành.

## 4. Plan B: mô hình và ý nghĩa số liệu

Mở Plan_B/index.html. Dự báo phụ tải giờ t bằng 5 đặc trưng: phụ tải t−1, t−24, t−168 giờ; giờ và thứ của t theo UTC. Random Forest hồi quy lấy trung bình dự báo từ nhiều cây. Hai baseline lần lượt dự báo bằng phụ tải giờ gần nhất và đúng 168 giờ trước.

Chia theo thời gian: huấn luyện tháng 1–8 (5.664 giờ có đủ lịch sử), chọn cấu hình bằng tháng 9 (720 giờ), đánh giá cuối trên tháng 10–12 (2.208 giờ). Chọn giữa 20 cây/sâu 5 và 50 cây/sâu 7 theo MAE validation; cấu hình được chọn là 50 cây, sâu tối đa 7, seed 42. Không chọn cấu hình bằng kết quả test.

| Phương pháp | MAE test (MW) | RMSE test (MW) |
|---|---:|---:|
| Random Forest | 1.135,29 | 1.538,11 |
| Giờ gần nhất | 1.977,92 | 2.636,01 |
| 168 giờ trước | 2.897,08 | 4.961,92 |

MAE = trung bình |thực tế − dự báo|. Ví dụ sai số −1.000 và +2.000 MW → MAE 1.500 MW. RMSE = căn bậc hai của trung bình bình phương sai số, nhạy hơn với sai số lớn. Cả hai có đơn vị MW; so sánh trên cùng tập test thì thấp hơn là tốt hơn.

RF giảm MAE 42,60% so với baseline giờ gần nhất: (1.977,92 − 1.135,29) / 1.977,92 × 100%. Không gọi đây là độ chính xác 42,60% và không kết luận RF tốt hơn mọi mô hình khác. Giá trị 872,93 MW trên dashboard là MAE validation tháng 9, không phải MAE test.

Biểu đồ tuần đầu test chỉ minh họa 01–07/10; bảng MAE test đánh giá cả 2.208 giờ quý IV. Mỗi giờ dự báo được dùng số đo thực tế của giờ trước đã có, không phải dự báo đồng thời cả quý từ một mốc. Chưa mô phỏng độ trễ công bố, chưa đánh giá năm khác hoặc thêm thời tiết/ngày lễ. Feature importance phản ánh cách mô hình sử dụng đặc trưng, không phải tỷ trọng công thức dự báo hoặc bằng chứng nhân quả.

## 5. Ảnh nào đưa vào báo cáo? Chú thích mẫu

Nên chọn 5–6 hình vừa đủ, không chụp toàn trang dashboard dài. Trong Anh_bieu_do có SVG để mở/chèn rõ nét; nếu có PNG thì có thể dùng trực tiếp. File SVG riêng tương ứng với biểu đồ gốc trong hai dashboard.

| Minh chứng | File/biểu đồ | Chú thích có thể dùng |
|---|---|---|
| A – xu hướng | A4_MA7_MA30.png hoặc .svg | So sánh phụ tải trung bình ngày (xám) với MA7 (xanh) và MA30 (cam) tại Đức năm 2019, cùng trục MW và ngày UTC. Hai đường MA làm mượt dao động ngày và có độ trễ. |
| A – mùa/tháng | A2_Trung_binh_theo_thang.svg | Phụ tải trung bình theo tháng UTC năm 2019. Mỗi cột được tính từ các quan sát giờ thuộc tháng, đơn vị MW. |
| A – nhịp ngày | A1_Trung_binh_theo_gio.svg hoặc A3_Ngay_thuong_cuoi_tuan.svg | Phụ tải trung bình theo giờ Đức hoặc nhóm ngày; nhóm ngày dùng Europe/Berlin, chưa tách ngày lễ. |
| A – nhiệt đồ | A5_Nhiet_do_thu_gio.svg | Phụ tải trung bình theo thứ và giờ địa phương Europe/Berlin, đơn vị MW; mỗi ô là trung bình một nhóm thứ–giờ. |
| B – kết quả chính | B1_So_sanh_MAE_test.svg | MAE của Random Forest và hai baseline trên cùng 2.208 giờ test từ tháng 10 đến tháng 12/2019. Sai số có đơn vị MW. |
| B – diễn biến dự báo | B2_Thuc_te_va_du_bao_tuan_dau_test.svg | Phụ tải thực tế và dự báo trước một giờ trong tuần 01–07/10/2019 UTC; chỉ là một phần minh họa của tập test. |

Cách tự chụp: giải nén toàn bộ → mở 00_MO_DAU_TIEN.html → chọn dashboard → đưa đúng biểu đồ vào màn hình → đặt zoom trình duyệt để đọc rõ → Win+Shift+S chụp trọn tiêu đề, trục, đơn vị, chú giải và vùng dữ liệu. Đặt ảnh vào đoạn báo cáo đang giải thích biểu đồ, đánh số hình, thêm nguồn “Kết quả thực nghiệm của nhóm từ dữ liệu OPSD, snapshot 19/11/2026”.

Sau mỗi hình viết 2–3 câu: quan sát được gì; con số chứng minh; giới hạn khi diễn giải. Không chỉ dán hình rồi để người đọc tự suy luận. Có thể cắt bỏ thanh trình duyệt và phần trống, nhưng không cắt trục, đơn vị, thang màu hoặc chú giải; không bóp méo tỷ lệ ảnh hay sửa số liệu trong ảnh.

## 6. Những việc Phúc làm và những việc cần Vinh xác nhận

Phúc: triển khai báo cáo theo thứ tự bài toán → dữ liệu → kiến trúc và lý do công nghệ → ETL/kiểm định → phân tích A → dự báo B → replay C → giới hạn/đóng góp; chọn hình và viết chú thích, diễn giải; thống nhất font, màu, đánh số hình và cách ghi đơn vị. Bản tổng quan ngắn đặt ở đầu; phần kỹ thuật chi tiết có thể đưa phụ lục nếu thầy không quy định độ dài khác.

Vinh đã cập nhật và chạy lại biểu đồ: có đường trung bình ngày cùng MA7/MA30, giữ tên trục MW/ngày UTC và đủ ba chú giải. Đối soát độc lập 403 nhóm thống kê, 365 giá trị ngày và các cửa sổ trượt đều đạt. Dùng ảnh A4 trong gói này để thay hình hai đường cũ ở báo cáo và slide. Bản báo cáo gốc trên Google Docs/Word chưa được tự thay hình; Phúc cần thay hình 2 và sửa chú thích cho khớp ba đường.

Giải thích SQL để viết đúng workflow: date_format(timestamp_utc, 'yyyy-MM') lấy năm–tháng UTC; GROUP BY gom các giờ cùng tháng; COUNT(*) đếm số giờ; AVG(load_mw) = tổng phụ tải các quan sát / số quan sát; ORDER BY month_utc sắp theo tháng tăng dần vì mặc định là ASC. Tháng 01 có 31×24 = 744 giờ và trung bình nguồn 61.597,139785 MW, khớp bảng analytics/monthly.csv. Đây là cách kiểm chứng phép tính, không chỉ viện dẫn code chạy thành công.

Sửa tài liệu bằng bản sao, dùng góp ý cho chỗ chưa chắc; giữ nguyên bản gốc và gói snapshot. Không xóa dữ liệu, file kết quả, script, checkpoint hoặc sửa số trong biểu đồ để hợp câu văn. Nếu muốn thay một kết luận hay chỉ số, ghi rõ câu đang nghi ngờ để Vinh đối chiếu với nguồn. Các thay đổi nhận xét cần dựa trên kết quả, không tự thêm tuyên bố production, dữ liệu live, LLM hoặc đã chạy nhiều máy.

## 7. Tài liệu gốc và kiểm chứng

Nguồn dữ liệu: https://data.open-power-system-data.org/time_series/2020-10-06/
Mã nguồn: https://github.com/Nogroly/opsd-electric-load-capstone
File Plan_A/summary.json và Plan_B/summary.json ghi lượt chạy; hai snapshot dùng cùng etl_run_id 6d1e7354-e251-4ba5-87ab-5fe31840b27d. Plan_B/metrics.csv chứa cả validation và test; khi viết kết quả cuối chỉ lấy split=test, period=all. Plan_B/predictions.csv chứa số đo và dự báo để tính lại sai số. analytics/ và quality/ là bảng tổng hợp và báo cáo kiểm định đi kèm.

Gói này giúp viết báo cáo từ kết quả đã có; không cần cài Docker để xem dashboard. Khi cần tái hiện pipeline, dùng README trong repo và chạy Plan A → B → C, sau đó demo_preflight.ps1.
