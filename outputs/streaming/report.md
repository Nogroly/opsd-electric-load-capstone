# Demo Spark Structured Streaming — 20261114T101440149

Nguồn: phát lại 24 giờ UTC ngày 01/10/2019 từ OPSD/HDFS; không phải dữ liệu trực tiếp.

25 tệp JSON được đọc: 24 giờ riêng biệt và một tệp lặp lại giờ 12:00. Spark Structured Streaming ghi 24 hàng Parquet duy nhất trên HDFS; chạy lại cùng checkpoint và nhận bản lặp đều không làm tăng số hàng.

Quan sát cuối: 2019-10-01 23:00:00 UTC, 45.922,00 MW. Dự báo giờ 2019-10-02 00:00:00 UTC: 45.136,16 MW.

MAE của 23 cặp đã có thực tế trong đoạn replay: 1.089,51 MW; không dùng con số này thay cho đánh giá test 2.208 giờ.

Đầu vào JSON, Parquet đầu ra và checkpoint ở các đường dẫn riêng; mô hình Random Forest huấn luyện offline được nạp để chấm điểm.
