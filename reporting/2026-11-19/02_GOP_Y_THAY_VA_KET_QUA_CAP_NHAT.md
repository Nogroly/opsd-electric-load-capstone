# Cập nhật sau buổi báo cáo 19/11/2026

1. **Thấy được tác dụng làm mượt:** đã bổ sung trung bình ngày màu xám bên cạnh MA7 xanh và MA30 cam, dùng chung trục MW. Trung bình ngày lấy từ avg_mw trong daily_trend.csv đã kiểm định, không tạo dữ liệu minh họa. MA7 có 359 điểm, MA30 có 336 điểm vì chỉ vẽ khi đủ cửa sổ; trung bình ngày có 365 điểm. Tooltip ghi cả ba giá trị và ghi rõ khi chưa đủ lịch sử.
2. **Chứng minh số liệu:** đã chạy lại check_analytics.py, đối chiếu 403 nhóm và 10 đỉnh với CSV nguồn; check_trends.py đối chiếu 365 ngày, 168 nhóm thứ–giờ, 288 nhóm tháng–giờ và cửa sổ trượt. Đều PASSED. Tháng 01 kiểm tra riêng từ CSV: 744 giờ, trung bình 61.597,139785 MW, làm tròn 61.597,14 MW; khớp monthly.csv.
3. **Giải thích kỹ thuật:** tóm tắt cho Phúc đã bổ sung cách đọc các khối dữ liệu, SQL GROUP BY/AVG/ORDER BY, công thức MAE/RMSE và giới hạn kết quả. README và kịch bản PowerShell cục bộ đã cập nhật lời giải thích biểu đồ ba đường.

## Việc Phúc tiếp tục làm

- Thay hình 2 trong báo cáo gốc và hình xu hướng trên slide bằng Anh_bieu_do/A4_MA7_MA30.png; dùng chú thích ba đường trong hướng dẫn.
- Viết 2–3 câu dưới hình: đường ngày dao động ra sao; MA7/MA30 làm mượt thế nào; làm mượt có độ trễ và chưa phải dự báo.
- Giữ các biểu đồ theo giờ/tháng/nhóm ngày, trục MW và mốc UTC/Europe-Berlin. Các cột theo tháng là trung bình công suất, không phải tổng năng lượng hay tiền điện.
- Không tự đổi kết luận sang dữ liệu Việt Nam. Chưa có dữ liệu thời tiết/ngày lễ để xác nhận nguyên nhân mùa vụ.
- Với kết quả ML, dùng B1 là MAE test trên 2.208 giờ; không nhầm biểu đồ validation tháng 9 với test. Không gọi mức giảm MAE là độ chính xác.

## Phạm vi thay đổi

Đã sửa chương trình dựng biểu đồ jobs/render_trends.py và chạy lại với đầu ra đã kiểm định. Không thay tập dữ liệu, các giá trị MA, mô hình hoặc phép đánh giá Plan B. Các kết quả A/B giữ cùng nguồn ETL. Gói này là bản bàn giao mới; không dùng lại ảnh A4 hai đường trong các gói cũ. Báo cáo gốc, Canva và bản nộp Drive trước đó chưa tự thay hình; người biên tập cần thay bằng ảnh mới trong gói này.

Kiểm tra cuối: demo_preflight.ps1 -CoreOnly đạt cho A/B; demo_verify_mae.ps1 tính lại MAE RF 1.135,2855 MW, khớp báo cáo. Bản bàn giao cho bạn nhóm trước đó chỉ gồm A/B. Phiên bản Git này đã chạy lại B rồi C theo ETL hiện tại và kiểm tra toàn tuyến đạt. Khi chạy lại A hoặc B ở lần sau, cần chạy tiếp các phần phụ thuộc để giữ đúng lượt dữ liệu và mô hình.
