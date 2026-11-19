# Thử nghiệm dự báo cơ sở — phụ tải Đức 2019

## Mục tiêu và cách chạy

Dự báo phụ tải MW của **giờ tiếp theo**, dùng dữ liệu thực tế đã kiểm định trên
HDFS. Đây là thử nghiệm hồi cứu trên năm 2019, không phải dịch vụ dự báo hiện tại.

Trong PowerShell tại thư mục BigData, khi Docker Desktop đã chạy:

```powershell
.\run_forecast.ps1
```

Script kiểm tra dữ liệu, tạo dự báo bằng Spark, đối soát độc lập với CSV và mở
`outputs/forecast/index.html`. Chỉ sử dụng kết quả khi thấy `FORECAST COMPLETED`.
Thêm `-NoOpen` nếu không muốn tự mở trang. Lần đầu cần internet để cài thư viện
vẽ Matplotlib vào container; các bước tính dự báo không cần thư viện học máy.
Script chỉ đọc dữ liệu HDFS hiện có, không chạy lại ETL. Nếu thiếu dữ liệu, chạy
`run_plan_a.ps1` trước. Các file minh chứng Plan A và CSV gốc được giữ riêng.

## Thiết kế cố định trước khi xem kết quả dự báo

| Khoảng thời gian UTC | Vai trò | Số giờ |
|---|---|---:|
| 01/01 đến trước 01/09/2019 | Lịch sử ban đầu | 5.832 |
| 01/09 đến trước 01/10/2019 | Validation: chọn baseline theo MAE | 720 |
| 01/10 đến trước 01/01/2020 | Test: đánh giá cuối | 2.208 |

Hai baseline không có tham số cần huấn luyện:

- `last_hour`: dự báo y(t) bằng y(t−1 giờ).
- `last_week_168h`: dự báo y(t) bằng y(t−168 giờ).

Tất cả mốc dùng UTC. 168 giờ là đúng bảy ngày theo UTC; qua thời điểm đổi giờ
mùa hè, nó có thể lệch một giờ so với đồng hồ địa phương tại Đức. Vì thế nhãn
trong báo cáo ghi “168 giờ trước”, không khẳng định luôn cùng giờ địa phương.

Mỗi dự báo tại giờ t được tạo ở mốc danh nghĩa t−1 giờ. Khi chuyển sang giờ
tiếp theo, giá trị thực tế mới được đưa vào lịch sử. Bởi vậy dự báo trong tập
test có thể dùng các quan sát test **đã xảy ra trước mốc dự báo**. Đây là đánh giá
cuốn chiếu trước một giờ (rolling one-step), không phải dự báo cả quý từ 30/09.

Giả định quan trọng: số đo y(t−1 giờ) đã có tại mốc phát dự báo. Snapshot OPSD
không mô phỏng độ trễ công bố hay các lần chỉnh sửa dữ liệu. Kết quả chưa chứng
minh hiệu quả vận hành trực tiếp nếu số đo thực tế đến muộn.

## Chọn phương pháp và đo sai số

Chọn baseline có MAE thấp hơn trên tháng 9; nếu hòa chọn `last_hour`.
Giữ nguyên lựa chọn khi báo cáo test tháng 10–12. Cả hai baseline vẫn được
trình bày để đối chiếu, nhưng không dùng test để thay lựa chọn hoặc chỉnh tham số.

- MAE = trung bình |dự báo − thực tế|, đơn vị MW.
- RMSE = căn của trung bình bình phương sai số, đơn vị MW; nhạy hơn với lỗi lớn.
- Bias = trung bình (dự báo − thực tế); dương là có xu hướng dự báo cao.
- Điểm cả quý tính trực tiếp trên 2.208 giờ, không lấy trung bình không trọng số
  của ba điểm tháng vì mỗi tháng có số giờ khác nhau.

Biểu đồ dùng hai cửa sổ xác định trước: 01–07/10 và 24–30/12 (UTC).
Cửa sổ cuối năm cho thấy giai đoạn có thể khó dự báo; không chọn cửa sổ theo
sai số thấp. Bảng tháng và CSV chứa toàn bộ test, không chỉ hai đoạn minh họa.

## Minh chứng và kiểm tra

- `predictions.csv`: 2.928 mục tiêu (validation + test), thực tế, hai dự báo,
  thời điểm phát và thời điểm nguồn của mỗi độ trễ.
- `metrics.csv`: MAE/RMSE/bias cho validation, test và từng tháng test.
- `summary.json`: phạm vi, giả định, phương pháp được chọn, hash nguồn/đầu ra.
- `verification.json`: đối chiếu độc lập mọi dự báo và mọi chỉ số từ CSV gốc.
- `report.md`: kết luận thực nghiệm tự sinh từ kết quả đã kiểm tra.
- `*.png`, `*.svg`: biểu đồ để chèn vào Docs hoặc slide.

Kiểm thử nhánh lỗi và chống dùng thông tin tương lai:

```powershell
docker exec spark /opt/spark/bin/spark-submit --master "local[2]" /workspace/tests/test_forecast_contract.py
docker exec spark python3 /workspace/tests/test_forecast_report.py
```

Kiểm thử gồm: dự báo đầu validation sử dụng đúng lịch sử trước ranh giới;
thay giá trị mục tiêu/tương lai không làm đổi dự báo tại mục tiêu; thiếu giờ,
trùng giờ, giá trị vô hạn bị từ chối; điểm test không quyết định chọn baseline.
Phép đối soát độc lập còn kiểm tra đủ 2.928 mục tiêu liên tục, 5.856 giá trị dự báo,
thời điểm nguồn, mẫu số và toàn bộ 10 dòng chỉ số.
Kiểm thử báo cáo từ chối lần chạy FAILED, xác nhận của lần chạy cũ hoặc CSV
bị thay đổi sau kiểm tra. Tổng cộng có 6 kiểm thử hợp đồng dự báo và 4 kiểm thử
bảo vệ báo cáo; đây là kiểm thử phần mềm, khác với 5.856 giá trị đối soát.

## Cách trình bày với thầy

“Nhóm đã bổ sung hai dự báo cơ sở trước một giờ. Nhóm chọn phương pháp bằng
validation tháng 9 và báo cáo sai số trên tháng 10–12, giữ đúng thứ tự thời gian.
Các kết quả tạo mốc so sánh cho mô hình tiếp theo.”

Đọc MAE/RMSE cụ thể trong dashboard; không gọi baseline là mô hình học máy đã
huấn luyện. Chưa có biến thời tiết, ngày lễ, khoảng dự báo hoặc kiểm định nhiều
năm. Dữ liệu 2019 từng được khảo sát mô tả trong Plan A; đây là phép thử minh họa
với một mốc chia, chưa phải đánh giá độc lập trên dữ liệu vận hành mới.

## Nguồn phương pháp

- Hyndman & Athanasopoulos, Forecasting: Principles and Practice, phương pháp
  naïve/seasonal naïve: https://otexts.com/fpp3/simple-methods.html
- Đánh giá cuốn chiếu theo thời gian: https://otexts.com/fpp3/tscv.html
- Nguồn dữ liệu: https://data.open-power-system-data.org/time_series/2020-10-06/
