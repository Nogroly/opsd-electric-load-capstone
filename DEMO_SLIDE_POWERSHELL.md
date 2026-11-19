# Kịch bản thuyết trình và demo trong Antigravity

Luồng trình bày dùng 11 slide và chạy lần lượt Plan A, B, C. Bản này ghi lời nói, màn hình cần mở, lệnh cần gõ và dấu hiệu để chuyển bước. Số slide theo bố cục đã chốt. Khi bản Canva của nhóm có vị trí khác, tìm theo tên trang.

## Trước giờ chia sẻ màn hình

1. Mở Docker Desktop, đợi Engine running. Mở đúng thư mục BigData trong Antigravity.
2. Mở bảng Terminal ở cạnh dưới. Chọn profile PowerShell hoặc mở terminal PowerShell mới từ menu chọn profile. Dòng nhắc thường bắt đầu bằng PS. Có thể xác nhận bằng lệnh $PSVersionTable.PSVersion.
3. Chạy các lệnh chuẩn bị bên dưới. Lệnh tải CSV chỉ cần chạy trước giờ báo cáo. Chờ ba dịch vụ namenode, datanode, spark xuất hiện và NameNode healthy.
4. Mở sẵn Canva hoặc PowerPoint ở chế độ trình chiếu. Giữ trình duyệt để dashboard tự mở sau từng plan. Đặt cỡ chữ terminal đủ để người xem đọc được nhãn A/B/C.
5. Kiểm tra bấm chuyển được giữa slide, Antigravity và trình duyệt bằng Alt+Tab. Khi chia sẻ, chọn màn hình chứa cả ba cửa sổ để thầy theo dõi được lúc đổi ứng dụng.

```powershell
Set-Location -LiteralPath "D:\OneDrive\Tài liệu\ChatGPT\BigData"
$PSVersionTable.PSVersion
docker compose up -d
docker compose ps
.\download_opsd.ps1
```

Các lệnh run_plan luôn tự về thư mục chứa script trước khi chạy. Nếu dùng bản clone từ GitHub, thay đường dẫn Set-Location bằng thư mục clone của bạn.

## Slide 1 Giới thiệu đề tài

Hiển thị trang bìa. Nói khoảng 20 giây.

“Em chào các thầy. Nhóm em trình bày Project 4 về phân tích và dự báo phụ tải điện Đức. Em sẽ chạy lần lượt ba phần. Plan A tạo tập dữ liệu đã kiểm định và phân tích nhịp phụ tải. Plan B dự báo trước một giờ và đo sai số. Plan C dùng mô hình đó khi dữ liệu lịch sử được phát lại theo đợt.”

## Slide 2 Lộ trình

“Em sẽ đi theo luồng dữ liệu từ nguồn đến lưu trữ, kiểm định, phân tích và dự báo. Sau mỗi lệnh chạy, em sẽ mở kết quả và chỉ rõ điều gì đã được kiểm tra.”

Chỉ ba mục A, B, C trên slide. Chuyển đến bài toán.

## Slide 3 Bài toán và mục tiêu

“Phụ tải cho biết công suất hệ thống cần đáp ứng tại một thời điểm, đơn vị MW. Nhóm em muốn biết tải thường cao ở khung giờ nào, thay đổi qua các tháng và giữa ngày thường với cuối tuần ra sao. Từ lịch sử đó, nhóm đánh giá khả năng dự báo trước một giờ. Kết quả hỗ trợ theo dõi các khung giờ cần chú ý và cho phép đo mức cải thiện của mô hình.”

## Slide 4 Dữ liệu

“Nhóm dùng OPSD Time series bản phát hành ngày 06/10/2020. CSV có 50.401 dòng theo giờ. Em lấy hai cột là utc_timestamp và DE_load_actual_entsoe_transparency, lọc năm 2019 thành 8.760 giờ. Khóa và phạm vi năm dùng UTC. Khi phân tích hành vi theo giờ hoặc thứ, em đổi sang giờ Đức Europe/Berlin.”

Chỉ vào 8.760 giờ, đơn vị MW và hai tên cột. Chuyển sang sơ đồ.

## Slide 5 Kiến trúc và lý do chọn công nghệ

“PySpark đọc CSV, chuẩn hóa và ghi Parquet trên HDFS. Em đọc lại Parquet để kiểm định trước khi dùng cho phân tích và dự báo. Spark SQL tính các bảng tổng hợp, PySpark tính cửa sổ MA7/MA30, còn Spark ML huấn luyện Random Forest. Plan C nhận file JSON đến theo đợt và nạp mô hình đã huấn luyện.”

“Spark cho phép dùng cùng môi trường cho ETL, SQL, ML và xử lý luồng. Parquet lưu theo cột và giữ schema. HDFS cung cấp tập dữ liệu mà các job đọc lại. Docker Compose giúp dựng ba dịch vụ với cùng cấu hình trên một máy.”

Chỉ rõ mũi tên từ dữ liệu đã kiểm định đến cả hai nhánh, và mũi tên mô hình từ Plan B sang C. Chuyển sang Antigravity.

## Chạy Plan A rồi trình bày slide 6 và 7

Gõ riêng lệnh này và nhấn Enter.

```powershell
.\run_plan_a.ps1
```

Trước khi Enter, nói “Em bắt đầu từ dữ liệu CSV. Plan A sẽ xử lý, ghi HDFS, kiểm định rồi dựng dashboard phân tích.”

| Nhãn xuất hiện | Nói khi terminal đang làm việc |
|---|---|
| A0/6 | “Đây là bước khởi động ba dịch vụ của môi trường Docker.” |
| A1/6 | “Spark đang lấy dữ liệu Đức 2019 và ghi Parquet lên HDFS.” |
| A2/6 | “Spark đọc lại dữ liệu đã ghi, kiểm định rồi tính các bảng SQL. Dữ liệu đầu vào của các bước sau là tập Parquet này.” |
| A3/6 | “Kết quả tổng hợp được kiểm tra độc lập bằng cách tính từ CSV gốc.” |
| A4/6 | “Spark đang tính trung bình trượt và nhiệt đồ để tìm nhịp biến động của phụ tải.” |
| A5/6 | “Các cửa sổ trượt và các nhóm giờ đang được kiểm tra lại.” |
| A6/6 | “Chương trình dùng các bảng đã kiểm định để tạo dashboard HTML.” |

Trong lúc Spark chạy, nếu màn hình có nhiều INFO, nói “Các dòng INFO này mô tả Spark khởi tạo và thực thi job. Em đang theo dõi nhãn bước và sẽ kết luận khi có COMPLETED cùng kết quả kiểm định.”

Đợi PLAN A COMPLETED. Dashboard tự mở.

### Slide 6 Kiểm định

Chuyển về slide kiểm định khi dashboard đã có kết quả.

“Em đọc lại đủ 8.760 giờ từ HDFS. Tám nhóm kiểm định đều đạt, gồm nguồn, schema, báo cáo ETL, khóa và số dòng, tính liên tục của giờ, giá trị, cột dẫn xuất và đối soát. Khi so CSV với Parquet theo cả hai chiều, mỗi chiều có 0 dòng khác biệt.”

Nếu thầy muốn thấy dữ liệu thật trên HDFS, gõ lệnh đọc lại dưới đây. Lệnh này in schema, năm dòng mẫu và tổng số giờ.

```powershell
docker exec spark /opt/spark/bin/spark-submit --master "local[2]" /workspace/jobs/demo_readback.py
```

### Slide 7 Phân tích Plan A và dashboard

Mở dashboard tự bật sau Plan A. Nếu cửa sổ bị che, dùng Alt+Tab. Khi cần mở lại, chạy lệnh sau.

```powershell
Start-Process ".\outputs\trends\index.html"
```

Giải thích mỗi hình bằng thứ tự trục, cách tổng hợp, kết quả rồi ý nghĩa.

- Bấm Theo giờ. “Trục ngang là nhãn giờ Đức 00–23, trục tung là phụ tải trung bình MW. Mỗi cột gom cùng nhãn giờ trong mẫu. 11h cao nhất khoảng 64.120 MW. Mẫu theo giờ giúp chọn khung thời gian cần theo dõi.”
- Bấm Theo tháng. “Các tháng được nhóm theo UTC. Tháng 01 cao nhất khoảng 61.597 MW, tháng 08 thấp nhất khoảng 52.172 MW. Đây là biến động quan sát được trong năm 2019.”
- Bấm Ngày thường và cuối tuần. “Hai cột là trung bình trên số đo theo giờ của từng nhóm thứ. Cuối tuần là 48.330 MW, thấp hơn 18,14% so với ngày thường 59.043 MW. Nhóm chưa tách ngày lễ.”
- Bấm MA7 và MA30. “Đường xám là trung bình từng ngày UTC; đường xanh MA7 và đường cam MA30 làm mượt dao động ngày để thấy xu hướng. Trục ngang là ngày UTC/ngày kết thúc cửa sổ, trục tung là MW. MA7 lấy trung bình 7 ngày đã quan sát, MA30 lấy 30 ngày, gồm ngày đang xét. MA30 mượt hơn nhưng phản ứng chậm hơn; đây chưa phải dự báo. Cửa sổ MA30 cao nhất kết thúc ngày 07/02, thấp nhất ngày 25/08.”
- Nếu dùng nhiệt đồ, chỉ một ô và thang màu. “Hàng là thứ, cột là giờ Đức. Màu thể hiện MW trung bình. Thứ Tư 11h có mức khoảng 68.639 MW, được tính trên nhiều tuần.”

Khi thời gian hạn chế, dùng slide 7 để nói một câu về MA30 rồi chỉ trực tiếp biểu đồ cuối tuần. Các hình còn lại sẵn trên cùng dashboard để trả lời câu hỏi.

Chuyển phần “Từ cùng dữ liệu HDFS đã kiểm định, em chạy Plan B để so sánh các phương pháp dự báo.”

## Slide 8 và chạy Plan B

Chỉ sơ đồ dự báo trên slide 8.

“Giờ cần dự báo là t. Mô hình nhận phụ tải ở t−1, t−24, t−168, cộng giờ và thứ UTC của t. Hai baseline lấy giá trị giờ gần nhất hoặc đúng 168 giờ trước. Nhóm huấn luyện trên tháng 1–8, chọn cấu hình bằng tháng 9, đánh giá trên tháng 10–12.”

Chuyển sang PowerShell, đợi dấu nhắc PS rồi gõ.

```powershell
.\run_plan_b.ps1
```

| Nhãn xuất hiện | Nói khi terminal đang làm việc |
|---|---|
| B1/5 | “Chương trình tạo hai baseline từ Parquet của Plan A.” |
| B2/5 | “Bước này kiểm tra thời điểm mục tiêu, giá trị dự báo và cách tính sai số baseline.” |
| B3/5 | “Spark ML huấn luyện hai cấu hình Random Forest và chọn bằng MAE tháng 9. Mô hình được chấm trên cùng các giờ mục tiêu với baseline.” |
| B4/5 | “Các dự báo và chỉ số được tính lại để kiểm tra độc lập.” |
| B5/5 | “Chương trình tạo một dashboard so sánh cả ba phương pháp.” |

## Slide 9 Kết quả dự báo

Đợi PLAN B COMPLETED. Dashboard tự mở.

“Trục ngang là phương pháp, trục tung là MAE theo MW. Cả ba cùng dùng 2.208 giờ test. Random Forest đạt 1.135,29 MW, giờ gần nhất 1.977,92 MW, 168 giờ trước 2.897,08 MW. Trong tập này, Random Forest giảm MAE 42,60% so với giờ gần nhất.”

“MAE lấy trị tuyệt đối sai số của từng giờ rồi tính trung bình. Em có một phép tính lại từ file dự báo để kiểm tra số trên biểu đồ.”

Nếu trình bày phép tính, chuyển PowerShell và gõ.

```powershell
.\demo_verify_mae.ps1
```

Chỉ dòng ví dụ thực tế 44.267 MW, dự báo 44.909,66 MW, sai số tuyệt đối 642,66 MW. Sau đó chỉ tổng 2.208 giờ và dòng MAE CROSS-CHECK PASSED.

Có thể chỉ thêm bảng sai số theo tháng. “Tháng 12 có MAE cao hơn tháng 10 và 11 nên em cũng theo dõi kết quả từng giai đoạn, thay vì chỉ đọc số tổng.”

Chuyển phần “Sau khi đã có mô hình, em dùng tiếp mô hình này trong Plan C để dự báo khi dữ liệu đến theo đợt.”

## Slide 10 và chạy Plan C

“Đầu vào là 24 giờ lịch sử ngày 01/10/2019. Chương trình phát file JSON thành ba đợt. Spark Structured Streaming đọc file mới, nạp mô hình Plan B và ghi dự báo lên HDFS. Checkpoint lưu tiến trình cùng trạng thái bỏ trùng để thử chạy lại.”

Chuyển PowerShell và gõ.

```powershell
.\run_plan_c.ps1
```

| Nhãn xuất hiện | Lời nói và thao tác |
|---|---|
| C1/8 | “Đang lấy 24 giờ trên HDFS và gắn lượt replay với đúng dữ liệu và mô hình vừa tạo.” |
| C2/8 | “Em phát 8 giờ đầu. Dashboard sẽ mở khi đã xử lý và kiểm tra đầu ra của đợt này.” |
| C3/8 | Giữ dashboard mở. “Em phát tiếp 8 giờ, tổng tăng từ 8 lên 16.” |
| C4/8 | “Đợt cuối đưa tổng lên 24 giờ. Mô hình tạo dự báo giờ kế tiếp từ lịch sử đã có.” |
| C5/8 | “Em khởi động lại query với cùng checkpoint. Em kiểm tra việc chạy lại có làm tăng số hàng hay không.” |
| C6/8 | “Em đưa thêm một file chứa timestamp đã có. Tổng file là 25, đầu ra mong đợi vẫn 24 giờ duy nhất.” |
| C7/8 | “Chương trình kiểm tra các giai đoạn và đối chiếu dự báo với đầu ra mô hình offline.” |
| C8/8 | “Đang hoàn tất dashboard và báo cáo kiểm định của lượt này.” |

Dashboard tự mở sau đợt đầu và cập nhật sau từng giai đoạn. Nếu cần mở lại, dùng lệnh dưới từ một PowerShell khác trong cùng thư mục hoặc đợi lệnh Plan C kết thúc rồi mở lại.

```powershell
Start-Process ".\outputs\streaming\index.html"
```

Đợi PLAN C COMPLETED trước khi nêu kết quả cuối.

“Đã đọc 25 JSON và lưu 24 giờ duy nhất trên HDFS. Chạy lại thêm 0 hàng, file trùng thêm 0 hàng. Điều này minh họa luồng dự báo bằng mô hình đã huấn luyện, cùng khả năng dùng checkpoint và xử lý trùng trong đoạn replay.”

## Slide 11 Kết luận và kiểm tra chuỗi

Chuyển PowerShell và gõ.

```powershell
.\demo_preflight.ps1
```

Chỉ các mục PASSED và quan hệ lượt chạy True.

“Em đã chạy từ nguồn CSV đến dữ liệu kiểm định, phân tích, dự báo và replay luồng. Kiểm tra cuối xác nhận các phần dùng đúng dữ liệu và mô hình của chuỗi vừa chạy. Các giới hạn hiện tại là dữ liệu một năm ở Đức và môi trường một máy. Bước tiếp theo cần kiểm tra thêm nhiều năm, thời tiết, ngày lễ và thời điểm số đo thực tế được công bố.”

“Em xin nhận góp ý của các thầy về phần phân tích và cách đánh giá kết quả.”

## Khi có lỗi hoặc thầy hỏi thêm

- Nếu terminal báo lỗi, đọc nhãn A/B/C trước rồi đọc phần lỗi cuối. Thông báo lỗi nguyên nhân và giữ màn hình tại đó để xác định bước cần xử lý.
- Nếu script bị chặn chạy trong PowerShell, đọc thông báo cụ thể trước khi thay đổi cấu hình. Có thể mở thư mục và chạy bằng PowerShell profile khác nếu đó là vấn đề môi trường.
- Nếu dashboard bị che, mở lại bằng Start-Process sau khi terminal rảnh. Một cửa sổ PowerShell chỉ chạy lệnh tiếp khi tác vụ trước đã trả lại dấu nhắc.
- Nếu chạy lại A, chạy tiếp B rồi C để kết quả cùng bộ dữ liệu mới. Nếu chạy lại B, chạy tiếp C để dùng mô hình mới.
- Nếu hỏi vì sao Spark/HDFS với 8.760 giờ, trả lời “Em dùng để thực hành đầy đủ kiến trúc và kiểm định qua các bước. Phạm vi chạy là một máy và em chưa có kết quả benchmark để kết luận về tốc độ phân tán.”
- Nếu hỏi tại sao baseline vẫn cần, trả lời “Baseline cho biết Random Forest cải thiện sai số bao nhiêu trên cùng mốc dự báo. Nó được thực hiện trong Plan B.”

## Thời gian cần tính khi tập

Một lượt đã đo khoảng A 3 phút, B 2,5 phút và C 5 phút. Đây là thời gian xử lý, cần cộng thêm phần giới thiệu, giải thích biểu đồ và hỏi đáp. Hãy chạy một lượt bấm giờ trên máy trước buổi bảo vệ và báo thầy thời lượng cần cho phần chạy trực tiếp. Các kết quả đã lưu có thể dùng để tiếp tục giải thích trong khi job đang chạy.
