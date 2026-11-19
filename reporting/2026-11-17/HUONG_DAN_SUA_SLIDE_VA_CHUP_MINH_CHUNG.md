# Hướng dẫn sửa slide và chụp minh chứng Project 4

Bài bảo vệ ngày 19/11 dùng 11 slide. Luồng demo đã chốt là **chạy Plan A dữ liệu và xu hướng → chạy Plan B dự báo trước 1 giờ → chạy Plan C replay lịch sử → kết luận**. Dùng [kịch bản A → B → C](../../DEMO_A_B_C.md) để giải thích terminal và chuyển giữa ba dashboard.

## 1. Mở đúng bản và chuẩn bị dữ liệu

1. Mở [bản Canva 11 slide để chỉnh sửa](https://www.canva.com/d/38WkXqBjUBGzYt6). Bản dùng để sửa có phần sơ đồ khối, MA7/MA30, Random Forest và replay 24 giờ. Nếu thứ tự trang của bạn đã đổi, dò theo **tiêu đề**.
2. Tạo một bản sao trong Canva để sửa bản final. Đặt tên **Project 4 Phụ tải điện Đức 2019 Final**. Sửa slide 5, 7 và 9 trước vì cả ba dùng lại trong bài nói hoặc báo cáo.
3. Mở Docker Desktop. Trong PowerShell, về thư mục dự án và kiểm tra bộ kết quả đã có:

```powershell
Set-Location -LiteralPath "D:\OneDrive\Tài liệu\ChatGPT\BigData"
.\demo_preflight.ps1 -CoreOnly
.\demo_verify_mae.ps1
```

Khi thấy `EVIDENCE PREFLIGHT PASSED` và `MAE CROSS-CHECK PASSED`, bạn có thể mở dashboard, lấy ảnh và tập demo. Nếu cần tạo lại bộ kết quả A/B, chạy `.\run_plan_a.ps1 -NoOpen` rồi `.\run_plan_b.ps1 -NoOpen`. Đợi từng lệnh báo `COMPLETED`. `-NoOpen` cho phép bạn tự mở dashboard khi cần.

| Cần mở | Đường dẫn tính từ thư mục dự án |
|---|---|
| Plan A, MA7/MA30, trung bình theo giờ, tháng và nhóm ngày, hai nhiệt đồ | `outputs\trends\index.html` |
| Plan B, so sánh ba phương pháp trên cùng tập test | `outputs\ml_forecast\index.html` |
| Plan C, minh chứng replay | `outputs\streaming\index.html` |

## 2. Sửa từng slide

| Slide sau khi sắp xếp | Bạn sửa gì trên Canva | Chữ ngắn để đặt lên trang |
|---|---|---|
| **1. Mở bài** | Giữ bố cục và tên hai thành viên. Đổi tiêu đề để thầy hiểu ngay đối tượng và chân trời dự báo. | **Phân tích và dự báo phụ tải điện Đức** · 2019 · Dự báo trước 1 giờ |
| **2. Lộ trình** | Đặt ba plan theo thứ tự chạy và kết luận ở cuối. | **Plan A Dữ liệu và xu hướng** · **Plan B Dự báo trước 1 giờ** · **Plan C Replay lịch sử** · **Kết luận và giới hạn** |
| **3. Bài toán** | Đặt một câu nêu mục đích thực tế ở trên, hai câu hỏi ở dưới. | *Xác định khung giờ phụ tải thường cao để theo dõi và đánh giá dự báo trước một giờ.* / *Phụ tải thay đổi thế nào trong năm và trong tuần?* / *Random Forest giảm sai số bao nhiêu so với hai baseline?* |
| **4. Dữ liệu** | Giữ số nguồn và số sau lọc. Ghi nguyên tên hai cột ở dòng nhỏ. | **OPSD bản 06/10/2020** · **50.401 dòng nguồn → 8.760 giờ Đức năm 2019** · MW · Khóa UTC, nhiệt đồ theo giờ Berlin. Cột dùng: `utc_timestamp`, `DE_load_actual_entsoe_transparency`. |
| **5. Sơ đồ khối** | Sửa nhãn, hướng mũi tên và đặt nhánh Plan C thấp hơn hai nhánh chính. Dùng chi tiết ngay dưới bảng. | **Từ CSV đến dữ liệu kiểm định, xu hướng và dự báo** |
| **6. Kiểm định Plan A** | Đưa **8.760 giờ** và **8/8 nhóm đạt** lên to. Nếu thêm ảnh terminal, dùng ảnh cắt ở mục 3. | **8.760 giờ đọc lại từ HDFS** · **0 giờ thiếu · 0 khóa trùng · 0 dòng khác biệt mỗi chiều** |
| **7. Xu hướng Plan A** | Tìm trang đang có tiêu đề “Plan B Phân tích xu hướng” và đổi thành **Plan A Xu hướng phụ tải**. Dùng MA7/MA30 làm hình chính. Đặt kết quả ngày thường/cuối tuần trong một thẻ số hoặc một biểu đồ hai cột nếu đủ chỗ. Mở các biểu đồ theo giờ, tháng và nhiệt đồ trực tiếp từ dashboard khi cần. | **MA30 cao nhất 62.918 MW ngày 07/02, thấp nhất 51.446 MW ngày 25/08** · **Phụ tải trung bình cuối tuần thấp hơn 18,14% so với ngày thường** |
| **8. Phương pháp Plan B** | Gộp hai baseline và Random Forest vào trang này. Dùng ba thẻ và một dòng chia tập theo thời gian. | **Giờ gần nhất t−1** · **Cùng giờ tuần trước t−168** · **Random Forest t−1, t−24, t−168 và lịch UTC**. Huấn luyện 1–8 → chọn 9 → test 10–12. |
| **9. Kết quả Plan B** | Hiện ba cột MAE của tập test. Chuyển số validation tháng 9 vào ghi chú khi nói. Đặt “2.208 giờ test” cạnh tên biểu đồ. | **Random Forest 1.135,29 MW** · **Giờ gần nhất 1.977,92 MW** · **168 giờ trước 2.897,08 MW** · **Giảm 42,60% MAE so với giờ gần nhất trên tập này** |
| **10. Plan C** | Đặt trang replay sau kết quả dự báo và dùng khi chạy Plan C. | **JSON lịch sử đến theo đợt 8 → 16 → 24** · **25 JSON đọc → 24 giờ duy nhất** · **Chạy lại +0 · File trùng +0** |
| **11. Kết luận** | Đặt sau cả ba plan. Viết kết quả và hướng phát triển có căn cứ. | **Dữ liệu đạt kiểm định. Phân tích được nhịp phụ tải. Random Forest giảm MAE trên test 2019. Replay kiểm tra được checkpoint và trùng.** / *Kiểm tra thêm năm khác và độ trễ số đo.* |

### Slide 5 Vẽ lại đúng hướng dữ liệu

1. Giữ bốn khối ngang **OPSD CSV → PySpark ETL → HDFS và Parquet → Kiểm định**. Mỗi khối dùng tối đa hai dòng mô tả ngoài tên. Ghi thứ tự lần lượt **50.401 dòng theo giờ**, **chọn Đức 2019**, **8.760 giờ Parquet**, **8/8 nhóm đạt**.
2. Từ khối **Kiểm định**, vẽ hai mũi tên sang hai khối kết quả. Khối xanh gắn logo Spark và chữ **Plan A · Spark SQL và PySpark**, bên dưới ghi **Giờ, tháng, nhóm ngày · MA7/MA30, nhiệt đồ → HTML**. Khối còn lại gắn logo Spark và chữ **Plan B · Spark ML**, bên dưới ghi **Hai baseline và Random Forest → MAE/RMSE → HTML**.
3. Bên dưới hai nhánh chính, đặt một đoạn nhỏ **Plan C · Replay lịch sử**. Nối dữ liệu Parquet đến hộp **phát 24 giờ JSON**, rồi đến **Spark Structured Streaming**. Nối thêm mũi tên nét đứt **mô hình đã huấn luyện** từ Plan B đến hộp Spark Structured Streaming. Đầu ra là **dự báo Parquet và checkpoint trên HDFS**. HDFS lưu hai phần ở các đường dẫn riêng.
4. Dùng lại logo Spark, Hadoop/HDFS và Parquet đang có, đi kèm nhãn chữ. Có thể đặt nhãn **Docker Compose · NameNode, DataNode, Spark trên một máy** bên dưới toàn sơ đồ. Docker dựng môi trường, nên để nhãn ngoài đường mũi tên dữ liệu.
5. Khi nói, dừng một nhịp ở HDFS để giải thích “lưu Parquet rồi đọc lại kiểm tra”, sau đó chỉ hai mũi tên tới xu hướng và dự báo. Xuất riêng slide này theo hướng dẫn **Hình 1** ở mục 3.

### Slide 7 và 9 Đặt biểu đồ vào Canva

- **Slide 7.** Dùng `outputs\trends\daily_trend.svg` hoặc ảnh chụp cùng tên biểu đồ trên dashboard. Giữ trục **Phụ tải trung bình (MW)**, trục **Ngày kết thúc cửa sổ (UTC)** và chú giải MA7/MA30. Nếu dùng hình hai cột, lấy `outputs\analytics\day_type.svg` và giữ trục tung từ 0 MW. Nếu trang đã có nhiệt đồ, có thể giữ hình đó và thêm một thẻ chữ **−18,14% vào cuối tuần**. Nhiệt đồ cần giữ nhãn giờ Đức và thang màu MW.
- **Slide 9.** Dùng `outputs\ml_forecast\test_mae.svg` hoặc chụp riêng mục “So sánh ba cách dự báo trên 2.208 giờ test”. Giữ trục **MAE (MW)** từ 0 và trục **Phương pháp dự báo**, tên và số trên từng cột. Khi nói, giải thích MAE là sai số tuyệt đối trung bình.
- Giữ màu MA30 và màu Random Forest nhất quán giữa dashboard, slide và bài nói. Chiếu thử ở chế độ toàn màn hình để kiểm tra nhãn và số ngày.

## 3. Chụp và đưa ảnh vào Word

Lưu PNG vào một thư mục riêng trong `evidence` để dễ tìm. Với ảnh chụp màn hình, dùng `Win + Shift + S`, khoanh vùng cần lấy, rồi lưu PNG. Cắt thông tin ở viền cửa sổ để khi chiếu người xem tập trung vào số và biểu đồ.

| Tệp ảnh | Thao tác lấy ảnh | Đặt ở đâu |
|---|---|---|
| **01_so_do_khoi.png** | Trong Canva, xuất **riêng slide 5 sau khi sửa** ở định dạng PNG. Cắt gọn quanh khối, mũi tên và chú giải Plan C. | **Hình 1, trang 1 Word.** |
| **02_ma7_ma30.png** | Mở `outputs\trends\index.html`, bấm **MA7 và MA30** rồi chụp biểu đồ. Giữ tên hai trục, mốc tháng và chú giải hai đường. Nếu ảnh đã chụp trước khi có tên trục, hãy chụp lại ảnh này. | **Hình 2, trang 2 Word.** Dùng lại cho slide 7. |
| **03_hdfs_qa.png** | Chạy lệnh đọc lại hoặc lệnh tóm tắt kiểm định ở dưới. Ảnh đọc lại cần thấy **8760**, hai mốc UTC và **PASSED**. Ảnh tóm tắt cần thấy **8/8**, **8760** và **PASSED**. | **Slide 6** hoặc lưu dự phòng để thầy xem thêm. |
| **04_nhiet_do_thu_gio.png** | Bấm **Nhiệt đồ** trên dashboard Plan A. Giữ các nhãn thứ, giờ, thang màu MW và dòng nhận định. | **Slide 7** nếu dùng nhiệt đồ hoặc lưu cho hỏi đáp. |
| **05_mae_test.png** | Chụp mục so sánh 2.208 giờ trong dashboard dự báo. Nếu muốn kèm phép tính, chụp riêng phần cuối sau khi chạy `.\demo_verify_mae.ps1`. | **Slide 9** hoặc minh chứng nói về MAE. Bản Word đã có bảng số liệu. |
| **06_replay_24h.png** | Mở dashboard streaming hoặc dùng lệnh tóm tắt replay ở dưới. Giữ **25**, **24**, **+0 sau chạy lại** và **+0 khi trùng** trong cùng vùng. | **Slide 10 Plan C**. |
| **07_ngay_thuong_cuoi_tuan.png** | Bấm **Ngày thường và cuối tuần** trên dashboard Plan A. Giữ cả hai cột, tên nhóm ngày, trục MW và dòng **thấp hơn 18,14%**. | **Slide 7** nếu dùng hình hai cột. |

Chèn **hai ảnh đầu** vào Word. Bôi đen dòng `CHÈN HÌNH` trong khung rồi chèn PNG với bố trí **Cùng dòng với văn bản**. Giữ tỷ lệ ảnh. Hình 1 rộng tối đa khoảng **15,5 cm** và cao khoảng **6 cm**. Hình 2 rộng tối đa khoảng **15,5 cm** và cao khoảng **5,5 cm**. Giữ chú thích dưới ảnh. Kiểm tra lại ba trang trong Print Layout, xác nhận phân công hai thành viên và xóa các comment góp ý khi đã duyệt.

### Lệnh đọc lại trực tiếp trên HDFS

```powershell
docker exec spark /opt/spark/bin/spark-submit --master "local[2]" /workspace/jobs/demo_readback.py
```

Khi terminal đang chạy, nói rằng Spark đọc Parquet đã ghi, in schema, năm dòng mẫu và số dòng của cả tập. Dừng ở bảng có `8760`, hai đầu mốc UTC và `READBACK CHECK PASSED`. Kết quả kiểm định đủ 8 nhóm nằm ở lệnh dưới.

### Xem gọn 8 nhóm kiểm định

```powershell
$qa = Get-Content -LiteralPath '.\outputs\quality\quality_summary.json' -Raw -Encoding UTF8 | ConvertFrom-Json
$qaTests = @($qa.tests.PSObject.Properties)
$passed = @($qaTests | Where-Object { $_.Value.passed -eq $true }).Count
[pscustomobject]@{
    Status = $qa.overall_status
    PassedGroups = "$passed/$($qaTests.Count)"
    HDFSRows = $qa.data_lineage.readback_rows
    MissingHours = $qa.tests.test_2_timestamp_continuity.timeline_gaps_count
    DuplicateKeys = $qa.tests.test_1_row_count_and_keys.duplicate_keys_count
    CSVMinusParquet = $qa.tests.test_5_exact_reconciliation_except_all.source_except_processed_count
    ParquetMinusCSV = $qa.tests.test_5_exact_reconciliation_except_all.processed_except_source_count
} | Format-List
```

### Xem gọn kết quả replay đã lưu

```powershell
Get-Content -LiteralPath '.\outputs\streaming\checks.json' -Raw -Encoding UTF8 |
    ConvertFrom-Json |
    Select-Object status, spark_input_rows, unique_hdfs_rows, restart_added_rows, duplicate_added_rows |
    Format-List
```

## 4. Tập trình bày một lượt rồi xuất PowerPoint

| Thứ tự tập | Màn hình | Câu nói trọng tâm |
|---|---|---|
| Mở đầu | Slide 1–5 | “Nguồn là hai cột phụ tải theo giờ. Em sẽ chạy lần lượt ba plan theo sơ đồ.” |
| Chạy Plan A | Slide 6 rồi 7 và dashboard A | “Em ghi rồi đọc lại HDFS, kiểm tra 8.760 giờ, sau đó xem mức tải thay đổi theo thời gian và nhóm ngày.” |
| Chạy Plan B | Slide 8 rồi 9 và dashboard B | “Em tính hai baseline, huấn luyện Random Forest và so sai số trên cùng 2.208 giờ test.” |
| Chạy Plan C | Slide 10 và dashboard C | “Em dùng mô hình Plan B để dự báo khi dữ liệu lịch sử đến theo đợt. Tiếp theo kiểm tra chạy lại và file trùng.” |
| Kiểm tra và kết luận | `demo_preflight.ps1` và slide 11 | “Cả ba phần dùng đúng dữ liệu và mô hình của chuỗi chạy. Phạm vi là Đức 2019 trên một máy.” |

Luồng demo chạy trực tiếp `run_plan_a.ps1`, `run_plan_b.ps1`, `run_plan_c.ps1` theo thứ tự. Xem nhãn A/B/C để giải thích phần đang xử lý, đợi COMPLETED trước khi trình bày kết quả rồi chuyển phần. Bấm giờ khi tập theo [kịch bản A → B → C](../../DEMO_A_B_C.md) để tính cả thời gian chờ Spark và phần nói.

Sau khi sửa xong, trên Canva chọn **Share → Download → Microsoft PowerPoint (.pptx)**, chọn cả **11 trang**. Mở tệp vừa tải trong PowerPoint và rà riêng slide 5, 7, 9, 11. Nộp file PowerPoint, Word đã chèn hai hình, mã/README và thông tin dữ liệu theo thư mục Drive của lớp. [Cách tải thiết kế từ Canva](https://www.canva.com/help/download-or-purchase/) · [Định dạng PowerPoint và lưu ý khi mở lại](https://www.canva.com/help/download-file-types/)
