# Demo Project 4 theo thứ tự A B C

Mở PowerShell ở thư mục dự án và Docker Desktop trước khi chia sẻ màn hình. Tải hoặc kiểm tra CSV bằng .\download_opsd.ps1 trước giờ trình bày. Trình bày từng dashboard sau khi plan tương ứng chạy xong, rồi mới chạy lệnh tiếp.

## Plan A Dữ liệu và phân tích

```powershell
.\run_plan_a.ps1
```

Trước khi chạy, nói “Em bắt đầu từ CSV OPSD theo giờ. Plan A chọn phụ tải Đức năm 2019, ghi lên HDFS và kiểm tra dữ liệu đọc lại. Sau đó Spark tính các bảng tổng hợp và xu hướng.”

| Khi thấy | Giải thích cho thầy |
|---|---|
| A1/6 | “Spark đang đọc CSV, lấy thời gian UTC và phụ tải MW, lọc năm 2019 rồi ghi Parquet lên HDFS.” |
| A2/6 | “Spark đọc lại Parquet, kiểm tra chất lượng và tính trung bình theo giờ, tháng và nhóm ngày.” |
| A3/6 | “Bước này đối chiếu các bảng tổng hợp với cách tính độc lập từ CSV nguồn.” |
| A4/6 và A5/6 | “Spark tính MA7/MA30 cùng nhiệt đồ, rồi kiểm tra các cửa sổ và số nhóm.” |
| A6/6 | “Các bảng đã kiểm định được dựng thành dashboard HTML.” |
| PLAN A COMPLETED | Mở dashboard tự bật. Nói “Có đủ 8.760 giờ và 8/8 nhóm kiểm định đạt.” |

Chọn biểu đồ để giải thích, giới thiệu trục trước khi nêu kết luận. Trục tung là phụ tải trung bình MW. Theo giờ và nhóm ngày dùng giờ Đức, theo tháng và MA7/MA30 dùng UTC.

- Theo giờ, 11h có phụ tải trung bình cao nhất khoảng 64.120 MW.
- Theo tháng, tháng 01 cao nhất khoảng 61.597 MW và tháng 08 thấp nhất khoảng 52.172 MW.
- Theo nhóm ngày, cuối tuần thấp hơn ngày thường 18,14% trong mẫu 2019.
- MA7/MA30 làm rõ mức tải biến động trong năm. Với MA30, mốc 07/02 là ngày kết thúc cửa sổ cao nhất, 25/08 là ngày kết thúc cửa sổ thấp nhất.
- Nhiệt đồ cho phép nhìn đồng thời thứ và giờ. Giá trị MW được đọc theo thang màu.

Chuyển phần “Từ tập Parquet đã kiểm định này, em chạy Plan B để đo xem mô hình dự báo trước một giờ cải thiện sai số bao nhiêu.”

## Plan B Dự báo trước một giờ

```powershell
.\run_plan_b.ps1
```

Nói “Plan B dùng cùng dữ liệu HDFS của Plan A. Em so hai cách dự báo đơn giản với Random Forest. Mọi phương pháp dùng cùng các giờ test.”

| Khi thấy | Giải thích cho thầy |
|---|---|
| B1/5 và B2/5 | “Em tạo dự báo từ phụ tải giờ gần nhất và đúng 168 giờ trước, rồi kiểm tra mốc thời gian cùng sai số.” |
| B3/5 | “Random Forest nhận lag 1, 24, 168 giờ và thông tin giờ, thứ UTC. Huấn luyện tháng 1–8, chọn cấu hình bằng tháng 9 rồi đánh giá tháng 10–12.” |
| B4/5 và B5/5 | “Chương trình đối chiếu lại dự báo và chỉ số, rồi dựng dashboard so sánh.” |
| PLAN B COMPLETED | Chỉ vào ba cột MAE trên cùng 2.208 giờ test. |

Nói “Trục ngang là phương pháp, trục tung là MAE tính bằng MW. MAE lấy trị tuyệt đối của từng sai số rồi tính trung bình. Random Forest đạt 1.135,29 MW, giảm 42,60% so với baseline giờ gần nhất trong tập test này.”

Nếu cần kiểm chứng phép tính trước mặt thầy, dùng lệnh sau.

```powershell
.\demo_verify_mae.ps1
```

Chuyển phần “Mô hình vừa được huấn luyện sẽ dùng tiếp ở Plan C. Em phát lại dữ liệu lịch sử theo đợt để minh họa tạo dự báo khi có dữ liệu mới đến.”

## Plan C Replay lịch sử và kiểm tra luồng

```powershell
.\run_plan_c.ps1
```

Nói “Plan C lấy 24 giờ lịch sử ngày 01/10/2019. Chương trình phát file JSON theo ba đợt, Spark Structured Streaming đọc file mới, dùng mô hình Plan B dự báo giờ kế và ghi kết quả Parquet cùng checkpoint vào hai đường dẫn HDFS riêng.”

| Khi thấy | Giải thích và thao tác |
|---|---|
| C1/8 | “Em chuẩn bị 24 giờ từ HDFS, gắn lượt chạy này với đúng dữ liệu và mô hình vừa tạo.” |
| C2/8 | “Đợt đầu nhận 8 giờ. Dashboard mở khi đã có đầu ra kiểm tra được.” |
| C3/8 và C4/8 | Giữ dashboard mở. Chỉ số giờ tăng 8 → 16 → 24 sau từng đợt. |
| C5/8 | “Em khởi động lại query với checkpoint đã lưu. Kết quả mong đợi là 24 giờ vẫn giữ nguyên.” |
| C6/8 | “Em gửi thêm một file chứa mốc giờ trùng. Tổng file tăng lên 25, nhưng đầu ra chỉ có 24 giờ duy nhất.” |
| C7/8 và C8/8 | “Chương trình kiểm tra các giai đoạn, so dự báo trong luồng với mô hình offline và hoàn tất báo cáo.” |
| PLAN C COMPLETED | Chỉ kết quả 25 JSON đọc, 24 giờ duy nhất, chạy lại +0 và file trùng +0. |

## Kết thúc và kiểm tra toàn chuỗi

```powershell
.\demo_preflight.ps1
```

Nói “Kiểm tra này xác nhận A, B, C đều đạt và sử dụng đúng dữ liệu, baseline, mô hình của cùng chuỗi chạy. Kết quả hiện tại phản ánh dữ liệu Đức 2019 trên môi trường một máy. Bước phát triển tiếp là kiểm tra nhiều năm và xét độ trễ khi nhận số đo.”

## Khi tập demo

Chạy lần lượt, đợi COMPLETED và giải thích dashboard trước khi chuyển plan. Dòng INFO, Stage và Task mô tả tiến trình Spark. Nếu có lỗi, dừng tại phần đó, đọc thông báo và kiểm tra nguyên nhân trước khi tiếp tục.

Một lượt đã đo khoảng A 3 phút, B 2,5 phút và C 5 phút. Lúc tập, ghi cả thời gian chạy và thời gian bạn nói để trao đổi với thầy nếu tổng thời gian vượt khung 10 phút. Chi tiết đầu vào, đầu ra và các tùy chọn có trong [README](README.md).
