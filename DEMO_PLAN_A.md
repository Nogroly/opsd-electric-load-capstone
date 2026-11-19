# Demo Plan A Dữ liệu và xu hướng

## Chuẩn bị kết quả

Trong PowerShell tại thư mục dự án, bật Docker Desktop và chạy:

```powershell
.\download_opsd.ps1
.\run_plan_a.ps1
```

Các bước in nhãn `[A0/6]` đến `[A6/6]`. Plan A đọc CSV OPSD, ghi Parquet lên HDFS, đọc lại và kiểm định, tính các bảng SQL, rồi tính và kiểm tra MA7/MA30 cùng nhiệt đồ. Khi thấy `PLAN A COMPLETED`, mở `outputs/trends/index.html` để xem tất cả biểu đồ của Plan A. Các bảng CSV và trang SQL chi tiết nằm ở `outputs/analytics`.

Nếu đã có Parquet của cùng lượt ETL, dùng `.\run_plan_a.ps1 -SkipEtl` để tính lại các bảng và xu hướng. Dùng `-NoOpen` nếu muốn tự mở trình duyệt sau.

## Cho thầy xem dữ liệu đang có trên HDFS

```powershell
docker compose ps
docker exec spark /opt/spark/bin/spark-submit --master "local[2]" /workspace/jobs/demo_readback.py
```

Nói ba dịch vụ đang chạy là NameNode, DataNode và Spark. Lệnh thứ hai đọc Parquet thật, in schema, năm dòng và số đếm toàn tập. Chỉ vào dòng **8760**, khoảng thời gian từ **01/01/2019 00:00 UTC đến 31/12/2019 23:00 UTC** và `READBACK CHECK PASSED`. **8/8 nhóm kiểm định** và đối soát CSV có trong `outputs/quality/quality_summary.json`.

## Chọn biểu đồ để giải thích kết quả

- **MA7/MA30.** Trục ngang là ngày kết thúc cửa sổ UTC. Trên đường MA30, chỉ mốc cao nhất kết thúc ngày 07/02 và thấp nhất kết thúc ngày 25/08. Mỗi điểm lấy trung bình của 7 hoặc 30 ngày đã quan sát.
- **Ngày thường và cuối tuần.** Bấm mục cùng tên ở đầu trang. Chỉ hai cột **59.042,57 MW** và **48.330,13 MW**. Cuối tuần thấp hơn **18,14%** trong mẫu 2019. Các nhóm lấy trung bình trên số đo từng giờ, phân loại theo thứ tại Đức và chưa tách ngày lễ.
- **Khi thầy hỏi thêm.** Bấm **Theo giờ** để chỉ 11:00 giờ Đức có mức trung bình cao nhất. Bấm **Theo tháng** để so tháng 01 và tháng 08 UTC. Mở **Nhiệt đồ** để xem phân bố theo cả thứ và giờ, đọc MW theo thang màu.

Sau Plan A, chạy `.\run_plan_b.ps1` để tự tính hai baseline và đánh giá Random Forest. Xem [cách sửa slide và lấy ảnh minh chứng](reporting/2026-11-17/HUONG_DAN_SUA_SLIDE_VA_CHUP_MINH_CHUNG.md) để tập toàn bộ bài.
