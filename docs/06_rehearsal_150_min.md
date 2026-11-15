# Lịch 2 giờ 30 phút — chốt bài và tập demo

| Phút | Việc làm | Đầu ra phải có |
|---:|---|---|
| 0–15 | Mở bản slide Canva, bốn dashboard; chạy `./demo_preflight.ps1` và `./demo_verify_mae.ps1`. | Biết ba container đang Up, tất cả PASSED, MAE tính lại khớp. |
| 15–40 | Rà slide 1–11 theo `docs/05_slide_content_review.md`; đọc thành tiếng câu chính của mỗi trang. | Mỗi slide có đúng **một thông điệp**; ghi 3 chỗ chữ nhỏ/khó hiểu cho bạn nhóm sửa. |
| 40–60 | Đọc báo cáo gửi thầy; tự trả lời vì sao chọn Spark/HDFS/Docker, giới hạn dữ liệu. | Giải thích được mỗi công nghệ trong 1–2 câu, không đọc thuộc slide. |
| 60–100 | Demo lượt 1 theo `docs/03_demo_7_phut.md`, tự bấm từng lệnh **chỉ đọc**. Thu âm hoặc bấm giờ. | Không im lặng lúc terminal chạy; nói được đầu vào, thao tác, đầu ra từng bước. |
| 100–125 | Tự hỏi đáp 8 câu bên dưới, mở bằng chứng khi cần. | Trả lời từng câu trong 30–60 giây bằng số/đường dẫn cụ thể. |
| 125–140 | Demo lượt 2 không nhìn script; ghi thời gian từng đoạn và lỗi nói. | Bài 7–9 phút, kết luận rõ giới hạn và hướng tiếp. |
| 140–150 | Chốt gói gửi nhóm/thầy, giao việc và câu hỏi cần thầy góp ý. | Gửi slide + repo + báo cáo, nêu bạn nhóm chỉnh **trình bày**, Vinh giữ **số liệu/kỹ thuật**. |

## Tám câu tự kiểm tra trước khi thầy hỏi

1. **8.760 từ đâu?** 365 ngày × 24 mốc UTC, đọc lại HDFS không thiếu/trùng.
2. **Dùng cột gì?** `utc_timestamp` và `DE_load_actual_entsoe_transparency`; MW.
3. **MA30 khác dự báo thế nào?** Trung bình trượt 30 ngày đã xảy ra, chỉ làm mượt lịch sử.
4. **MAE 1.135 MW nghĩa gì?** Trung bình trị tuyệt đối sai số của 2.208 giờ test; không phải mỗi giờ sai 1.135 MW. Chạy `demo_verify_mae.ps1` để tính lại.
5. **Tránh rò rỉ tương lai?** Đặc trưng lấy phụ tải `t−1`, `t−24`, `t−168`; chia train/validation/test theo thời gian.
6. **Vì sao Random Forest?** Mô hình hồi quy nhỏ nằm trong Spark ML, có baseline công bằng; chưa chứng minh tốt hơn mọi mô hình.
7. **Streaming này là gì?** Phát lại 24 giờ 2019 thành JSON theo đợt; Structured Streaming xử lý file mới và lưu checkpoint, không phải điện kế trực tiếp.
8. **Docker thay VM vì sao?** Compose dựng ba dịch vụ/mạng/volume trên một máy dễ lặp lại; không tuyên bố chịu lỗi nhiều máy.

## Câu mở đầu và câu chốt

**Mở:** “Em trình bày một pipeline phụ tải điện có thể kiểm chứng từ dữ liệu gốc đến biểu đồ, dự báo và replay luồng. Em sẽ chỉ rõ mỗi lệnh nhận gì, tạo gì và kết quả được kiểm tra ở đâu.”

**Chốt:** “Em đã hoàn thành ba nhánh ở phạm vi demo một máy. Kết luận xu hướng và MAE chỉ dựa trên dữ liệu Đức 2019; bước tiếp là kiểm tra nhiều giai đoạn và thêm biến nghiệp vụ như thời tiết/ngày lễ trước khi nghĩ đến triển khai trực tiếp.”
