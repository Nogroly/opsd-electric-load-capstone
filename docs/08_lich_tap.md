# Lịch tập và chốt bản cuối

**Thứ Bảy 14/11:** đã chạy lại Plan A → baseline → Plan B → Plan C; preflight xác nhận mọi nhánh cùng ETL run, Plan C dùng đúng ML run. Vinh đọc [bản học kỹ thuật](02_ky_thuat_cho_vinh.md), chạy `./demo_verify_mae.ps1`, học cách giải thích MAE bằng 3 số, rà slide 5/8/9/10.

**Tối Thứ Ba 17/11 — buổi xin nhận xét:** trước buổi học bật Docker, `docker compose up -d`, `docker compose ps`, chạy preflight và mở sẵn 4 dashboard. Tập [thẻ nói 7 phút](03_demo_7_phut.md) với bạn nhóm một lượt có bấm lệnh, một lượt chỉ dùng slide/dashboard. Mời thầy hỏi từng khối: dữ liệu vào, lệnh làm gì, đầu ra, vì sao chọn công nghệ, giới hạn. Ghi nguyên văn ý kiến và tách “phải sửa trước Thứ Năm” với “hướng sau capstone”.

**Thứ Tư 18/11:** sửa đúng phản hồi thầy; so số trên slide với `outputs/`; không đổi số chỉ để biểu đồ đẹp. Không cần chạy lại toàn pipeline nếu không sửa dữ liệu/code. Nếu sửa một bước ảnh hưởng đầu ra, chạy lại theo thứ tự từ bước đó (Plan A → baseline → Plan B → Plan C), đợi từng dòng `COMPLETED`, rồi `./demo_preflight.ps1` và mở lại dashboard.

**Thứ Năm 19/11 — bản cuối:** chốt deck, dashboard, tệp QA, bản demo. Kiểm tra bốn điều: (1) slide nói đúng đã làm, (2) lệnh chạy được, (3) số MAE và 8/8 QA được chứng minh, (4) Vinh tự giải thích được một dòng log và từng mũi tên sơ đồ. Không cần thêm công nghệ mới sát giờ báo cáo.

**Tiêu chí rehearsal đạt:** nói được toàn mạch dưới 7 phút; giải thích MAE bằng ví dụ và số thật; chỉ rõ replay JSON khác dữ liệu trực tiếp; biết lệnh nào chỉ đọc, lệnh nào chạy lại; thừa nhận giới hạn một máy và chứng minh các nhánh hiện cùng ETL run ID nếu được hỏi.
