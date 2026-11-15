# Minh chứng để trình chiếu hoặc chụp màn hình

Chạy lệnh trong PowerShell tại `D:\OneDrive\Tài liệu\ChatGPT\BigData`. Ảnh nên chụp bằng `Win + Shift + S`, đặt tên theo số thứ tự và lưu trong `evidence/`. Mỗi ảnh chỉ cần khoanh một kết luận; không chụp nguyên màn hình đầy log Spark.

| # | Mở/chạy | Điều cần thấy và chú thích ngắn |
|---|---|---|
| 1 | `docker compose ps` | `namenode` healthy, `datanode` và `spark` Up. “Môi trường một máy, ba dịch vụ.” |
| 2 | `docker exec namenode hdfs dfs -ls -R /bigdata/opsd/processed/de_2019` | File `.snappy.parquet` dưới `year=2019`. “Tập đã chọn thực sự nằm trên HDFS.” |
| 3 | `Get-Content .\outputs\quality\quality_summary.json -Raw` | `overall_status: PASSED`, 8 nhóm kiểm định. “8.760 giờ không thiếu/trùng và khớp CSV đã chọn.” |
| 4 | `Start-Process .\outputs\trends\index.html` | MA30 và nhiệt đồ. “Biến động phụ tải theo ngày, giờ, thứ trong năm 2019.” |
| 5 | `Start-Process .\outputs\ml_forecast\index.html`; `./demo_verify_mae.ps1` | MAE RF 1.135,29 MW; script báo `MAE CROSS-CHECK PASSED`. “Tự chấm lại 2.208 giờ test, so cùng baseline.” |
| 6 | `Start-Process .\outputs\streaming\index.html`; `Get-Content .\outputs\streaming\checks.json -Raw` | 8→16→24; 25 JSON, 24 giờ duy nhất; restart và duplicate +0. “Replay lịch sử, HDFS + checkpoint.” |
| 7 | `./demo_preflight.ps1` | Tất cả PASSED; A/B cùng ETL run, C cùng ETL/ML run. “Bằng chứng các nhánh đồng bộ.” |

**Không phải chụp mọi mục nếu thầy chỉ có vài phút.** Ưu tiên 1, 2, 5, 6 và slide sơ đồ. Bản chạy đồng bộ ngày 14/11 có `ETL run_id = 73be36bd-74fa-41c3-9bc9-28582e11da7b`, `ML run_id = 091c8a2c-b5ec-4eba-bdd7-1d37b92aef54`, `Plan C run_id = 20261114T101440149`. Nếu chạy lại job, run ID sẽ đổi; lấy số mới từ `outputs/` thay vì dùng ảnh cũ.
