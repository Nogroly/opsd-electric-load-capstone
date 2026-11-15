# Random Forest dự báo trước 1 giờ

Validation tháng 9: MAE 872,93 MW. Test tháng 10–12: MAE 1.135,29 MW, RMSE 1.538,11 MW. So với baseline test tốt nhất (Giờ gần nhất, MAE 1.977,92 MW), mức giảm MAE là 42,60% trong tập này.

Chọn cấu hình: {'num_trees': 50, 'max_depth': 7, 'validation_mae_mw': 872.9262923895631, 'validation_rmse_mw': 1124.1875981190326}. Không chọn bằng test. Dữ liệu chỉ năm 2019, một máy, giả định số đo gần nhất sẵn có.
