"""Portable ML comparison dashboard from independently checked CSV outputs."""
import csv
import hashlib
import html
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'outputs/ml_forecast'
LABELS={'random_forest':'Random Forest','last_hour':'Giờ gần nhất','last_week_168h':'168 giờ trước'}
COLORS={'random_forest':'#047f9b','last_hour':'#d47933','last_week_168h':'#8562b6'}
ESC=html.escape


def read_csv(name):
    with (OUT/name).open(encoding='utf-8',newline='') as handle:
        return list(csv.DictReader(handle))


def num(value):
    return f'{float(value):,.2f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def bars(rows, split):
    selected={r['model']:r for r in rows if r['split']==split and r['period']=='all'}
    high=max(float(r['mae_mw']) for r in selected.values())*1.18
    shapes=[]
    for i,name in enumerate(LABELS):
        v=float(selected[name]['mae_mw']); x=105+i*325; y=325-v/high*260
        shapes.append(f'<rect x="{x}" y="{y:.1f}" width="130" height="{325-y:.1f}" fill="{COLORS[name]}"><title>{ESC(LABELS[name])}: {num(v)} MW</title></rect>')
        shapes.append(f'<text x="{x+65}" y="{y-9:.1f}" text-anchor="middle">{num(v)}</text>')
        shapes.append(f'<text x="{x+65}" y="350" text-anchor="middle">{ESC(LABELS[name])}</text>')
    return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1030 375" role="img" aria-label="MAE ba phương pháp"><style>text{font:16px Arial;fill:#334b61}</style><text x="15" y="30">MAE (MW), thấp hơn là tốt hơn</text>'+''.join(shapes)+'</svg>'


def comparison_line(predictions):
    rows=[r for r in predictions if '2019-10-01'<=r['target_utc']<'2019-10-08']
    if len(rows)!=168:
        raise ValueError('Fixed first test week is not complete')
    fields=[('actual_mw','#20344f',2.5),('random_forest_mw','#047f9b',2),
            ('last_hour_mw','#d47933',1.2)]
    values=[float(r[f]) for r in rows for f,_,_ in fields]
    low=min(values)-1500; high=max(values)+1500
    xy=lambda i,v:(70+i*960/167,30+(high-v)*280/(high-low))
    elements=[]
    for field,color,width in fields:
        points=' '.join(f'{xy(i,float(r[field]))[0]:.1f},{xy(i,float(r[field]))[1]:.1f}' for i,r in enumerate(rows))
        elements.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="{width}"/>')
    for i,r in enumerate(rows):
        x,y=xy(i,float(r['actual_mw']))
        title=f"{r['target_utc']} UTC: thực tế {num(r['actual_mw'])}; RF {num(r['random_forest_mw'])}; giờ trước {num(r['last_hour_mw'])} MW"
        elements.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="transparent"><title>{ESC(title)}</title></circle>')
    for day in range(7):
        elements.append(f'<text x="{xy(day*24,low)[0]:.1f}" y="345" text-anchor="middle">{day+1:02d}/10</text>')
    elements.append('<text x="65" y="375">Đen: thực tế</text><text x="300" y="375">Xanh: Random Forest</text><text x="620" y="375">Cam: giờ gần nhất</text>')
    return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1080 395" role="img" aria-label="Thực tế và dự báo tuần đầu test"><style>text{font:14px Arial;fill:#3c5267}</style><text x="8" y="20">MW</text>'+''.join(elements)+'</svg>'


def main():
    summary=json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    check=json.loads((OUT/'verification.json').read_text(encoding='utf-8'))
    if summary['status']!=check['status'] or summary['status']!='PASSED' or check['ml_run_id']!=summary['run_id']:
        raise ValueError('ML output is not independently verified')
    for name,digest in summary['files'].items():
        if hashlib.sha256((OUT/name).read_bytes()).hexdigest()!=digest:
            raise ValueError('ML output changed: '+name)
    metrics=read_csv('metrics.csv'); predictions=read_csv('predictions.csv')
    get=lambda split,model: next(r for r in metrics if r['split']==split and r['period']=='all' and r['model']==model)
    rf=get('test','random_forest'); best_baseline=min((get('test',m) for m in ('last_hour','last_week_168h')),
                                            key=lambda r:float(r['mae_mw']))
    improvement=(1-float(rf['mae_mw'])/float(best_baseline['mae_mw']))*100
    val_chart=bars(metrics,'validation'); test_chart=bars(metrics,'test'); line=comparison_line(predictions)
    (OUT/'test_mae.svg').write_text(test_chart,encoding='utf-8')
    (OUT/'test_first_week.svg').write_text(line,encoding='utf-8')
    monthly=''.join('<tr>'+''.join(f'<td>{ESC(x)}</td>' for x in
           (month,)+tuple(num(next(r for r in metrics if r['period']==month and r['model']==model)['mae_mw'])
                          for model in LABELS))+'</tr>' for month in ('2019-10','2019-11','2019-12'))
    importance=json.loads((OUT/'feature_importance.json').read_text(encoding='utf-8'))
    features=''.join(f'<tr><td>{ESC(k)}</td><td>{num(v*100)}%</td></tr>' for k,v in
                     sorted(importance.items(),key=lambda pair:-pair[1]))
    css='body{margin:0;background:#eef3f7;color:#19344d;font:16px/1.6 system-ui,Arial}main{max-width:1140px;margin:auto;padding:30px 20px}header,section{background:white;border:1px solid #dce5ef;border-radius:16px;padding:24px;margin:18px 0}h1{font-size:34px;line-height:1.2}h2{font-size:23px}svg{width:100%;height:auto}p{color:#465d71}a{color:#006e83}li{margin:10px 0}table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:8px;border-bottom:1px solid #dce5ef}.scroll{overflow:auto}.hero{font-size:21px;font-weight:700;color:#006c88}.note{background:#e9f8fc;padding:12px;border-radius:8px}small{color:#60788a}@media(max-width:600px){main{padding:10px}header,section{padding:12px}}'
    page=f'<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OPSD · Random Forest</title><style>{css}</style><main><header><h1>Random Forest dự báo phụ tải giờ tiếp theo</h1><p>Đức 2019 · dữ liệu Parquet từ HDFS · cùng 720 giờ validation và 2.208 giờ test với hai baseline.</p><p class="hero">MAE test: {num(rf["mae_mw"])} MW; thấp hơn {LABELS[best_baseline["model"]]} {num(improvement)}% trên tập test này.</p><small>Run ID {ESC(summary["run_id"])}</small></header>'
    page+='<section><h2>Chọn mô hình bằng tháng 9</h2><p>Chỉ so hai cấu hình Random Forest trên validation. Tháng 10–12 được giữ riêng để đánh giá cuối; hai baseline dùng cùng mốc dự báo.</p>'+val_chart+'</section>'
    page+='<section><h2>Đánh giá tháng 10–12</h2>'+test_chart+'<p>MAE và RMSE dùng đơn vị MW; thấp hơn là sai số nhỏ hơn. Kết quả không chứng minh mô hình sẽ tốt hơn ở năm khác hoặc khi số đo đến trễ.</p><a href="metrics.csv">Bảng chỉ số</a> · <a href="test_mae.svg">Biểu đồ SVG</a></section>'
    page+='<section><h2>Tuần đầu test: thực tế và dự báo</h2>'+line+'<p>Mỗi điểm dự báo trước một giờ; trước mỗi giờ, lịch sử được cập nhật bằng số đo thực tế đã xảy ra. Di chuột vào đồ thị để xem giá trị.</p><a href="predictions.csv">Toàn bộ dự báo</a> · <a href="test_first_week.svg">Biểu đồ SVG</a></section>'
    page+='<section><h2>MAE theo từng tháng test</h2><div class="scroll"><table><tr><th>Tháng</th><th>Random Forest</th><th>Giờ gần nhất</th><th>168 giờ trước</th></tr>'+monthly+'</table></div></section>'
    page+='<section><h2>Đầu vào mô hình</h2><p>Chỉ sử dụng phụ tải tại 1, 24 và 168 giờ trước, cộng giờ và thứ UTC của mục tiêu. Đây là hồi quy trên biến liên tục MW, không phải phân loại hoặc LLM.</p><table><tr><th>Đặc trưng</th><th>Độ quan trọng trong mô hình</th></tr>'+features+'</table><p>Độ quan trọng do Random Forest tính cho tập huấn luyện; không chứng minh quan hệ nhân quả.</p></section>'
    page+='<section><h2>Giới hạn và khả năng kiểm tra</h2><p class="note">Huấn luyện tháng 1–8 (5.664 giờ có đủ lag), chọn cấu hình tháng 9, test tháng 10–12. Dùng quan sát thực tế của các giờ đã qua; giả định số đo t−1h sẵn có tại mốc dự báo. Chưa mô phỏng độ trễ công bố, chưa đánh giá năm khác.</p><p>Đã đối chiếu 2.928 mục tiêu và hai baseline với kết quả Plan A, tính lại 15 dòng chỉ số độc lập từ CSV.</p><a href="verification.json">Kiểm tra độc lập</a> · <a href="summary.json">Thông tin lần chạy</a></section></main></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    report=f'# Random Forest dự báo trước 1 giờ\n\nValidation tháng 9: MAE {num(get("validation","random_forest")["mae_mw"])} MW. Test tháng 10–12: MAE {num(rf["mae_mw"])} MW, RMSE {num(rf["rmse_mw"])} MW. So với baseline test tốt nhất ({LABELS[best_baseline["model"]]}, MAE {num(best_baseline["mae_mw"])} MW), mức giảm MAE là {num(improvement)}% trong tập này.\n\nChọn cấu hình: {summary["selected_candidate"]}. Không chọn bằng test. Dữ liệu chỉ năm 2019, một máy, giả định số đo gần nhất sẵn có.\n'
    (OUT/'report.md').write_text(report,encoding='utf-8')
    print('ML DASHBOARD READY:',OUT/'index.html')


if __name__=='__main__':
    main()
