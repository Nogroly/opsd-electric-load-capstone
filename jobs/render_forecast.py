"""Create portable forecasting report and Matplotlib PNG/SVG figures from verified outputs."""
import csv
import hashlib
import html
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'outputs/forecast'
MODELS = ('last_hour', 'last_week_168h')
LABELS = {'last_hour': 'Giờ gần nhất (1h)', 'last_week_168h': '168 giờ trước'}
COLORS = {'actual_mw': '#20344f', 'last_hour': '#d17b21', 'last_week_168h': '#13858a'}


def number(value):
    return f'{float(value):,.2f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def save(fig, name):
    fig.savefig(OUT/(name+'.png'), dpi=160, bbox_inches='tight', facecolor='white')
    fig.savefig(OUT/(name+'.svg'), bbox_inches='tight', facecolor='white')
    plt.close(fig)


def verified_inputs():
    summary = json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    proof = json.loads((OUT/'verification.json').read_text(encoding='utf-8'))
    if summary['status'] != 'PASSED' or proof['status'] != 'PASSED' or proof['forecast_run_id'] != summary['run_id']:
        raise ValueError('Missing successful verification for current forecast run')
    for filename, digest in summary['files'].items():
        if hashlib.sha256((OUT/filename).read_bytes()).hexdigest() != digest or proof['files'].get(filename)!=digest:
            raise ValueError('Verified output changed: '+filename)
    with (OUT/'metrics.csv').open(encoding='utf-8') as f:
        metrics = list(csv.DictReader(f))
    with (OUT/'predictions.csv').open(encoding='utf-8') as f:
        predictions = list(csv.DictReader(f))
    return summary, proof, metrics, predictions


def plots(metrics, predictions):
    plt.rcParams.update({'font.family':'DejaVu Sans', 'font.size':11,
                         'axes.spines.top':False, 'axes.spines.right':False,
                         'axes.titleweight':'bold', 'svg.fonttype':'none'})
    test = {r['model']:r for r in metrics if r['split']=='test' and r['period']=='all'}
    fig, axes = plt.subplots(1,2,figsize=(12,4),layout='constrained')
    for ax, field, label in zip(axes, ['mae_mw','rmse_mw'], ['MAE','RMSE']):
        bars = ax.bar([LABELS[m] for m in MODELS], [float(test[m][field]) for m in MODELS], color=[COLORS[m] for m in MODELS],width=.55)
        ax.bar_label(bars, labels=[number(test[m][field]) for m in MODELS],padding=4)
        ax.set_ylim(0,max(float(test[m][field]) for m in MODELS)*1.25)
        ax.set_title(label+' · càng thấp càng tốt')
        ax.set_ylabel('MW')
        ax.grid(axis='y',alpha=.2)
        ax.set_axisbelow(True)
    fig.suptitle('Sai số test 01/10–31/12/2019 UTC · 2.208 dự báo trước 1 giờ',fontsize=13)
    save(fig,'test_metrics')
    months = ['2019-10','2019-11','2019-12']
    fig, ax = plt.subplots(figsize=(12,4),layout='constrained')
    for i,model in enumerate(MODELS):
        values = [float(next(r for r in metrics if r['period']==month and r['model']==model)['mae_mw']) for month in months]
        bars = ax.bar([j+(i-.5)*.34 for j in range(3)],values,width=.32,label=LABELS[model],color=COLORS[model])
        ax.bar_label(bars,labels=[number(v) for v in values],padding=3,fontsize=9)
    ax.set_xticks(range(3),months)
    ax.set_title('MAE theo tháng test · mỗi điểm tính trên toàn bộ giờ của tháng')
    ax.set_ylabel('MAE (MW)')
    ax.margins(y=.24)
    ax.grid(axis='y',alpha=.2)
    ax.set_axisbelow(True)
    ax.legend(loc='upper left',ncols=2)
    save(fig,'monthly_mae')
    for start,end,name,title in [('2019-10-01','2019-10-08','test_first_week','Tuần đầu test · 01–07/10/2019 UTC'),
                                 ('2019-12-24','2019-12-31','test_year_end','Cửa sổ cuối năm · 24–30/12/2019 UTC')]:
        rows = [r for r in predictions if start<=r['target_utc']<end]
        if len(rows)!=168:
            raise ValueError('Incomplete fixed illustration window')
        times = [datetime.fromisoformat(r['target_utc']).replace(tzinfo=timezone.utc) for r in rows]
        fig, ax = plt.subplots(figsize=(12,4.5),layout='constrained')
        ax.plot(times,[float(r['actual_mw']) for r in rows],label='Thực tế',color=COLORS['actual_mw'],lw=2)
        for model in MODELS:
            ax.plot(times,[float(r[model+'_mw']) for r in rows],label=LABELS[model],color=COLORS[model],lw=1.25,alpha=.9,linestyle='--')
        ax.set_title(title)
        ax.set_ylabel('Phụ tải (MW)')
        ax.set_xlabel('Thời điểm cần dự báo (UTC) · cuốn chiếu từng giờ')
        ax.xaxis.set_major_locator(mdates.DayLocator(tz=timezone.utc))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m',tz=timezone.utc))
        ax.legend(ncols=3,loc='upper center',bbox_to_anchor=(.5,1.17),fontsize=10)
        ax.grid(alpha=.2)
        save(fig,name)


def main():
    summary, proof, metrics, predictions = verified_inputs()
    plots(metrics,predictions)
    selected = summary['selected_model']
    score = lambda split, model: next(r for r in metrics if r['split']==split and r['period']=='all' and r['model']==model)
    chosen = score('test',selected)
    alternative = next(m for m in MODELS if m!=selected)
    diff = (1-float(chosen['mae_mw'])/float(score('test',alternative)['mae_mw']))*100
    comparison = f"MAE test của phương pháp được chọn {'thấp' if diff>=0 else 'cao'} hơn phương pháp còn lại {number(abs(diff))}%."
    conclusion = f"Chọn {LABELS[selected]} bằng MAE validation tháng 9. Trên 2.208 giờ test tháng 10–12, MAE = {number(chosen['mae_mw'])} MW và RMSE = {number(chosen['rmse_mw'])} MW. {comparison}"
    if diff < 0:
        conclusion += ' Phương pháp tốt hơn ở validation đã không giữ được lợi thế trên test; kết quả này cho thấy hạn chế của việc chỉ dựa vào một tháng để chọn baseline.'
    table_rows = ''
    md_rows = []
    for r in metrics:
        period = ('Tháng 9 · chọn baseline' if r['split']=='validation' else 'Tháng 10–12 · test') if r['period']=='all' else r['period']+' · test'
        values = [period,LABELS[r['model']],r['observations'],number(r['mae_mw']),number(r['rmse_mw'])]
        table_rows += '<tr>'+''.join('<td>'+html.escape(v)+'</td>' for v in values)+'</tr>'
        md_rows.append('| '+' | '.join(values)+' |')
    intro = 'Dự báo phụ tải giờ tiếp theo từ hai quy tắc đơn giản: lấy số đo giờ gần nhất hoặc số đo 168 giờ trước. Mỗi giờ cập nhật lịch sử bằng số đo thực tế vừa có. Đây là thử nghiệm hồi cứu trên năm 2019.'
    assumptions = [
        'Tháng 1–8 là lịch sử; hai baseline không có tham số cần huấn luyện. Tháng 9 dùng chọn phương pháp theo MAE; tháng 10–12 dùng đánh giá cuối. Không chia ngẫu nhiên.',
        'Mục tiêu t được dự báo tại mốc danh nghĩa t−1h, chỉ dùng số đo có timestamp không muộn hơn t−1h. Giả định số đo gần nhất đã có; chưa mô phỏng độ trễ công bố hoặc chỉnh sửa số liệu OPSD.',
        '168 giờ là độ trễ cố định theo UTC. Khi Đức đổi giờ mùa hè, đó có thể không phải cùng giờ trên đồng hồ địa phương.',
        'Đây là dự báo cuốn chiếu trước 1 giờ, không phải dự báo cả ba tháng từ một mốc duy nhất. Các quan sát test đã xảy ra được dùng làm lịch sử cho giờ tiếp theo.',
        'MAE đo sai số tuyệt đối trung bình; RMSE nhạy hơn với lỗi lớn. Cả hai tính bằng MW, càng thấp càng tốt. Điểm cả quý tính trên toàn bộ 2.208 giờ.',
        'Kết quả là mốc so sánh cho mô hình sau; chưa chứng minh hiệu quả nhiều năm hoặc vận hành thực tế. Dữ liệu 2019 từng được khảo sát mô tả trong Plan A.']
    report = '# Kết quả thử nghiệm dự báo cơ sở\n\n'+intro+'\n\n## Kết luận\n\n'+conclusion+'\n\n## Sai số\n\n| Giai đoạn | Phương pháp | Số giờ | MAE (MW) | RMSE (MW) |\n|---|---|---:|---:|---:|\n'+'\n'.join(md_rows)+'\n\n## Cách đánh giá và giới hạn\n\n'+'\n\n'.join('- '+t for t in assumptions)
    report += '\n\n## Kiểm tra\n\nĐã đối soát độc lập 2.928 mục tiêu, 5.856 dự báo và 10 dòng chỉ số với CSV gốc; kiểm tra timestamp nguồn không ở tương lai.\n\nLần chạy: '+summary['run_id']+'\n\nPhương pháp: https://otexts.com/fpp3/simple-methods.html và https://otexts.com/fpp3/tscv.html\n'
    (OUT/'report.md').write_text(report,encoding='utf-8')
    cards = ''.join('<div class="card"><span>'+label+'</span><strong>'+value+'</strong></div>' for label,value in [
        ('Baseline chọn bằng validation',LABELS[selected]),('MAE test',number(chosen['mae_mw'])+' MW'),
        ('RMSE test',number(chosen['rmse_mw'])+' MW'),('Giờ kiểm tra cuối','2.208')])
    page = '''<!doctype html><html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OPSD · Thử nghiệm dự báo cơ sở</title><style>
body{margin:0;background:#edf3f7;color:#20344f;font:16px/1.65 Arial,sans-serif}main{max-width:1120px;margin:auto;padding:32px 24px}h1{font-size:36px;line-height:1.2}h2{font-size:23px}section,header{padding:26px;background:white;border:1px solid #dce5ee;border-radius:14px;margin:20px 0}p{color:#4e6175}.badge{color:#086a5f;background:#def2ec;padding:6px 14px;border-radius:22px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{padding:16px;border-radius:10px;background:#eef5f8}.card span{font-size:13px;display:block}.card strong{font-size:21px;display:block;margin-top:10px}img{width:100%;height:auto}table{width:100%;border-collapse:collapse;font-size:14px}td,th{padding:10px;border-bottom:1px solid #dce5ee;text-align:left}th{background:#edf4f8}a{color:#087e88}.meta{font-size:12px;overflow-wrap:anywhere}li{margin-bottom:12px}.scroll{overflow-x:auto}@media(max-width:700px){main{padding:12px}.cards{grid-template-columns:repeat(2,1fr)}section,header{padding:16px}h1{font-size:29px}}
</style></head><body><main><header><span class="badge">Thử nghiệm đã kiểm chứng · Dự báo trước 1 giờ</span><h1>Dự báo phụ tải: bắt đầu từ hai baseline</h1>'''
    page += '<p>'+intro+'</p><div class="cards">'+cards+'</div><p>'+conclusion+'</p><p class="meta">Lần chạy '+html.escape(summary['created_utc'])+' · '+summary['run_id']+'</p></header>'
    page += '<section><h2>Đọc kết quả đúng cách</h2><p>Lịch sử tháng 1–8 → chọn baseline trên tháng 9 → đánh giá test tháng 10–12. Phương pháp được chọn giữ nguyên khi xem test.</p><p><b>Giả định:</b> số đo giờ gần nhất đã có tại mốc phát dự báo. Dùng UTC; “tuần trước” ở đây là 168 giờ trước.</p></section>'
    for name,title,note in [
        ('test_metrics','Hai baseline trên cùng tập test','Đánh giá toàn bộ 2.208 giờ; không lựa chọn lại phương pháp theo tập test.'),
        ('test_first_week','Dự báo so với thực tế: tuần đầu test','Cửa sổ 01–07/10 xác định trước; mỗi điểm là một dự báo trước một giờ.'),
        ('test_year_end','Dự báo so với thực tế: giai đoạn cuối năm','Cửa sổ 24–30/12 xác định trước để quan sát một giai đoạn khác. Xem toàn bộ sai số theo tháng bên dưới.'),
        ('monthly_mae','Sai số có ổn định qua các tháng không?','Mỗi cột dùng toàn bộ giờ trong tháng, không chỉ các cửa sổ minh họa.')]:
        page += f'<section><h2>{title}</h2><p>{note}</p><img src="{name}.png" alt="{title}"><p><a href="{name}.png">Ảnh PNG để chèn báo cáo</a> · <a href="{name}.svg">Biểu đồ SVG</a></p></section>'
    page += '<section><h2>Bảng sai số đầy đủ</h2><div class="scroll"><table><tr><th>Giai đoạn</th><th>Phương pháp</th><th>Số giờ</th><th>MAE (MW)</th><th>RMSE (MW)</th></tr>'+table_rows+'</table></div><p><a href="metrics.csv">Tải chỉ số CSV</a> · <a href="predictions.csv">Tải toàn bộ dự báo CSV</a></p></section>'
    page += '<section><h2>Phương pháp, kiểm định và giới hạn</h2><ul>'+''.join('<li>'+t+'</li>' for t in assumptions)+'</ul><p>Đã đối chiếu độc lập 5.856 dự báo và toàn bộ 10 dòng chỉ số từ CSV gốc.</p><p><a href="verification.json">Kết quả đối soát</a> · <a href="summary.json">Thông tin lần chạy</a> · <a href="report.md">Báo cáo kết quả</a></p><p><a href="https://otexts.com/fpp3/simple-methods.html">Nguồn phương pháp naïve</a> · <a href="https://otexts.com/fpp3/tscv.html">Đánh giá theo thời gian</a></p></section></main></body></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    print('FORECAST REPORT READY:', OUT/'index.html')


if __name__=='__main__':
    main()
