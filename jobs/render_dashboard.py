"""Offline HTML/SVG dashboard, generated only from verified SQL exports. Stdlib only."""
import csv
import hashlib
import html
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'analytics'
esc = html.escape


def chart(rows, key, title, line=False):
    values = [float(r['avg_mw']) for r in rows]
    ceiling = max(values) * 1.15
    w, h, left, top, pw, ph = 900, 300, 70, 25, 800, 225
    elements = []
    for i in range(5):
        y = top + ph - i * ph / 4
        elements.append(f'<line x1="{left}" y1="{y}" x2="870" y2="{y}" stroke="#dce5ef"/><text x="60" y="{y+4}" text-anchor="end">{ceiling*i/4:,.0f}</text>')
    points = []
    for i, (r,v) in enumerate(zip(rows, values)):
        x = left + (i + .5) * pw / len(rows)
        y = top + ph * (1-v/ceiling)
        tip = esc(f"{r[key]}: {v:,.2f} MW; n={r['observations']}")
        points.append(f'{x:.2f},{y:.2f}')
        if line:
            elements.append(f'<circle cx="{x}" cy="{y}" r="3" fill="#147d92"><title>{tip}</title></circle>')
        else:
            bw = pw / len(rows)*.7
            elements.append(f'<rect x="{x-bw/2}" y="{y}" width="{bw}" height="{top+ph-y}" rx="2" fill="#147d92"><title>{tip}</title></rect>')
        if not line or (i % 60 == 0 and i < len(rows)-20) or i == len(rows)-1:
            label = r[key] if not line else r[key][5:]
            label = {'weekday': 'Ngày thường', 'weekend': 'Cuối tuần'}.get(label, label)
            elements.append(f'<text x="{x}" y="275" text-anchor="middle">{esc(label)}</text>')
    if line:
        elements.insert(0, '<polyline points="'+' '.join(points)+'" fill="none" stroke="#147d92" stroke-width="1.6"/>')
    return f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" role="img" aria-label="{esc(title)}"><style>text{{font:12px Arial;fill:#526274}}</style><text x="10" y="15">MW</text>'+''.join(elements)+'</svg>'


def main():
    manifest = json.loads((OUT/'summary.json').read_text())
    verification = json.loads((OUT/'verification.json').read_text())
    assert manifest['status'] == verification['status'] == 'PASSED'
    assert manifest['run_id'] == verification['analytics_run_id']
    for name, digest in manifest['files'].items():
        assert hashlib.sha256((OUT/name).read_bytes()).hexdigest() == digest, name
    tables = {}
    for name in ('daily','monthly','hourly','day_type','peaks'):
        with (OUT/(name+'.csv')).open(encoding='utf-8') as f:
            tables[name] = list(csv.DictReader(f))
    monthly, hourly, types, peaks = [tables[n] for n in ('monthly','hourly','day_type','peaks')]
    high = max(monthly,key=lambda r:float(r['avg_mw']))
    low = min(monthly,key=lambda r:float(r['avg_mw']))
    hour = max(hourly,key=lambda r:float(r['avg_mw']))
    weekday, weekend = [next(r for r in types if r['day_type']==n) for n in ('weekday','weekend')]
    delta = (1-float(weekend['avg_mw'])/float(weekday['avg_mw']))*100
    insights = [f"Tháng {high['month_utc']} có phụ tải trung bình cao nhất: {float(high['avg_mw']):,.2f} MW; thấp nhất là {low['month_utc']}: {float(low['avg_mw']):,.2f} MW (tháng UTC).",
                f"Giờ {hour['hour_berlin']}:00 có phụ tải trung bình cao nhất theo giờ Đức: {float(hour['avg_mw']):,.2f} MW. Đây là trung bình theo nhãn giờ, không phải thời điểm cực đại đơn lẻ.",
                f"Trung bình cuối tuần thấp hơn ngày thường {delta:.2f}%, tính trên từng quan sát theo giờ. Phân nhóm chỉ theo thứ trong tuần, chưa tách ngày lễ hoặc kiểm soát yếu tố mùa.",
                f"Cực đại quan sát: {float(peaks[0]['load_mw']):,.2f} MW lúc {peaks[0]['timestamp_utc']} UTC ({peaks[0]['timestamp_berlin']} giờ Đức). Phụ tải cao chưa đủ để kết luận lưới điện quá tải."]
    sections = []
    for name,key,title,line in [('daily','day_utc','Xu hướng theo ngày · UTC',True),('monthly','month_utc','Trung bình theo tháng · UTC',False),('hourly','hour_berlin','Trung bình theo giờ · Europe/Berlin',False),('day_type','day_type','Ngày thường và cuối tuần · Europe/Berlin',False)]:
        svg = chart(tables[name],key,title,line)
        (OUT/(name+'.svg')).write_text(svg,encoding='utf-8')
        sections.append(f'<section><h2>{title}</h2>{svg}<p>Trục dọc: phụ tải trung bình (MW). Di chuột vào điểm/cột để xem số liệu và số quan sát.</p><a href="{name}.csv">Tải bảng CSV</a> · <a href="{name}.svg">Mở biểu đồ SVG</a></section>')
    peak_rows = ''.join('<tr>'+''.join('<td>'+esc(str(r[k]))+'</td>' for k in ('timestamp_utc','timestamp_berlin','load_mw'))+'</tr>' for r in peaks)
    intro = 'Phạm vi chọn theo năm UTC 2019: 8.760 quan sát. Ngày/tháng dùng UTC để đủ ngày và tháng; nhãn giờ và cuối tuần dùng Europe/Berlin. Khi đổi giờ mùa hè, giữ cả hai quan sát có cùng giờ địa phương vì chúng có UTC khác nhau. Phạm vi giờ Đức tương ứng 01/01/2019 01:00 đến 01/01/2020 00:00; không phải toàn bộ năm theo lịch Đức.'
    page = '''<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OPSD | Plan A</title><style>
body{margin:0;background:#eef3f7;color:#172c43;font:16px/1.6 system-ui,Arial}main{max-width:1120px;margin:auto;padding:36px 24px}h1{font-size:36px;line-height:1.2}h2{font-size:23px}section,.hero{background:white;border:1px solid #dce5ef;border-radius:16px;padding:24px;margin:20px 0}svg{width:100%;height:auto}p{color:#526274}a{color:#006e83}li{margin:12px 0}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:9px;border-bottom:1px solid #dce5ef}.badge{background:#d7f2e9;color:#116547;padding:5px 12px;border-radius:20px}.meta{font-size:13px;overflow-wrap:anywhere}@media(max-width:600px){main{padding:12px}section{padding:12px}td{font-size:12px}}
</style><main><div class="hero"><span class="badge">Plan A · Kết quả đã kiểm chứng</span><h1>Phụ tải điện Đức năm 2019</h1><p>OPSD CSV → Spark ETL → HDFS / Parquet → Spark SQL → Biểu đồ</p>'''
    page += '<p>'+intro+'</p><p class="meta">Kết quả lưu từ lần chạy '+esc(manifest['created_utc'])+' · ID '+esc(manifest['run_id'])+'</p></div>'
    page += '<section><h2>Bốn nhận xét từ dữ liệu</h2><ol>'+''.join('<li>'+esc(t)+'</li>' for t in insights)+'</ol></section>'
    page += ''.join(sections)
    page += '<section><h2>10 thời điểm phụ tải cao nhất</h2><table><tr><th>UTC</th><th>Europe/Berlin</th><th>MW</th></tr>'+peak_rows+'</table><a href="peaks.csv">Tải CSV</a></section>'
    page += '<section><h2>Chất lượng và giới hạn</h2><p>Đã đối soát dữ liệu HDFS với nguồn bằng validation; kiểm tra độc lập 403 nhóm tổng hợp và 10 thời điểm đỉnh từ CSV. Đủ 365 ngày UTC và 12 tháng UTC. MW là phụ tải, không phải điện năng MWh. Mẫu chạy trên một máy, Spark local[2]; chưa đánh giá hiệu năng phân tán. Trang Plan A này chỉ trình bày thống kê mô tả; dự báo ML và streaming replay có dashboard riêng.</p><a href="summary.json">Thông tin lần chạy</a> · <a href="verification.json">Kiểm tra độc lập</a> · <a href="https://data.open-power-system-data.org/time_series/2020-10-06/">Nguồn OPSD</a></section></main></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    (OUT/'insights.md').write_text('# Nhận xét Plan A\n\n'+intro+'\n\n'+'\n\n'.join('- '+t for t in insights),encoding='utf-8')
    print('DASHBOARD READY:', OUT/'index.html')


if __name__ == '__main__':
    main()
