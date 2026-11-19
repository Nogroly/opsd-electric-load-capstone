"""Offline HTML/SVG dashboard, generated only from verified SQL exports. Stdlib only."""
import csv
import hashlib
import html
import json
import math
from pathlib import Path

from chart_svg import axes, number, svg

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'analytics'
esc = html.escape
PERIOD_CHARTS = (
    ('hourly', 'hour_berlin', 'Phụ tải trung bình theo giờ', 'Giờ trong ngày (Europe/Berlin)'),
    ('monthly', 'month_utc', 'Phụ tải trung bình theo tháng', 'Tháng (UTC)'),
    ('day_type', 'day_type', 'Ngày thường và cuối tuần', 'Nhóm ngày (Europe/Berlin)'),
)
DAY_LABELS = {'weekday': 'Ngày thường (T2–T6)', 'weekend': 'Cuối tuần (T7–CN)'}


def period_label(row, key):
    if key == 'hour_berlin':
        return f'{int(row[key]):02d}:00'
    if key == 'month_utc':
        return row[key][-2:]
    if key == 'day_type':
        return DAY_LABELS[row[key]]
    return row[key][5:]


def chart(rows, key, title, x_title, *, ceiling, line=False):
    left, top, width, height = 112, 24, 926, 228
    bottom = top + height
    elements = axes(left, top, width, height, 0, ceiling, 10000,
                    x_title, 'Phụ tải trung bình (MW)')
    points = [
        (left + (i + .5) * width / len(rows), bottom - float(r['avg_mw']) * height / ceiling)
        for i, r in enumerate(rows)
    ]
    if line:
        path = ' '.join(f'{x:.2f},{y:.2f}' for x, y in points)
        elements.append(f'<polyline points="{path}" fill="none" stroke="#147d92" stroke-width="2"/>')
    for i, (r, (x, y)) in enumerate(zip(rows, points)):
        label = period_label(r, key)
        tip = esc(f"{label} · {number(r['avg_mw'])} MW · {number(r['observations'], 0)} quan sát")
        if line:
            elements.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="3" fill="#147d92"><title>{tip}</title></circle>')
        else:
            bar_width = min(width / len(rows) * .7, 170)
            color = '#31869a' if r[key] == 'weekend' else '#146a84'
            elements.append(f'<rect x="{x-bar_width/2:.2f}" y="{y:.2f}" width="{bar_width:.2f}" '
                            f'height="{bottom-y:.2f}" fill="{color}"><title>{tip}</title></rect>')
        if key == 'day_type':
            elements.append(f'<text class="axis-title" x="{x:.2f}" y="{y-8:.2f}" text-anchor="middle">{number(r["avg_mw"])}</text>')
        if not line or (i % 60 == 0 and i < len(rows) - 20) or i == len(rows) - 1:
            tick_label = label[:2] if key == 'hour_berlin' else label
            elements.append(f'<text class="tick" x="{x:.2f}" y="{bottom+25}" text-anchor="middle">{esc(tick_label)}</text>')
    return svg(1080, bottom + 72, title, elements)


def read_verified_analytics():
    manifest = json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    verification = json.loads((OUT/'verification.json').read_text(encoding='utf-8'))
    if (manifest['status'] != 'PASSED'
            or verification['status'] != 'PASSED'
            or manifest['run_id'] != verification['analytics_run_id']):
        raise ValueError('SQL outputs are not independently verified')
    for name, digest in manifest['files'].items():
        if hashlib.sha256((OUT/name).read_bytes()).hexdigest() != digest:
            raise ValueError('SQL output changed: ' + name)
    tables = {}
    for name in ('daily','monthly','hourly','day_type','peaks'):
        with (OUT/(name+'.csv')).open(encoding='utf-8') as f:
            tables[name] = list(csv.DictReader(f))
    return manifest, tables


def period_insights(tables):
    average = lambda row: float(row['avg_mw'])
    high_month = max(tables['monthly'], key=average)
    low_month = min(tables['monthly'], key=average)
    high_hour = max(tables['hourly'], key=average)
    low_hour = min(tables['hourly'], key=average)
    day_types = {row['day_type']: average(row) for row in tables['day_type']}
    weekday, weekend = day_types['weekday'], day_types['weekend']
    delta = 100 * (weekday - weekend) / weekday
    return {
        'hourly': (
            f"Giờ Đức {period_label(high_hour, 'hour_berlin')} có phụ tải trung bình cao nhất "
            f"({number(average(high_hour))} MW) và {period_label(low_hour, 'hour_berlin')} thấp nhất "
            f"({number(average(low_hour))} MW). Mỗi cột tổng hợp cùng một nhãn giờ trong năm."
        ),
        'monthly': (
            f"Tháng {period_label(high_month, 'month_utc')} cao nhất ({number(average(high_month))} MW) "
            f"và tháng {period_label(low_month, 'month_utc')} thấp nhất ({number(average(low_month))} MW). "
            "Tháng được xác định theo UTC."
        ),
        'day_type': (
            f"Cuối tuần trung bình {number(weekend)} MW, thấp hơn {number(delta)}% so với "
            f"ngày thường ({number(weekday)} MW). Trung bình tính trên các quan sát theo giờ "
            "của từng nhóm thứ theo giờ Đức. Ngày lễ chưa được tách riêng."
        ),
    }


def period_charts(tables):
    maximum = max(float(row['avg_mw']) for name, _, _, _ in PERIOD_CHARTS for row in tables[name])
    ceiling = max(10000, math.ceil(maximum * 1.08 / 10000) * 10000)
    return {
        name: chart(tables[name], key, title, x_title, ceiling=ceiling)
        for name, key, title, x_title in PERIOD_CHARTS
    }


def period_sections(charts, insights, asset_path=''):
    return ''.join(
        f'<section id="{name}"><h2>{title}</h2><p>{esc(insights[name])}</p>'
        f'<div class="chart-scroll">{charts[name]}</div>'
        f'<a href="{asset_path}{name}.csv">Tải bảng CSV</a> · '
        f'<a href="{asset_path}{name}.svg">Mở biểu đồ SVG</a></section>'
        for name, _, title, _ in PERIOD_CHARTS
    )


def main():
    manifest, tables = read_verified_analytics()
    findings = period_insights(tables)
    charts = period_charts(tables)
    daily_limit = math.ceil(max(float(r['avg_mw']) for r in tables['daily']) / 10000) * 10000
    daily_chart = chart(tables['daily'], 'day_utc', 'Phụ tải trung bình theo ngày',
                        'Ngày (UTC)', ceiling=daily_limit, line=True)
    for name, image in {**charts, 'daily': daily_chart}.items():
        (OUT/(name+'.svg')).write_text(image, encoding='utf-8')
    peaks = tables['peaks']
    top_peak = peaks[0]
    insights = [*findings.values(),
                f"Cực đại quan sát {number(top_peak['load_mw'])} MW lúc {top_peak['timestamp_utc']} UTC "
                f"({top_peak['timestamp_berlin']} giờ Đức)."]
    peak_rows = ''.join('<tr>'+''.join('<td>'+esc(str(r[k]))+'</td>' for k in ('timestamp_utc','timestamp_berlin','load_mw'))+'</tr>' for r in peaks)
    intro = 'Phạm vi chọn theo năm UTC 2019: 8.760 quan sát. Ngày/tháng dùng UTC để đủ ngày và tháng; nhãn giờ và cuối tuần dùng Europe/Berlin. Khi đổi giờ mùa hè, giữ cả hai quan sát có cùng giờ địa phương vì chúng có UTC khác nhau. Phạm vi giờ Đức tương ứng 01/01/2019 01:00 đến 01/01/2020 00:00; không phải toàn bộ năm theo lịch Đức.'
    page = '''<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OPSD | Plan A</title><style>
body{margin:0;background:#eef3f7;color:#172c43;font:16px/1.6 system-ui,Arial}main{max-width:1120px;margin:auto;padding:36px 24px}h1{font-size:36px;line-height:1.2}h2{font-size:23px}section,.hero{background:white;border:1px solid #dce5ef;border-radius:16px;padding:24px;margin:20px 0}.chart-scroll{overflow-x:auto}svg{display:block;width:100%;min-width:640px;height:auto}p{color:#526274}a{color:#006e83}li{margin:12px 0}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:9px;border-bottom:1px solid #dce5ef}.badge{background:#d7f2e9;color:#116547;padding:5px 12px;border-radius:20px}.meta{font-size:13px;overflow-wrap:anywhere}@media(max-width:600px){main{padding:12px}section{padding:12px}td{font-size:12px}}
</style><main><div class="hero"><span class="badge">Plan A · Kết quả đã kiểm chứng</span><h1>Phụ tải điện Đức năm 2019</h1><p>OPSD CSV → Spark ETL → HDFS / Parquet → Spark SQL → Biểu đồ</p>'''
    page += '<p>'+intro+'</p><p class="meta">Kết quả lưu từ lần chạy '+esc(manifest['created_utc'])+' · ID '+esc(manifest['run_id'])+'</p></div>'
    page += '<p><a href="../trends/index.html">Mở phân tích xu hướng MA7 và MA30</a> · <a href="daily.csv">Tải số liệu tổng hợp theo ngày</a></p>'
    page += '<section><h2>Bốn nhận xét từ dữ liệu</h2><ol>'+''.join('<li>'+esc(t)+'</li>' for t in insights)+'</ol></section>'
    page += period_sections(charts, findings)
    page += '<section><h2>10 thời điểm phụ tải cao nhất</h2><table><tr><th>UTC</th><th>Europe/Berlin</th><th>MW</th></tr>'+peak_rows+'</table><a href="peaks.csv">Tải CSV</a></section>'
    page += '<section><h2>Chất lượng và giới hạn</h2><p>Đã đối soát dữ liệu HDFS với nguồn bằng validation; kiểm tra độc lập 403 nhóm tổng hợp và 10 thời điểm đỉnh từ CSV. Đủ 365 ngày UTC và 12 tháng UTC. MW là phụ tải, không phải điện năng MWh. Mẫu chạy trên một máy, Spark local[2]; chưa đánh giá hiệu năng phân tán. Trang Plan A này chỉ trình bày thống kê mô tả; dự báo ML và streaming replay có dashboard riêng.</p><a href="summary.json">Thông tin lần chạy</a> · <a href="verification.json">Kiểm tra độc lập</a> · <a href="https://data.open-power-system-data.org/time_series/2020-10-06/">Nguồn OPSD</a></section></main></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    (OUT/'insights.md').write_text('# Nhận xét Plan A\n\n'+intro+'\n\n'+'\n\n'.join('- '+t for t in insights),encoding='utf-8')
    print('DASHBOARD READY:', OUT/'index.html')


if __name__ == '__main__':
    main()
