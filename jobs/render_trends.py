"""Offline, inspectable trend dashboard from verified CSV outputs; no extra packages."""
import csv
import hashlib
import html
import json
import math
from pathlib import Path

from chart_svg import axes, number as num, svg
from render_dashboard import period_charts, period_insights, period_sections, read_verified_analytics

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/trends'
ESC = html.escape
WEEKDAYS = ['Thứ hai', 'Thứ ba', 'Thứ tư', 'Thứ năm', 'Thứ sáu', 'Thứ bảy', 'Chủ nhật']


def read_csv(name):
    with (OUT/name).open(encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def trend_svg(daily):
    left, top, width, height = 112, 25, 925, 320
    bottom = top + height
    curves = [('avg_mw', '#8999a8', 1.4),
              ('ma7_mw', '#087eaa', 2.8), ('ma30_mw', '#e08b36', 3.2)]
    series = {
        field: [(i, float(row[field])) for i, row in enumerate(daily)
                if row[field] not in ('', 'None')]
        for field, _, _ in curves
    }
    all_values = [value for points in series.values() for _, value in points]
    low = math.floor(min(all_values) / 5000) * 5000
    high = max(low + 5000, math.ceil(max(all_values) / 5000) * 5000)
    xy = lambda i, v: (left + i * width / (len(daily) - 1), top + (high - v) * height / (high - low))
    parts = axes(left, top, width, height, low, high, 5000,
                 'Ngày UTC / ngày kết thúc cửa sổ MA', 'Phụ tải trung bình (MW)')
    for field, color, stroke in curves:
        points = ' '.join(f'{xy(i,v)[0]:.1f},{xy(i,v)[1]:.1f}' for i, v in series[field])
        label = {'avg_mw': 'Trung bình ngày', 'ma7_mw': 'MA7', 'ma30_mw': 'MA30'}[field]
        parts.append(f'<polyline aria-label="{label}" points="{points}" fill="none" stroke="{color}" stroke-width="{stroke}"/>')
    for i, v in series['avg_mw']:
        row = daily[i]
        ma7 = row['ma7_mw']
        ma30 = row['ma30_mw']
        x, y = xy(i, v)
        ma7_text = f'{num(ma7)} MW' if ma7 not in ('', 'None') else 'chưa đủ 7 ngày'
        ma30_text = f'{num(ma30)} MW' if ma30 not in ('', 'None') else 'chưa đủ 30 ngày'
        title = f"{row['day_utc']} · Trung bình ngày {num(v)} MW · MA7 {ma7_text} · MA30 {ma30_text}"
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="transparent"><title>{ESC(title)}</title></circle>')
    for i, row in enumerate(daily):
        year, month, day = row['day_utc'].split('-')
        if day == '01':
            parts.append(f'<text class="tick" x="{xy(i,low)[0]:.1f}" y="{bottom+25}" text-anchor="middle">{month}/{year}</text>')
    parts.extend((
        f'<path d="M 112 {bottom+79} h 26" stroke="#8999a8" stroke-width="1.4"/>',
        f'<text x="145" y="{bottom+84}">Trung bình ngày</text>',
        f'<path d="M 345 {bottom+79} h 26" stroke="#087eaa" stroke-width="3"/>',
        f'<text x="378" y="{bottom+84}">MA7</text>',
        f'<path d="M 480 {bottom+79} h 26" stroke="#e08b36" stroke-width="3"/>',
        f'<text x="513" y="{bottom+84}">MA30</text>',
    ))
    return svg(1080, bottom + 100, 'So sánh phụ tải trung bình ngày với MA7 và MA30', parts)


def heatmap_svg(rows, group_field, groups, y_title, gradient_id, title, limits):
    cells = {(int(r[group_field]), int(r['hour_local'])): r for r in rows}
    low, high = limits
    left, top, cw, ch = 134, 15, 37, 32
    bottom = top + ch * len(groups)
    center = left + 24 * cw / 2
    parts = [
        f'<defs><linearGradient id="{gradient_id}"><stop offset="0%" stop-color="#ddeff8"/>'
        '<stop offset="100%" stop-color="#1f7faf"/></linearGradient></defs>',
        f'<text class="axis-title" transform="translate(20 {(top+bottom)/2:g}) rotate(-90)" text-anchor="middle">{y_title}</text>',
    ]
    for i, (group, label) in enumerate(groups):
        y = top + i * ch
        parts.append(f'<text class="tick" x="{left-10}" y="{y+20}" text-anchor="end">{label}</text>')
        for hour in range(24):
            cell = cells[group, hour]
            value = float(cell['avg_mw'])
            mix = (value - low) / (high - low)
            color = f'#{int(221-190*mix):02x}{int(239-112*mix):02x}{int(248-73*mix):02x}'
            tip = ESC(f'{label} · {hour:02d}:00 · {num(value)} MW · {cell["observations"]} quan sát')
            parts.append(f'<rect x="{left+hour*cw}" y="{y}" width="{cw-3}" height="{ch-3}" '
                         f'rx="3" fill="{color}"><title>{tip}</title></rect>')
    for hour in range(24):
        parts.append(f'<text class="tick" x="{left+(hour+.5)*cw:g}" y="{bottom+20}" text-anchor="middle">{hour:02d}</text>')
    parts.extend((
        f'<text class="axis-title" x="{center:g}" y="{bottom+43}" text-anchor="middle">Giờ trong ngày (Europe/Berlin)</text>',
        f'<text x="{center:g}" y="{bottom+72}" text-anchor="middle">Phụ tải trung bình (MW)</text>',
        f'<rect x="{center-180:g}" y="{bottom+82}" width="360" height="12" rx="3" fill="url(#{gradient_id})"/>',
    ))
    for x, value in ((center - 180, low), (center, (low + high) / 2), (center + 180, high)):
        parts.append(f'<text class="tick" x="{x:g}" y="{bottom+114}" text-anchor="middle">{num(value, 0)}</text>')
    return svg(1080, bottom + 124, title, parts)


def main():
    summary=json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    proof=json.loads((OUT/'verification.json').read_text(encoding='utf-8'))
    if summary['status'] != proof['status'] or summary['status'] != 'PASSED' or proof['trend_run_id'] != summary['run_id']:
        raise ValueError('Trend outputs are not independently verified')
    for name,digest in summary['files'].items():
        if hashlib.sha256((OUT/name).read_bytes()).hexdigest()!=digest:
            raise ValueError('Trend output changed: '+name)
    analytics, tables = read_verified_analytics()
    if (analytics['etl_run_id'] != summary['etl_run_id']
            or analytics['source_sha256'] != summary['source_sha256']):
        raise ValueError('SQL and trend outputs must use the same verified ETL run and source')
    profiles = period_insights(tables)
    daily = read_csv('daily_trend.csv')
    hours = read_csv('hour_week.csv')
    month_hours = read_csv('month_hour.csv')
    deviations = read_csv('largest_deviations.csv')
    line = trend_svg(daily)
    values = [float(row['avg_mw']) for row in hours + month_hours]
    low_scale = math.floor(min(values) / 5000) * 5000
    high_scale = max(low_scale + 5000, math.ceil(max(values) / 5000) * 5000)
    limits = low_scale, high_scale
    heat = heatmap_svg(hours, 'weekday_local', list(enumerate(WEEKDAYS)), 'Thứ trong tuần',
                      'week-scale', 'Nhiệt đồ phụ tải theo thứ và giờ Đức', limits)
    month_groups = [(i, f'Tháng {i:02d}') for i in range(1, 13)]
    month_heat = heatmap_svg(month_hours, 'month_utc', month_groups, 'Tháng (UTC)',
                            'month-scale', 'Nhiệt đồ phụ tải theo tháng UTC và giờ Đức', limits)
    (OUT/'daily_trend.svg').write_text(line,encoding='utf-8')
    (OUT/'hour_week.svg').write_text(heat,encoding='utf-8')
    (OUT/'month_hour.svg').write_text(month_heat,encoding='utf-8')
    low=summary['ma30_low']; high=summary['ma30_high']; hlow=summary['hour_week_low']; hhigh=summary['hour_week_high']
    mlow=summary['month_low']; mhigh=summary['month_high']
    fall=summary['strongest_ma30_fall_30d']; rise=summary['strongest_ma30_rise_30d']
    change=high['mw']-low['mw']
    insights=[
        f"Trung bình trượt 30 ngày đạt cao nhất {num(high['mw'])} MW vào {high['day_utc']} và thấp nhất {num(low['mw'])} MW vào {low['day_utc']}; khoảng cách {num(change)} MW trong mẫu 2019. Đây là mô tả hai cửa sổ, không chứng minh xu hướng nhiều năm.",
        f"Tháng UTC cao nhất là {mhigh['month_utc']:02d}/2019 ({num(mhigh['avg_mw'])} MW), thấp nhất là {mlow['month_utc']:02d}/2019 ({num(mlow['avg_mw'])} MW). MA30 giảm mạnh nhất {num(abs(fall['change_mw']))} MW từ {fall['start_day_utc']} đến {fall['end_day_utc']}; tăng mạnh nhất {num(rise['change_mw'])} MW từ {rise['start_day_utc']} đến {rise['end_day_utc']}.",
        f"Trong bảng theo giờ và thứ địa phương, ô cao nhất là {WEEKDAYS[hhigh['weekday_local']]} {hhigh['hour_local']:02d}:00 ({num(hhigh['mw'])} MW); thấp nhất là {WEEKDAYS[hlow['weekday_local']]} {hlow['hour_local']:02d}:00 ({num(hlow['mw'])} MW). Mỗi ô là trung bình của nhiều tuần.",
        'Bảng sai khác so từng giờ tháng 9–12 với mức trung bình của cùng thứ và giờ trong tháng 1–8. Chênh lệch lớn là tín hiệu cần xem xét, chưa đủ để gọi là lỗi dữ liệu hoặc xác định nguyên nhân.'
    ]
    table=''.join('<tr>'+''.join(f'<td>{ESC(str(x))}</td>' for x in
              (r['direction'],r['timestamp_utc'],num(r['load_mw']),num(r['reference_mw']),num(r['difference_mw'])))+'</tr>'
              for r in deviations)
    css = 'body{margin:0;background:#eef3f7;color:#19344d;font:16px/1.6 system-ui,Arial}main{max-width:1140px;margin:auto;padding:30px 20px}header,section{background:white;border:1px solid #dce5ef;border-radius:16px;padding:24px;margin:18px 0}h1{font-size:34px;line-height:1.2}h2{font-size:23px}.chart-scroll{overflow-x:auto}svg{display:block;width:100%;min-width:640px;height:auto}p{color:#465d71}a{color:#006e83}nav{display:flex;flex-wrap:wrap;gap:8px 18px;margin-top:14px}table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:7px;border-bottom:1px solid #dce5ef}.scroll{overflow:auto}.note{background:#e9f8fc;padding:12px;border-radius:8px}small{color:#60788a}@media(max-width:600px){main{padding:10px}header,section{padding:12px}}'
    page = f'<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OPSD · Plan A</title><style>{css}</style><main><header><h1>Plan A · Phụ tải điện Đức năm 2019</h1><p>8.760 quan sát theo giờ · MW · nguồn Parquet trên HDFS · kiểm định độc lập đạt.</p><small>Run ID {ESC(summary["run_id"])}</small>'
    page += '<nav aria-label="Biểu đồ"><a href="#ma">MA7 và MA30</a><a href="#hourly">Theo giờ</a><a href="#monthly">Theo tháng</a><a href="#day_type">Ngày thường và cuối tuần</a><a href="#heatmap">Nhiệt đồ</a></nav></header>'
    page += '<section id="ma"><h2>Trung bình ngày và trung bình trượt 7, 30 ngày</h2><p>'+ESC(insights[0])+'</p><div class="chart-scroll">'+line+'</div><p class="note">Đường xám là trung bình 24 số đo phụ tải theo giờ của từng ngày UTC. MA7 (xanh) và MA30 (cam) lấy trung bình 7 hoặc 30 ngày đã quan sát, gồm ngày đang xét. Cửa sổ trượt làm mượt dao động ngày để thấy xu hướng; MA30 thường mượt hơn nhưng phản ứng chậm hơn MA7. Đây là mô tả lịch sử, không phải dự báo.</p><p>Ví dụ: MA7 ngày 07/01 dùng ngày 01–07/01; sang 08/01 bỏ ngày 01 và thêm ngày 08. Chưa đủ 7/30 ngày thì chưa vẽ MA tương ứng. Ba đường dùng chung trục MW; thang dọc không bắt đầu từ 0 để nhìn rõ biến động.</p><a href="daily_trend.csv">Tải trung bình ngày và MA7/MA30</a> · <a href="daily_trend.svg">Biểu đồ SVG</a></section>'
    page += period_sections(period_charts(tables), profiles, asset_path='../analytics/')
    page += '<section id="heatmap"><h2>Nhịp phụ tải theo thứ và giờ Đức</h2><p>'+ESC(insights[2])+'</p><div class="chart-scroll">'+heat+'</div><p>Đổi giờ mùa hè có thể làm số quan sát của các ô khác nhau. Di chuột vào ô để xem MW và cỡ mẫu.</p><a href="hour_week.csv">Bảng 168 ô</a> · <a href="hour_week.svg">Nhiệt đồ SVG</a></section>'
    page += '<section><h2>Nhịp theo tháng và giờ Đức</h2><div class="chart-scroll">'+month_heat+'</div><p>Nhóm tháng theo UTC và giờ trong ngày theo Europe/Berlin. Hai nhiệt đồ dùng chung thang màu MW.</p><a href="month_hour.csv">Bảng 288 ô</a> · <a href="month_hour.svg">Nhiệt đồ SVG</a></section>'
    page+='<section><h2>20 thời điểm lệch nhiều so với mức tham chiếu</h2><p class="note">Mức tham chiếu chỉ học từ tháng 1–8 theo cùng thứ và giờ Đức; chỉ đánh giá giờ từ tháng 9–12. Mùa và ngày lễ có thể làm thay đổi phụ tải, nên đây không phải nhãn bất thường đã được xác minh.</p><div class="scroll"><table><tr><th>Hướng</th><th>UTC</th><th>Thực tế (MW)</th><th>Tham chiếu (MW)</th><th>Lệch (MW)</th></tr>'+table+'</table></div><a href="largest_deviations.csv">Tải CSV</a></section>'
    page+='<section><h2>Giới hạn và kiểm định</h2><p>Một năm ở một quốc gia chỉ cho phép mô tả biến động trong mẫu. Đã đối chiếu 365 trung bình ngày với kết quả Spark SQL, kiểm tra cửa sổ 7/30 ngày và các nhóm giờ.</p><a href="verification.json">Kiểm tra xu hướng</a> · <a href="../quality/quality_summary.json">Kiểm định Parquet</a> · <a href="../analytics/index.html">Kết quả Spark SQL chi tiết</a> · <a href="summary.json">Thông tin lần chạy</a></section></main></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    report = '# Phân tích phụ tải Đức 2019\n\n'
    report += '\n\n'.join('- ' + text for text in (*profiles.values(), *insights))
    report += '\n\nPhạm vi 2019 UTC, giờ địa phương Europe/Berlin, đơn vị MW. Các kết quả mô tả mẫu một năm.\n'
    report += '\nBiểu đồ daily_trend.svg đối chiếu trung bình ngày (24 số đo giờ UTC) với MA7/MA30 trên cùng trục MW. Cửa sổ trượt làm mượt dao động ngắn hạn, đồng thời có độ trễ; không phải dự báo tương lai.\n'
    (OUT/'report.md').write_text(report, encoding='utf-8')
    print('TREND DASHBOARD READY:', OUT/'index.html')


if __name__=='__main__':
    main()
