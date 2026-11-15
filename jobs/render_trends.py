"""Offline, inspectable trend dashboard from verified CSV outputs; no extra packages."""
import csv
import hashlib
import html
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/trends'
ESC = html.escape
WEEKDAYS = ['Thứ hai', 'Thứ ba', 'Thứ tư', 'Thứ năm', 'Thứ sáu', 'Thứ bảy', 'Chủ nhật']


def read_csv(name):
    with (OUT/name).open(encoding='utf-8', newline='') as handle:
        return list(csv.DictReader(handle))


def num(value):
    return f'{float(value):,.2f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def trend_svg(daily):
    width, height, left, top, cw, ch = 1100, 390, 80, 30, 980, 295
    all_values = [float(r['avg_mw']) for r in daily]
    low = math.floor(min(all_values)/5000)*5000
    high = math.ceil(max(all_values)/5000)*5000
    xy = lambda i, v: (left + i*cw/(len(daily)-1), top + (high-v)*ch/(high-low))
    parts = []
    for tick in range(low, high+1, 5000):
        y = xy(0,tick)[1]
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+cw}" y2="{y:.1f}" stroke="#dbe7ef"/>')
        parts.append(f'<text x="{left-9}" y="{y+4:.1f}" text-anchor="end">{tick:,.0f}</text>')
    for field, color, stroke in [('avg_mw','#a5b8c5',1.2),('ma7_mw','#087eaa',2.8),('ma30_mw','#e08b36',3.2)]:
        values=[(i,float(r[field])) for i,r in enumerate(daily) if r[field] not in ('','None')]
        points=' '.join(f'{xy(i,v)[0]:.1f},{xy(i,v)[1]:.1f}' for i,v in values)
        parts.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="{stroke}"/>')
    for i,r in enumerate(daily):
        v=float(r['avg_mw']); x,y=xy(i,v)
        title=f"{r['day_utc']}: {num(v)} MW; MA7 {num(r['ma7_mw']) if r['ma7_mw'] not in ('','None') else 'chưa đủ 7 ngày'}; MA30 {num(r['ma30_mw']) if r['ma30_mw'] not in ('','None') else 'chưa đủ 30 ngày'}"
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="transparent"><title>{ESC(title)}</title></circle>')
    for month in (1,3,5,7,9,11):
        key=f'2019-{month:02d}-01'; i=next(i for i,r in enumerate(daily) if r['day_utc']==key)
        parts.append(f'<text x="{xy(i,low)[0]:.1f}" y="352" text-anchor="middle">{month:02d}/2019</text>')
    legend='<text x="80" y="380">Xám: từng ngày</text><text x="300" y="380" fill="#087eaa">Xanh: MA7</text><text x="460" y="380" fill="#e08b36">Cam: MA30</text>'
    return f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="Phụ tải trung bình ngày và trung bình trượt"><style>text{{font:13px Arial;fill:#405a70}}</style><text x="8" y="20">MW</text>'+''.join(parts)+legend+'</svg>'


def heatmap_svg(rows):
    d={(int(r['weekday_local']),int(r['hour_local'])):r for r in rows}
    vals=[float(r['avg_mw']) for r in rows]; lo=min(vals); hi=max(vals)
    parts=['<text x="5" y="18">Giờ địa phương Europe/Berlin →</text>']
    for day in range(7):
        parts.append(f'<text x="85" y="{73+day*47}" text-anchor="end">{WEEKDAYS[day]}</text>')
        for hour in range(24):
            r=d[(day,hour)]; v=float(r['avg_mw']); mix=(v-lo)/(hi-lo)
            # More load means deeper blue.
            color=f'#{int(221-190*mix):02x}{int(239-112*mix):02x}{int(248-73*mix):02x}'
            x=105+hour*39; y=46+day*47
            parts.append(f'<rect x="{x}" y="{y}" width="35" height="37" rx="4" fill="{color}"><title>{WEEKDAYS[day]}, {hour:02d}:00: {num(v)} MW; n={r["observations"]}</title></rect>')
    for hour in range(0,24,2):
        parts.append(f'<text x="{123+hour*39}" y="36" text-anchor="middle">{hour:02d}</text>')
    return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1080 405" role="img" aria-label="Nhiệt đồ phụ tải theo thứ và giờ Đức"><style>text{font:13px Arial;fill:#405a70}</style>'+''.join(parts)+'</svg>'


def month_heatmap_svg(rows):
    cells = {(int(r['month_utc']), int(r['hour_local'])): r for r in rows}
    values = [float(r['avg_mw']) for r in rows]
    low, high = min(values), max(values)
    parts = ['<text x="5" y="18">Giờ địa phương Europe/Berlin →</text>']
    for month in range(1, 13):
        parts.append(f'<text x="85" y="{73+(month-1)*35}" text-anchor="end">Tháng {month:02d}</text>')
        for hour in range(24):
            cell = cells[(month, hour)]
            value = float(cell['avg_mw'])
            mix = (value-low)/(high-low)
            color = f'#{int(221-190*mix):02x}{int(239-112*mix):02x}{int(248-73*mix):02x}'
            parts.append(f'<rect x="{105+hour*39}" y="{46+(month-1)*35}" width="35" height="30" rx="4" fill="{color}"><title>Tháng UTC {month:02d}, giờ Đức {hour:02d}:00: {num(value)} MW; n={cell["observations"]}</title></rect>')
    for hour in range(0, 24, 2):
        parts.append(f'<text x="{123+hour*39}" y="36" text-anchor="middle">{hour:02d}</text>')
    return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1080 490" role="img" aria-label="Nhiệt đồ phụ tải theo tháng UTC và giờ Đức"><style>text{font:13px Arial;fill:#405a70}</style>'+''.join(parts)+'</svg>'


def main():
    summary=json.loads((OUT/'summary.json').read_text(encoding='utf-8'))
    proof=json.loads((OUT/'verification.json').read_text(encoding='utf-8'))
    if summary['status'] != proof['status'] or summary['status'] != 'PASSED' or proof['trend_run_id'] != summary['run_id']:
        raise ValueError('Trend outputs are not independently verified')
    for name,digest in summary['files'].items():
        if hashlib.sha256((OUT/name).read_bytes()).hexdigest()!=digest:
            raise ValueError('Trend output changed: '+name)
    daily=read_csv('daily_trend.csv'); hours=read_csv('hour_week.csv'); month_hours=read_csv('month_hour.csv'); deviations=read_csv('largest_deviations.csv')
    line=trend_svg(daily); heat=heatmap_svg(hours); month_heat=month_heatmap_svg(month_hours)
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
    css='body{margin:0;background:#eef3f7;color:#19344d;font:16px/1.6 system-ui,Arial}main{max-width:1140px;margin:auto;padding:30px 20px}header,section{background:white;border:1px solid #dce5ef;border-radius:16px;padding:24px;margin:18px 0}h1{font-size:34px;line-height:1.2}h2{font-size:23px}svg{width:100%;height:auto}p{color:#465d71}a{color:#006e83}li{margin:12px 0}table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:7px;border-bottom:1px solid #dce5ef}.scroll{overflow:auto}.note{background:#e9f8fc;padding:12px;border-radius:8px}small{color:#60788a}@media(max-width:600px){main{padding:10px}header,section{padding:12px}}'
    page=f'<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OPSD · Phân tích xu hướng</title><style>{css}</style><main><header><h1>Xu hướng và nhịp phụ tải Đức năm 2019</h1><p>8.760 quan sát theo giờ · MW · nguồn Parquet trên HDFS · kiểm định độc lập đạt.</p><small>Run ID {ESC(summary["run_id"])}</small></header>'
    page+='<section><h2>Những điều rút ra từ dữ liệu</h2><ol>'+''.join('<li>'+ESC(t)+'</li>' for t in insights)+'</ol></section>'
    page+='<section><h2>Biến động theo ngày và trung bình trượt</h2>'+line+'<p>MA7/MA30 chỉ hiển thị khi đã đủ 7/30 ngày. Trục dọc tính bằng MW và được phóng gần phạm vi dữ liệu; di chuột vào từng ngày để xem số.</p><a href="daily_trend.csv">Bảng ngày</a> · <a href="daily_trend.svg">Biểu đồ SVG</a></section>'
    page+='<section><h2>Nhịp phụ tải theo thứ và giờ Đức</h2>'+heat+'<p>Thứ và giờ dùng Europe/Berlin; khóa gốc vẫn UTC. Đổi giờ mùa hè có thể làm số quan sát của một số ô khác nhau. Di chuột vào ô để xem MW và cỡ mẫu.</p><a href="hour_week.csv">Bảng 168 ô</a> · <a href="hour_week.svg">Nhiệt đồ SVG</a></section>'
    page+='<section><h2>Nhịp theo tháng và giờ Đức</h2>'+month_heat+'<p>Tháng được gán theo timestamp UTC của nguồn; giờ trong ngày đổi sang Europe/Berlin. Cách gán này giữ đúng 8.760 giờ của mẫu UTC năm 2019. Giờ đổi mùa có thể khiến số quan sát ở một ô khác nhau. Màu chỉ cho thấy mẫu hình theo tháng, chưa xác định nguyên nhân.</p><a href="month_hour.csv">Bảng 288 ô</a> · <a href="month_hour.svg">Nhiệt đồ SVG</a></section>'
    page+='<section><h2>20 thời điểm lệch nhiều so với mức tham chiếu</h2><p class="note">Mức tham chiếu chỉ học từ tháng 1–8 theo cùng thứ và giờ Đức; chỉ đánh giá giờ từ tháng 9–12. Mùa và ngày lễ có thể làm thay đổi phụ tải, nên đây không phải nhãn bất thường đã được xác minh.</p><div class="scroll"><table><tr><th>Hướng</th><th>UTC</th><th>Thực tế (MW)</th><th>Tham chiếu (MW)</th><th>Lệch (MW)</th></tr>'+table+'</table></div><a href="largest_deviations.csv">Tải CSV</a></section>'
    page+='<section><h2>Giới hạn và kiểm định</h2><p>Một năm ở một quốc gia chỉ cho phép mô tả biến động trong mẫu. Chưa chứng minh nguyên nhân, chưa có thời tiết/ngày lễ và không suy ra xu hướng dài hạn. Đã đối chiếu 365 trung bình ngày với xuất dữ liệu Plan A; kiểm tra 7/30 ngày, 168 ô giờ-thứ và 288 ô tháng-giờ.</p><a href="verification.json">Kết quả kiểm tra</a> · <a href="summary.json">Thông tin lần chạy</a></section></main></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    (OUT/'report.md').write_text('# Xu hướng phụ tải Đức 2019\n\n'+'\n\n'.join('- '+x for x in insights)+'\n\nPhạm vi: 2019 UTC; giờ/thứ: Europe/Berlin; đơn vị MW. Chưa suy ra nguyên nhân hoặc xu hướng nhiều năm.\n',encoding='utf-8')
    print('TREND DASHBOARD READY:', OUT/'index.html')


if __name__=='__main__':
    main()
