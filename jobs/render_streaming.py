"""Offline HTML dashboard that refreshes as historical replay stages complete."""
import argparse
import csv
import html
import json
import math
from pathlib import Path

from stream_paths import ROOT, checked_id

OUT = ROOT/'outputs/streaming'


def num(value):
    return f'{float(value):,.2f}'.replace(',', '_').replace('.', ',').replace('_', '.')


def chart(rows):
    values = [float(r['actual_mw']) for r in rows]
    values += [float(r['forecast_for_observed_mw']) for r in rows if r['forecast_for_observed_mw']]
    lo = math.floor(min(values)/5000)*5000
    hi = math.ceil(max(values)/5000)*5000
    left, top, width, height = 75, 28, 990, 260
    xy = lambda i, v: (left+i*width/24, top+(hi-v)*height/(hi-lo))
    parts = ['<style>text{font:13px Arial;fill:#476274}</style>']
    for tick in range(lo, hi+1, 5000):
        y=xy(0,tick)[1]
        parts.append(f'<line x1="{left}" x2="{left+width}" y1="{y:.1f}" y2="{y:.1f}" stroke="#dbe6ed"/><text x="{left-8}" y="{y+4:.1f}" text-anchor="end">{tick:,.0f}</text>')
    for field,color in [('actual_mw','#087ea4'),('forecast_for_observed_mw','#e28b31')]:
        points=' '.join(f'{xy(i,float(r[field]))[0]:.1f},{xy(i,float(r[field]))[1]:.1f}'
                        for i,r in enumerate(rows) if r[field])
        parts.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>')
    for i,r in enumerate(rows):
        x,y=xy(i,float(r['actual_mw']))
        label=f"{r['observed_utc']}: {num(r['actual_mw'])} MW"
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="#087ea4"><title>{html.escape(label)}</title></circle>')
    for hour in (0,4,8,12,16,20,24):
        parts.append(f'<text x="{xy(hour,lo)[0]:.1f}" y="318" text-anchor="middle">{hour:02d}:00</text>')
    parts.append('<text x="80" y="344" fill="#087ea4">Xanh: quan sát</text><text x="290" y="344" fill="#e28b31">Cam: dự báo trước 1 giờ</text><text x="8" y="20">MW</text>')
    return '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1100 355" role="img" aria-label="Quan sát và dự báo trước một giờ">'+''.join(parts)+'</svg>'


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',required=True)
    run_id=checked_id(parser.parse_args().run_id)
    latest=json.loads((OUT/'latest.json').read_text(encoding='utf-8'))
    if latest['status']!='PASSED' or latest['run_id']!=run_id:
        raise ValueError('Latest snapshot does not match requested run')
    with (OUT/'events.csv').open(encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))
    if len(rows)!=latest['unique_hdfs_rows']:
        raise ValueError('Dashboard event table does not match verified snapshot')
    checks_path=OUT/'checks.json'
    complete=(latest['stage']=='duplicate' and checks_path.exists()
              and json.loads(checks_path.read_text(encoding='utf-8')).get('run_id')==run_id
              and json.loads(checks_path.read_text(encoding='utf-8')).get('status')=='PASSED')
    refresh='' if complete else '<meta http-equiv="refresh" content="5">'
    badge='Đã hoàn tất và kiểm định' if complete else 'Đang phát lại — trang tự cập nhật mỗi 5 giây'
    css='body{margin:0;background:#edf4f8;color:#17364e;font:16px/1.6 system-ui,Arial}main{max-width:1120px;margin:auto;padding:24px}header,section{background:white;border:1px solid #dbe7ef;border-radius:16px;padding:22px;margin:18px 0}h1{font-size:32px;line-height:1.2}h2{font-size:22px}p{color:#4a6275}svg{width:100%;height:auto}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.card{background:#eaf7fb;padding:16px;border-radius:12px}.big{font-size:27px;font-weight:700}table{width:100%;border-collapse:collapse}th,td{text-align:left;border-bottom:1px solid #dbe7ef;padding:8px}.scroll{overflow:auto}a{color:#006f91}.note{background:#fff4de;padding:13px;border-radius:8px}@media(max-width:680px){.grid{grid-template-columns:1fr}main{padding:10px}header,section{padding:14px}}'
    page=f'<!doctype html><html lang="vi"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">{refresh}<title>OPSD · Streaming replay</title><style>{css}</style><main><header><h1>Phát lại 24 giờ phụ tải lịch sử</h1><p>{badge} · 01/10/2019 UTC · dữ liệu OPSD trên HDFS · Spark Structured Streaming · Random Forest đã huấn luyện offline.</p><small>Run ID {html.escape(run_id)} · giai đoạn {html.escape(latest["stage"])}</small></header>'
    page+='<section class="grid">'
    page+=f'<div class="card">JSON đã phát<div class="big">{latest["emitted_files"]}</div></div>'
    page+=f'<div class="card">Giờ duy nhất trên HDFS<div class="big">{latest["unique_hdfs_rows"]}/24</div></div>'
    page+=f'<div class="card">Dự báo cho {html.escape(latest["next_hour_utc"])} UTC<div class="big">{num(latest["forecast_next_mw"])} MW</div></div></section>'
    page+='<section><h2>Quan sát mới và dự báo trước một giờ</h2>'+chart(rows)+'<p>Mỗi điểm cam tại giờ t được tạo khi quan sát giờ t−1 đến. Điểm mới nhất chưa có giá trị thực của giờ kế tiếp cho đến khi tệp tiếp theo được phát.</p>'
    if latest['observed_mae_mw'] is not None:
        page+=f'<p>MAE tạm thời trên {latest["observed_residuals"]} giờ đã nhận đủ dự báo và thực tế: <strong>{num(latest["observed_mae_mw"])} MW</strong>. Đây chỉ là đoạn demo, không thay thế MAE 2.208 giờ test của P2.</p>'
    page+='</section>'
    table=''.join('<tr><td>'+html.escape(r['observed_utc'])+'</td><td>'+num(r['actual_mw'])+'</td><td>'+('—' if not r['forecast_for_observed_mw'] else num(r['forecast_for_observed_mw']))+'</td><td>'+('—' if not r['error_actual_minus_forecast_mw'] else num(r['error_actual_minus_forecast_mw']))+'</td></tr>' for r in rows[-8:])
    page+='<section><h2>8 giờ mới nhất</h2><div class="scroll"><table><tr><th>Giờ UTC</th><th>Thực tế (MW)</th><th>Dự báo từ giờ trước (MW)</th><th>Thực tế − dự báo (MW)</th></tr>'+table+'</table></div><a href="events.csv">Toàn bộ 24 hàng CSV</a></section>'
    page+='<section><h2>Checkpoint và phép thử trùng</h2><p class="note">Đây là <strong>replay dữ liệu 2019</strong>, không phải luồng đo trực tiếp. File JSON là đầu vào; Spark đọc theo microbatch; Parquet và checkpoint nằm ở hai thư mục HDFS riêng. Mô hình chỉ được nạp để dự báo, không huấn luyện lại trong stream.</p>'
    if complete:
        page+='<p>Kiểm định đạt: 25 tệp JSON đã được đọc (24 giờ + 1 bản trùng); HDFS vẫn chỉ có 24 giờ duy nhất. Chạy lại với cùng checkpoint không thêm hàng. Cả 24 dự báo khớp đầu ra ML offline.</p><a href="checks.json">Bằng chứng kiểm định</a> · '
    page+='<a href="latest.json">Trạng thái hiện tại</a></section></main></html>'
    (OUT/'index.html').write_text(page,encoding='utf-8')
    if complete:
        report=(f'# Demo Spark Structured Streaming — {run_id}\n\n'
                'Nguồn: phát lại 24 giờ UTC ngày 01/10/2019 từ OPSD/HDFS; không phải dữ liệu trực tiếp.\n\n'
                '25 tệp JSON được đọc: 24 giờ riêng biệt và một tệp lặp lại giờ 12:00. '
                'Spark Structured Streaming ghi 24 hàng Parquet duy nhất trên HDFS; '
                'chạy lại cùng checkpoint và nhận bản lặp đều không làm tăng số hàng.\n\n'
                f'Quan sát cuối: {latest["latest_observed_utc"]} UTC, {num(latest["latest_observed_mw"])} MW. '
                f'Dự báo giờ {latest["next_hour_utc"]} UTC: {num(latest["forecast_next_mw"])} MW.\n\n'
                f'MAE của 23 cặp đã có thực tế trong đoạn replay: {num(latest["observed_mae_mw"])} MW; '
                'không dùng con số này thay cho đánh giá test 2.208 giờ.\n\n'
                'Đầu vào JSON, Parquet đầu ra và checkpoint ở các đường dẫn riêng; '
                'mô hình Random Forest huấn luyện offline được nạp để chấm điểm.\n')
        (OUT/'report.md').write_text(report,encoding='utf-8')
    print('STREAM DASHBOARD READY:', OUT/'index.html', badge)


if __name__=='__main__':
    main()
