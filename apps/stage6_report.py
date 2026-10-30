"""Stage 6 offline benchmark report. Python standard library only.

Runs on Ubuntu host. Charts are original SVG generated from real benchmark JSON.
"""
from __future__ import annotations

import html
import json
import statistics
from pathlib import Path

BASE = Path.home() / "nyc-taxi-bigdata" / "data" / "reports"


def load(kind):
    path = BASE / f"stage6_{kind}.json"
    d = json.loads(path.read_text(encoding="utf-8"))
    if d.get("status") != "SUCCESS" or d.get("mode") != kind or len(d.get("cases", [])) != 3:
        raise RuntimeError(f"Stage6 {kind} missing or incomplete: {path}")
    for case in d["cases"]:
        assert len(case["runs"]) >= 2, "Need repeated trials"
        assert all(r["rows"] == case["rows_expected"] for r in case["runs"])
    return d


def bar_chart(items, *, title, ylabel, color="#2777bd", suffix="", inverse=False):
    """SVG as a standalone artifact and an inline HTML visualization."""
    W, H = 1060, 510
    left, right, top, bottom = 115, 70, 80, 110
    width, height = W-left-right, H-top-bottom
    values = [float(i[1]) for i in items]
    hi = max(values) * 1.2 if max(values) else 1
    labels = [v[0] for v in items]
    n = len(items)
    bar_w = min(140, width/(n*2.4))
    els = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
           f'role="img" aria-label="{html.escape(title, quote=True)}">',
           f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
           f'<text x="{left}" y="40" fill="#173152" font-family="Arial, sans-serif" font-size="27" font-weight="700">{html.escape(title)}</text>']
    for i in range(6):
        v = hi * i / 5
        y = top + height * (1 - i/5)
        els.append(f'<line x1="{left}" x2="{W-right}" y1="{y:.1f}" y2="{y:.1f}" stroke="#e7edf5" stroke-width="1"/>')
        els.append(f'<text x="{left-15}" y="{y+5:.1f}" fill="#65768c" font-size="15" text-anchor="end" font-family="Arial">{v:,.0f}</text>')
    for i, (label, value) in enumerate(items):
        x = left + width*(i+0.5)/n
        y = top + height * (1 - value/hi)
        bh = top+height-y
        curr_color = '#2674b8' if i != 0 else '#2b8b75'
        els.append(f'<rect x="{x-bar_w/2:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
                   f'fill="{curr_color}" rx="7"/>')
        fmt = f"{value:,.2f}{suffix}" if abs(value)<1000 else f"{value:,.0f}{suffix}"
        els.append(f'<text x="{x:.1f}" y="{max(y-14, top-10):.1f}" font-size="19" font-family="Arial" '
                   f'font-weight="700" text-anchor="middle" fill="#213a58">{fmt}</text>')
        els.append(f'<text x="{x:.1f}" y="{top+height+33}" font-size="18" font-family="Arial" '
                   f'text-anchor="middle" fill="#3c526d">{html.escape(str(label))}</text>')
    els.append(f'<text x="{W/2}" y="{H-18}" text-anchor="middle" font-size="16" '
               f'fill="#60738b" font-family="Arial">{html.escape(ylabel)}</text>')
    els.append('</svg>')
    return ''.join(els)


def table(data, mode):
    cols = [('months', '数据月数'), ('partitions','分区数'), ('rows_expected','有效记录数'),
            ('input_mib','源文件 MiB'), ('median_seconds','中位耗时(s)'),
            ('min_seconds','最快(s)'), ('max_seconds','最慢(s)'),
            ('median_rows_per_second','处理记录/秒')]
    thead=''.join(f'<th>{html.escape(name)}</th>' for _,name in cols)
    rows=[]
    for case in data['cases']:
        row=[]
        for key,_ in cols:
            v=case[key]
            if key=='rows_expected': val=f'{v:,}'
            elif key in {'median_seconds','min_seconds','max_seconds'}: val=f'{v:,.3f}'
            elif key=='median_rows_per_second': val=f'{v:,.1f}'
            else: val=str(v)
            row.append(f'<td>{val}</td>')
        rows.append('<tr>'+''.join(row)+'</tr>')
    return '<div class="table-scroll"><table><thead><tr>'+thead+'</tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>'


def main():
    scale=load('scale')
    partition=load('partition')
    if any(c['partitions'] != scale['cases'][0]['partitions'] for c in scale['cases']):
        raise RuntimeError('Scale cases do not share fixed partition count')
    if any(c['months'] != 1 for c in partition['cases']):
        raise RuntimeError('Partition cases must be January only')
    # Actual cluster results, not illustrations or hard-coded benchmark metrics.
    svgs={
        'scale':bar_chart([(f"{c['months']} 个月",c['median_seconds']) for c in scale['cases']],
                          title='数据规模与 Spark 执行时间', ylabel='Spark 实际执行耗时（秒）'),
        'partition':bar_chart([(f"{c['partitions']} 分区",c['median_seconds']) for c in partition['cases']],
                              title='Spark 分区数与执行时间（1 月数据）',ylabel='Spark 实际执行耗时（秒）'),
        'throughput':bar_chart([(f"{c['months']} 个月",c['median_rows_per_second']) for c in scale['cases']],
                               title='不同数据规模的处理吞吐量',ylabel='有效订单记录/秒')
    }
    for name,svg in svgs.items():
        (BASE/f'stage6_{name}.svg').write_text(svg,encoding='utf-8')
    results=[]
    for c in scale['cases']:
        results.append(f"{c['months']}个月：{c['median_seconds']}秒")
    for c in partition['cases']:
        results.append(f"{c['partitions']}分区：{c['median_seconds']}秒")
    page=f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NYC Taxi · Spark 性能实验</title>
<style>body{{font-family:system-ui,-apple-system,"Microsoft YaHei",Arial,sans-serif;background:#f3f6fb;color:#173152;margin:0}}
main{{max-width:1190px;margin:0 auto;padding:38px 30px}}h1{{font-size:35px}}h2{{margin:30px 0 14px}}
.summary{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}.metric,.panel{{background:white;border-radius:13px;border:1px solid #e4eaf3;padding:20px;margin-bottom:14px}}
.metric strong{{display:block;font-size:27px;margin-top:10px}}.muted{{color:#61758e;font-size:14px}}svg{{width:100%;height:auto}}
table{{width:100%;border-collapse:collapse;font-size:15px}}th,td{{padding:12px 10px;border-bottom:1px solid #e7edf5;text-align:right;white-space:nowrap}}th{{background:#f3f6fb}}th:first-child,td:first-child{{text-align:left}}
.table-scroll{{overflow-x:auto}}.note{{background:#e8f1fc;border-radius:10px;padding:16px;line-height:1.7}}@media(max-width:700px){{.summary{{grid-template-columns:1fr}}main{{padding:20px}}}}</style></head>
<body><main><h1>🚕 Spark 分布式计算性能实验</h1><p class="muted">NYC Yellow Taxi 2024 年 1～6 月真实订单 · 单机 Ubuntu 虚拟机 · Spark Standalone · 每组 {scale['repeats']} 次测量</p>
<div class="summary"><div class="metric">最大测试规模<strong>{scale['cases'][-1]['rows_expected']:,}</strong><span class="muted">清洗后的有效订单</span></div>
<div class="metric">数据规模实验<strong>{len(scale['cases'])} 组</strong><span class="muted">固定 {scale['cases'][0]['partitions']} 个分区</span></div>
<div class="metric">分区性能实验<strong>{len(partition['cases'])} 组</strong><span class="muted">使用 2024 年 1 月</span></div></div>
<h2>实验一：不同数据规模</h2><div class="panel">{svgs['scale']}</div><div class="panel">{svgs['throughput']}</div>{table(scale,'scale')}
<h2>实验二：不同 Spark 分区数</h2><div class="panel">{svgs['partition']}</div>{table(partition,'partition')}
<h2>实验条件与解释</h2><div class="note">所有数据来自真实的 TLC Parquet 文件，执行相同的数据清洗、重分区和按区域/小时分组聚合。每组记录多次壁钟时间，表格显示中位数与最小/最大值。<br>
<strong>环境：</strong>{html.escape(scale['machine_note'])}；Spark {html.escape(scale['spark_version'])}；关闭 AQE。<br>
<strong>限制：</strong>这是同一台物理宿主机上的虚拟机模拟集群，不能将多容器结果当成多物理机加速比；缓存、共享磁盘 I/O、JVM 热身和调度开销可能造成波动。分区变多不保证速度更快。</div>
<p class="muted">实验 JSON: stage6_scale.json、stage6_partition.json；图表由原始测量自动生成，可复现。</p></main></body></html>'''
    dest=BASE/'stage6_dashboard.html'
    dest.write_text(page,encoding='utf-8')
    print('STAGE6_REPORT_SUCCESS')
    print('REPORT_PATH =', dest)
    print('SVG_CHARTS =',len(svgs))
    print('SUMMARY =', '; '.join(results))

if __name__=='__main__':
    main()
