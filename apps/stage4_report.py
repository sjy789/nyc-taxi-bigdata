"""Stage 4D: offline, dependency-free HTML/SVG report using exported predictions and metrics."""
import csv
import html
import json
import math
from pathlib import Path

BASE=Path.home()/'nyc-taxi-bigdata'/'data'
REPORT=BASE/'reports'
EXPORT=BASE/'ml'/'exports'
with (REPORT/'stage4_metrics.json').open(encoding='utf-8') as f:
    summary=json.load(f)
if summary.get('status')!='SUCCESS':
    raise RuntimeError('stage4_metrics.json has no SUCCESS status')

def read_csv(filename):
    with (EXPORT/filename).open(encoding='utf-8') as f:
        return list(csv.DictReader(f))

daily=read_csv('daily_june.csv')
week=read_csv('sample_zone_week.csv')
if len(daily)!=30 or len(week)!=168:
    raise RuntimeError('Expected 30 daily points and 168 hourly points')
models=['baseline','rf','gbt']
labels={'baseline':'历史同期 Baseline','rf':'随机森林 RF','gbt':'梯度提升树 GBT'}
colors={'baseline':'#9a6a24','rf':'#2e77c7','gbt':'#209a81','actual':'#202d43'}
chosen=summary['best_model_by_validation_mae']

def txt(x): return html.escape(str(x))

def bar_chart(metric, title, suffix=''):
    w,h=940,348
    x0=235; span=620; bar_h=30
    candidates=[summary['test']['models'][n][metric] for n in models]
    maximum=max(candidates)*1.18 or 1
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" role="img" aria-label="{txt(title)}">',
           f'<text x="24" y="34" font-size="22" font-weight="700" fill="#17243b">{txt(title)}</text>']
    for i,n in enumerate(models):
        y=78+i*76; val=candidates[i]; length=val/maximum*span
        parts.extend([f'<text x="25" y="{y+21}" font-size="17" fill="#33465e">{txt(labels[n])}</text>',
            f'<rect x="{x0}" y="{y-3}" width="{span}" height="{bar_h}" fill="#f0f3f8" rx="5"/>',
            f'<rect x="{x0}" y="{y-3}" width="{length:.1f}" height="{bar_h}" fill="{colors[n]}" rx="5"/>',
            f'<text x="{min(x0+length+10,w-70):.1f}" y="{y+19}" font-size="17" fill="#17243b">{val:,.2f}{suffix}</text>'])
    parts.append('<text x="235" y="320" font-size="14" fill="#64748b">数值越小越好 · 2024 年 6 月独立测试集</text>')
    parts.append('</svg>')
    return '\n'.join(parts)

def line_chart(rows, field, title, label_title):
    w,h=1140,435
    left,right,top,bottom=80,25,65,72
    area_w=w-left-right; area_h=h-top-bottom
    arr=[float(row['actual']) for row in rows]
    pred=[float(row['pred_'+chosen]) for row in rows]
    scale_max=max(arr+pred)*1.13 or 1
    n=len(arr)
    def xy(i, value):
        return (left+i*area_w/max(1,n-1),top+area_h*(1-value/scale_max))
    def points(series):
        return ' '.join(f'{x:.1f},{y:.1f}' for i,val in enumerate(series) for x,y in [xy(i,val)])
    s=[f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" role="img" aria-label="{txt(title)}">',
       f'<text x="20" y="34" font-size="23" font-weight="700" fill="#17243b">{txt(title)}</text>']
    for tick in range(5):
        yy=top+area_h*tick/4
        label=scale_max*(1-tick/4)
        s.extend([f'<line x1="{left}" y1="{yy:.1f}" x2="{w-right}" y2="{yy:.1f}" stroke="#e2e8f0"/>',
                  f'<text x="{left-12}" y="{yy+5:.1f}" text-anchor="end" font-size="13" fill="#69788c">{label:,.0f}</text>'])
    ticks=6 if n<60 else 8
    for tick in range(ticks):
        i=round(tick*(n-1)/(ticks-1))
        x=left+i*area_w/(n-1)
        label=rows[i][field]
        if field=='date': label=label[5:]
        if field=='hour_key':label=label[5:]+'时'
        s.append(f'<text x="{x:.1f}" y="{h-39}" text-anchor="middle" font-size="13" fill="#64748b">{txt(label)}</text>')
    s.append(f'<polyline points="{points(arr)}" fill="none" stroke="{colors["actual"]}" stroke-width="2.5" stroke-linejoin="round"/>')
    s.append(f'<polyline points="{points(pred)}" fill="none" stroke="{colors[chosen]}" stroke-width="2.3" stroke-linejoin="round"/>')
    for i in range(n):
        if n <= 35:
            x,y=xy(i,arr[i]); s.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.4" fill="{colors["actual"]}"/>')
    s.extend([f'<rect x="{w-350}" y="16" width="14" height="4" fill="{colors["actual"]}"/>',
              f'<text x="{w-332}" y="23" font-size="14" fill="#33465e">实际订单量</text>',
              f'<rect x="{w-205}" y="16" width="14" height="4" fill="{colors[chosen]}"/>',
              f'<text x="{w-187}" y="23" font-size="14" fill="#33465e">预测（{txt(chosen.upper())}）</text>',
              f'<text x="{left}" y="{h-12}" font-size="12" fill="#738197">{txt(label_title)}</text>',
              '</svg>'])
    return '\n'.join(s)

plots={
    'stage4_mae.svg':bar_chart('MAE','三种模型 MAE 对比'),
    'stage4_rmse.svg':bar_chart('RMSE','三种模型 RMSE 对比'),
    'stage4_wape.svg':bar_chart('WAPE_percent','三种模型 WAPE 对比','%'),
    'stage4_daily.svg':line_chart(daily,'date','6 月每日总订单量：实际值与预测值','每天汇总 30 个区域的小时订单数'),
    'stage4_hourly.svg':line_chart(week,'hour_key',f'区域 {summary["example_zone_id"]}：一周小时级预测效果','2024-06-10 至 2024-06-16，168 个小时')}
for name,svg in plots.items():
    (REPORT/name).write_text(svg,encoding='utf-8')

met=summary['test']['models']
rows_html='\n'.join(f'<tr><td>{txt(labels[name])}</td><td>{met[name]["MAE"]:.3f}</td><td>{met[name]["RMSE"]:.3f}</td><td>{met[name]["WAPE_percent"]:.3f}%</td></tr>' for name in models)
imgs=''.join(f'<section><img src="data:image/svg+xml;charset=utf-8,{""}"/></section>' for _ in [])  # generated below as inline SVG
content='\n'.join(f'<section>{svg}</section>' for svg in plots.values())
doc=f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>NYC Taxi · 阶段四需求预测报告</title>
<style>body{{font-family:Arial,'Microsoft YaHei',sans-serif;background:#f4f7fc;color:#17243b;margin:0}}main{{max-width:1180px;margin:auto;padding:25px}}header{{background:#172c51;color:white;border-radius:16px;padding:28px 32px}}h1{{margin:0 0 8px}}p{{line-height:1.7}}.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:22px 0}}.card,section{{background:white;border:1px solid #e4eaf2;border-radius:14px;box-shadow:0 2px 8px #17243b09}}.card{{padding:18px}}.num{{font-size:29px;font-weight:750;margin-top:8px}}.label{{font-size:13px;color:#64748b}}section{{padding:16px;margin:18px 0}}svg{{max-width:100%;height:auto}}table{{border-collapse:collapse;width:100%;text-align:left}}th,td{{padding:12px;border-bottom:1px solid #e3e8ef}}th{{background:#f7f9fc}}.note{{font-size:13px;color:#62728b}}@media(max-width:740px){{.cards{{grid-template-columns:repeat(2,1fr)}}main{{padding:10px}}}}</style>
</head><body><main><header><h1>NYC Taxi · 时空订单预测</h1><p>基于 Spark MLlib · 2024 年 1—4 月训练 / 5 月验证 / 6 月测试 · 30 个区域 · 下一小时订单预测</p></header>
<div class="cards"><div class="card"><div class="label">训练样本</div><div class="num">{summary['train_rows']:,}</div></div><div class="card"><div class="label">测试样本</div><div class="num">{summary['test_rows']:,}</div></div><div class="card"><div class="label">验证集最佳模型</div><div class="num">{txt(chosen.upper())}</div></div><div class="card"><div class="label">测试集 MAE</div><div class="num">{met[chosen]['MAE']:,.2f}</div></div></div>
<section><h2>测试集指标汇总（2024 年 6 月）</h2><table><tr><th>模型</th><th>MAE ↓</th><th>RMSE ↓</th><th>WAPE ↓</th></tr>{rows_html}</table><p class="note">模型通过 2024 年 5 月验证集 MAE 选择，而不是按测试集指标选择。历史特征仅包含预测时刻之前的真实订单量。使用的是单步滚动预测，不是一次性预测未来整个月。</p></section>
{content}
<section><h2>实验说明</h2><p>原始来源：NYC TLC 2024 Yellow Taxi Parquet。区域选取仅依据 1—4 月历史订单量；每区域每小时补齐 0 订单时段。预测模型：前一周同小时基线、Spark 随机森林、Spark GBT。</p><p class="note">运行时间和精度因机器资源、训练配置与样本而不同。示例区域 ID：{summary['example_zone_id']}。图表数据来自真实实验结果。</p></section>
</main></body></html>'''
out=REPORT/'stage4_dashboard.html'
out.write_text(doc,encoding='utf-8')
print('STAGE4_DASHBOARD_SUCCESS')
print('REPORT_PATH =',str(out))
print('CHART_FILES =',len(plots))
print('HTML_SIZE_BYTES =',out.stat().st_size)
