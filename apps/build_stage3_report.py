import csv
import glob
import html
import json
from pathlib import Path

ROOT = Path.home() / "nyc-taxi-bigdata" / "data"
MARTS = ROOT / "marts"
REPORTS = ROOT / "reports"

with (REPORTS / "stage3_summary.json").open(encoding="utf-8") as f:
    summary = json.load(f)


def read_table(name):
    paths = glob.glob(str(MARTS / name / "part-*.csv"))
    if len(paths) != 1:
        raise RuntimeError(f"Expected one CSV part in {name}, found {len(paths)}")
    with open(paths[0], newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def esc(s):
    return html.escape(str(s))


def chart(title, rows, label_key, value_key="trip_count", style="bars", every=1):
    w, h, left, right, top, bottom = 900, 300, 78, 25, 35, 60
    values = [int(r[value_key]) for r in rows]
    labels = [str(r[label_key]) for r in rows]
    ymax = max(values) * 1.12 if values else 1
    width, height = w - left - right, h - top - bottom
    parts = [f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="{esc(title)}">']
    for step in range(5):
        y = top + height * step / 4
        val = ymax * (1 - step / 4)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{w-right}" y2="{y:.1f}" stroke="#e5eaf1"/>')
        parts.append(f'<text x="{left-10}" y="{y+4:.1f}" text-anchor="end" font-size="12" fill="#667085">{val:,.0f}</text>')
    if values:
        if style == "line":
            coords = []
            for i, v in enumerate(values):
                x = left + width * i / max(1, len(values)-1)
                y = top + height * (1 - v/ymax)
                coords.append((x, y))
            points = " ".join(f"{x:.1f},{y:.1f}" for x,y in coords)
            parts.append(f'<polyline points="{points}" fill="none" stroke="#2367b3" stroke-width="3"/>')
            for x,y in coords:
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="#2367b3"/>')
        else:
            step = width / len(values)
            bw = step * 0.66
            for i,v in enumerate(values):
                x = left + i*step + (step-bw)/2
                bh = height * v / ymax
                y = top + height - bh
                parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" rx="3" fill="#3279bd"/>')
        for i, label in enumerate(labels):
            if i % every == 0 or i == len(labels)-1:
                x = left + (width*i/max(1,len(values)-1) if style == "line" else width*(i+0.5)/len(values))
                display = label[5:] if label.startswith("2024-") else label
                parts.append(f'<text x="{x:.1f}" y="{h-30}" text-anchor="middle" font-size="12" fill="#667085">{esc(display)}</text>')
    parts.append('</svg>')
    return f'<section><h2>{esc(title)}</h2>{"".join(parts)}</section>'


def ranked_chart(title, rows, name_key="zone", count_key="trip_count"):
    n = min(len(rows), 15)
    w, h, left, right, row_h = 900, 35 + n*30 + 20, 295, 95, 30
    maxv = max(int(r[count_key]) for r in rows[:n]) if n else 1
    parts = [f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="{esc(title)}">']
    for i,r in enumerate(rows[:n]):
        label = str(r[name_key])[:38]
        if "borough" in r:
            label = f'{r["borough"]} / {label}'
        v = int(r[count_key]); y = 24+i*row_h
        barw = (w-left-right) * v / maxv
        parts.append(f'<text x="{left-9}" y="{y+12}" font-size="13" text-anchor="end" fill="#344054">{esc(label)}</text>')
        parts.append(f'<rect x="{left}" y="{y}" width="{barw:.1f}" height="18" rx="3" fill="#3279bd"/>')
        parts.append(f'<text x="{left+barw+7:.1f}" y="{y+13}" font-size="12" fill="#475467">{v:,}</text>')
    parts.append('</svg>')
    return f'<section><h2>{esc(title)}</h2>{"".join(parts)}</section>'

k = summary["kpi"]
daily = sorted(read_table("daily_trips"), key=lambda r:r["date"])
hourly = sorted(read_table("hourly_trips"), key=lambda r:int(r["hour"]))
weekday = sorted(read_table("weekday_trips"), key=lambda r:int(r["weekday"]))
weekday_names = {1:"周日", 2:"周一", 3:"周二", 4:"周三", 5:"周四", 6:"周五", 7:"周六"}
for r in weekday:
    r["day"] = weekday_names[int(r["weekday"])]
zones = sorted(read_table("top_pickup_zones"),key=lambda r:-int(r["trip_count"]))
routes = sorted(read_table("top_routes"),key=lambda r:-int(r["trip_count"]))
dist = read_table("distance_distribution")
amount = read_table("amount_distribution")

cards = [
    ("清洗后订单量", f'{k["trip_count"]:,}'),
    ("有订单的上车区域数", str(k["pickup_zone_count"])),
    ("平均行程距离（英里）", str(k["avg_distance_miles"])),
    ("平均行程时间（分钟）", str(k["avg_duration_min"])),
    ("平均订单金额（美元）", str(k["avg_order_amount_usd"])),
]
card_html = "".join(f'<div class="metric"><small>{esc(a)}</small><b>{esc(b)}</b></div>' for a,b in cards)

plots = [
    chart("每日订单量 · 2024年1月", daily, "date", style="line", every=5),
    chart("24小时上车订单分布", hourly, "hour", every=2),
    chart("星期分布", weekday, "day"),
    ranked_chart("热门上车区域 Top 15", zones),
    chart("行程距离分布（英里）", dist, "distance_bin"),
    chart("订单金额分布（美元）", amount, "amount_bin"),
]
route_rows = "".join(
    f'<tr><td>{esc(r["pickup_zone"])}</td><td>{esc(r["dropoff_zone"])}</td><td>{int(r["trip_count"]):,}</td></tr>'
    for r in routes
)
html_doc = f'''<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>纽约出租车 · 阶段三数据分析报告</title>
<style>
*{{box-sizing:border-box}} body{{margin:0;background:#f2f5f9;color:#182230;font:16px Arial,sans-serif}}
main{{max-width:1050px;margin:auto;padding:32px 22px}} h1{{margin:0 0 9px}} p{{color:#667085;margin:0 0 24px}}
.metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:24px 0}}
.metric,section{{background:white;border:1px solid #e5eaf1;border-radius:14px;box-shadow:0 2px 12px #102a4410}}
.metric{{padding:19px}}small{{display:block;color:#667085;margin-bottom:10px;font-size:13px}}b{{font-size:23px}}
section{{padding:16px 24px;margin:18px 0}}section h2{{font-size:19px;margin:5px 0 12px}}
svg{{width:100%;height:auto;display:block}}table{{width:100%;border-collapse:collapse;font-size:14px}}
th,td{{text-align:left;padding:12px;border-bottom:1px solid #e5eaf1}}th{{background:#f8fafc}}
footer{{font-size:13px;color:#667085;padding:22px 0}}
</style></head><body><main>
<h1>纽约黄色出租车 · 2024 年 1 月</h1>
<p>阶段三 | Spark SQL 出行数据分析 | 基于清洗后的真实订单</p>
<div class="metrics">{card_html}</div>
{''.join(plots)}
<section><h2>热门出行路线 Top 20</h2>
<table><thead><tr><th>上车区域</th><th>下车区域</th><th>订单数量</th></tr></thead><tbody>{route_rows}</tbody></table></section>
<footer>数据来源：NYC TLC。数据经过阶段二预处理；统计范围为 2024 年 1 月，并非实时数据。</footer>
</main></body></html>'''
output = REPORTS / "stage3_dashboard.html"
output.write_text(html_doc, encoding="utf-8")
print("STAGE3_DASHBOARD_SUCCESS")
print("REPORT_PATH =", output)
print("HTML_SIZE_BYTES =", output.stat().st_size)
