#!/usr/bin/env python3
"""
5-D: 注入效果分析仪表盘
输入: audit/write/ 下的审计日志
输出: HTML报告 → $SIKU_ROOT/reports/
"""
import json, os, re
from datetime import datetime, timedelta
from collections import defaultdict

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

BASE = _SIKU_ROOT
AUDIT_DIR = os.path.join(BASE, "audit", "write")
REPORT_DIR = os.path.join(BASE, "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

def parse_audit_logs():
    """解析所有审计日志，提取注入事件"""
    events = []
    if not os.path.isdir(AUDIT_DIR):
        return events

    for fname in sorted(os.listdir(AUDIT_DIR)):
        if not fname.endswith(".yaml"):
            continue
        fpath = os.path.join(AUDIT_DIR, fname)
        with open(fpath) as f:
            content = f.read()
        # 解析YAML块
        blocks = re.split(r'\n---\n', content)
        for block in blocks:
            lines = block.strip().split('\n')
            event = {}
            for line in lines:
                if ':' in line:
                    k, v = line.split(':', 1)
                    event[k.strip()] = v.strip().strip('"').strip("'")
            if event.get("op_type") in ("g2_label", "g3_verify", "write", "inject"):
                events.append(event)
    return events

def generate_html(events):
    """生成仪表盘HTML"""
    total = len(events)
    passes = sum(1 for e in events if e.get("op_type") == "g3_verify" and "fuse: no" in str(e))
    fails = sum(1 for e in events if e.get("fuse") == "yes")
    
    # 按Agent分组
    by_agent = defaultdict(list)
    for e in events:
        agent = e.get("operator", e.get("op", "unknown"))
        by_agent[agent].append(e)

    # 近7天趋势
    now = datetime.now()
    seven_days = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    daily_counts = defaultdict(int)
    for e in events:
        ts = e.get("ts", e.get("timestamp", ""))[:10]
        if ts in seven_days:
            daily_counts[ts] += 1

    # 最低分条目Top-5
    scored = []
    for e in events:
        try:
            rate = float(e.get("rate", 1.0))
        except (ValueError, TypeError):
            rate = 1.0
        scored.append((rate, e))
    scored.sort(key=lambda x: x[0])
    bottom5 = scored[:5]

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="utf-8">
<title>四库系统 - 注入效果仪表盘</title>
<style>
  body {{ font-family: -apple-system, sans-serif; max-width: 960px; margin: 0 auto; padding: 20px; background: #f5f5f7; }}
  h1 {{ color: #1d1d1f; }}
  .card {{ background: white; border-radius: 12px; padding: 16px; margin: 12px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .stat {{ display: inline-block; text-align: center; margin: 0 20px; }}
  .stat-value {{ font-size: 2em; font-weight: bold; }}
  .stat-label {{ color: #86868b; font-size: 0.85em; }}
  .bar {{ height: 20px; border-radius: 10px; background: #e9e9ed; margin: 4px 0; }}
  .bar-fill {{ height: 100%; border-radius: 10px; background: #34c759; }}
  .bar-fill.warn {{ background: #ff9f0a; }}
  .bar-fill.danger {{ background: #ff3b30; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #e9e9ed; }}
  th {{ color: #86868b; font-weight: 500; }}
  .ok {{ color: #34c759; }} .warn {{ color: #ff9f0a; }} .fail {{ color: #ff3b30; }}
  .trend {{ display: flex; gap: 4px; align-items: end; height: 80px; padding: 10px 0; }}
  .trend-bar {{ flex: 1; background: #007aff; border-radius: 4px 4px 0 0; min-height: 4px; position: relative; }}
  .trend-label {{ text-align: center; font-size: 0.75em; color: #86868b; margin-top: 4px; }}
</style>
</head><body>
<h1>📊 四库系统 — 注入效果分析</h1>
<p style="color:#86868b">生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

<div class="card">
  <div class="stat"><div class="stat-value">{total}</div><div class="stat-label">总事件</div></div>
  <div class="stat"><div class="stat-value">{passes}</div><div class="stat-label">通过</div></div>
  <div class="stat"><div class="stat-value">{fails}</div><div class="stat-label">熔断</div></div>
  <div class="stat"><div class="stat-value">{'%.0f%%' % (passes/total*100 if total else 0)}</div><div class="stat-label">通过率</div></div>
</div>

<div class="card">
  <h3>按Agent分组</h3>
  <table>
    <tr><th>Agent</th><th>事件数</th><th>通过率</th><th>进度</th></tr>"""

    for agent, evts in sorted(by_agent.items()):
        agent_pass = sum(1 for e in evts if e.get("fuse") == "no")
        rate = agent_pass / len(evts) * 100 if evts else 0
        bar_class = "bar-fill" if rate >= 80 else ("bar-fill warn" if rate >= 50 else "bar-fill danger")
        html += f"""
    <tr>
      <td>{agent}</td>
      <td>{len(evts)}</td>
      <td class="{'ok' if rate>=80 else 'warn' if rate>=50 else 'fail'}">{'%.0f' % rate}%</td>
      <td><div class="bar"><div class="{bar_class}" style="width:{'%.0f' % rate}%"></div></div></td>
    </tr>"""

    html += """</table></div>

<div class="card">
  <h3>近7天趋势</h3>
  <div class="trend">"""
    max_count = max(daily_counts.values()) if daily_counts else 1
    for day in seven_days:
        cnt = daily_counts.get(day, 0)
        height = max(4, int(cnt / max_count * 70)) if max_count else 4
        html += f"""
    <div style="flex:1;text-align:center">
      <div class="trend-bar" style="height:{height}px" title="{day}: {cnt}条"></div>
      <div class="trend-label">{day[-5:]}</div>
    </div>"""

    html += """</div></div>

<div class="card">
  <h3>最低分条目 Top-5</h3>
  <table><tr><th>排名</th><th>操作</th><th>Agent</th><th>通过率</th></tr>"""
    for i, (rate, e) in enumerate(bottom5, 1):
        op = e.get("op_type", e.get("operation", "?"))
        agent = e.get("operator", e.get("op", "?"))
        html += f"""<tr><td>{i}</td><td>{op}</td><td>{agent}</td><td class="fail">{'%.0f' % (rate*100)}%</td></tr>"""

    html += """</table></div>
<p style="color:#86868b;font-size:0.85em">四库系统 v4.1 | 每日自动生成</p>
</body></html>"""
    return html

def main():
    events = parse_audit_logs()
    html = generate_html(events)
    fname = f"dashboard-{datetime.now().strftime('%Y%m%d-%H%M%S')}.html"
    fpath = os.path.join(REPORT_DIR, fname)
    with open(fpath, "w") as f:
        f.write(html)
    print(f"✅ 仪表盘已生成: {fpath}")
    print(f"   事件数: {len(events)}, 通过率: {'%.0f' % (sum(1 for e in events if e.get('fuse')=='no')/len(events)*100 if events else 0)}%")
    # 同时生成 latest.html
    latest = os.path.join(REPORT_DIR, "latest.html")
    with open(latest, "w") as f:
        f.write(html)
    print(f"   latest.html 已更新")

if __name__ == "__main__":
    main()
