#!/usr/bin/env python3
"""
四库系统速查卡 — 每日自动更新（07:30）
动态刷新：脚本数量、记忆库条数、图谱边数、哨兵状态、快照信息
"""
import json, os, sqlite3, subprocess
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

BASE = _SIKU_ROOT
DB = os.path.join(BASE, "memory_store.db")
CHEATSHEET = os.path.join(BASE, "四库系统速查卡.md")

def get_db_stats():
    conn = sqlite3.connect(DB)
    total = conn.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
    by_type = conn.execute("SELECT type, COUNT(*) as c FROM memory_store GROUP BY type ORDER BY c DESC").fetchall()
    latest = conn.execute("SELECT type, summary, created_at FROM memory_store ORDER BY created_at DESC LIMIT 3").fetchall()
    edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0] if total > 0 else 0
    conn.close()
    return total, dict(by_type), latest, edges

def get_script_count():
    scripts_dir = os.path.join(BASE, "scripts")
    return len([f for f in os.listdir(scripts_dir) if f.endswith(".py")]) if os.path.isdir(scripts_dir) else 0

def get_latest_snapshot():
    snap_dir = os.path.join(BASE, "smart", "snapshots")
    if not os.path.isdir(snap_dir):
        return None
    snaps = sorted([f for f in os.listdir(snap_dir) if f.endswith(".json")])
    return snaps[-1] if snaps else None

def get_sentinel_status():
    log_dir = os.path.join(BASE, "logs")
    if not os.path.isdir(log_dir):
        return "无数据"
    reports = sorted([f for f in os.listdir(log_dir) if f.startswith("sentinel-") and f.endswith(".json")])
    if not reports:
        return "无记录"
    latest = reports[-1]
    with open(os.path.join(log_dir, latest)) as f:
        report = json.load(f)
    status = report.get("status", "unknown")
    passed = sum(1 for r in report.get("results", []) if r.get("ok"))
    return f"{status.upper()} ({passed}/6)"

def update():
    total, by_type, latest, edges = get_db_stats()
    scripts = get_script_count()
    snapshot = get_latest_snapshot()
    sentinel = get_sentinel_status()
    today = datetime.now().strftime("%Y-%m-%d %H:%M")

    if not os.path.exists(CHEATSHEET):
        print(f"速查卡不存在: {CHEATSHEET}")
        return

    with open(CHEATSHEET, "r") as f:
        content = f.read()

    # 替换动态数据标记（如果有），否则追加状态块
    status_block = f"""---

## 实时状态（自动更新于 {today}）

| 指标 | 当前值 |
|:-----|:------|
| 脚本数 | {scripts} 个 |
| 记忆库 | {total} 条 |
| 知识图谱 | {edges} 条边 |
| 哨兵状态 | {sentinel} |
| 最新快照 | {snapshot or "无"} |
| 内存分布 | {', '.join(f'{t}={c}' for t, c in sorted(by_type.items(), key=lambda x: -x[1])[:5])} |

"""

    # 查找并替换旧的状态块，或追加
    if "## 实时状态" in content:
        parts = content.split("## 实时状态")
        content = parts[0] + status_block
        # 如果后面还有其他内容，保留
        if len(parts) > 1 and "\n## " in parts[1]:
            after = parts[1].split("\n## ", 1)
            content += "\n## " + after[1]
    else:
        content += status_block

    with open(CHEATSHEET, "w") as f:
        f.write(content)

    print(f"✅ 速查卡已更新: {today}")
    print(f"   脚本:{scripts} 记忆:{total} 图谱:{edges} 哨兵:{sentinel}")

if __name__ == "__main__":
    update()
