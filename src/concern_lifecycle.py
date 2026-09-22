#!/usr/bin/env python3
"""
L3 Concern 冲突仲裁 + 活性检测 — 2026-07-08

用法:
  python3 concern_lifecycle.py audit       # 检查仲裁队列
  python3 concern_lifecycle.py activity    # 更新活性（每日一次，对齐L4b蒸馏）
  python3 concern_lifecycle.py status      # 查看状态
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from collections import defaultdict

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

SIKU_BASE = _SIKU_ROOT
DB = os.path.join(SIKU_BASE, "memory_store.db")
ACTIVITY_CACHE = os.path.join(SIKU_BASE, ".concern_activity_cache.json")
ARBITRATION_QUEUE = os.path.join(SIKU_BASE, ".concern_arbitration_queue.json")
CONFLICT_THRESHOLD = 5
ACTIVITY_WINDOW_DAYS = 7
ACTIVITY_CACHE_SIZE = 200

LOG_DIR = os.path.join(SIKU_BASE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def log(msg):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] [CONCERN] {msg}"
    with open(os.path.join(LOG_DIR, "concern_lifecycle.log"), "a") as f:
        f.write(line + "\n")
    print(line)


def atomic_write(path, data):
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except:
        os.unlink(tmp)
        raise


def atomic_read(path, default=None):
    if not os.path.exists(path):
        return default or {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return default or {}


def cmd_audit(args):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    pairs = conn.execute("""
        SELECT id1, id2, COUNT(*) as cnt
        FROM graph_edges
        WHERE relation = 'conflicts'
        GROUP BY id1, id2
        HAVING cnt >= ?
    """, (CONFLICT_THRESHOLD,)).fetchall()
    conn.close()

    queue = atomic_read(ARBITRATION_QUEUE, [])
    if not isinstance(queue, list):
        queue = []
    existing = set((q["id1"], q["id2"]) for q in queue)

    new_arbitrations = 0
    for p in pairs:
        key = (p["id1"], p["id2"])
        if key not in existing:
            queue.append({
                "id1": p["id1"],
                "id2": p["id2"],
                "conflict_count": p["cnt"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "status": "pending"
            })
            new_arbitrations += 1

    if new_arbitrations > 0:
        atomic_write(ARBITRATION_QUEUE, queue)
        log(f"冲突仲裁: {new_arbitrations} 对新触发仲裁（阈值={CONFLICT_THRESHOLD}）")
    else:
        log("冲突仲裁: 无新触发")

    pending = [q for q in queue if q["status"] == "pending"]
    log(f"仲裁队列: {len(pending)} 对待审, {len(queue)} 累计")

    result = {"new_arbitrations": new_arbitrations, "pending": len(pending), "total": len(queue)}
    print(json.dumps(result, indent=2))
    return result


def cmd_activity(args):
    cache = atomic_read(ACTIVITY_CACHE, {})
    if not isinstance(cache, dict):
        cache = {}

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    days_str = f"-{ACTIVITY_WINDOW_DAYS} days"
    rows = conn.execute("""
        SELECT concern_id, COUNT(*) as hit_count
        FROM memory_store
        WHERE concern_id IS NOT NULL
          AND concern_id != 'unclassified'
          AND created_at >= datetime('now', ?)
        GROUP BY concern_id
    """, (days_str,)).fetchall()
    if not rows:
        rows = conn.execute("""
            SELECT concern_id, COUNT(*) as hit_count
            FROM memory_store
            WHERE concern_id IS NOT NULL
              AND concern_id != 'unclassified'
            GROUP BY concern_id
        """).fetchall()
    conn.close()

    now = datetime.now(timezone.utc).isoformat()

    for r in rows:
        cid = r["concern_id"]
        hits = r["hit_count"]
        if hits >= 3:
            activity = 1.0
        elif hits >= 1:
            activity = 0.7
        else:
            activity = 0.5

        cache[cid] = {
            "activity": activity,
            "hits_7d": hits,
            "updated_at": now
        }

        if len(cache) > ACTIVITY_CACHE_SIZE:
            sorted_items = sorted(cache.items(), key=lambda x: x[1].get("updated_at", ""))
            for k, _ in sorted_items[:len(cache) - ACTIVITY_CACHE_SIZE]:
                del cache[k]

    atomic_write(ACTIVITY_CACHE, cache)
    log(f"活性更新: {len(rows)} 个concern已更新（7天窗口）")

    result = {"updated": len(rows), "cache_size": len(cache)}
    print(json.dumps(result, indent=2))
    return result


def cmd_status(args):
    queue = atomic_read(ARBITRATION_QUEUE, [])
    if not isinstance(queue, list):
        queue = []
    pending = [q for q in queue if q["status"] == "pending"]
    resolved = [q for q in queue if q["status"] == "resolved"]

    cache = atomic_read(ACTIVITY_CACHE, {})
    if not isinstance(cache, dict):
        cache = {}

    print(f"=== Concern 生命周期 ===")
    print(f"仲裁队列: {len(pending)} 待审, {len(resolved)} 已决, {len(queue)} 累计")
    print(f"活性缓存: {len(cache)}/{ACTIVITY_CACHE_SIZE}")
    print()
    print("--- 活性概要 ---")
    for cid, info in sorted(cache.items()):
        print(f"  {cid}: activity={info['activity']}, hits_7d={info.get('hits_7d', '?')}")
    print()
    print("--- 待仲裁 ---")
    for q in pending[:5]:
        print(f"  {q['id1'][:8]}... ↔ {q['id2'][:8]}... (x{q['conflict_count']})")

    return {"pending": len(pending), "resolved": len(resolved), "cache_size": len(cache)}


def main():
    parser = argparse.ArgumentParser(description="Concern生命周期管理")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("audit", help="检查冲突仲裁队列")
    sub.add_parser("activity", help="更新concern活性（每日一次）")
    sub.add_parser("status", help="查看状态")
    args = parser.parse_args()

    if args.command == "audit":
        cmd_audit(args)
    elif args.command == "activity":
        cmd_activity(args)
    elif args.command == "status":
        cmd_status(args)


if __name__ == "__main__":
    main()
