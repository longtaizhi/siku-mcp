#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
l3_usage_feedback.py — A3-P1 使用反馈强化（memory_access_log → L3 importance 回写）

目标：检索使用反馈闭环。聚合 memory_access_log（90 天窗口，与 log_access TTL 一致），
把 usage_count + recency 折算成 importance 微调回写 memory_store。

设计约束：
- 零加列：复用 memory_store 既有 importance 字段（INTEGER 1-10，默认 5）
- 只加不减：回写只提升 importance，不降低（ceil 归一化 + boost ≥ 0）
- 上限钳制：importance 钳制在 [1,10]，越界脏值一并修复
- 幂等：可重复执行，importance 不变的行不写
- 写后清 query_cache（复用 query_cache_invalidate.py，机制已有）
- 纯 stdlib，不 import l3_retrieval 等重模块（背 jieba/chromadb）

调度：pipeline_runner.py SCHEDULE 新增 clock 条目（daily 06:40）：
    {"name": "usage-feedback", "type": "clock", "times": ["06:40"],
     "script": "l3_usage_feedback.py", "args": [], "desc": "使用反馈回写(每日)"}

用法:
  python3 l3_usage_feedback.py            # 实跑
  python3 l3_usage_feedback.py --dry-run  # 只预览不写
"""
import os
import sys
import json
import sqlite3
import math
from datetime import datetime, timedelta

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

BASE = _SIKU_ROOT
DB_PATH = os.path.join(BASE, "memory_store.db")
LOG_PATH = os.path.join(BASE, "logs", "l3_usage_feedback.log")
WINDOW_DAYS = 90          # 与 log_access TTL 一致（l3_retrieval.py log_access 注释）
IMPORTANCE_MIN = 1
IMPORTANCE_MAX = 10
DEFAULT_IMPORTANCE = 5


def perm_audit(layer):
    """L3 写路径权限审计（audit 模式只记录不拦截，与 importance_scoring.py 一致）"""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    try:
        import check_permission as cp
        cp.check(os.environ.get("SIKU_AGENT", "用户"), "write", layer, mode="audit")
    except Exception as e:
        print(f"  (perm_audit skipped: {e})")


def now_iso():
    return datetime.now().isoformat()


def log_line(msg):
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"[{now_iso()}] {msg}\n")
    except Exception:
        pass


def normalize_importance(cur):
    """归一化 importance：非数值→默认5；钳制 [1,10]；ceil 取整（保证只加不减）。"""
    try:
        cur = float(cur)
    except (TypeError, ValueError):
        return DEFAULT_IMPORTANCE
    if math.isnan(cur) or math.isinf(cur):
        return DEFAULT_IMPORTANCE
    return max(IMPORTANCE_MIN, min(IMPORTANCE_MAX, math.ceil(cur)))


def compute_target(usage_count, days_since_last):
    """usage_count + recency → importance 目标值（只加不减锚点）。

    幂等关键：target 只由 usage_count 累计窗口和 recency 决定，
    new = max(cur, target) —— 追上即停，不重复叠加。usage 随时间增长时
    target 单调不降，cur 追平后不再写（幂等）；usage 继续涨则继续抬。
    """
    target = DEFAULT_IMPORTANCE  # 5
    if usage_count >= 20:
        target += 4
    elif usage_count >= 10:
        target += 3
    elif usage_count >= 5:
        target += 2
    elif usage_count >= 2:
        target += 1
    # usage == 1 且不近期：不提升（避免一次性访问就永久加权）
    if days_since_last <= 7:
        target += 1
    return min(IMPORTANCE_MAX, target)


def collect_access_stats(conn):
    """聚合 90 天窗口 access log → {entry_id: {"usage_count": N, "days_since_last": D}}"""
    cutoff = (datetime.now() - timedelta(days=WINDOW_DAYS)).isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT entry_id,
               COUNT(*) AS usage_count,
               MAX(accessed_at) AS last_at
        FROM memory_access_log
        WHERE accessed_at >= ?
        GROUP BY entry_id
        """,
        (cutoff,),
    )
    stats = {}
    for eid, usage_count, last_at in cur.fetchall():
        days = 999
        if last_at:
            try:
                days = max(0, (datetime.now() - datetime.fromisoformat(last_at)).days)
            except (ValueError, TypeError):
                days = 999
        stats[eid] = {"usage_count": usage_count, "days_since_last": days}
    return stats


def run(dry_run=False):
    perm_audit("L3")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    cur = conn.cursor()

    stats = collect_access_stats(conn)
    if not stats:
        print(json.dumps({"dry_run": dry_run, "updated": 0, "total_entries": 0,
                          "detail": "no access log in window"}, ensure_ascii=False))
        conn.close()
        return 0

    # join memory_store 取当前 importance（跳过孤儿 entry_id）
    placeholders = ",".join("?" * len(stats))
    cur.execute(f"SELECT id, importance FROM memory_store WHERE id IN ({placeholders})",
                list(stats.keys()))
    rows = {r["id"]: r["importance"] for r in cur.fetchall()}

    updated = 0
    unchanged = 0
    skipped_orphan = 0
    boosted = 0
    clamped = 0
    changes = []

    for eid, stat in stats.items():
        if eid not in rows:
            skipped_orphan += 1
            continue
        cur_imp = rows[eid]
        norm = normalize_importance(cur_imp)
        target = compute_target(stat["usage_count"], stat["days_since_last"])
        new_imp = max(norm, target)   # 只加不减 + 目标锚定（幂等）
        if new_imp == cur_imp:        # 幂等：无变化不写
            unchanged += 1
            continue
        if new_imp > norm:
            boosted += 1
        if new_imp >= IMPORTANCE_MAX and cur_imp < IMPORTANCE_MAX:
            clamped += 1
        changes.append((eid, cur_imp, new_imp, stat["usage_count"], stat["days_since_last"]))
        if not dry_run:
            cur.execute(
                "UPDATE memory_store SET importance=?, updated_at=? WHERE id=?",
                (new_imp, now_iso(), eid),
            )
        updated += 1

    if not dry_run:
        conn.commit()

    # 全库钳制：修复历史越界/非整数 importance（只提升不降低，符合只加不减）
    # 背景：2026-08-01 批量写入曾产生 0.7~0.9 浮点脏值（下限越界），本步兜底归一到 [1,10]
    fix_updated = 0
    if not dry_run:
        cur.execute("SELECT id, importance FROM memory_store")
        for r in cur.fetchall():
            norm = normalize_importance(r["importance"])
            if norm != r["importance"]:
                cur.execute(
                    "UPDATE memory_store SET importance=?, updated_at=? WHERE id=?",
                    (norm, now_iso(), r["id"]),
                )
                fix_updated += 1
        if fix_updated:
            conn.commit()

    # 回写后清 query_cache（机制已有）
    cache_cleared = False
    if not dry_run and (updated > 0 or fix_updated > 0):
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            import query_cache_invalidate as qci
            cache_cleared = qci.invalidate_query_cache()
        except Exception as e:
            log_line(f"query_cache invalidate failed: {e}")
            cache_cleared = False

    conn.close()

    # 越界检查（dry-run 也用当前库值粗验）
    result = {
        "dry_run": dry_run,
        "updated": updated,
        "fix_normalized": fix_updated,
        "unchanged": unchanged,
        "skipped_orphan": skipped_orphan,
        "boosted": boosted,
        "clamped_to_max": clamped,
        "window_days": WINDOW_DAYS,
        "cache_cleared": cache_cleared,
    }
    if updated > 0:
        result["top_changes"] = [
            {"entry_id": c[0], "importance": f"{c[1]}→{c[2]}",
             "usage_count": c[3], "days_since_last": c[4]}
            for c in changes[:5]
        ]
    log_line(json.dumps(result, ensure_ascii=False))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    sys.exit(run(dry_run=dry_run))
