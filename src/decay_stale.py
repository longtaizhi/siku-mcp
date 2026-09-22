#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
decay_stale.py — L3 半衰期衰减执行器（A1-P0 衰减激活 + R3 衰减分级）

背景：memory_store 表 half_life 字段曾 100% 为 permanent → 衰减 0 触发 → 离场机制空转
（经验判例）。R3 起实现分型分级：低层/事实/事件类默认 half_life='30d'，
洞察/想法类 90d，高价值沉淀类 permanent，每周扫描一次对超期条目做 confidence 指数降权
（只降权不删除）。

功能：
  0. --grade 衰减分级（R3）：按类型分级表幂等回填 half_life（--dry-run 预演）
  1. 扫描 memory_store 中 half_life 非 permanent 且超期（timestamp 距今 > half_life）的条目
  2. confidence 按半衰期公式降权：conf = base * 0.5^(elapsed_days/half_life_days)，下限 0.05
     - base 取首次衰减时的 confidence，存入 audit_log（event=decay_base），保证幂等
       （同一时间点重复执行结果一致；随时间推移 confidence 单调递减直至下限）
     - 只动 confidence，不动 importance（避免与 importance_scoring 冲突）
  3. confidence < 0.3 → 标记 deprecated：expires_at=now + audit_log 追加 deprecated 事件
     （可检索但语义已废弃，供检索层排序后置）
  4. 每轮输出衰减日志（处理条数/降权明细/新增 deprecated 数）

用法：
  python3 decay_stale.py --grade --dry-run   # 分级预演（只打印不写库）
  python3 decay_stale.py --grade             # 分级回填（幂等，只改 half_life）
  python3 decay_stale.py                      # 实跑衰减
  python3 decay_stale.py --dry-run           # 衰减预演（只打印不写库）
  python3 decay_stale.py --limit N           # 只处理前 N 条超期条目（调试用）

调度（沿用既有四库 crontab，每周一 03:00—— 已挂；pipeline_runner 不加条目
防双入口重复触发，见 pipeline_runner.py L74 先例注释）：
  0 3 * * 1 /usr/bin/python3 $HOME/四库全书/scripts/decay_stale.py >> $HOME/四库全书/logs/decay_stale.log 2>&1

回滚：UPDATE memory_store SET half_life='permanent' WHERE type IN ('record','info','result','fact','insight','idea');
      confidence 恢复用库备份 .bak-memimpl-20260807 / .bak-timepipe-20260828
"""
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone, timedelta

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

import query_cache_invalidate  # S2 M3：衰减写库后清查询缓存（纯 stdlib）

BASE = _SIKU_ROOT
DB_PATH = os.path.join(BASE, "memory_store.db")
TZ = timezone(timedelta(hours=8))

CONFIDENCE_FLOOR = 0.05      # confidence 下限（<0.3 阈值，deprecated 明确可达）
DEPRECATED_THRESHOLD = 0.3   # confidence 低于此值 → 标记 deprecated
DETAIL_MAX = 200             # 明细最大打印条数，超出折叠为计数

# ── R3衰减分级：分型分级表 ─────────────────────────
# 设计决策（可在此调整；宁放过不误杀——未知类型不触碰保持现状）：
#   T1 永久（不衰减）：高价值沉淀类（lesson/principle/instruction/decision/rule/skill/correction）
#       + 项目资产/基建类（reference/spec/design/research/workflow/script/asset/config/
#         cron/monitor/benchmark/estimate）——衰减会伤害长期知识
#   T2 慢衰减（90d）：insight（洞察，中价值）/ idea（想法）
#   T3 标准衰减（30d，方案已批）：record/info/result/fact（低层/事实/事件类——离场主目标）
TIER_PERMANENT = {
    "lesson", "principle", "instruction", "decision", "rule", "skill", "correction",
    "reference", "spec", "design", "research", "workflow", "script", "asset",
    "config", "cron", "monitor", "benchmark", "estimate",
}
TIER_SLOW = {"insight": "90d", "idea": "90d"}
TIER_STD = {"record": "30d", "info": "30d", "result": "30d", "fact": "30d"}
GRADE_AUDIT = os.path.join(BASE, "scripts", "siku_option", "decay_grade_audit.jsonl")


def target_half_life(mtype):
    """类型 → 目标 half_life；未知类型返回 None（不触碰）"""
    if mtype in TIER_PERMANENT:
        return "permanent"
    if mtype in TIER_SLOW:
        return TIER_SLOW[mtype]
    if mtype in TIER_STD:
        return TIER_STD[mtype]
    return None


def grade_half_life(dry_run=False):
    """分级回填：按类型表幂等设置 half_life。返回统计 dict（只写 half_life，不动其余字段）"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()
    cur.execute("SELECT id, type, half_life FROM memory_store")
    rows = cur.fetchall()
    stats = {"scanned": len(rows), "graded": 0, "by_tier": {"permanent": 0, "90d": 0, "30d": 0},
             "unchanged": 0, "unknown_type_skipped": 0, "details": []}
    cur_upd = conn.cursor()
    for r in rows:
        target = target_half_life(r["type"])
        if target is None:
            stats["unknown_type_skipped"] += 1
            continue
        cur_hl = (r["half_life"] or "").strip() or None
        if cur_hl == target:
            stats["unchanged"] += 1
            continue
        if not dry_run:
            cur_upd.execute("UPDATE memory_store SET half_life=?, updated_at=? WHERE id=?",
                            (target, datetime.now(TZ).isoformat(), r["id"]))
        stats["graded"] += 1
        stats["by_tier"][{"permanent": "permanent", "90d": "90d", "30d": "30d"}.get(target, target)] += 1
        if len(stats["details"]) < DETAIL_MAX:
            stats["details"].append("%s | %-10s | %s → %s" % (r["id"][:12], r["type"], cur_hl or "<NULL>", target))
    if not dry_run:
        conn.commit()
        try:
            with open(GRADE_AUDIT, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "op": "decay_grade", "ts": datetime.now(TZ).isoformat(),
                    "graded": stats["graded"], "by_tier": stats["by_tier"],
                    "dry_run": False,
                }, ensure_ascii=False) + "\n")
        except OSError:
            pass
        query_cache_invalidate.invalidate_query_cache()
    conn.close()
    return stats


def parse_half_life(hl):
    """解析 half_life 字符串 → 天数；permanent/非法 → None（不衰减）。支持 30d/8w/6m/2y"""
    if not hl or hl.strip() == "permanent":
        return None
    m = re.fullmatch(r"\s*(\d+)\s*([dwmy])\s*", hl)
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2)
    return {"d": n, "w": n * 7, "m": n * 30, "y": n * 365}[unit]


def elapsed_days(timestamp_iso):
    """timestamp（入库时间）距今的天数，非法值返回 0。

    兼容 Python 3.9 的 fromisoformat 不支持 'Z' 后缀：先归一为 '+00:00'，
    否则带 Z 的旧条目会被静默跳过永不衰减。
    """
    try:
        if timestamp_iso.endswith("Z"):
            timestamp_iso = timestamp_iso[:-1] + "+00:00"
        ts = datetime.fromisoformat(timestamp_iso)
        return max(0, (datetime.now(TZ) - ts).days)
    except (ValueError, TypeError):
        return 0


def half_life_decay(base_conf, days, hl_days):
    """半衰期指数降权：conf = base * 0.5^(days/hl_days)，下限 CONFIDENCE_FLOOR"""
    if days <= 0 or hl_days <= 0:
        return round(max(base_conf, CONFIDENCE_FLOOR), 2)
    conf = base_conf * (0.5 ** (days / hl_days))
    return round(max(conf, CONFIDENCE_FLOOR), 2)


def read_audit_log(row):
    """读取并解析 audit_log JSON 数组；脏数据返回空列表"""
    try:
        data = json.loads(row["audit_log"] or "[]")
        return data if isinstance(data, list) else []
    except (ValueError, TypeError):
        return []


def find_base_conf(audit_log):
    """从 audit_log 找首次衰减 base（event=decay_base）；无则用当前 confidence"""
    for ev in audit_log:
        if isinstance(ev, dict) and ev.get("event") == "decay_base":
            return ev.get("base")
    return None


def scan_candidates(conn, limit=None):
    """扫描 half_life 非 permanent 且超期的条目"""
    cur = conn.cursor()
    cur.execute("SELECT id, type, summary, confidence, half_life, timestamp, expires_at, audit_log "
                "FROM memory_store WHERE half_life IS NOT NULL AND half_life != 'permanent'")
    rows = cur.fetchall()
    candidates = []
    for r in rows:
        hl_days = parse_half_life(r["half_life"])
        if hl_days is None:
            continue
        days = elapsed_days(r["timestamp"])
        if days >= hl_days:  # 超期
            candidates.append((r, hl_days, days))
    if limit:
        candidates = candidates[:limit]
    return candidates


def process_one(r, hl_days, days):
    """单条衰减。返回 (new_conf, audit_log_new, expires_at_new, deprecated_flag, changed)"""
    audit = read_audit_log(r)
    base = find_base_conf(audit)
    new_conf = half_life_decay(base if base is not None else r["confidence"], days, hl_days)
    changed = abs(new_conf - r["confidence"]) >= 0.005
    expires_new = r["expires_at"]
    deprecated = new_conf < DEPRECATED_THRESHOLD

    new_audit = audit
    if base is None:
        # 首次衰减：记录 base，保证后续幂等
        new_audit = audit + [{
            "event": "decay_base", "at": datetime.now(TZ).isoformat(),
            "base": r["confidence"], "half_life": r["half_life"],
        }]
        changed = True
    if deprecated:
        if not any(isinstance(ev, dict) and ev.get("event") == "deprecated" for ev in audit):
            new_audit = new_audit + [{
                "event": "deprecated", "at": datetime.now(TZ).isoformat(),
                "from_conf": r["confidence"], "to_conf": new_conf,
            }]
            expires_new = datetime.now(TZ).isoformat()  # expires_at 作标记字段
            changed = True
    return new_conf, new_audit, expires_new, deprecated, changed


def apply_decay(dry_run=False, limit=None):
    """全量衰减。返回统计 dict"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    candidates = scan_candidates(conn, limit)
    stats = {"scanned": len(candidates), "decayed": 0, "deprecated": 0, "details": []}

    cur = conn.cursor()
    for r, hl_days, days in candidates:
        new_conf, new_audit, expires_new, deprecated, changed = process_one(r, hl_days, days)
        if not changed:
            continue
        if not dry_run:
            cur.execute(
                "UPDATE memory_store SET confidence=?, audit_log=?, expires_at=?, updated_at=? WHERE id=?",
                (new_conf, json.dumps(new_audit, ensure_ascii=False), expires_new,
                 datetime.now(TZ).isoformat(), r["id"]),
            )
        stats["decayed"] += 1
        if deprecated:
            stats["deprecated"] += 1
        if len(stats["details"]) < DETAIL_MAX:
            stats["details"].append(
                f"{r['id'][:12]} | {r['type']:8s} | conf {r['confidence']}→{new_conf}"
                f"{' | DEPRECATED' if deprecated else ''} | {r['half_life']} | {(r['summary'] or '')[:28]}"
            )

    if not dry_run:
        conn.commit()
        query_cache_invalidate.invalidate_query_cache()
    conn.close()
    return stats


def print_report(stats, dry_run):
    tag = "[DRY-RUN] " if dry_run else ""
    print(f"{tag}== 衰减执行 ==")
    print(f"{tag}超期条目: {stats['scanned']} | 降权: {stats['decayed']} | 新增 deprecated: {stats['deprecated']}")
    for line in stats["details"]:
        print(f"{tag}  {line}")
    overflow = stats["decayed"] - len(stats["details"])
    if overflow > 0:
        print(f"{tag}  ... 其余 {overflow} 条明细已折叠（日志上限 {DETAIL_MAX} 条）")
    print(f"{tag}完成（exit 0）")


def print_grade_report(stats, dry_run):
    tag = "[DRY-RUN] " if dry_run else ""
    print(f"{tag}== 衰减分级（half_life 回填）==")
    print(f"{tag}扫描: {stats['scanned']} | 分级回填: {stats['graded']} | 不变: {stats['unchanged']} | 未知类型跳过: {stats['unknown_type_skipped']}")
    print(f"{tag}by_tier: permanent+{stats['by_tier']['permanent']} | 90d+{stats['by_tier']['90d']} | 30d+{stats['by_tier']['30d']}")
    for line in stats["details"]:
        print(f"{tag}  {line}")
    overflow = stats["graded"] - len(stats["details"])
    if overflow > 0:
        print(f"{tag}  ... 其余 {overflow} 条明细已折叠（上限 {DETAIL_MAX} 条）")
    print(f"{tag}分级完成（幂等：重跑 graded=0）")


def main():
    dry_run = "--dry-run" in sys.argv
    if "--grade" in sys.argv:
        stats = grade_half_life(dry_run=dry_run)
        print_grade_report(stats, dry_run)
        return 0
    limit = None
    if "--limit" in sys.argv:
        try:
            limit = int(sys.argv[sys.argv.index("--limit") + 1])
        except (ValueError, IndexError):
            print("--limit 需要数字", file=sys.stderr)
            return 2
    stats = apply_decay(dry_run=dry_run, limit=limit)
    print_report(stats, dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
