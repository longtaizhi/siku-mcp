#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""extract_temporal_batch.py — 事件时间提取批处理驱动脚本

对 AMB temporal 133 题的记忆（source_ref 前缀 amb:longmemeval:<qid>）提取事件时间，
force 覆盖写 event_date（替换 doc_ts 直填值）——B″ 复测前置。

红线：LLM 触发率 ≤30%（extract_batch 内建预算）；本地 8081（云端零调用）；
写库前备份已在 event_date_backup_longmemeval_20260828.jsonl（可回滚）。
用法：python3 extract_temporal_batch.py [--limit N] [--no-llm] [--write]
"""
import json
import os
import sqlite3
import sys

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import extract_event_time as eet

DB = os.environ.get("SIKU_TEMPORAL_DB", os.path.join(_SIKU_ROOT, "sandbox", "memory_store_amb.db"))
QS = os.environ.get("SIKU_TEMPORAL_QS", os.path.join(_SIKU_ROOT, "sandbox", "r2_questions.json"))
AUDIT = os.environ.get("SIKU_TEMPORAL_AUDIT", os.path.join(_SIKU_ROOT, "scripts", "siku_option", "extract_temporal_audit.jsonl"))


def temporal_qids():
    qs = json.load(open(QS))
    return [q["qid"] for q in qs if q["type"] == "temporal-reasoning"]


def load_temporal_rows(limit=None):
    """加载 temporal 题全部记忆（force：含已有 event_date——覆盖写）。"""
    qids = temporal_qids()
    conn = sqlite3.connect(DB, timeout=120)
    conn.execute("PRAGMA busy_timeout=120000")
    # 每题一个前缀 OR —— 拼 IN 风格
    rows = []
    for qid in qids:
        rs = conn.execute(
            "SELECT id, summary, content, doc_ts FROM memories WHERE source_ref LIKE ?",
            ("amb:longmemeval:" + qid + "%",)).fetchall()
        rows.extend(rs)
    conn.close()
    if limit:
        rows = rows[:limit]
    return rows, qids


def main():
    write = "--write" in sys.argv
    no_llm = "--no-llm" in sys.argv
    limit = None
    for a in sys.argv[1:]:
        if a.startswith("--limit="):
            limit = int(a.split("=")[1])
    rows, qids = load_temporal_rows(limit)
    print("temporal 题=%d 记忆=%d 覆盖写=%s llm=%s" % (len(qids), len(rows), write, "关" if no_llm else "开"))

    def prog(i, n, st):
        print("  进度 %d/%d 规则=%d LLM调用=%d 产出=%d" % (
            i, n, st["rule"], st["llm_called"], st["written"]), flush=True)

    out, stats = eet.extract_batch(rows, use_llm=not no_llm, llm_max_ratio=eet.LLM_MAX_RATIO,
                                   on_progress=prog)
    n_written = [r for r in out if r["event_date"]]
    print("提取完成: 无信号=%d 规则=%d LLM调用=%d(触发率=%.1f%%≤30) 高置信产出=%d" % (
        stats["no_signal"], stats["rule"], stats["llm_called"],
        stats["llm_called"] / len(rows) * 100, stats["written"]))

    # 抽样核
    import random
    random.seed(20260827)
    sample = random.sample(n_written, min(30, len(n_written)))
    idx_by_id = {r[0]: r for r in rows}
    print("\n=== 抽样核 %d 条（池 %d）===" % (len(sample), len(n_written)))
    ok = 0
    for s in sample:
        src = idx_by_id.get(s["id"], (None, "", "", ""))
        snip = (src[1] or src[2] or "")[:90].replace("\n", " ")
        print("  %s | %s->%s [%s conf=%.2f] | %s" % (
            s["id"][:16], s["method"], s["event_date"], s["method"], s["conf"], snip))
    print("\n注：以上为自动抽样展示——人工核验准确率需逐条对照 content 判定（验收项）。")

    if write:
        conn = sqlite3.connect(DB, timeout=120)
        conn.execute("PRAGMA busy_timeout=120000")
        n_upd = eet.write_event_dates(conn, "amb", n_written, AUDIT)
        filled = conn.execute("SELECT COUNT(*) FROM memories WHERE event_date IS NOT NULL AND event_date!=''").fetchone()[0]
        print("\n已写 %d 条 → event_date 非空 %d/%d (%.1f%%)" % (
            n_upd, filled,
            conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0],
            filled / conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0] * 100))
        conn.close()
    else:
        print("\n[dry-run] 未写库。--write 才写。")


if __name__ == "__main__":
    main()
