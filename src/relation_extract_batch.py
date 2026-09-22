#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""relation_extract_batch.py — 维护者：relation_extract 生产接入批处理

应开尽开生产接入——增量分批后台跑——graph_edges 写入——防卡死+断点续跑。

设计（对齐 relation_extract.py 契约）：
 ① 增量：只处理 memory_store.relations 列为空/未留痕的条目（已处理自动跳过）
 ② 分批：500 条/批循环，批间 commit，不阻塞其他写入（timeout=120 连接）
 ③ 断点续跑：relations 列 JSON 留痕即断点——有产出写抽取结果，无产出写空留痕；
    中断后重跑自动从断点继续
 ④ 防卡死：批级墙钟预算（默认 300s，超时记 batch_skip 不中断整体）；
    单条 try/except 记 err 跳过；纯规则 0token（use_llm=False——无网络、
    不依赖 8081、云端零调用红线）
 ⑤ 幂等：INSERT OR REPLACE + 主键(id1,id2,relation)——重复跑不产生重复边
 ⑥ 伪边铁律：写边前置 validate_edge_relation（supports/same_type 拒绝计数留痕）

用法：
  python3 relation_extract_batch.py --db <库> --audit <jsonl>          # 生产增量（后台）
  python3 relation_extract_batch.py --db <库> --dry-run --max-batches 1  # 演练 1 批
  python3 relation_extract_batch.py --db <库> --batch-size 200          # 自定义批大小
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import relation_extract as re  # 复用 extract_one/write_edges/write_relations_col/_conn

DB_DEFAULT = re.DB_DEFAULT
AUDIT_DEFAULT = re.AUDIT_DEFAULT
PROGRESS_DEFAULT = os.path.join(_SIKU_ROOT, "scripts/siku_option/relation_extract_batch_progress.jsonl")


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_pending(conn, batch_size):
    """增量：取 relations 列未留痕的待处理条目（断点续跑核心）。

    排除 id 无效条目（NULL/''——TEXT PRIMARY KEY 允许 NULL 的数据脏，
    UPDATE WHERE id=NULL 恒 0 行→无法留痕→死循环源，2026-08-28 实证）。
    """
    return conn.execute(
        "SELECT id, summary, content FROM memory_store "
        "WHERE ((content IS NOT NULL AND content != '') "
        "OR (summary IS NOT NULL AND summary != '')) "
        "AND (relations IS NULL OR relations = '' OR relations = '[]') "
        "AND id IS NOT NULL AND id != '' "
        "LIMIT ?", (batch_size,)).fetchall()


def mark_done(conn, rid, relations, events, ts):
    """断点留痕：有产出写抽取结果，无产出写空留痕（格式与 write_relations_col 一致）。"""
    payload = json.dumps({"relations": relations, "events": events,
                          "updated_at": ts}, ensure_ascii=False)
    conn.execute("UPDATE memory_store SET relations=? WHERE id=?", (payload, rid))


def main():
    ap = argparse.ArgumentParser(description="relation_extract 生产批处理（增量/断点/防卡死）")
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--audit", default=AUDIT_DEFAULT)
    ap.add_argument("--progress", default=PROGRESS_DEFAULT)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--batch-timeout", type=int, default=300,
                    help="单批墙钟预算（秒），超时记 batch_skip 继续下一批（防卡死）")
    ap.add_argument("--max-batches", type=int, default=None, help="最多处理批数（演练用）")
    ap.add_argument("--dry-run", action="store_true", help="演练：抽取但不写库不留痕")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print("库不存在: %s" % args.db, file=sys.stderr)
        sys.exit(1)

    conn = re._conn(args.db)
    t0 = time.time()
    total_rows = total_edges = total_rejected = total_skip = total_err = 0
    n_batches = 0
    last_pending_n = None
    stall_rounds = 0
    pending_first = conn.execute(
        "SELECT COUNT(*) FROM memory_store "
        "WHERE ((content IS NOT NULL AND content != '') "
        "OR (summary IS NOT NULL AND summary != '')) "
        "AND (relations IS NULL OR relations = '' OR relations = '[]')"
    ).fetchone()[0]
    print("[batch] db=%s 待处理=%d 批大小=%d 批超时=%ds dry_run=%s" %
          (args.db, pending_first, args.batch_size, args.batch_timeout, args.dry_run),
          flush=True)

    while True:
        rows = load_pending(conn, args.batch_size)
        if not rows:
            print("[batch] 无剩余待处理条目——完成", flush=True)
            break
        if args.max_batches and n_batches >= args.max_batches:
            print("[batch] 达 max_batches=%d 演练停止" % args.max_batches, flush=True)
            break
        # 防死循环保险丝：连续 3 轮待处理数无进展 → 报错退出（2026-08-28 死循环实证）
        if len(rows) == last_pending_n:
            stall_rounds += 1
            if stall_rounds >= 3:
                print("[fatal] 连续 %d 轮待处理数不变（%d 条）——疑似死循环，退出。"
                      "检查 id 有效性/留痕 UPDATE 是否生效。" % (stall_rounds, len(rows)),
                      file=sys.stderr, flush=True)
                sys.exit(2)
        else:
            stall_rounds = 0
        last_pending_n = len(rows)
        n_batches += 1
        b_start = time.time()
        b_rows = b_edges = b_rejected = b_skip = b_err = 0
        b_have = 0
        ts = _now_iso()
        print("[batch#%d] 本批 %d 条" % (n_batches, len(rows)), flush=True)
        for rid, summary, content in rows:
            # 防卡死：批级墙钟预算（超时→本批剩余跳过，不中断整体）
            if time.time() - b_start > args.batch_timeout:
                b_skip += len(rows) - b_rows
                print("[batch#%d] 批超时预算 %ds 超限——跳过剩余 %d 条" %
                      (n_batches, args.batch_timeout, len(rows) - b_rows), flush=True)
                break
            try:
                rels, evs, method = re.extract_one(rid, summary, content, use_llm=False)
            except Exception as e:
                b_err += 1
                print("[err] %s: %s" % (rid[:14], str(e)[:120]), flush=True)
                continue
            b_rows += 1
            if not rels and not evs:
                if not args.dry_run:
                    mark_done(conn, rid, [], [], ts)
                continue
            b_have += 1
            if args.dry_run:
                continue
            w, rj = re.write_edges(conn, rid, rels, args.audit)
            b_edges += w
            b_rejected += rj
            re.write_relations_col(conn, rid, rels, evs)
        conn.commit()
        b_elapsed = time.time() - b_start
        total_rows += b_rows
        total_edges += b_edges
        total_rejected += b_rejected
        total_skip += b_skip
        total_err += b_err
        rec = {"batch": n_batches, "rows": b_rows, "have_output": b_have,
               "edges": b_edges, "rejected": b_rejected, "skipped": b_skip,
               "errors": b_err, "elapsed_s": round(b_elapsed, 1), "ts": _now_iso()}
        with open(args.progress, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print("[batch#%d] 处理=%d 有产出=%d 边=%d 伪边拒=%d 跳过=%d 错误=%d 耗时=%.1fs" %
              (n_batches, b_rows, b_have, b_edges, b_rejected, b_skip, b_err, b_elapsed),
              flush=True)

    n_filled = conn.execute(
        "SELECT COUNT(*) FROM memory_store WHERE relations IS NOT NULL AND relations != ''"
    ).fetchone()[0]
    n_enum = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE relation IN "
        "('proposes','decides','implements','develops','depends_on','uses',"
        "'part_of','conflicts','related_to','event_occurred')"
    ).fetchone()[0]
    n_pseudo = conn.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE relation IN ('supports','same_type')"
    ).fetchone()[0]
    conn.close()
    print("[done] 批数=%d 处理=%d 写边=%d 伪边拒=%d 批跳过=%d 单条错误=%d 总耗时=%.0fs" %
          (n_batches, total_rows, total_edges, total_rejected, total_skip,
           total_err, time.time() - t0), flush=True)
    print("[done] relations 留痕=%d graph_edges 枚举关系边=%d 伪边=%d(零新增=%s)" %
          (n_filled, n_enum, n_pseudo, n_pseudo == 0), flush=True)


if __name__ == "__main__":
    main()
