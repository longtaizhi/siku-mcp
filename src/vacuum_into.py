#!/usr/bin/env python3
"""四库 memory_store.db 物理修复——VACUUM INTO + 索引重建（卡）。

背景：生产库 5.5GB（freelist 1192407 页闲置）+ integrity 3 异常
（freelist 计数差 1 / page 1415518 never used / idx_access_entry 缺 row 98476）。
VACUUM INTO 不锁原库（读快照写新文件），新文件再重建损坏索引 → integrity 全绿 → 切换。

用法：
  python3 vacuum_into.py --src <源库> --dst <新库路径> [--rebuild-idx idx_access_entry]
输出：JSON 一行（新旧对比 + integrity 结果）。

注意：本脚本只写 --dst 新文件，绝不触碰 --src（源库只读打开 VACUUM INTO）。
切换（备份+上位）由外部带 SIKU_WRITE_GATE=1 令牌执行。
"""
import argparse
import json
import os
import sqlite3
import sys

DEFAULT_REBUILD_IDX = "idx_access_entry"


def db_stat(path):
    # WAL 模式下 mode=ro URI 需 -shm 可写，生产库活跃时可能失败；
    # 普通连接 + PRAGMA/SELECT = 只读数据访问，不写表数据。
    conn = sqlite3.connect(path)
    try:
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchall()
        return {
            "size_bytes": os.path.getsize(path),
            "size_mb": round(os.path.getsize(path) / 1048576, 1),
            "page_count": page_count,
            "freelist_count": freelist,
            "page_size": page_size,
            "integrity": [r[0] for r in integrity],
            "integrity_ok": len(integrity) == 1 and integrity[0][0] == "ok",
        }
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="源库（只读）")
    ap.add_argument("--dst", required=True, help="VACUUM INTO 目标新库（必须不存在）")
    ap.add_argument("--rebuild-idx", default=DEFAULT_REBUILD_IDX, help="需重建的损坏索引")
    args = ap.parse_args()

    src = os.path.abspath(os.path.expanduser(args.src))
    dst = os.path.abspath(os.path.expanduser(args.dst))

    if os.path.exists(dst):
        sys.exit(json.dumps({"ok": False, "error": f"目标已存在: {dst}"}, ensure_ascii=False))

    report = {"src": src, "dst": dst, "ok": False}

    # 1) 源库只读基线
    report["src_before"] = db_stat(src)

    # 2) VACUUM INTO（源库连接，不锁原库写路径——VACUUM INTO 读快照写新文件）
    conn = sqlite3.connect(src)
    try:
        conn.execute(f"VACUUM INTO '{dst}'")
    finally:
        conn.close()

    # 3) 新库重建损坏索引（VACUUM 会复制损坏索引，需重建）
    if args.rebuild_idx:
        c2 = sqlite3.connect(dst)
        try:
            c2.execute(f"DROP INDEX IF EXISTS {args.rebuild_idx}")
            # 从 sqlite_master 取原始 DDL，避免硬编码列
            row = c2.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                (args.rebuild_idx,),
            ).fetchone()
            # 索引已被 DROP，DDL 需从源库取
            if row is None:
                c1 = sqlite3.connect(src)
                ddl = c1.execute(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                    (args.rebuild_idx,),
                ).fetchone()[0]
                c1.close()
            else:
                ddl = row[0]
            if ddl:
                c2.execute(ddl)
        finally:
            c2.close()

    # 4) 新库全量校验
    report["dst_after"] = db_stat(dst)
    report["ok"] = report["dst_after"]["integrity_ok"]
    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit(0 if report["ok"] else 2)


if __name__ == "__main__":
    main()
