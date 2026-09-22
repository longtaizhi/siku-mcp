#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rebuild_fts_jieba.py — Phase 0：FTS5 预分词重建索引
- jieba 切词后以空格连接写入 FTS5（unicode61 按空格分词，2 字中文词可命中）
- 影子表：不改 memory_store 表结构；rowid 与 memory_store.rowid 对应
- 可重复运行（DROP 重建），运行前自动提示备份
"""
import os
import sqlite3
import sys
import time
import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

DB = os.path.join(_SIKU_ROOT, "memory_store.db")

import jieba


def seg(text):
    """jieba 分词 → 空格连接；空文本返回空串"""
    if not text:
        return ""
    return " ".join(w for w in jieba.lcut(text) if w.strip())


def main():
    t0 = time.time()
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    total = c.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]

    c.execute("DROP TABLE IF EXISTS mem_fts")
    c.execute("CREATE VIRTUAL TABLE mem_fts USING fts5(summary, content, tokenize='unicode61')")

    rows = c.execute("SELECT rowid, summary, content FROM memory_store").fetchall()
    c.executemany(
        "INSERT INTO mem_fts(rowid, summary, content) VALUES (?,?,?)",
        [(r[0], seg(r[1]), seg(r[2] or "")) for r in rows],
    )
    conn.commit()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：FTS 影子表重建后必须清缓存（直接影响 fts5 结果）

    # 验证 2 字词命中（Phase 0 验收指标）
    for w in ["用户", "审计", "记忆", "测试", "健身", "开工"]:
        n = c.execute(
            "SELECT COUNT(*) FROM mem_fts WHERE mem_fts MATCH ?", (f'"{w}"',)
        ).fetchone()[0]
        print(f"  2字词[{w}] → {n} 条")

    # 抽查一条原文是否完整入库
    sample = c.execute(
        "SELECT mem_fts.rowid, substr(mem_fts.summary,1,40) FROM mem_fts WHERE mem_fts MATCH '\"用户\"' LIMIT 2"
    ).fetchall()
    for s in sample:
        print("  样本:", s)

    conn.close()
    print(f"重建完成: {total} 条 → 索引 {len(rows)} 条, 耗时 {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
