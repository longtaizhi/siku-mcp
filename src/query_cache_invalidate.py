#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
query_cache_invalidate.py — S2 M3 清缓存轻量模块（纯 stdlib，零重依赖）

用途：memory_store / mem_fts 写入侧 commit 成功后调用 invalidate_query_cache()，
全清 query_cache 表（写入侧不知道哪些 query 受影响，全清最简单可靠）。

设计约束（门禁条件 1/4 口径）：
- 纯 stdlib（禁止 import l3_retrieval 重模块——背 jieba/chromadb 重依赖，import 慢/可能失败）
- 每写脚本仅 +2 行：顶部 import + commit 后调用
- 失败静默（清缓存失败不影响写入主流程；陈旧判据 db_max_updated 为副防线兜底）
"""
import os
import sqlite3

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

DB_PATH = os.path.join(_SIKU_ROOT, "memory_store.db")


def invalidate_query_cache(db_path=None):
    """DELETE FROM query_cache 全清。返回 True=成功，False=失败（表不存在/锁）"""
    conn = None
    try:
        conn = sqlite3.connect(db_path or DB_PATH, timeout=5.0)
        conn.execute("DELETE FROM query_cache")
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    ok = invalidate_query_cache()
    print("query_cache 已清空" if ok else "清空失败（表不存在或锁）")
    raise SystemExit(0 if ok else 1)
