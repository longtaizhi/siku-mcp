#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill_embeddings.py — 步1b embedding 回填（卡）
- 为 memory_store 中缺失 embedding 的记忆生成向量（BAAI/bge-small-zh-v1.5, 512维 float32）
- 通道：18790 bge daemon 优先（MLX 纯推理，不占 8081）；daemon 不可用 fallback mlx_bge 直推
- 只写 SQLite memory_store.embedding BLOB；chroma 对齐由 chroma_sync.py 负责
- 幂等/断点续跑：只处理 embedding IS NULL OR length(embedding)=0 的行，重跑自动跳过已完成
- 分批：每 100 条 commit（s1c 同库写锁串行友好）；--limit N 支持小批量验证
- 失败单条记入 failed 列表并继续（断点续跑保底：重跑只处理剩余缺失）
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

import numpy as np

import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）

DB = os.path.join(_SIKU_ROOT, "memory_store.db")
DAEMON_HOST = os.environ.get("SIKU_BGE_HOST", "127.0.0.1")
DAEMON_PORT = int(os.environ.get("SIKU_BGE_PORT", "18790"))
EMBEDDING_MODEL = "BAAI/bge-small-zh-v1.5"

sys.path.insert(0, os.path.join(_HERMES_HOME, "scripts"))

_MLX = None  # 本地 MLX 模型缓存


def _daemon_vec(text):
    """走 18790 bge daemon /embed；失败/超时返回 None。"""
    data = json.dumps({"text": text}).encode()
    req = urllib.request.Request(
        "http://%s:%d/embed" % (DAEMON_HOST, DAEMON_PORT),
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        result = json.loads(resp.read())
        if result.get("vec"):
            return np.array(result["vec"], dtype=np.float32)
    return None


def _local_vec(text):
    """fallback：纯 MLX 推理 bge（零 semaphore 泄漏），仅首次调用加载模型。"""
    global _MLX
    if _MLX is None:
        from mlx_bge import MLXBgeEmbedding
        _MLX = MLXBgeEmbedding(EMBEDDING_MODEL)
    return np.asarray(_MLX.encode(text), dtype=np.float32)


def get_vec(text, use_daemon=True):
    """daemon 优先（重试 3 次），失败回退本地 MLX。"""
    if use_daemon:
        for _ in range(3):
            try:
                v = _daemon_vec(text)
                if v is not None:
                    return v
            except Exception:
                pass
            time.sleep(1)
    return _local_vec(text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 条缺失（小批量验证用）")
    ap.add_argument("--no-daemon", action="store_true", help="跳过 daemon 直接本地 MLX")
    ap.add_argument("--batch", type=int, default=100, help="每 N 条 commit 一次（默认 100）")
    args = ap.parse_args()

    t0 = time.time()
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = "SELECT id, summary, content FROM memory_store WHERE embedding IS NULL OR length(embedding)=0"
    if args.limit:
        sql += " LIMIT %d" % int(args.limit)
    cur.execute(sql)
    missing = cur.fetchall()
    total_missing = len(missing)
    print("缺失向量: %d 条%s" % (total_missing, "（小批量模式）" if args.limit else ""))
    if total_missing == 0:
        conn.close()
        print("无需回填，退出。")
        return

    n_ok, n_fail = 0, 0
    failed = []
    for r in missing:
        text = ((r["summary"] or "") + " " + (r["content"] or "")).strip()[:1000]
        try:
            vec = get_vec(text, use_daemon=not args.no_daemon)
            if vec is None or len(vec) != 512:
                raise ValueError("向量维度异常: %s" % (None if vec is None else len(vec)))
            blob = np.asarray(vec, dtype=np.float32).tobytes()
            cur.execute("UPDATE memory_store SET embedding=? WHERE id=?", (blob, r["id"]))
            n_ok += 1
        except Exception as e:
            n_fail += 1
            failed.append(r["id"])
            print("  !! 失败 id=%s: %s" % (r["id"], e))
        if (n_ok + n_fail) % args.batch == 0:
            conn.commit()
            print("  已处理 %d/%d（成功 %d，失败 %d）" % (n_ok + n_fail, total_missing, n_ok, n_fail))
    conn.commit()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：补写全部 commit 后清缓存

    covered = cur.execute(
        "SELECT COUNT(*) FROM memory_store WHERE embedding IS NOT NULL AND length(embedding)>0"
    ).fetchone()[0]
    total = cur.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
    conn.close()
    print("补齐 %d 条（失败 %d），覆盖 %d/%d (%d%%)，耗时 %.1fs"
          % (n_ok, n_fail, covered, total, covered * 100 // total, time.time() - t0))
    if failed:
        print("失败 id 列表: %s" % ",".join(failed))


if __name__ == "__main__":
    main()
