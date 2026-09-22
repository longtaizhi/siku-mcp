#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
chroma_sync.py — SQLite → Chroma 向量同步对齐（步1b，卡）

将 memory_store 中有 embedding BLOB 但不在 Chroma 的条目直接搬 BLOB 写入，
不重新 embed——保证 Chroma 与 SQLite 完全一致（SQLite BLOB 是真相源）。
幂等：已有 id 跳过。分批 upsert 200 条/次。
"""
import os
import sqlite3

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

import chromadb
import numpy as np

BASE = _SIKU_ROOT
DB = os.path.join(BASE, "memory_store.db")
CHROMA = os.path.join(BASE, "memory_store.chromadb")
BATCH = 200

conn = sqlite3.connect(DB, timeout=30)
c = conn.cursor()

client = chromadb.PersistentClient(path=CHROMA)
try:
    collection = client.get_collection("siku_memories")
except Exception:
    collection = client.create_collection("siku_memories")
existing_ids = set()
_offset = 0
while True:
    _batch = collection.get(limit=1000, offset=_offset)
    _ids = _batch["ids"]
    if not _ids:
        break
    existing_ids.update(_ids)
    _offset += len(_ids)
print("Chroma 已有: %d 条" % len(existing_ids))

rows = c.execute(
    "SELECT id, summary, content, type, source_agent, embedding "
    "FROM memory_store WHERE embedding IS NOT NULL AND length(embedding) > 0"
).fetchall()
conn.close()

to_add = [r for r in rows if r[0] not in existing_ids]
print("需同步: %d 条（有 embedding 且不在 Chroma）" % len(to_add))

n = 0
for i in range(0, len(to_add), BATCH):
    batch = to_add[i:i + BATCH]
    ids = [r[0] for r in batch]
    embs = [np.frombuffer(r[5], dtype=np.float32).tolist() for r in batch]
    docs = ["%s %s" % (r[1], r[2] or "") for r in batch]
    metas = [{"type": r[3] or "", "source_agent": r[4] or ""} for r in batch]
    collection.upsert(ids=ids, embeddings=embs, metadatas=metas, documents=docs)
    n += len(batch)
    print("  已同步 %d/%d" % (n, len(to_add)))

print("同步完成: 新增 %d 条到 Chroma（共 %d 条）" % (n, collection.count()))
