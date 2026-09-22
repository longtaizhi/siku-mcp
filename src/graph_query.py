#!/usr/bin/env python3
"""
知识图谱查询扩展（5-A）
集成到l3_retrieval.py的expand模式
"""
import sqlite3, os

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

BASE = _SIKU_ROOT
DB = os.environ.get("SIKU_DB_PATH", os.path.join(BASE, "memory_store.db"))

def get_related(entry_id, top_k=5):
    """
    查询与给定entry_id关联的记忆条目
    返回: [(id, summary, source_agent, weight, relation), ...]
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT
            CASE WHEN e.id1 = ? THEN e.id2 ELSE e.id1 END AS related_id,
            m.summary,
            m.source_agent,
            e.weight,
            e.relation
        FROM graph_edges e
        JOIN memory_store m ON m.id = CASE WHEN e.id1 = ? THEN e.id2 ELSE e.id1 END
        WHERE e.id1 = ? OR e.id2 = ?
        ORDER BY e.weight DESC
        LIMIT ?
    """, (entry_id, entry_id, entry_id, entry_id, top_k)).fetchall()
    conn.close()
    return [(r["related_id"], r["summary"], r["source_agent"], r["weight"], r["relation"])
            for r in rows]

def batch_expand(entry_ids, top_k=3):
    """
    批量查询关联记忆（去重后按weight排序）
    """
    results = {}
    for eid in entry_ids:
        for rid, summary, agent, w, rel in get_related(eid, top_k):
            if rid not in results or w > results[rid][3]:
                results[rid] = (rid, summary, agent, w, rel)
    return sorted(results.values(), key=lambda x: -x[3])


# ── S8 P1b: 条目图通道（仅真信号）──────────────────────────────────────
# S0 盘点实锤（13,253,066 边）：supports 481.9 万（36.4%）=同 concern 共现伪信号、
# same_type 773.4 万（58.4%）=同类组合爆炸——必须排除；
# 真信号仅 keyword_overlap 34.1 万 + semantic_similar 17.8 万 + same_agent 18.2 万 ≈ 70 万（5.3%）
TRUE_SIGNAL_RELATIONS = ("keyword_overlap", "semantic_similar", "same_agent")


def batch_expand_multi(entry_ids, top_k=3, relations=None, max_seeds=50):
    """
    S8 P1b: 批量查询关联条目（单 SQL 批查 + 真信号关系白名单 + eid 去重）。

    - entry_ids: 种子条目 id 列表（超 max_seeds 截断护栏）
    - top_k: 每个种子保留的邻居上限（按 weight 降序）
    - relations: 关系白名单；默认仅真信号（TRUE_SIGNAL_RELATIONS），
      调用方传 None 之外的值可放宽（本卡纪律：默认绝不引入 supports/same_type）
    返回: [dict(related_id, summary, source_agent, type, confidence, timestamp,
                expires_at, summary_hash, deprecated, industry, source_ref,
                weight, relation), ...]
          按 weight 降序，related_id 唯一（同 eid 保留最高 weight）
    """
    if not entry_ids:
        return []
    entry_ids = list(entry_ids)[:max_seeds]
    rels = tuple(relations) if relations else TRUE_SIGNAL_RELATIONS
    marks = ",".join("?" * len(entry_ids))
    rel_marks = ",".join("?" * len(rels))
    # id1 IN seeds OR id2 IN seeds：graph_edges 有 idx_graph_id1/id2 双索引 → MULTI-INDEX OR 快路径
    params = list(entry_ids) * 2 + list(rels)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    try:
        edges = conn.execute(
            "SELECT id1, id2, relation, weight FROM graph_edges "
            "WHERE (id1 IN (%s) OR id2 IN (%s)) AND relation IN (%s)"
            % (marks, marks, rel_marks),
            params,
        ).fetchall()
        if not edges:
            return []
        seed_set = set(entry_ids)
        by_seed = {}
        for e in edges:
            if e["id1"] in seed_set:
                seed, rid = e["id1"], e["id2"]
            else:
                seed, rid = e["id2"], e["id1"]
            by_seed.setdefault(seed, []).append((rid, e["weight"], e["relation"]))
        # 每种子 top_k（weight 降序），跨种子 eid 去重保留最高 weight
        chosen = {}
        for seed, lst in by_seed.items():
            lst.sort(key=lambda x: -x[1])
            for rid, w, rel in lst[:top_k]:
                if rid not in chosen or w > chosen[rid][0]:
                    chosen[rid] = (w, rel)
        if not chosen:
            return []
        rid_marks = ",".join("?" * len(chosen))
        rows = conn.execute(
            "SELECT id, summary, source_agent, type, confidence, timestamp, "
            "expires_at, summary_hash, deprecated, industry, source_ref "
            "FROM memory_store WHERE id IN (%s) AND (deprecated IS NULL OR deprecated = 0)"
            % rid_marks,
            list(chosen.keys()),
        ).fetchall()
        by_id = {r["id"]: r for r in rows}
        out = []
        for rid, (w, rel) in chosen.items():
            r = by_id.get(rid)
            if r is None:
                continue
            out.append({
                "related_id": rid,
                "summary": r["summary"],
                "source_agent": r["source_agent"],
                "type": r["type"],
                "confidence": r["confidence"],
                "timestamp": r["timestamp"],
                "expires_at": r["expires_at"],
                "summary_hash": r["summary_hash"],
                "deprecated": r["deprecated"],
                "industry": r["industry"],
                "source_ref": r["source_ref"],
                "weight": w,
                "relation": rel,
            })
        out.sort(key=lambda x: -x["weight"])
        return out
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        eid = sys.argv[1]
        related = get_related(eid)
        print(f"与 {eid[:8]}... 关联的记忆 ({len(related)} 条):")
        for rid, summary, agent, w, rel in related[:10]:
            print(f"  [{rel} w={w}] {rid[:8]}... | {agent} | {summary[:50]}")
    else:
        print("用法: python3 graph_query.py <entry_id>")
