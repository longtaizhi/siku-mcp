#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ingest_filter.py —  摄入过滤（影子模式，纯 stdlib 零依赖）

对标：Mnemosyne / OpenAI 记忆治理——写入端 substance/冗余过滤（34364 条信噪比痛点）。
影子模式：只检测+标记（调用方 reta_pipeline 在 SIKU_INGEST_FILTER=1 时调用，标记写 audit_log），
绝不拦截写入——开关默认关（生产零行为变化）；off 时本模块零调用零开销。

检测两类：
  1) substance（低价值）：summary 过短（< SUBSTANCE_MIN_SUMMARY_LEN）或
     content 为空且 summary 为纯套话（命中停用词集）→ state=low_value
  2) redundant（冗余）：与库内既有条目近似重复（summary_hash 前缀 8 相同
     或 summary 编辑距离相似度 ≥0.85——与检索侧 S4 阈值同源）→ state=redundant

返回：{"flagged": bool, "state": "low_value"|"redundant"|"", "reason": str, "redundant_of": id|None}
任何异常全吞（检测失败照常放行——影子模式绝不阻断管道）。
"""
import difflib
import hashlib

SUBSTANCE_MIN_SUMMARY_LEN = 15      # summary 少于 15 字 = 疑似低价值（影子标记阈值）
SUBSTANCE_STOPWORDS = ("你好", "好的", "收到", "明白了", "嗯", "ok", "好的收到",
                       "了解", "知道了", "没问题", "谢谢", "再见", "好的好的")
REDUNDANT_HASH_PREFIX = 8           # summary_hash 前缀 8 相同 = 同事实变体（对齐检索侧 S4）
REDUNDANT_SIM_THRESHOLD = 0.85      # 编辑距离相似度 ≥0.85 = 近似重复（对齐检索侧 S4 阈值）


def _text_sim(a, b):
    """文本相似度（difflib SequenceMatcher，对齐检索侧 S4 口径）"""
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def detect(entry, conn):
    """检测单条写入（影子模式）。entry: {"summary", "content", "type"}；conn: 目标库连接。

    返回 {"flagged", "state", "reason", "redundant_of"}——flagged=True 表示应标记；
    调用方按 SIKU_INGEST_FILTER=1 门控决定是否写 audit_log（影子：只记录不拦截）。
    """
    summary = (entry.get("summary") or "").strip()
    content = (entry.get("content") or "").strip()
    if not summary:
        return {"flagged": True, "state": "low_value",
                "reason": "summary 为空", "redundant_of": None}
    # ── substance 过滤：低价值条目 ──
    if len(summary) < SUBSTANCE_MIN_SUMMARY_LEN:
        return {"flagged": True, "state": "low_value",
                "reason": "summary 过短(%d字<%d)" % (len(summary), SUBSTANCE_MIN_SUMMARY_LEN),
                "redundant_of": None}
    if not content and summary.lower() in SUBSTANCE_STOPWORDS:
        return {"flagged": True, "state": "low_value",
                "reason": "无 content 且 summary 为套话/无实义", "redundant_of": None}
    # ── redundant 过滤：与库内既有条目近似重复（summary_hash 前缀 8 或编辑距离 ≥0.85）──
    try:
        sh = hashlib.sha256(summary.encode("utf-8")).hexdigest()[:12]
        c = conn.cursor()
        rows = c.execute(
            "SELECT id, summary FROM memory_store WHERE summary_hash LIKE ? LIMIT 20",
            (sh[:REDUNDANT_HASH_PREFIX] + "%",)).fetchall()
        for rid, rs in rows:
            if _text_sim(summary, rs or "") >= REDUNDANT_SIM_THRESHOLD:
                return {"flagged": True, "state": "redundant",
                        "reason": "与既有条目近似重复(相似度≥%.2f)" % REDUNDANT_SIM_THRESHOLD,
                        "redundant_of": rid}
    except Exception:
        pass  # 冗余检测失败 → 照常放行（影子模式不阻断）
    return {"flagged": False, "state": "", "reason": "", "redundant_of": None}


if __name__ == "__main__":
    import sys
    sys.path.insert(0, __import__("os").path.dirname(__file__))
    # 自测（沙盒）：构造用例验证 detect 输出
    import sqlite3
    _os = __import__("os")
    db = _os.environ.get("SIKU_DB_PATH") or _os.path.join(_os.environ.get("SIKU_ROOT", _os.path.expanduser("~/siku-core")), "memory_store.db")
    db = _os.path.expanduser(db)
    conn = sqlite3.connect(db)
    cases = [
        {"summary": "好的", "content": "", "type": "fact"},
        {"summary": "今天天气不错", "content": "", "type": "record"},
        {"summary": "三库优化第三批检索侧改造完成，两级粒度级联检索接入 l3_retrieval，门控默认关", "content": "细节", "type": "result"},
    ]
    for i, case in enumerate(cases):
        print("case%d:" % i, detect(case, conn))
    conn.close()
