#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""retrieval_eval_runner.py — 四库检索评估统一 runner（评估统一）

背景（维护者 2026-08-26 拍板 P0 第二批 A 评估统一，D-A1 定案：不新增 cron，手动/事件触发）：
  四库三套独立评估体系（eval_bank / golden / rrf）→ 统一为单一 runner + 趋势记录，
  不新建基准（防第四套基准分裂）。本脚本只读复用三套数据源，既有脚本/数据零改动。

三套数据源（只读）：
  A. eval_bank 体系：$SIKU_ROOT/scripts/siku_option/eval_bank_v2.json（48 条分级题库）
     判定口径与 eval_retrieval.py 对齐：hit 类 top5 文本含任一 GT（大小写不敏感）；refuse 类
     空结果 或 top1 不含 query 实义词 = 合理拒答。
  B. golden 体系：$SIKU_GOLDEN_DIR/golden-set-20260823.json（113 条可答
     g001-g113，relational=28）+ golden_neg_20260823.json（18 条不可答 neg001-neg018）
     指标口径与 rrf_golden_monitor.py / rrf_monthly_check.py 对齐：hit_rate(top5)、
     top1_hit_rate、mean_ndcg_valid、mean_p5_valid（expected_top5 剔 deprecated）、
     负样本被作答率（幻觉信号，≤5% 为达标参考）。
  C. rrf 体系：$SIKU_GOLDEN_DIR/rrf_golden_split_train.json / _val.json
     （当前 91+22=113 全量切分；meta.split 历史口径 train76/val19/other18，95=train+val 早期口径）
     —— 周调参/月检链共用数据，本 runner 只读其 query/expected 用于统一口径快照。

多跳基准门（--multi-hop）：
  概念层多跳 = l3_retrieval.multi_hop_search（P2b，依赖语义层概念关系
  $HERMES_HOME/memory-bank/concepts/domain-graph.jsonld 只读）。用 golden 可答全量（113 条）跑
  multi_hop 模式 → hit_rate；基准门按 golden 定义：total≥1 且 hit_rate≥阈值（默认 0.5，
  与 rrf_golden_monitor 预警阈值一致，可用 --multi-hop-threshold 覆盖）。

基因注入纪律：
  - 经验（评测工具假安全感）：gate 对 NOT_RUN/全失败判 PASS=缺省通过陷阱
    → 本 runner gate 判定三态 PASS/FAIL/UNKNOWN：suite 未实际运行或就绪度不足 → UNKNOWN（绝不 PASS）；
    suite 实跑但全失败/未达标 → FAIL。只有实跑且达标 → PASS。
  - 经验（评测管道就绪度是评测前提）：运行前就绪度检查先行（数据源/DB/语义层/
    l3 导入），NOT_READY 如实标注，不产出假报告。
  - 经验（RRF 数据侧排查）：报告记录数据快照（路径/大小/mtime/版本），数据侧异常如实标注。
  - 既有三套脚本/数据零改动（本脚本只读复用）；趋势追加式写入 eval_trend.jsonl（格式统一防第四套基准）。

根源修复（2026-08-27，评测环境一致性机制缺失实证）：
  06:50 基线跑在 g1 污染态（33330 行）无人知晓——原 runner 只记 db_rows 不记关键列状态。
  → ① env_snapshot_of()：报告新增 env_snapshot 段（总行数/g1_check_passed 分布/train_eligible 计数/
    数据最新更新时间戳 MAX(updated_at) 与 MAX(timestamp)），列缺失降级跳过不崩（cols_missing 如实记录）；
  → ② baseline_check() + --baseline-db：评测前置校验，与基线库对比（总行数相对差 >0.5% 或
    g1=1 计数绝对差 >100 → 警告 + 该 run 不入基线），警告写报告基线一致性段 + suite 标注 baseline_warn。

用法：
  # 全量三套 + 趋势（生产默认，报告落 siku_option/eval_reports）
  $SIKU_VENV_PYTHON retrieval_eval_runner.py
  # 指定 suite
  ... retrieval_eval_runner.py --suite eval-bank
  ... retrieval_eval_runner.py --suite golden --gate        # 门禁判定（PASS/FAIL/UNKNOWN，exit 码传导）
  # 多跳基准门
  ... retrieval_eval_runner.py --suite multi-hop --gate
  # 基线前置校验（--baseline-db 指向基线库，如基线报告对应时刻的库副本）
  ... retrieval_eval_runner.py --baseline-db /path/to/baseline.db --suite eval-bank
  # 沙盒/测试（--db 指向临时库副本，--report-dir 隔离，--limit 截断）
  ... retrieval_eval_runner.py --suite all --db /tmp/copy.db --report-dir /tmp/report --limit 5 --dry-run
"""
import argparse
import datetime
import importlib.util
import json
import os
import re
import sqlite3
import sys

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
_CONCEPTS_DIR = os.environ.get("SIKU_CONCEPTS_DIR", os.path.join(_HERMES_HOME, "memory-bank", "concepts"))  # 概念集目录
_GOLDEN_DIR = os.environ.get("SIKU_GOLDEN_DIR", os.path.join(_SIKU_ROOT, "golden"))  # 评测 golden 集目录

# ── 常量：三套数据源（只读） ──────────────────────────────────────
SIKU_SCRIPTS = os.path.join(_SIKU_ROOT, "scripts")
GOLDEN_DIR = _GOLDEN_DIR
EVAL_BANK_V2 = os.path.join(SIKU_SCRIPTS, "siku_option", "eval_bank_v2.json")
GOLDEN_SET = os.path.join(GOLDEN_DIR, "golden-set-20260823.json")
GOLDEN_NEG = os.path.join(GOLDEN_DIR, "golden_neg_20260823.json")
GOLDEN_SPLIT_TRAIN = os.path.join(GOLDEN_DIR, "rrf_golden_split_train.json")
GOLDEN_SPLIT_VAL = os.path.join(GOLDEN_DIR, "rrf_golden_split_val.json")
SEMANTIC_LAYER = os.environ.get("SIKU_LAYER_JSONLD", os.path.join(_CONCEPTS_DIR, "domain-graph.jsonld"))
DB_DEFAULT = os.path.join(_SIKU_ROOT, "memory_store.db")
REPORT_DIR_DEFAULT = os.path.join(SIKU_SCRIPTS, "siku_option", "eval_reports")
TREND_PATH_DEFAULT = os.path.join(SIKU_SCRIPTS, "siku_option", "eval_trend.jsonl")
HIT_THRESHOLD_DEFAULT = 0.5     # golden/多跳 top5 命中率基准门（与 rrf_golden_monitor 预警阈值一致）
HALLUC_RATE_MAX = 0.05          # 负样本被作答率上限（rrf_monthly_check 幻觉率门禁 ≤5%）
TASK_ID = ""

# ── GT 语义锚点判定（2026-08-27，第一步） ──────────
# 拍板：GT 语义锚点化（治本——意思对就认）。多跳级 10 条失败中 5 条=GT 字面匹配缺陷
# （答案在 top1 但判定不认——FTS5 vs FTS 别名硬伤）。判定=字面包含（快路径保留）+语义锚点
# （本地 bge embedding 相似度 ≥ 阈值）；无锚点 GT 走旧逻辑（兼容）。
ANCHOR_SIM_THRESHOLD = 0.65     # 锚点语义判定阈值（bge-small-zh-v1.5 余弦；校准见沙盒报告，5 案例 min=0.683）
ANCHOR_EMBED_DIR = os.environ.get("SIKU_EMBED_DIR", os.path.join(_HERMES_HOME, "scripts", "embedding"))  # 复用 l3_retrieval 既有 embedding 通道（不新建）

# 拒答场景实义词停用词（与 eval_retrieval.py 同集）
STOP_WORDS = {
    "帮我", "请", "的", "了", "是", "怎么", "什么", "多少", "吗", "呢", "哪个",
    "写", "做", "推荐", "翻译", "一首", "一部", "一个", "一条", "给我", "成", "今天",
    "明天", "最近", "一下", "如何", "为什么", "要不要", "该不该", "先", "还是", "用",
    "在", "有", "到", "从", "了", "和", "与", "或", "中", "上", "下", "时",
}


def now_iso():
    # 使用本地时区 Asia/Shanghai（CST, UTC+8）标注，避免 UTC 歧义
    from datetime import timezone, timedelta
    cst = timezone(timedelta(hours=8), "CST")
    return datetime.datetime.now(cst).strftime("%Y-%m-%dT%H:%M:%S CST")


def file_snapshot(path):
    """数据快照（数据侧排查：路径/大小/mtime/版本入报告）。"""
    if not path or not os.path.exists(path):
        return {"path": path, "exists": False}
    st = os.stat(path)
    return {"path": path, "exists": True, "size": st.st_size,
            "mtime": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")}


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── 口径工具：与 rrf_golden_monitor.py 同款 ─────────────────────────
def ndcg_at5(ranked_ids, relevant):
    dcg = 0.0
    for i, eid in enumerate(ranked_ids[:5]):
        if eid in relevant:
            dcg += 1.0 / (i + 2)
    k = min(5, len(relevant))
    idcg = sum(1.0 / (i + 2) for i in range(k))
    return dcg / idcg if idcg else 0.0


def p5_at5(ranked_ids, relevant):
    k = min(5, len(relevant))
    if k == 0:
        return 0.0
    hits = sum(1 for eid in ranked_ids[:5] if eid in relevant)
    return hits / k


def extract_content_words(query):
    """提取 query 实义词（与 eval_retrieval.py 同款）。"""
    words = re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z]{3,}", query)
    return [w for w in words if w not in STOP_WORDS]


# ── GT 语义锚点判定工具（2026-08-27，复用 l3 既有本地 embedding 通道，不新建） ────
_EMBEDDER = None


def get_embedder():
    """惰性加载本地 bge embedding（复用 $HERMES_HOME/scripts/embedding/embed.py，与 l3_retrieval 同款）。
    加载失败返回 None → 语义锚点判定降级为纯字面（旧逻辑兼容，如实标注）。"""
    global _EMBEDDER
    if _EMBEDDER is not None:
        return _EMBEDDER
    try:
        sys.path.insert(0, ANCHOR_EMBED_DIR)
        from embed import embed_text
        _EMBEDDER = embed_text
        return _EMBEDDER
    except Exception:
        return None


def _cos_sim(v1, v2):
    import numpy as np
    a, b = np.asarray(v1, dtype=np.float32), np.asarray(v2, dtype=np.float32)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-9))


def anchor_semantic_hit(anchors, doc_text, embed_fn, threshold):
    """语义锚点判定：doc_text 与 anchor_sentence / 任一 synonyms 的 embedding 余弦相似度 ≥ 阈值。
    返回 (bool, best_sim, matched_text)。embed_fn 不可用 → (False, 0.0, None)（降级旧逻辑）。"""
    if not anchors or not doc_text or embed_fn is None:
        return False, 0.0, None
    cands = [anchors.get("anchor_sentence") or ""] + list(anchors.get("synonyms") or [])
    cands = [c for c in cands if c and c.strip()]
    if not cands:
        return False, 0.0, None
    best = 0.0
    best_txt = ""
    try:
        dvec = embed_fn(doc_text)
        for c in cands:
            cvec = embed_fn(c)
            s = _cos_sim(dvec, cvec)
            if s > best:
                best, best_txt = s, c
    except Exception:
        return False, 0.0, None
    return best >= threshold, best, best_txt


def dep_ids_of(db_path):
    try:
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        ids = {r[0] for r in c.execute("SELECT id FROM memory_store WHERE deprecated=1")}
        c.close()
        return ids
    except Exception:
        return set()


def db_row_count(db_path):
    try:
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        n = c.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
        c.close()
        return n
    except Exception:
        return None


# ── 环境快照 + 基线前置校验（根源修复 2026-08-27） ──────────────────
ROW_DIFF_PCT = 0.005   # 基线对比：总行数相对差异阈值 ±0.5%
G1_DIFF_ABS = 100      # 基线对比：g1_check_passed=1 计数绝对差异阈值


def _table_cols_of(db_path):
    """只读读取 memory_store 表列名（缺失/不可读 → None）。"""
    try:
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cols = {r[1] for r in c.execute("PRAGMA table_info(memory_store)")}
        c.close()
        return cols
    except Exception:
        return None


def env_snapshot_of(db_path):
    """环境快照：总行数 / g1_check_passed 分布 / train_eligible 计数 / 数据更新时间戳。
    列缺失降级（跳过该维度并记录 cols_missing，不崩）；库不可读 → note 如实标注。"""
    snap = {"db": db_path, "total_rows": None, "g1_check_passed": {},
            "train_eligible": {}, "data_updated_at": None, "data_created_at": None,
            "cols_missing": [], "note": None}
    cols = _table_cols_of(db_path)
    if cols is None:
        snap["note"] = "库不可读或 memory_store 表缺失"
        return snap
    try:
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        snap["total_rows"] = c.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
        if "g1_check_passed" in cols:
            snap["g1_check_passed"] = {str(k): v for k, v in c.execute(
                "SELECT g1_check_passed, COUNT(*) FROM memory_store GROUP BY g1_check_passed")}
        else:
            snap["cols_missing"].append("g1_check_passed")
        if "train_eligible" in cols:
            snap["train_eligible"] = {str(k): v for k, v in c.execute(
                "SELECT train_eligible, COUNT(*) FROM memory_store GROUP BY train_eligible")}
        else:
            snap["cols_missing"].append("train_eligible")
        if "updated_at" in cols:
            snap["data_updated_at"] = c.execute("SELECT MAX(updated_at) FROM memory_store").fetchone()[0]
        else:
            snap["cols_missing"].append("updated_at")
        if "timestamp" in cols:
            snap["data_created_at"] = c.execute("SELECT MAX(timestamp) FROM memory_store").fetchone()[0]
        else:
            snap["cols_missing"].append("timestamp")
        c.close()
    except Exception as e:
        snap["note"] = f"快照读取失败: {e}"
    return snap


def baseline_check(cur, base):
    """基线前置校验：当前库 vs 基线库（--baseline-db）。
    总行数相对差 >ROW_DIFF_PCT 或 g1=1 计数绝对差 >G1_DIFF_ABS → 警告（该 run 不入基线）。
    基线库不可读/缺列 → 降级如实标注，不崩。"""
    bc = {"compared": False, "ok": True, "warnings": [],
          "thresholds": {"row_diff_pct": ROW_DIFF_PCT, "g1_diff_abs": G1_DIFF_ABS},
          "baseline_db": (base or {}).get("db")}
    if not base or base.get("note"):
        bc["ok"] = False
        bc["warnings"].append("基线库不可读或快照失败——本 run 环境一致性未确认（不入基线）")
        return bc
    bc["compared"] = True
    cur_rows, base_rows = cur.get("total_rows"), base.get("total_rows")
    if cur_rows is not None and base_rows:
        diff_pct = abs(cur_rows - base_rows) / base_rows
        if diff_pct > ROW_DIFF_PCT:
            bc["ok"] = False
            bc["warnings"].append(
                f"总行数差异超阈值: 当前 {cur_rows} vs 基线 {base_rows}（相对差 {diff_pct:.2%} > {ROW_DIFF_PCT:.2%}）")
    cur_g1 = (cur.get("g1_check_passed") or {}).get("1")
    base_g1 = (base.get("g1_check_passed") or {}).get("1")
    if cur_g1 is not None and base_g1 is not None:
        diff_g1 = abs(cur_g1 - base_g1)
        if diff_g1 > G1_DIFF_ABS:
            bc["ok"] = False
            bc["warnings"].append(
                f"g1=1 计数差异超阈值: 当前 {cur_g1} vs 基线 {base_g1}（差 {diff_g1} > {G1_DIFF_ABS}）")
    if "g1_check_passed" in (cur.get("cols_missing") or []) or "g1_check_passed" in (base.get("cols_missing") or []):
        bc["warnings"].append("g1_check_passed 列缺失——g1 分布维度未对比（降级）")
    if not bc["warnings"]:
        bc["warnings"].append("环境一致（总行数/g1 分布均在阈值内）")
    return bc


# ── 三套评测 ─────────────────────────────────────────────────────
def run_eval_bank(l3, top_k, limit, anchor_threshold=None, no_anchor=False):
    """A 套：eval_bank_v2 48 条分级题库（口径对齐 eval_retrieval.py + GT 语义锚点化 2026-08-27）。
    判定：字面包含（快路径保留，与旧口径一致）+ 语义锚点（本地 bge embedding 相似度 ≥ 阈值）。
    无锚点 GT 走旧逻辑（字面判定）；embedding 不可用 → 自动降级纯字面（兼容）。
    db_path 参数已移除（P1-③：函数体不使用 deprecated 过滤，判定为文本 GT 匹配）。"""
    bank = load_json(EVAL_BANK_V2)
    cases = bank["cases"]
    if limit:
        cases = cases[:limit]
    total = hit = 0
    sem_rescued = 0      # 字面不中但语义锚点判定命中
    level_stats = {}
    detail = []
    embed_fn = get_embedder() if not no_anchor else None
    thr = anchor_threshold if anchor_threshold is not None else ANCHOR_SIM_THRESHOLD
    for c in cases:
        try:
            out = l3.search_memories(c["query"], mode="auto", top_k=top_k, tier="compact")
            results = out.get("results", []) if isinstance(out, dict) else []
        except Exception as e:
            results, out = [], {}
        top1 = results[0].get("summary", "")[:120] if results else ""
        text = " ".join(r.get("summary", "") for r in results[:top_k])
        gts = c.get("gts") or []
        anchors = c.get("semantic_anchors")
        judge = "literal"
        if c["expected"] == "refuse":
            words = extract_content_words(c["query"])
            related = any(w in top1 for w in words) if (results and words) else False
            ok = bool(not results or not words or not related)
            fail_type = None if ok else "refuse_unexpected"
        else:
            # 快路径：字面包含（旧口径，与无锚点 GT 一致）
            h5 = any(gt.lower() in text.lower() for gt in gts) if gts else False
            h1 = any(gt.lower() in top1.lower() for gt in gts)
            if anchors and embed_fn:
                # 有锚点 GT 且 embedding 可用：语义锚点判定为准（意思对就认）。
                # 字面 top5+top1 全中 → 快路径直接过（与旧判定一致）；否则逐条 top5 语义兜底
                # （取 top5 各条 summary 与锚点相似度 max ≥ 阈值——拼接文本会稀释单条答案信号）。
                if h5 and h1:
                    ok, fail_type = True, None
                else:
                    h5_sem = False
                    best_sim = 0.0
                    for r in results[:top_k]:
                        s_ok, s_sim, _mt = anchor_semantic_hit(anchors, r.get("summary", ""), embed_fn, thr)
                        best_sim = max(best_sim, s_sim)
                        if s_ok:
                            h5_sem = True
                            break
                    if h5_sem:
                        ok, fail_type = True, None
                        sem_rescued += 1
                        judge = f"semantic_top5(sim={best_sim:.3f})"
                    else:
                        ok, fail_type = False, "top5_miss"
                        judge = f"semantic_miss(sim={best_sim:.3f})"
            else:
                # 无锚点 GT / embedding 不可用：旧逻辑（字面判定，兼容）
                ok = bool(h5 and h1)
                fail_type = None if ok else ("top5_miss" if not h5 else "top1_miss")
        total += 1
        hit += ok
        ls = level_stats.setdefault(c["level"], {"ok": 0, "n": 0})
        ls["n"] += 1
        ls["ok"] += ok
        detail.append({"id": c["id"], "level": c["level"], "query": c["query"],
                       "expected": c["expected"], "ok": ok, "fail_type": fail_type,
                       "judge": judge, "has_anchor": bool(anchors)})
    summary = {"version": bank.get("version"), "total": total, "hit": hit,
               "hit_rate": round(hit / total, 4) if total else 0.0,
               "judge_caliber": "literal+semantic_anchor" if embed_fn else "literal",
               "anchor_threshold": thr,
               "semantic_rescued": sem_rescued,
               "levels": {k: {"ok": v["ok"], "n": v["n"],
                              "rate": round(v["ok"] / v["n"], 4) if v["n"] else 0.0}
                          for k, v in level_stats.items()}}
    return summary, detail


def run_golden(l3, db_path, top_k, limit, neg_path=None):
    """B 套：golden-set 113 条可答 + golden_neg 18 条不可答（口径对齐 rrf_golden_monitor.py）。"""
    golden = load_json(GOLDEN_SET)
    queries = golden["queries"]
    dep_ids = dep_ids_of(db_path)
    if limit:
        queries = queries[:limit]
    phase = []
    for q in queries:
        qid, query = q["id"], q["query"]
        exp = q.get("expected_top5", [])
        try:
            out = l3.search_memories(query, mode="auto", top_k=top_k, tier="compact")
            top5 = [x["id"] for x in out.get("results", [])] if isinstance(out, dict) else []
            mode = out.get("search_mode", "?") if isinstance(out, dict) else "ERR"
        except Exception as e:
            top5, mode = [], f"ERR:{e}"
        rel_valid = {e for e in exp if e not in dep_ids}
        phase.append({
            "id": qid, "query": query[:80], "top5": top5, "expected": exp,
            "search_mode": mode, "hit": bool(set(top5) & rel_valid),
            "top1_hit": bool(top5) and top5[0] in rel_valid,
            "ndcg_valid": ndcg_at5(top5, rel_valid), "p5_valid": p5_at5(top5, rel_valid),
        })
    n = len(phase)
    summary = {
        "golden_total": len(queries),
        "golden_relational": sum(1 for q in queries if q.get("relational")),
        "hit_rate": round(sum(1 for p in phase if p["hit"]) / n, 4) if n else 0.0,
        "top1_hit_rate": round(sum(1 for p in phase if p["top1_hit"]) / n, 4) if n else 0.0,
        "mean_ndcg_valid": round(sum(p["ndcg_valid"] for p in phase) / n, 4) if n else 0.0,
        "mean_p5_valid": round(sum(p["p5_valid"] for p in phase) / n, 4) if n else 0.0,
        "n": n,
    }
    neg_summary = {"n": 0, "answered": 0, "answered_rate": 0.0, "detail": []}
    if neg_path and os.path.exists(neg_path):
        ng = load_json(neg_path)
        neg_qs = ng["queries"]
        if limit:
            neg_qs = neg_qs[:limit]
        answered = 0
        neg_detail = []
        for q in neg_qs:
            try:
                out = l3.search_memories(q["query"], mode="auto", top_k=top_k, tier="compact")
                results = out.get("results", []) if isinstance(out, dict) else []
                top5 = [x["id"] for x in results]
                mode = out.get("search_mode", "?") if isinstance(out, dict) else "?"
                rq = out.get("result_quality") if isinstance(out, dict) else None
                weak = bool(results) and (rq == "weak" or all(
                    x.get("result_quality") == "weak" or x.get("weak_match") for x in results[:5]))
                ans = bool(top5) and not weak
                top1_score = round(results[0].get("score", 0.0), 4) if results else None
            except Exception as e:
                ans, mode, top1_score = False, f"ERR:{e}", None
            answered += ans
            neg_detail.append({"id": q["id"], "answered": ans, "mode": mode, "top1_score": top1_score})
        m = len(neg_qs)
        neg_summary = {"n": m, "answered": answered,
                       "answered_rate": round(answered / m, 4) if m else 0.0, "detail": neg_detail}
    return summary, phase, neg_summary


def run_rrf_split(l3, db_path, top_k, limit):
    """C 套：rrf split train/val（当前 91+22=113）——周调参/月检链共用数据快照。"""
    out = {"train": {"n": 0, "hit_rate": 0.0, "mean_ndcg_valid": 0.0, "mean_p5_valid": 0.0},
           "val": {"n": 0, "hit_rate": 0.0, "mean_ndcg_valid": 0.0, "mean_p5_valid": 0.0},
           "total_queries": 0}
    dep_ids = dep_ids_of(db_path)
    for key, path in (("train", GOLDEN_SPLIT_TRAIN), ("val", GOLDEN_SPLIT_VAL)):
        if not os.path.exists(path):
            out[key]["note"] = "split 文件缺失"
            continue
        queries = load_json(path)
        if limit:
            queries = queries[:limit]
        hits = ndcgs = p5s = 0
        for q in queries:
            exp = q.get("expected_top5", [])
            try:
                out_r = l3.search_memories(q["query"], mode="auto", top_k=top_k, tier="compact")
                top5 = [x["id"] for x in out_r.get("results", [])] if isinstance(out_r, dict) else []
            except Exception:
                top5 = []
            rel_valid = {e for e in exp if e not in dep_ids}
            hits += bool(set(top5) & rel_valid)
            ndcgs += ndcg_at5(top5, rel_valid)
            p5s += p5_at5(top5, rel_valid)
        n = len(queries)
        out[key] = {"n": n,
                    "hit_rate": round(hits / n, 4) if n else 0.0,
                    "mean_ndcg_valid": round(ndcgs / n, 4) if n else 0.0,
                    "mean_p5_valid": round(p5s / n, 4) if n else 0.0}
        out["total_queries"] += n
    out["weights"] = dict(getattr(l3, "CHANNEL_WEIGHTS", {}))
    return out


def run_multi_hop_gate(l3, db_path, top_k, limit, threshold):
    """多跳基准门：golden 可答全量（113 条）用 l3.multi_hop_search（概念层多跳）跑。
    门禁定义（按 golden）：total≥1 且 hit_rate≥threshold → PASS；否则 FAIL/UNKNOWN。"""
    golden = load_json(GOLDEN_SET)
    queries = [q for q in golden["queries"] if not q.get("unanswerable")]
    dep_ids = dep_ids_of(db_path)
    if limit:
        queries = queries[:limit]
    total = hit = 0
    complex_n = simple_n = 0
    detail = []
    for q in queries:
        qid, query = q["id"], q["query"]
        exp = q.get("expected_top5", [])
        try:
            out = l3.multi_hop_search(query, top_k=top_k, mode="auto", tier="compact")
            top5 = [x["id"] for x in out.get("results", [])] if isinstance(out, dict) else []
            mh = out.get("multi_hop", {}) if isinstance(out, dict) else {}
        except Exception as e:
            top5, mh = [], {"note": f"ERR:{e}"}
        rel_valid = {e for e in exp if e not in dep_ids}
        h = bool(set(top5) & rel_valid)
        total += 1
        hit += h
        cx = mh.get("complexity", "?")
        if cx == "complex":
            complex_n += 1
        elif cx == "simple":
            simple_n += 1
        detail.append({"id": qid, "query": query[:80], "hit": h,
                       "complexity": cx, "top5": top5})
    hit_rate = round(hit / total, 4) if total else 0.0
    ok_total = total >= 1
    ok_rate = hit_rate >= threshold
    verdict = "PASS" if (ok_total and ok_rate) else "FAIL"
    return {
        "n": total, "hit": hit, "hit_rate": hit_rate,
        "complex_n": complex_n, "simple_n": simple_n,
        "threshold": threshold, "ok_total": ok_total, "ok_rate": ok_rate,
        "verdict": verdict, "detail": detail,
    }


# ── 就绪度（评测管道就绪度是评测前提） ─────
def readiness(need_multi_hop):
    r = {}
    for name, path in [("eval_bank_v2", EVAL_BANK_V2), ("golden_set", GOLDEN_SET),
                       ("golden_neg", GOLDEN_NEG), ("rrf_split_train", GOLDEN_SPLIT_TRAIN),
                       ("rrf_split_val", GOLDEN_SPLIT_VAL)]:
        r[name] = file_snapshot(path)
    r["semantic_layer"] = file_snapshot(SEMANTIC_LAYER)
    r["semantic_layer"]["needed"] = need_multi_hop
    return r


# ── 报告与趋势 ────────────────────────────────────────────────────
def render_md(report):
    L = ["# 四库检索评估统一报告（A 评估统一）", "",
         f"- 任务: {TASK_ID}",
         f"- 运行时间: {report['time']}",
         f"- runner: retrieval_eval_runner.py v1.1.0（三套数据源只读复用，零改动既有脚本/数据）",
         f"- 检索库: {report['db']}（行数 {report['db_rows']}）",
         f"- 多跳模式: {'开（概念层多跳 + 基准门）' if report['multi_hop'] else '关'}",
         ""]
    for name, snap in report["readiness"].items():
        if name == "semantic_layer":
            continue
        L.append(f"- {name}: {'✓ ' + str(snap['size']) + 'B' if snap['exists'] else '✗ 缺失'}"
                 f"（mtime {snap['mtime'] if snap['exists'] else '-'}）")
    L.append(f"- 语义层 jsonld: {'✓ 存在' if report['readiness']['semantic_layer']['exists'] else '✗ 缺失'}"
             f"（多跳{'启用' if report['multi_hop'] else '未用'}）")
    L.append("")
    L.append("## 环境快照")
    L.append("")
    L.append("| 维度 | 值 |")
    L.append("|---|---|")
    es = report.get("env_snapshot", {})
    L.append(f"| 总行数 | {es.get('total_rows')} |")
    g1 = es.get("g1_check_passed") or {}
    L.append(f"| g1_check_passed 分布 | {g1 if g1 else '（列缺失/未记录）'} |")
    te = es.get("train_eligible") or {}
    L.append(f"| train_eligible 分布 | {te if te else '（列缺失/未记录）'} |")
    L.append(f"| 数据最新更新（updated_at） | {es.get('data_updated_at') or '-'} |")
    L.append(f"| 数据最新创建（timestamp） | {es.get('data_created_at') or '-'} |")
    if es.get("cols_missing"):
        L.append(f"| 缺失列（降级跳过） | {', '.join(es['cols_missing'])} |")
    if es.get("note"):
        L.append(f"| 快照备注 | ⚠️ {es['note']} |")
    L.append("")
    L.append("## 基线一致性校验")
    L.append("")
    bc = report.get("baseline_check", {})
    if not bc.get("compared"):
        L.append("- 未启用基线校验（未传 --baseline-db）。")
    else:
        mark = "✅" if bc.get("ok") else "⚠️"
        L.append(f"- {mark} 对比库: {bc.get('baseline_db')}")
        for w in bc.get("warnings", []):
            L.append(f"  - {w}")
        if not bc.get("ok"):
            L.append("- **本 run 环境与基线不一致 → 不入基线（差异详情见上）**")
    L.append("")
    L.append("## 各套指标与判定")
    L.append("")
    L.append("| 套件 | 指标 | 值 | 判定 |")
    L.append("|---|---|---|---|")
    for name in report["order"]:
        s = report["suites"][name]
        if s.get("verdict") == "NOT_RUN":
            L.append(f"| {name} | — | — | ⛔ NOT_RUN（未选中） |")
            continue
        if s.get("verdict") == "UNKNOWN":
            L.append(f"| {name} | — | — | ⚠️ UNKNOWN（{s.get('reason','就绪度不足')}） |")
            continue
        if name == "eval-bank":
            L.append(f"| eval-bank | 命中率 | {s['summary']['hit_rate']:.4f} ({s['summary']['hit']}/{s['summary']['total']}) | "
                     f"{'✅ PASS' if s['verdict']=='PASS' else '❌ FAIL'} |")
            L.append(f"| eval-bank | 判定口径 | {s['summary'].get('judge_caliber','literal')}（锚点阈值 "
                     f"{s['summary'].get('anchor_threshold')}；语义救援 {s['summary'].get('semantic_rescued')} 条） | |")
            for lv, st in s["summary"]["levels"].items():
                L.append(f"| eval-bank·{lv} | 命中率 | {st['rate']:.4f} ({st['ok']}/{st['n']}) | |")
        elif name == "golden":
            L.append(f"| golden | top5 命中率 | {s['summary']['hit_rate']:.4f} ({s['summary']['n']} 条) | "
                     f"{'✅ PASS' if s['verdict']=='PASS' else '❌ FAIL'} |")
            L.append(f"| golden | top1 命中率 / NDCG@5 / P@5 | {s['summary']['top1_hit_rate']:.4f} / "
                     f"{s['summary']['mean_ndcg_valid']:.4f} / {s['summary']['mean_p5_valid']:.4f} | |")
            L.append(f"| golden | 负样本被作答率 | {s['neg']['answered_rate']:.4f} "
                     f"({s['neg']['answered']}/{s['neg']['n']}) | {'✅ ≤5%' if s['neg']['answered_rate'] <= HALLUC_RATE_MAX else '⚠️ >5%'} |")
        elif name == "rrf":
            L.append(f"| rrf | train hit/ndcg/p5 | {s['summary']['train']['hit_rate']:.4f} / "
                     f"{s['summary']['train']['mean_ndcg_valid']:.4f} / {s['summary']['train']['mean_p5_valid']:.4f} "
                     f"({s['summary']['train']['n']}) | |")
            L.append(f"| rrf | val hit/ndcg/p5 | {s['summary']['val']['hit_rate']:.4f} / "
                     f"{s['summary']['val']['mean_ndcg_valid']:.4f} / {s['summary']['val']['mean_p5_valid']:.4f} "
                     f"({s['summary']['val']['n']}) | |")
            L.append(f"| rrf | 权重 | {s['summary'].get('weights')} | |")
        elif name == "multi-hop":
            L.append(f"| multi-hop | top5 命中率 | {s['summary']['hit_rate']:.4f} ({s['summary']['hit']}/{s['summary']['n']}) | "
                     f"{'✅ PASS' if s['verdict']=='PASS' else '❌ FAIL'} |")
            L.append(f"| multi-hop | 复杂度分布 | complex={s['summary']['complex_n']} simple={s['summary']['simple_n']} | |")
            L.append(f"| multi-hop | 基准门 | total≥1 且 hit≥{s['summary']['threshold']} | "
                     f"{'✅ 达标' if s['verdict']=='PASS' else '❌ 未达标'} |")
    L.append("")
    L.append("## 门禁汇总")
    L.append("")
    for name in report["order"]:
        s = report["suites"][name]
        mark = {"PASS": "✅", "FAIL": "❌", "UNKNOWN": "⚠️", "NOT_RUN": "⛔"}.get(s["verdict"], "?")
        L.append(f"- {mark} {name}: **{s['verdict']}**{('（' + s.get('reason', '') + '）') if s.get('reason') else ''}"
                 + (" ⚠️ 环境与基线不一致（不入基线）" if s.get("baseline_warn") else ""))
    L.append("")
    L.append("## 趋势记录")
    L.append("")
    L.append(f"- {TREND_PATH_DEFAULT}（追加式；本 run 已写 {sum(1 for s in report['suites'].values() if s.get('trend_written'))} 条）")
    L.append("")
    L.append("> 由 retrieval_eval_runner.py 生成（评估统一口径；"
             "NOT_RUN/UNKNOWN 不判 PASS）")
    return "\n".join(L)


def append_trend(path, entries):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


# ── 主流程 ───────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="四库检索评估统一 runner（A 评估统一）")
    ap.add_argument("--suite", choices=["eval-bank", "golden", "rrf", "multi-hop", "all"],
                    default="all", help="评测套件（默认 all=三套全跑；multi-hop 独立）")
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 条 query（测试用）")
    ap.add_argument("--db", default=None, help="检索库路径（默认生产库；沙盒测试指向副本）")
    ap.add_argument("--report-dir", default=REPORT_DIR_DEFAULT, help="报告输出目录")
    ap.add_argument("--trend", default=TREND_PATH_DEFAULT, help="趋势记录 jsonl（追加式）")
    ap.add_argument("--no-trend", action="store_true", help="不写趋势记录")
    ap.add_argument("--gate", action="store_true", help="门禁判定：选中 suite 全 PASS → exit 0；FAIL/UNKNOWN → exit 1")
    ap.add_argument("--multi-hop", action="store_true", help="多跳模式：golden 可答集用概念层多跳检索 + 基准门")
    ap.add_argument("--multi-hop-threshold", type=float, default=HIT_THRESHOLD_DEFAULT,
                    help="多跳基准门命中率阈值（默认 0.5，按 golden 定义 total≥1 且 hit≥阈值）")
    ap.add_argument("--dry-run", action="store_true", help="不落盘（报告/趋势），只打印")
    ap.add_argument("--baseline-db", default=None,
                    help="基线库路径（前置校验：评测前对比总行数/g1 分布，差异超阈值=警告该 run 不入基线）")
    ap.add_argument("--anchor-threshold", type=float, default=None,
                    help="GT 语义锚点判定阈值（默认 0.65，bge 余弦；多跳级锚点专用）")
    ap.add_argument("--no-anchor", action="store_true",
                    help="关闭语义锚点判定（纯字面旧口径，A/B 对照用）")
    args = ap.parse_args()

    # 就绪度先行
    db_path = args.db or DB_DEFAULT
    need_mh = args.multi_hop or args.suite == "multi-hop"
    ready = readiness(need_mh)
    ready_issues = [k for k, v in ready.items()
                    if k != "semantic_layer" and not v["exists"]] + \
                   (["semantic_layer"] if need_mh and not ready["semantic_layer"]["exists"] else [])

    # 导入 l3（设 DB 路径 env 后 import——l3 模块级读 SIKU_DB_PATH）
    os.environ.setdefault("SIKU_ROUTER", "off")
    if args.db:
        os.environ["SIKU_DB_PATH"] = args.db
    sys.path.insert(0, SIKU_SCRIPTS)
    try:
        import l3_retrieval as l3
    except Exception as e:
        l3 = None
        ready_issues.append(f"l3_retrieval 导入失败: {e}")

    want = args.suite
    order = ["eval-bank", "golden", "rrf", "multi-hop"] if want == "all" else [want]
    suites = {}
    top_k = args.top_k
    limit = args.limit

    # 环境快照 + 基线前置校验（根源修复 2026-08-27：评测环境一致性机制——评测前对比，差异超阈值不入基线）
    env_snap = env_snapshot_of(db_path)
    base_snap = env_snapshot_of(args.baseline_db) if args.baseline_db else None
    bc = baseline_check(env_snap, base_snap)
    if args.baseline_db and not bc["ok"]:
        print(f"⚠️ 基线前置校验警告（本 run 不入基线）: {'; '.join(bc['warnings'])}", file=sys.stderr)

    for name in order:
        base = {"verdict": "NOT_RUN"}
        # multi-hop：显式 --suite multi-hop 或 --suite all（--multi-hop 标志已处理 readiness）
        if name == "multi-hop" and args.suite != "multi-hop" and args.suite != "all":
            suites[name] = base
            continue
        if l3 is None or (name == "multi-hop" and not ready["semantic_layer"]["exists"]) or \
           (name in ("eval-bank", "golden", "rrf") and any(k in ready_issues for k in
            ("eval_bank_v2", "golden_set", "golden_neg", "rrf_split_train", "rrf_split_val"))):
            suites[name] = {"verdict": "UNKNOWN",
                            "reason": "就绪度不足" + (f": {ready_issues}" if ready_issues else "")}
            continue
        try:
            if name == "eval-bank":
                summary, detail = run_eval_bank(l3, top_k, limit,
                                                anchor_threshold=args.anchor_threshold,
                                                no_anchor=args.no_anchor)
                threshold = 0.8  # eval_retrieval.py 验收阈值（推广态基准 hit_rate≥0.8）
                verdict = "PASS" if summary["hit_rate"] >= threshold and summary["total"] > 0 else "FAIL"
                suites[name] = {"summary": summary, "detail": detail, "verdict": verdict,
                                "threshold": threshold}
            elif name == "golden":
                summary, phase, neg = run_golden(l3, db_path, top_k, limit, GOLDEN_NEG)
                threshold = HIT_THRESHOLD_DEFAULT
                verdict = "PASS" if summary["hit_rate"] >= threshold and summary["n"] > 0 else "FAIL"
                suites[name] = {"summary": summary, "phase": phase, "neg": neg,
                                "verdict": verdict, "threshold": threshold}
            elif name == "rrf":
                summary = run_rrf_split(l3, db_path, top_k, limit)
                suites[name] = {"summary": summary, "verdict": "PASS"}  # rrf 套=数据快照，无独立门禁（门禁在 weekly-tune/monthly-check）
            elif name == "multi-hop":
                summary = run_multi_hop_gate(l3, db_path, top_k, limit, args.multi_hop_threshold)
                suites[name] = {"summary": summary, "detail": summary.pop("detail"),
                                "verdict": summary["verdict"], "threshold": summary["threshold"]}
        except Exception as e:
            suites[name] = {"verdict": "UNKNOWN", "reason": f"运行异常: {e}"}

    # 环境与基线不一致 → 实际运行 suite 标注 baseline_warn（不入基线；门禁汇总可见）
    # P1 修复 2026-08-27：仅 --baseline-db 启用时才标注——默认模式（无基线库）bc.ok=False 是降级态，不得误标
    if args.baseline_db and not bc["ok"]:
        for n in order:
            if suites[n]["verdict"] != "NOT_RUN":
                suites[n]["baseline_warn"] = True

    # 趋势记录（追加式；失败/异常也记录——如实）
    # P1-①：--dry-run 抑制趋势写入（dry-run 语义=不落盘：报告/趋势均不写，仅构造展示）
    trend_entries = []
    if not args.no_trend and not args.dry_run:
        for name in order:
            s = suites[name]
            if s["verdict"] == "NOT_RUN":
                continue
            entry = {"ts": now_iso(), "suite": name, "top_k": top_k, "limit": limit,
                     "verdict": s["verdict"], "task_id": TASK_ID,
                     "weights": dict(getattr(l3, "CHANNEL_WEIGHTS", {})) if l3 else {}}
            if args.baseline_db:
                entry["baseline_ok"] = bc["ok"]
            if name == "eval-bank" and s["verdict"] != "UNKNOWN":
                entry.update({"hit_rate": s["summary"]["hit_rate"], "n": s["summary"]["total"]})
            elif name == "golden" and s["verdict"] != "UNKNOWN":
                entry.update({"hit_rate": s["summary"]["hit_rate"],
                              "top1_hit_rate": s["summary"]["top1_hit_rate"],
                              "mean_ndcg_valid": s["summary"]["mean_ndcg_valid"],
                              "mean_p5_valid": s["summary"]["mean_p5_valid"], "n": s["summary"]["n"],
                              "negative_answered_rate": s["neg"]["answered_rate"]})
            elif name == "rrf" and s["verdict"] != "UNKNOWN":
                entry.update({"train_hit_rate": s["summary"]["train"]["hit_rate"],
                              "val_hit_rate": s["summary"]["val"]["hit_rate"],
                              "train_ndcg": s["summary"]["train"]["mean_ndcg_valid"],
                              "val_ndcg": s["summary"]["val"]["mean_ndcg_valid"]})
            elif name == "multi-hop" and s["verdict"] != "UNKNOWN":
                entry.update({"hit_rate": s["summary"]["hit_rate"], "n": s["summary"]["n"],
                              "complex_n": s["summary"]["complex_n"],
                              "threshold": s["summary"]["threshold"]})
            if s["verdict"] == "UNKNOWN":
                entry["reason"] = s.get("reason", "")
            trend_entries.append(entry)
        try:
            append_trend(args.trend, trend_entries)
            for e in trend_entries:
                suites[e["suite"]]["trend_written"] = True
        except Exception as e:
            print(f"⚠️ 趋势写入失败: {e}", file=sys.stderr)

    report = {
        "runner": "retrieval_eval_runner.py", "task_id": TASK_ID, "time": now_iso(),
        "suite": args.suite, "multi_hop": need_mh, "db": db_path,
        "db_rows": db_row_count(db_path), "readiness": ready,
        "env_snapshot": env_snap, "baseline_check": bc,
        "suites": suites, "order": order,
    }
    # gate 判定：仅实际运行的 suite 参与（NOT_RUN 不参与——未选中不等于失败）
    judged = [n for n in order if suites[n]["verdict"] != "NOT_RUN"]
    all_pass = bool(judged) and all(suites[n]["verdict"] == "PASS" for n in judged)
    report["gate"] = {"enabled": args.gate, "judged": judged,
                      "verdicts": {k: v["verdict"] for k, v in suites.items()},
                      "summary": "all_pass" if args.gate and all_pass else
                                 ("has_fail" if args.gate else "disabled")}

    # 落盘（dry-run 只打印）
    os.makedirs(args.report_dir, exist_ok=True)
    day = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = os.path.join(args.report_dir, f"eval_report_{day}.json")
    out_md = os.path.join(args.report_dir, f"eval_report_{day}.md")
    if not args.dry_run:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=1)
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(render_md(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=1)[:4000])

    print(f"报告: {out_md}")
    print(f"报告 JSON: {out_json}")
    for name in order:
        s = suites[name]
        mark = {"PASS": "✅", "FAIL": "❌", "UNKNOWN": "⚠️", "NOT_RUN": "⛔"}[s["verdict"]]
        print(f"  {mark} {name}: {s['verdict']}"
              + (f"（{s.get('reason','')}）" if s.get('reason') else ""))
    if trend_entries:
        if args.dry_run:
            print(f"趋势记录: dry-run 未写入（{args.trend} 保持原样），本 run 构造 {len(trend_entries)} 条")
        elif args.no_trend:
            print(f"趋势记录: --no-trend 未写入，本 run 构造 {len(trend_entries)} 条")
        else:
            print(f"趋势记录: {args.trend} +{len(trend_entries)} 条（追加）")

    if args.gate:
        failed = [n for n in order if suites[n]["verdict"] in ("FAIL", "UNKNOWN")]
        return 1 if failed else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
