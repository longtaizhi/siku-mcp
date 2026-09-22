#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RRF 金标周度监控（no_agent 纯脚本 0 token）——金标 top5 命中率统计 + 阈值预警 + 报告落盘。

背景（②，2026-08-16）：
  每周跑一次 golden 全量 query（读 golden 集 + 复用 l3_retrieval 只读检索），统计：
    - top5 命中率（可答 query 中 set(top5) ∩ set(expected_valid) 非空 的占比）
    - top1 命中率 / 均值 NDCG@5 / 均值 P@5（valid 口径，与 rrf_monthly_check.py 同口径）
    - 负样本（unanswerable=true）被作答率（幻觉信号，R3 卡  口径）
  命中率低于阈值（默认 0.5）→ stdout 输出 JSON 预警证据（cron deliver 通知）；
  报告落盘 JSON + MD（rrf_golden_monitor_YYYYMMDD.json / .md），周度去重防重复触发。

只读纪律：复用 l3 检索（写 query_cache 属 l3 既有行为，同月度复查）；不删缓存、不建卡；
唯一写入 = 报告文件 + 日志 JSONL append。

参数：
  --force  忽略周度去重标记强制重跑（同日报告已存在 → 默认静默跳过）
  --limit N 只跑前 N 条 query（测试用；默认全量）
环境变量（全部可选）：
  SIKU_GOLDEN    golden 路径（默认 $SIKU_GOLDEN_DIR/golden-set-20260823.json）
  SIKU_DB_PATH   memory_store.db（默认 $SIKU_ROOT/memory_store.db）
  SIKU_GOLDEN_OUT 报告输出目录（默认 golden 同目录）
  SIKU_GOLDEN_HIT_THRESHOLD  命中率预警阈值（默认 0.5）
  SIKU_LOG        运行日志 JSONL（默认 <out>/rrf-golden-monitor-log.jsonl）

用法：
  $SIKU_VENV_PYTHON rrf_golden_monitor.py            # 周度检查
  $SIKU_VENV_PYTHON rrf_golden_monitor.py --force    # 强制重跑
"""
import argparse
import datetime
import json
import os
import sqlite3
import sys

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_GOLDEN_DIR = os.environ.get("SIKU_GOLDEN_DIR", os.path.join(_SIKU_ROOT, "golden"))  # 评测 golden 集目录

GOLDEN_DEFAULT = os.path.join(_GOLDEN_DIR, "golden-set-20260823.json")
DB_DEFAULT = os.path.join(_SIKU_ROOT, "memory_store.db")
HIT_THRESHOLD_DEFAULT = 0.5  # top5 命中率预警阈值（低于则预警）


def _now_iso():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def ndcg_at5(ranked_ids, relevant):
    """NDCG@5（与 rrf_monthly_check.py 同款）。"""
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


def _dep_ids(db_path):
    try:
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        ids = {r[0] for r in c.execute("SELECT id FROM memory_store WHERE deprecated=1")}
        c.close()
        return ids
    except Exception:
        return set()


def _db_row_count(db_path):
    try:
        c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        n = c.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
        c.close()
        return n
    except Exception:
        return None


def load_golden(path):
    with open(path, encoding="utf-8") as f:
        golden = json.load(f)
    return golden["queries"], golden.get("meta", {})


def run_queries(golden_path, db_path, limit):
    """复用 l3_retrieval 只读检索跑 golden query，返回 (phase, neg_phase, weights, stats)。"""
    os.environ.setdefault("SIKU_ROUTER", "off")
    os.environ["SIKU_DB_PATH"] = db_path
    sys.path.insert(0, os.path.join(_SIKU_ROOT, "scripts"))
    import l3_retrieval as l3

    weights = dict(l3.CHANNEL_WEIGHTS)
    dep_ids = _dep_ids(db_path)
    queries, _gmeta = load_golden(golden_path)
    if limit:
        queries = queries[:limit]
    answer_q = [q for q in queries if not q.get("unanswerable")]
    neg_q = [q for q in queries if q.get("unanswerable")]

    # 预热（加载 jieba/embedding，不计结果）
    try:
        l3.search_memories("预热 检索 通道", mode="auto", top_k=5, tier="compact")
    except Exception:
        pass

    phase = []
    for q in answer_q:
        qid, query = q["id"], q["query"]
        exp = q.get("expected_top5", [])
        try:
            r = l3.search_memories(query, mode="auto", top_k=5, tier="compact")
            top5 = [x["id"] for x in r.get("results", [])]
            mode = r.get("search_mode", "?")
        except Exception as e:
            top5, mode = [], f"ERR:{e}"
        rel_valid = {e for e in exp if e not in dep_ids}
        hit = bool(set(top5) & rel_valid)
        top1_hit = bool(top5) and top5[0] in rel_valid
        phase.append({
            "id": qid,
            "query": query[:80],
            "top5": top5,
            "expected": exp,
            "search_mode": mode,
            "hit": hit,
            "top1_hit": top1_hit,
            "ndcg_valid": ndcg_at5(top5, rel_valid),
            "p5_valid": p5_at5(top5, rel_valid),
        })

    neg_phase = []
    for q in neg_q:
        qid, query = q["id"], q["query"]
        try:
            r = l3.search_memories(query, mode="auto", top_k=5, tier="compact")
            results = r.get("results", [])
            top5 = [x["id"] for x in results]
            mode = r.get("search_mode", "?")
            rq = r.get("result_quality")
            top1_score = round(results[0].get("score", 0.0), 4) if results else None
            weak_marked = bool(results) and (
                r.get("result_quality") == "weak"
                or all(x.get("result_quality") == "weak" or x.get("weak_match")
                       for x in results[:5]))
        except Exception as e:
            top5, mode, rq, top1_score, weak_marked = [], f"ERR:{e}", None, None, False
        neg_phase.append({
            "id": qid, "query": query[:80], "top5": top5, "search_mode": mode,
            "answered": len(top5) > 0 and not weak_marked,
            "result_quality": rq, "top1_score": top1_score, "weak_marked": weak_marked,
        })

    stats = {"weights": weights, "graph_on": l3.GRAPH_CHANNEL_ENABLED,
             "gate_on": l3.GRAPH_GATE_ENABLED, "per_seed": l3.GRAPH_PER_SEED,
             "rerank": l3.RERANK_ENABLED, "corpus_rows": _db_row_count(db_path),
             "n": len(phase), "neg_n": len(neg_phase)}
    return phase, neg_phase, stats


def summarize(phase):
    n = len(phase)
    return {
        "hit_rate": round(sum(1 for p in phase if p["hit"]) / n, 4) if n else 0.0,
        "top1_hit_rate": round(sum(1 for p in phase if p["top1_hit"]) / n, 4) if n else 0.0,
        "mean_ndcg_valid": round(sum(p["ndcg_valid"] for p in phase) / n, 4) if n else 0.0,
        "mean_p5_valid": round(sum(p["p5_valid"] for p in phase) / n, 4) if n else 0.0,
        "n": n,
    }


def negative_summary(neg_phase):
    n = len(neg_phase)
    answered = sum(1 for p in neg_phase if p.get("answered"))
    return {"n": n, "answered": answered,
            "answered_rate": round(answered / n, 4) if n else 0.0,
            "detail": [{"id": p["id"], "answered": p.get("answered"),
                        "mode": p.get("search_mode"), "top1_score": p.get("top1_score")}
                       for p in neg_phase]}


def write_md_report(out_md, rep, golden_meta, hit_threshold):
    lines = [
        "# RRF 金标周度监控报告",
        "",
        f"- 运行时间: {rep['time']}",
        f"- golden: {os.path.basename(rep['golden_path'])}（total={rep['golden']['total']}，"
        f"可答={rep['golden']['answerable']}，负样本={rep['golden']['negative']}，"
        f"关系型={rep['golden']['relational']}）",
        f"- 环境: 权重 {rep['weights']}，图通道 {'开' if rep['stats']['graph_on'] else '关'}，"
        f"gate={'开' if rep['stats']['gate_on'] else '关'}，per_seed={rep['stats']['per_seed']}，"
        f"rerank={'开' if rep['stats']['rerank'] else '关'}，语料行数={rep['stats'].get('corpus_rows')}",
        "",
        "## 汇总",
        "",
        "| 指标 | 值 |",
        "|---|---|",
        f"| top5 命中率 | **{rep['summary']['hit_rate']:.4f}** |",
        f"| top1 命中率 | {rep['summary']['top1_hit_rate']:.4f} |",
        f"| 均值 NDCG@5 (valid) | {rep['summary']['mean_ndcg_valid']:.4f} |",
        f"| 均值 P@5 (valid) | {rep['summary']['mean_p5_valid']:.4f} |",
        f"| 可答 query 数 | {rep['summary']['n']} |",
        f"| 负样本被作答率 | {rep['negative']['answered']}/{rep['negative']['n']} "
        f"({rep['negative']['answered_rate']:.4f}) |",
        "",
        f"## 预警判定",
        "",
        f"阈值: top5 命中率 < **{hit_threshold}** → 预警",
        "",
        f"**{'⚠️ 命中率低于阈值，触发预警' if rep['below_threshold'] else '✅ 命中率达标'}**",
        "",
    ]
    lines.append("## 最低 NDCG Top10（观察项）")
    lines.append("")
    lines.append("| id | query | ndcg_valid | p5_valid | 命中 |")
    lines.append("|---|---|---|---|---|")
    for p in sorted(rep["detail"], key=lambda x: x["ndcg_valid"])[:10]:
        lines.append(f"| {p['id']} | {p['query'][:40]} | {p['ndcg_valid']:.4f} | "
                     f"{p['p5_valid']:.4f} | {'✅' if p['hit'] else '❌'} |")
    if rep["neg_phase"]:
        lines.append("")
        lines.append("## 负样本被作答明细（幻觉信号）")
        lines.append("")
        lines.append("| id | mode | top1_score |")
        lines.append("|---|---|---|")
        for d in rep["neg_phase"]:
            if d.get("answered"):
                lines.append(f"| {d['id']} | {d.get('search_mode')} | {d.get('top1_score')} |")
    lines.append("")
    lines.append("> 由 rrf_golden_monitor.py 生成（no_agent 周度监控，②）")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def append_log(log_path, entry):
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser(description="RRF 金标周度监控（top5 命中率统计 + 预警 + 报告落盘）")
    ap.add_argument("--force", action="store_true", help="忽略周度去重标记强制重跑")
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 条 query（测试用）")
    args = ap.parse_args()

    golden_path = os.environ.get("SIKU_GOLDEN", GOLDEN_DEFAULT)
    db_path = os.environ.get("SIKU_DB_PATH", DB_DEFAULT)
    out_dir = os.environ.get("SIKU_GOLDEN_OUT", os.path.dirname(os.path.abspath(golden_path)))
    hit_threshold = float(os.environ.get("SIKU_GOLDEN_HIT_THRESHOLD", str(HIT_THRESHOLD_DEFAULT)))
    log_path = os.environ.get("SIKU_LOG", os.path.join(out_dir, "rrf-golden-monitor-log.jsonl"))

    if not os.path.exists(golden_path):
        sys.stderr.write(f"[rrf_golden_monitor] golden 不存在: {golden_path}\n")
        return 2

    today = datetime.datetime.now().strftime("%Y%m%d")
    out_json = os.path.join(out_dir, f"rrf_golden_monitor_{today}.json")
    out_md = os.path.join(out_dir, f"rrf_golden_monitor_{today}.md")

    # 周度去重：同日报告已存在 → 静默跳过（--force 覆盖）
    if os.path.exists(out_json) and not args.force:
        return 0

    queries, golden_meta = load_golden(golden_path)
    phase, neg_phase, stats = run_queries(golden_path, db_path, args.limit)
    summary = summarize(phase)
    neg = negative_summary(neg_phase)
    below_threshold = summary["hit_rate"] < hit_threshold

    report = {
        "time": _now_iso(),
        "golden_path": golden_path,
        "golden": {"total": len(queries),
                   "answerable": sum(1 for q in queries if not q.get("unanswerable")),
                   "negative": sum(1 for q in queries if q.get("unanswerable")),
                   "relational": sum(1 for q in queries if q.get("relational"))},
        "weights": stats["weights"],
        "stats": stats,
        "summary": summary,
        "negative": {"n": neg["n"], "answered": neg["answered"],
                     "answered_rate": neg["answered_rate"],
                     "answered_detail": [f"{d['id']}({d.get('mode')},score={d.get('top1_score')})"
                                         for d in neg["detail"] if d.get("answered")][:20]},
        "hit_threshold": hit_threshold,
        "below_threshold": below_threshold,
        "detail": phase,
        "neg_phase": neg_phase,
    }

    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=1)
        write_md_report(out_md, report, golden_meta, hit_threshold)
    except Exception as e:
        sys.stderr.write(f"[rrf_golden_monitor] 报告落盘失败: {e}\n")

    append_log(log_path, {"time": report["time"], "golden": report["golden"],
                          "hit_rate": summary["hit_rate"], "top1_hit_rate": summary["top1_hit_rate"],
                          "mean_ndcg_valid": summary["mean_ndcg_valid"],
                          "mean_p5_valid": summary["mean_p5_valid"],
                          "negative": {"answered": neg["answered"], "n": neg["n"]},
                          "hit_threshold": hit_threshold, "below_threshold": below_threshold,
                          "out_json": out_json})

    # 预警：命中率低于阈值 → stdout JSON 证据（cron deliver 通知）；达标 → stdout 空静默
    if below_threshold:
        print(json.dumps({"golden_monitor_warning": True, "hit_rate": summary["hit_rate"],
                          "hit_threshold": hit_threshold, "top1_hit_rate": summary["top1_hit_rate"],
                          "mean_ndcg_valid": summary["mean_ndcg_valid"],
                          "mean_p5_valid": summary["mean_p5_valid"],
                          "negative_answered_rate": neg["answered_rate"],
                          "report": out_json}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
