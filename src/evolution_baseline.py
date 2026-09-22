#!/usr/bin/env python3
"""evolution_baseline.py — 进化评估基线收集（0token，交付）

进化感知层数据接口：把三个评分源（importance 每日评分 / capability-eval 技能评测 /
route_weekly_eval 路由周度评估）的量化结果统一落盘到进化基线库：
  $SIKU_ROOT/evolve/baselines/evolution-baseline.json   （追加式 records，按 date+source upsert）
  $SIKU_ROOT/evolve/baselines/importance-<date>.json    （importance 源独立快照）
  $SIKU_ROOT/evolve/baselines/skill-eval/<date>/        （技能评测 report.json 归档）
  $SIKU_ROOT/evolve/baselines/route-weekly-<date>.json  （路由周度指标快照）

用法：
  python3 evolution_baseline.py --source importance [--dry-run]
  python3 evolution_baseline.py --source skill-eval  [--dry-run]
  python3 evolution_baseline.py --source route-weekly [--dry-run]

0token：纯 stdlib（os/sys/json/glob/re/statistics/argparse/datetime），零网络零模型。
幂等：同 date+source 已存在 → upsert 覆盖，不重复追加（防 cron 重跑污染）。
"""
import argparse
import glob
import json
import os
import re
import statistics
import sys
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

BASE = _SIKU_ROOT
CURATION = os.path.join(BASE, "smart", "curation")
BASELINES = os.path.join(BASE, "evolve", "baselines")
INDEX_FILE = os.path.join(BASELINES, "evolution-baseline.json")
ROUTE_LOG_DIR = os.path.join(_HERMES_HOME, "scripts/reflex/logs")
SKILL_EVAL_ARCHIVE = os.path.join(BASELINES, "skill-eval")
# 默认技能评测 report 根（runner.py 产物落点；skill-eval-weekly.py 会归档到 SKILL_EVAL_ARCHIVE）
SKILL_EVAL_DEFAULT_ROOT = os.path.join(_HERMES_HOME, "skills/capability-eval-optimizer/evals/artifacts")
SCHEMA = "evolution-baseline/1.0"


# ── importance 源 ──
def _read_frontmatter_importance(fpath):
    """只读解析 importance 字段，兼容两种存量格式：
    格式A（46/31211）：`---` 包裹的 frontmatter + body → 只取 fm 段；
    格式C（31165/31211）：纯 YAML（顶层键，id: 开头）→ 全文行首匹配。
    失败返回 None。"""
    try:
        with open(fpath, encoding="utf-8") as f:
            raw = f.read()
    except OSError:
        return None
    m = re.match(r"^---\n(.*?)\n---", raw, re.DOTALL)
    if m:
        fm = m.group(1)
    else:
        fm = raw
    mm = re.search(r"^importance:\s*([\d.]+)", fm, re.MULTILINE)
    if not mm:
        return None
    try:
        return float(mm.group(1))
    except ValueError:
        return None


def collect_importance(top_n=50):
    """扫描 curation 全量 importance 分布 + top-N。轻量只读（仅解析 frontmatter 数值）。"""
    yamls = sorted(glob.glob(os.path.join(CURATION, "*/*.yaml")))
    pairs = []  # (score, fpath)
    for fpath in yamls:
        v = _read_frontmatter_importance(fpath)
        if v is not None:
            pairs.append((v, fpath))
    if not pairs:
        return {"total_files": len(yamls), "scored": 0, "error": "no importance found"}
    vals = [v for v, _ in pairs]
    top = sorted(pairs, key=lambda p: -p[0])[:top_n]
    top_ids = [os.path.basename(fp) for _, fp in top]
    ge8 = sum(1 for v in vals if v >= 8)
    ge6 = sum(1 for v in vals if 6 <= v < 8)
    ge4 = sum(1 for v in vals if 4 <= v < 6)
    lt4 = sum(1 for v in vals if v < 4)
    return {
        "total_files": len(yamls),
        "scored": len(vals),
        "mean": round(statistics.mean(vals), 4),
        "min": min(vals), "max": max(vals),
        "p50": round(statistics.median(vals), 4),
        "p90": round(sorted(vals)[int(len(vals) * 0.9) - 1], 4) if len(vals) >= 10 else None,
        "top%d_mean" % top_n: round(statistics.mean(v for v, _ in top), 4),
        "dist_ge8": ge8, "dist_6_8": ge6, "dist_4_6": ge4, "dist_lt4": lt4,
        "top%d_ids" % top_n: top_ids,
    }


# ── skill-eval 源 ──
def _latest_report(candidates):
    """取候选目录下 mtime 最新的 report*.json。"""
    best, best_m = None, -1
    for d in candidates:
        if not os.path.isdir(d):
            continue
        for f in glob.glob(os.path.join(d, "**", "report*.json"), recursive=True):
            m = os.path.getmtime(f)
            if m > best_m:
                best, best_m = f, m
    return best


def collect_skill_eval():
    """读最新 report.json → 指标（asset/gate/各层 passed/total）。"""
    archive = _latest_report([SKILL_EVAL_ARCHIVE, SKILL_EVAL_DEFAULT_ROOT])
    if not archive:
        return {"error": "no report.json found",
                "roots": [SKILL_EVAL_ARCHIVE, SKILL_EVAL_DEFAULT_ROOT]}
    with open(archive, encoding="utf-8") as f:
        try:
            rep = json.load(f)
        except json.JSONDecodeError:
            return {"error": "bad json", "file": archive}
    out = {"file": os.path.relpath(archive, os.path.expanduser("~"))}
    for k in ("schema", "asset", "label", "version"):
        if k in rep:
            out[k] = rep[k]
    if "gate" in rep:
        g = rep["gate"]
        # gate 对象字段名以 decision 为准（capability-eval/2.0 契约），兼容旧版 verdict
        out["gate"] = (g.get("decision") or g.get("verdict")) if isinstance(g, dict) else g
        if isinstance(g, dict) and g.get("holds"):
            out["gate_holds"] = g.get("holds")
    if "run" in rep:
        r = rep["run"]
        if isinstance(r, dict):
            out["run_ts"] = r.get("timestamp") or r.get("ts") or r.get("started_at")
    layers = rep.get("layers")
    if isinstance(layers, list):
        out["layers_total"] = len(layers)
        out["layers_passed"] = sum(1 for L in layers if L.get("status") == "pass" or L.get("passed") is True)
    return out


# ── route-weekly 源 ──
def _latest_route_report():
    fs = sorted(glob.glob(os.path.join(ROUTE_LOG_DIR, "route_weekly_*.md")))
    return fs[-1] if fs else None


def collect_route_weekly():
    """读最新 route_weekly_<date>.md → 提取量化指标。"""
    rep = _latest_route_report()
    if not rep:
        return {"error": "no route_weekly_*.md in %s" % ROUTE_LOG_DIR}
    with open(rep, encoding="utf-8") as f:
        body = f.read()
    m_total = re.search(r"建议总数：\*\*(\d+)\*\*", body)
    m_rate = re.search(r"采纳率（hit/\(hit\+miss\)）：([\d.]+)%", body)
    m_hits = re.search(r"结晶命中总数：\*\*(\d+)\*\*", body)
    m_low = re.search(r"低频提示（命中 ≤\d+ 次）：(.+)|低频提示：(无（无命中|.+)", body)
    _low = (m_low.group(1) if m_low and m_low.group(1) else (m_low.group(2) if m_low else None))
    out = {
        "file": os.path.basename(rep),
        "advice_total": int(m_total.group(1)) if m_total else None,
        "adoption_rate_pct": float(m_rate.group(1)) if m_rate else None,
        "crystal_hits": int(m_hits.group(1)) if m_hits else None,
        "low_freq_entries": _low.strip() if _low else None,
    }
    return out


# ── 落盘 ──
def load_index():
    if os.path.exists(INDEX_FILE):
        try:
            with open(INDEX_FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {"schema": SCHEMA, "records": []}
    return {"schema": SCHEMA, "records": []}


def upsert_record(idx, rec):
    """按 date+source upsert（存在则覆盖，防重复追加）。"""
    recs = [r for r in idx.get("records", [])
            if not (r.get("date") == rec["date"] and r.get("source") == rec["source"])]
    recs.append(rec)
    recs.sort(key=lambda r: (r.get("date", ""), r.get("source", "")))
    idx["records"] = recs
    idx["updated"] = datetime.now().isoformat(timespec="seconds")
    return idx


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser(description="进化评估基线收集（0token）")
    ap.add_argument("--source", choices=["importance", "skill-eval", "route-weekly"], required=True)
    ap.add_argument("--dry-run", action="store_true", help="只打印不落盘")
    args = ap.parse_args()

    now = datetime.now()
    date = now.strftime("%Y%m%d")
    if args.source == "importance":
        metrics = collect_importance()
        snap_path = os.path.join(BASELINES, "importance-%s.json" % date)
        extra = {"file": os.path.relpath(snap_path, os.path.expanduser("~"))}
    elif args.source == "skill-eval":
        metrics = collect_skill_eval()
        extra = {"file": metrics.get("file")}
    else:
        metrics = collect_route_weekly()
        snap_path = os.path.join(BASELINES, "route-weekly-%s.json" % date)
        extra = {"file": metrics.get("file")}

    if metrics.get("error"):
        print("[evolution-baseline] %s 源收集失败: %s" % (args.source, metrics["error"]))
        return 1

    rec = {"date": date, "source": args.source, "metrics": metrics}
    rec.update({k: v for k, v in extra.items() if v})

    if args.dry_run:
        print(json.dumps(rec, ensure_ascii=False, indent=2))
        print("[evolution-baseline dry-run] 未落盘")
        return 0

    if args.source == "importance":
        save_json(snap_path, {"schema": SCHEMA, "date": date, "source": "importance", "metrics": metrics})
    elif args.source == "route-weekly":
        save_json(snap_path, {"schema": SCHEMA, "date": date, "source": "route-weekly", "metrics": metrics})

    idx = upsert_record(load_index(), rec)
    save_json(INDEX_FILE, idx)
    n = len(idx["records"])
    print("[evolution-baseline] %s %s 已写入基线库（records=%d）" % (date, args.source, n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
