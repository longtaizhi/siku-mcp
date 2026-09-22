#!/usr/bin/env python3
import sys, os

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
# 2026-08-14 修复：PYTHONPATH 注入 venv site-packages，系统 python(<3.11) 加载 3.11 编译的 numpy 崩溃
# → embed 通道失效 → 检索降级 fts5_only（69%）。系统 python 跑时自动重执行为 venv python。
if sys.version_info < (3, 11):
    _vp = os.environ.get("SIKU_VENV_PYTHON") or os.path.join(_HERMES_HOME, "hermes-agent/venv/bin/python")
    if os.path.exists(_vp):
        os.execv(_vp, [_vp] + sys.argv)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
eval_retrieval.py — 检索评测（R3 升级：分级题库 + 失败案例留存 + 题库版本化 + 无缝切换六要素）
====================================================================================
题库版本化：
  siku_option/eval_bank_v1.json   原始题库（32 条，抽取自内嵌 EVAL，回滚基线）
  siku_option/eval_bank_v2.json   分级题库（48 条：基础12/多跳20/时间推理8/拒答场景8）
  siku_option/eval_bank_state.json 激活版本 + 切换历史 + 稳定性记录（留痕）
失败留存：
  siku_option/eval_failures/YYYYMMDD_eval_bank_vN.jsonl  （查询/期望GT/实际top1/失败类型）
评测历史：
  siku_option/eval_history.jsonl  每次单版本评测摘要（稳定性监控：连续 3 次波动≤3%）

用法：
  python eval_retrieval.py                    # 默认：state.active_version 激活题库
  python eval_retrieval.py --bank v1|v2       # 指定题库版本
  python eval_retrieval.py --bank both        # 实验态：新旧题库并行对比（共同 query）
  python eval_retrieval.py --bank v2 --coverage   # 覆盖率全库扫描（GT 在库存在性）
  python eval_retrieval.py --switch status    # 当前激活版本 + 切换历史
  python eval_retrieval.py --switch check     # 切换标准判定（需先跑 --bank=both）
  python eval_retrieval.py --switch promote --confirm   # 推广态（维护者确认后执行，留痕）
  python eval_retrieval.py --switch rollback  # 回退（回滚旧题库，留痕）

切换标准（铁律⑤）：
  ① 新旧题库同一批 query（共同子集）命中率差异 ≤5%
  ② 新题库覆盖率 ≥90%（hit 类 GT 在库中全库存在性，--coverage）
  ③ 满足①② → 推送维护者确认 → --switch=promote --confirm 执行切换
  ④ 切换后连续 3 次评测命中率波动 ≤3%，超过 → 告警 + 建议回滚
"""
import argparse
import json
import time
import re
import hashlib
from datetime import datetime

sys.path.insert(0, os.path.join(_SIKU_ROOT, "scripts"))
from l3_retrieval import search_memories  # noqa: E402

SIKU_DIR = os.path.join(_SIKU_ROOT, "scripts/siku_option")
BANK_V1 = os.path.join(SIKU_DIR, "eval_bank_v1.json")
BANK_V2 = os.path.join(SIKU_DIR, "eval_bank_v2.json")
STATE = os.path.join(SIKU_DIR, "eval_bank_state.json")
FAIL_DIR = os.path.join(SIKU_DIR, "eval_failures")
HISTORY = os.path.join(SIKU_DIR, "eval_history.jsonl")

# 拒答场景实义词停用词（判定 top1 是否与 query 相关时剔除）
STOP_WORDS = {
    "帮我", "请", "的", "了", "是", "怎么", "什么", "多少", "吗", "呢", "哪个",
    "写", "做", "推荐", "翻译", "一首", "一部", "一个", "一条", "给我", "成", "今天",
    "明天", "最近", "一下", "如何", "为什么", "要不要", "该不该", "先", "还是", "用",
    "在", "有", "到", "从", "了", "和", "与", "或", "中", "上", "下", "时",
}


def load_bank(version):
    path = BANK_V1 if version == "v1" else BANK_V2
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_state():
    with open(STATE, encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def bank_common(bank_a, bank_b):
    """新旧题库共同 query（以 query 字符串为键，取 v1 侧 case 为准）。"""
    b_map = {c["query"]: c for c in bank_b["cases"]}
    common = []
    for c in bank_a["cases"]:
        if c["query"] in b_map:
            common.append((c, b_map[c["query"]]))
    return common


def extract_content_words(query):
    """提取 query 实义词（中文≥2 字片段 + 英文≥3 字母词），去停用词。"""
    words = re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z]{3,}", query)
    return [w for w in words if w not in STOP_WORDS]


def judge(bank, c, results, top_k=5):
    """判定单条 case。返回 (ok, fail_type, hit_top1)。
    hit 类：top5 含任一 GT 即命中；细分 top1 是否命中（精度）。
    refuse 类：空结果 或 top1 不含 query 实义词 → 合理拒答。
    """
    gts = c.get("gts") or []
    top1 = results[0]["summary"][:120] if results else ""
    text = " ".join(r["summary"] for r in results[:top_k])
    if c["expected"] == "refuse":
        words = extract_content_words(c["query"])
        if not results:
            return True, None, top1
        if not words:
            return True, None, top1
        related = any(w in top1 for w in words)
        if related:
            return False, "refuse_unexpected", top1
        return True, None, top1
    # hit 类
    h = any(gt.lower() in text.lower() for gt in gts) if gts else False  # 口径B：GT 大小写不敏感
    if not h:
        return False, "top5_miss", top1
    h1 = any(gt.lower() in top1.lower() for gt in gts)  # 口径B：GT 大小写不敏感
    if not h1:
        return False, "top1_miss", top1
    return True, None, top1


def coverage_of(bank, conn_db):
    """覆盖率：hit 类 case 的 GT 在库中全库存在性（SQL LIKE 扫描）。
    覆盖率 = 至少一个 GT 在库中存在的 case 数 / hit 类 case 总数。拒答类（负样本）不计入。
    """
    import sqlite3
    db = conn_db or os.path.join(_SIKU_ROOT, "memory_store.db")
    if not os.path.exists(db):
        db = os.path.join(_SIKU_ROOT, "scripts/memory_store.db")
    conn = sqlite3.connect(db)
    total, covered = 0, 0
    detail = []
    for c in bank["cases"]:
        if c["expected"] != "hit":
            continue
        gts = c.get("gts") or []
        total += 1
        ok = False
        found = []
        for gt in gts:
            n = conn.execute("SELECT COUNT(*) FROM memory_store WHERE summary LIKE ? OR content LIKE ?",
                             (f"%{gt}%", f"%{gt}%")).fetchone()[0]
            if n > 0:
                ok = True
                found.append(f"{gt}:{n}")
        detail.append({"id": c["id"], "query": c["query"], "covered": ok, "gt_found": found})
        if ok:
            covered += 1
    conn.close()
    return (covered / total if total else 1.0), covered, total, detail


def append_failures(bank_version, failures, summary):
    """失败案例留存：siku_option/eval_failures/YYYYMMDD_eval_bank_vN.jsonl"""
    os.makedirs(FAIL_DIR, exist_ok=True)
    day = datetime.now().strftime("%Y%m%d")
    path = os.path.join(FAIL_DIR, f"{day}_eval_bank_{bank_version}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        for rec in failures:
            rec["bank"] = bank_version
            rec["ts"] = datetime.now().isoformat(timespec="seconds")
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path


def append_history(bank_version, summary):
    with open(HISTORY, "a", encoding="utf-8") as f:
        summary["bank"] = bank_version
        summary["ts"] = datetime.now().isoformat(timespec="seconds")
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")


def stability_check(bank_version, hit_rate, state):
    """稳定性监控：同版本最近 3 次命中率波动 ≤3%。返回 (ok, rates, msg)。"""
    key = f"stability_{bank_version}"
    rates = state.get("stability", {}).get(key, [])
    rates = (rates + [round(hit_rate, 4)])[-3:]
    state.setdefault("stability", {})[key] = rates
    if len(rates) >= 3:
        wave = max(rates) - min(rates)
        ok = wave <= state["switch_standard"]["stability_wave_max"]
        msg = (f"稳定" if ok else "⚠️ 告警") + f"：最近{len(rates)}次命中率 {rates}，波动 {wave:.1%}（阈值≤{state['switch_standard']['stability_wave_max']:.0%}）"
        return ok, rates, msg
    return True, rates, f"样本不足（{len(rates)}/3），继续积累"


def run_bank(bank, version, top_k=5, do_coverage=False, use_vrf=False):
    """评测单个题库版本。返回摘要 dict + 失败记录列表。
    use_vrf=True：verifier 连续分打分（分数分布输出）。"""
    if use_vrf:  # 延迟导入（失败不阻断评测）
        try:
            sys.path.insert(0, os.path.join(_HERMES_HOME, "scripts"))
            import verifier as _vrf
        except Exception as e:
            print(f"⚠️ verifier 导入失败，跳过连续分: {e}")
            _vrf = None
    else:
        _vrf = None
    search_memories("预热预热", top_k=5)
    time.sleep(0.2)
    total_hit, n, lats = 0, 0, []
    level_stats = {}
    failures = []
    vrf_scores = []
    for c in bank["cases"]:
        t0 = time.time()
        out = search_memories(c["query"], top_k=top_k)
        lat = time.time() - t0
        lats.append(lat)
        results = out.get("results", []) if isinstance(out, dict) else []
        ok, fail_type, top1 = judge(bank, c, results, top_k)
        if _vrf is not None:
            s, _how = _vrf.continuous_score(c["query"], results, mode="top1")
            vrf_scores.append(s)
        n += 1
        total_hit += ok
        ls = level_stats.setdefault(c["level"], {"ok": 0, "n": 0})
        ls["n"] += 1
        ls["ok"] += ok
        mark = "✓" if ok else "✗"
        print(f"[{mark}] [{c['level']}] {c['query']} | GT:{'/'.join(c.get('gts') or ['拒答'])} | {lat:.2f}s | top1:{top1[:40]}")
        if not ok:
            rec = {
                "case_id": c["id"], "level": c["level"], "query": c["query"],
                "expected_gts": c.get("gts") or [], "expected": c["expected"],
                "actual_top1": top1, "fail_type": fail_type,
                "latency": round(lat, 3),
            }
            failures.append(rec)
    hit_rate = total_hit / n if n else 0
    summary = {
        "version": version, "total": n, "hit": total_hit,
        "hit_rate": round(hit_rate, 4),
        "avg_latency": round(sum(lats) / len(lats), 3),
        "max_latency": round(max(lats), 3),
        "levels": {k: {"ok": v["ok"], "n": v["n"], "rate": round(v["ok"] / v["n"], 4)} for k, v in level_stats.items()},
    }
    if do_coverage:
        cov, cov_ok, cov_total, cov_detail = coverage_of(bank, None)
        summary["coverage"] = {"rate": round(cov, 4), "covered": cov_ok, "total": cov_total}
        print(f"\n覆盖率({version}): {cov_ok}/{cov_total} = {cov:.0%} (验收≥90%)")
        for d in cov_detail:
            if not d["covered"]:
                print(f"  ⚠️ 未覆盖: {d['id']} {d['query']} (GT 库中无: {d['gt_found']})")
    if vrf_scores:  # verifier 连续分分布
        sv = sorted(vrf_scores)
        nv = len(sv)
        dist = {"n": nv, "min": sv[0], "max": sv[-1],
                "mean": round(sum(sv) / nv, 4),
                "p25": round(sv[nv // 4], 4), "p50": round(sv[nv // 2], 4),
                "p75": round(sv[3 * nv // 4], 4)}
        summary["verifier_scores"] = dist
        print(f"\n=== verifier 连续分分布（{nv} 条，top1 粒度，[0,1]）===")
        print(f"  min={dist['min']:.3f} p25={dist['p25']:.3f} p50={dist['p50']:.3f} "
              f"p75={dist['p75']:.3f} max={dist['max']:.3f} mean={dist['mean']:.3f}")
    print(f"\n=== {version} 命中率: {total_hit}/{n} = {hit_rate:.0%} ===")
    print(f"延迟: 平均 {summary['avg_latency']:.2f}s, 最慢 {summary['max_latency']:.2f}s (验收≤2s)")
    for k, v in level_stats.items():
        print(f"  分级[{k}]: {v['ok']}/{v['n']} = {v['ok']/v['n']:.0%}")
    return summary, failures


def compare_banks(bank1, bank2, top_k=5):
    """实验态：新旧题库并行对比（共同 query 子集）。"""
    common = bank_common(bank1, bank2)
    if not common:
        print("无共同 query，无法对比")
        return None
    print(f"=== 并行对比（共同 query {len(common)} 条）===")
    search_memories("预热预热", top_k=5)
    time.sleep(0.2)
    h1 = h2 = 0
    rows = []
    for c1, c2 in common:
        out1 = search_memories(c1["query"], top_k=top_k)
        out2 = search_memories(c2["query"], top_k=top_k)
        r1 = out1.get("results", []) if isinstance(out1, dict) else []
        r2 = out2.get("results", []) if isinstance(out2, dict) else []
        ok1, _, _ = judge(bank1, c1, r1, top_k)
        ok2, _, _ = judge(bank2, c2, r2, top_k)
        h1 += ok1
        h2 += ok2
        if ok1 != ok2:
            rows.append((c1["query"], ok1, ok2))
            print(f"  [差异] {c1['query']}: v1={'✓' if ok1 else '✗'} v2={'✓' if ok2 else '✗'}")
    rate1, rate2 = h1 / len(common), h2 / len(common)
    diff = abs(rate1 - rate2)
    std = {"hit_diff_max": 0.05, "coverage_min": 0.90}
    print(f"\nv1 命中率: {h1}/{len(common)} = {rate1:.0%}")
    print(f"v2 命中率: {h2}/{len(common)} = {rate2:.0%}")
    print(f"差异: {diff:.1%} (标准 ≤{std['hit_diff_max']:.0%}) → {'✅ 达标' if diff <= std['hit_diff_max'] else '❌ 超限'}")
    return {"common": len(common), "v1_hit": rate1, "v2_hit": rate2, "diff": diff,
            "diff_ok": diff <= std["hit_diff_max"], "diff_rows": rows}


def cmd_switch(args, state):
    action = args.switch
    if action == "status":
        print(f"激活题库: {state['active_version']}")
        for h in state["switch_history"]:
            print(f"  {h.get('ts')} [{h['action']}] {h.get('from', '-')} → {h.get('to', '-')} | {h.get('basis', '')} | 确认:{h.get('confirm', '-')}")
        for k, v in state.get("stability", {}).items():
            print(f"  稳定性[{k}]: {v}")
        return 0
    if action == "check":
        comp = state.get("last_compare")
        cov = state.get("last_coverage", {}).get("v2")
        if not comp:
            print("❌ 无对比数据：请先运行 python eval_retrieval.py --bank=both --coverage")
            return 1
        ok = comp.get("diff_ok") and cov and cov >= state["switch_standard"]["coverage_min"]
        print(f"切换标准判定（铁律⑤，需推送维护者确认）:")
        print(f"  ① 新旧命中率差异: {comp['diff']:.1%} (≤{state['switch_standard']['hit_diff_max']:.0%}) → {'✅' if comp['diff_ok'] else '❌'}")
        print(f"  ② 新题库覆盖率: {cov:.0%} (≥{state['switch_standard']['coverage_min']:.0%}) → {'✅' if cov and cov >= state['switch_standard']['coverage_min'] else '❌'}")
        print(f"  结论: {'✅ 达标，推送维护者确认后可 promote' if ok else '❌ 未达标，禁止切换'}")
        return 0 if ok else 1
    if action == "promote":
        comp = state.get("last_compare")
        cov = state.get("last_coverage", {}).get("v2")
        if not (comp and comp.get("diff_ok") and cov and cov >= state["switch_standard"]["coverage_min"]):
            print("❌ 切换标准未满足（先 --bank=both --coverage，再 --switch=check）。禁止 promote。")
            return 1
        if not args.confirm:
            print("❌ 需维护者确认：请推送对比数据给维护者，获确认后加 --confirm 执行。")
            return 1
        old = state["active_version"]
        state["active_version"] = "v2"
        state["switch_history"].append({
            "ts": datetime.now().isoformat(timespec="seconds"), "action": "promote",
            "from": old, "to": "v2",
            "basis": f"新旧命中率差异 {comp['diff']:.1%}≤{state['switch_standard']['hit_diff_max']:.0%}；新题库覆盖率 {cov:.0%}≥{state['switch_standard']['coverage_min']:.0%}；共同 query {comp['common']} 条",
            "confirm": "维护者确认",
        })
        save_state(state)
        print(f"✅ 已切换：{old} → v2（推广态：v2 为验收基准）。留痕已写 eval_bank_state.json")
        return 0
    if action == "rollback":
        old = state["active_version"]
        state["active_version"] = "v1"
        state["switch_history"].append({
            "ts": datetime.now().isoformat(timespec="seconds"), "action": "rollback",
            "from": old, "to": "v1", "basis": "回退请求（数据不丢，旧题库为基线）", "confirm": "维护者",
        })
        save_state(state)
        print(f"↩️ 已回退：{old} → v1。留痕已写 eval_bank_state.json")
        return 0
    print(f"未知 switch 动作: {action}")
    return 1


def main():
    ap = argparse.ArgumentParser(description="检索评测（R3 分级题库版）")
    ap.add_argument("--bank", choices=["v1", "v2", "both", "auto"], default="auto", help="题库版本（auto=state 激活版）")
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--coverage", action="store_true", help="覆盖率全库扫描（GT 在库存在性）")
    ap.add_argument("--verifier", action="store_true",
                    help="verifier 连续分打分并输出分数分布")
    ap.add_argument("--switch", choices=["status", "check", "promote", "rollback"], help="版本切换动作")
    ap.add_argument("--confirm", action="store_true", help="promote 确认（维护者确认后使用）")
    ap.add_argument("--no-history", action="store_true", help="不写评测历史/稳定性（只跑不改）")
    args = ap.parse_args()

    state = load_state()

    if args.switch:
        return cmd_switch(args, state)

    version = state["active_version"] if args.bank == "auto" else args.bank

    if args.bank == "both":
        bank1, bank2 = load_bank("v1"), load_bank("v2")
        comp = compare_banks(bank1, bank2, top_k=args.top_k)
        cov, cov_ok, cov_total, _ = coverage_of(bank2, None)
        print(f"新题库覆盖率(v2): {cov_ok}/{cov_total} = {cov:.0%} (验收≥90%)")
        state["last_compare"] = comp
        state["last_coverage"] = {"v2": round(cov, 4)}
        save_state(state)
        print(f"对比结果已存 state（供 --switch=check/promote 使用）")
        return 0

    bank = load_bank(version)
    summary, failures = run_bank(bank, version, top_k=args.top_k, do_coverage=args.coverage,
                                 use_vrf=args.verifier)

    # 失败案例留存（自动）
    if failures:
        path = append_failures(version, failures, summary)
        print(f"\n⚠️ 失败案例 {len(failures)} 条已留存: {path}")
        print(f"  失败类型: { {ft: sum(1 for r in failures if r['fail_type']==ft) for ft in {r['fail_type'] for r in failures}} }")
    else:
        print(f"\n✅ 零失败，无需留存")

    # 稳定性监控
    if not args.no_history:
        append_history(version, summary)
        ok, rates, msg = stability_check(version, summary["hit_rate"], state)
        save_state(state)
        print(f"稳定性({version}): {msg}")

    # 验收判定（推广态基准）
    std_hit = 0.8
    ok_total = summary["hit_rate"] >= std_hit
    print(f"\n通过: {'✅ 全部达标' if ok_total and summary['avg_latency'] <= 2.0 else '❌ 未达标'}")
    return 0 if ok_total else 1


if __name__ == "__main__":
    sys.exit(main())
