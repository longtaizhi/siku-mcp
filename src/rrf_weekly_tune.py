#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RRF 周度网格重调脚本（no_agent 纯脚本 0 token）——

背景（RRF自动微调-方案-20260813 推荐主干=候选③ 周期网格重调）：
  每周日 no_agent cron：train 网格扫描 → val 验证 → 零退化门禁（val ndcg 不降）
  → 达标更新 CHANNEL_WEIGHTS（l3_retrieval.py 权重表）+ 通知维护者；不达标静默留日志。

流程：
  0. golden split 自愈（内联 auto-split，方案②）：split 缺失或 golden-set mtime 晚于 split
     → 从最新 golden-set-*.json（golden 目录）固定种子 20260823 80/20 重生成 train/val（可复现）
  1. 克隆生产 DB 到工作目录（cp -c 秒级，零污染生产），网格在克隆库上跑
  2. train 网格（单进程循环，图关 SIKU_GRAPH_CHANNEL=0 生产形态，每组合前清 query_cache）
     = 基线 (1,1) + fts5 档 × emb 档叉积（默认 1.3/1.5/1.7 × 0.6/0.7/0.8，9+1 组合）
  3. train 最优 → val 验证（候选 vs 当前权重，均当次跑测，冷启动）
  4. 零退化门禁：cand.val_ndcg >= cur.val_ndcg 且退化条数=0（逐 query 对比）
     4b. 历史回归用例集校验（防 8-28 假过重演）：从 rrf-monthly-log.jsonl
         regress_detail 动态积累历史回归 qid（历次月测退化记录并集，不只硬编码 6 条）→ 从最新
         golden-set 取这些 qid 的 query 定义 → 候选 vs 当前当次跑测逐 qid 对比 → 任一回归 qid
         退化（ndcg 低于当前）即 gate_fail。dry_run 同样受门禁（判据计算在 dry-run 分支前）。
  5. 达标 → 备份 l3_retrieval.py（.bak-rrfweekly-<日期>）→ 文件更新 L47 CHANNEL_WEIGHTS
     → md5 双份核对 + diff 仅目标块 → audit 痕迹 → 建卡通知维护者（幂等）→ stdout JSON 证据
  6. 不达标/无变化 → stdout 空静默退出（0 输出 0 建卡 EXIT=0），运行日志留档可查

参数：
  --dry-run    只跑不改（不备份不更新不建卡，达标时仍输出 JSON 证据 + 日志）
  --force      忽略"当周已跑"去重标记，强制重跑（默认同一天只跑一次，防止 cron 重复触发）
  --fts5-grid  默认 "1.3,1.5,1.7"
  --emb-grid   默认 "0.6,0.7,0.8"
  --no-baseline 不自动加 (1,1) 基线组合

环境变量（全部可选）：
  SIKU_DB_PATH       检索库路径（默认自动克隆生产库到工作目录；测试时指向测试副本）
  SIKU_GOLDEN_DIR    golden 目录（默认 $SIKU_GOLDEN_DIR；auto-split 源 + 默认 split 落点）
  SIKU_GOLDEN_TRAIN  train golden（默认 <SIKU_GOLDEN_DIR>/rrf_golden_split_train.json；显式指定时跳过 auto-split）
  SIKU_GOLDEN_VAL    val golden（默认 <SIKU_GOLDEN_DIR>/rrf_golden_split_val.json；显式指定时跳过 auto-split）
  RRF_L3_PATH        l3_retrieval.py 路径（默认 $SIKU_ROOT/scripts/l3_retrieval.py，更新目标）
  RRF_WORKDIR        工作目录（默认 $SIKU_GOLDEN_DIR/rrf_weekly_work）
  SIKU_KANBAN_DB     看板库路径（默认 $HERMES_HOME/kanban/boards/default/kanban.db）
  RRF_CURRENT_WEIGHTS 覆盖当前权重解析（如 "1.5,0.7"；默认从 l3 文件解析）
  RRF_NOTIFY_TITLE   建卡标题（默认 "RRF 周度权重更新待复审"）
  RRF_MONTHLY_LOG    历史回归集数据源（默认 <SIKU_GOLDEN_DIR>/rrf-monthly-log.jsonl；回归集=该文件
                     regress_detail 历次记录 qid 并集——动态积累，文件缺失仅告警不阻断）

用法（cron wrapper 调用 canonical）：
  $SIKU_VENV_PYTHON $SIKU_ROOT/scripts/rrf_weekly_tune.py
"""
import argparse
import datetime
import glob
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
_GOLDEN_DIR = os.environ.get("SIKU_GOLDEN_DIR", os.path.join(_SIKU_ROOT, "golden"))  # 评测 golden 集目录

# ── 常量（env 可覆盖）──────────────────────────────────────────────
GOLDEN_DIR_DEFAULT = _GOLDEN_DIR
GOLDEN_TRAIN_DEFAULT = os.path.join(GOLDEN_DIR_DEFAULT, "rrf_golden_split_train.json")
GOLDEN_VAL_DEFAULT = os.path.join(GOLDEN_DIR_DEFAULT, "rrf_golden_split_val.json")
# 内联 auto-split（方案②）：缺/过期自动从最新 golden-set 重生成 train/val
AUTO_SPLIT_SEED = 20260823   # 固定种子（可复现铁证：同 seed 同 golden → 逐位一致）
AUTO_SPLIT_RATIO = 0.8       # 80/20 划分
L3_DEFAULT = os.path.join(_SIKU_ROOT, "scripts/l3_retrieval.py")
PROD_DB = os.path.join(_SIKU_ROOT, "memory_store.db")
WORKDIR_DEFAULT = os.path.join(_GOLDEN_DIR, "rrf_weekly_work/")
KANBAN_DEFAULT = os.path.join(_HERMES_HOME, "kanban", "boards", "default", "kanban.db")
NOTIFY_TITLE_DEFAULT = "RRF 周度权重更新待复审"
# 历史回归用例集数据源：月测日志 regress_detail 历次记录并集
MONTHLY_LOG_DEFAULT = os.path.join(GOLDEN_DIR_DEFAULT, "rrf-monthly-log.jsonl")
# 硬化②（precision-monitor 互斥）：监控紧急翻回后写此标记 → 本脚本跳过当次调参，防撤销调参成果
BLOCK_MARK_DEFAULT = os.path.join(_SIKU_ROOT, "scripts/.rrf_tune_blocked")
WEIGHT_LINE_RE = re.compile(r'CHANNEL_WEIGHTS\s*=\s*\{[^}]*\}.*')


def _now_iso():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _log(msg, log_path):
    line = f"[{_now_iso()}] {msg}"
    print(line, file=sys.stderr)  # 日志同步到 stderr（cron deliver=local 静默可见）
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ── golden split 内联 auto-split（方案②：不新建脚本不新增任务）──────
def latest_golden_set(golden_dir):
    """golden_dir 下 golden-set-*.json 最新 mtime 者。返回 (路径, mtime)；无 → (None, 0.0)。"""
    files = glob.glob(os.path.join(golden_dir, "golden-set-*.json"))
    if not files:
        return None, 0.0
    best = max(files, key=lambda p: os.path.getmtime(p))
    return best, os.path.getmtime(best)


def qid_hash(seed, qid):
    return hashlib.sha256(f"{seed}:{qid}".encode("utf-8")).hexdigest()


def load_history_regress_qids(monthly_log_path, log_path=None):
    """从 rrf-monthly-log.jsonl 动态积累历史回归用例 qid。

    历次月测记录的 regress_detail（[[qid, base_ndcg, cur_ndcg], ...]）中出现的 qid 并集，
    按首次出现顺序去重返回——只积累名单，不比历史 ndcg（库漂移下绝对参照不可靠，
    校验以候选 vs 当前当次跑测为准，与 val 门禁同构）。

    文件缺失 → 告警并返回 []（无历史回归记录=无校验项；fail-open 但显式留痕防数据源静默丢失）。
    """
    if not os.path.exists(monthly_log_path):
        msg = (f"[regress] 月测日志不存在: {monthly_log_path}（历史回归集=∅，跳过回归校验；"
               f"若日志被误删将丢失回归记忆，请核实）")
        if log_path:
            _log(msg, log_path)
        else:
            print(msg, file=sys.stderr)
        return []
    qids, seen = [], set()
    n_records = 0
    with open(monthly_log_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            for row in rec.get("regress_detail", []) or []:
                if isinstance(row, (list, tuple)) and row:
                    qid = str(row[0]).strip()
                    if qid and qid not in seen:
                        seen.add(qid)
                        qids.append(qid)
                n_records += 1
    if log_path:
        _log(f"[regress] 月测日志 {os.path.basename(monthly_log_path)}: "
             f"regress_detail 记录 {n_records} 条 → 历史回归 qid {len(qids)} 个 {qids}", log_path)
    return qids


def load_history_regress_queries(monthly_log_path, golden_set_path, log_path=None, qids=None):
    """把历史回归 qid 名单映射为可跑测的 query 条目列表（从最新 golden-set 全量取定义）。

    返回 (queries, unmatched)：queries=有定义的回归用例（含 id/query/expected_top5）；
    unmatched=在最新 golden-set 中找不到的 qid（golden 演进主动删除=用例退役，跳过但留痕）。
    qids 可预传（避免重复读日志）；None 时内部加载。
    """
    if qids is None:
        qids = load_history_regress_qids(monthly_log_path, log_path)
    if not qids:
        return [], []
    with open(golden_set_path, encoding="utf-8") as f:
        gs = json.load(f)
    queries = gs.get("queries") if isinstance(gs, dict) else gs
    by_id = {q["id"]: q for q in queries}
    matched, unmatched = [], []
    for qid in qids:
        if qid in by_id:
            matched.append(by_id[qid])
        else:
            unmatched.append(qid)
    if log_path:
        if unmatched:
            _log(f"[regress] qid 不在最新 golden-set（用例退役，跳过）: {unmatched}", log_path)
        _log(f"[regress] 回归用例集: 匹配 {len(matched)} 条 {[q['id'] for q in matched]}", log_path)
    return matched, unmatched


def auto_split_golden(golden_set_path, train_path, val_path,
                      seed=AUTO_SPLIT_SEED, train_ratio=AUTO_SPLIT_RATIO):
    """按固定种子 80/20 从最新 golden-set 重生成 train/val（结构=golden 条目列表，与 golden-set queries 一致）。

    确定性（与 rrf_golden_split.py 同法）：sha256(f"{seed}:{qid}") 哈希升序 → 前 train_ratio 为 train；
    同 query 不跨集（防泄漏）。返回 (train_n, val_n)。"""
    with open(golden_set_path, encoding="utf-8") as f:
        golden = json.load(f)
    queries = golden["queries"]
    qids = [q["id"] for q in queries]
    if len(set(qids)) != len(qids):
        raise ValueError(f"golden 存在重复 query id: {len(qids)} vs {len(set(qids))}")
    n = len(qids)
    n_train = int(round(n * train_ratio))
    if n_train < 1 or n_train >= n:
        raise ValueError(f"train 数非法: {n_train}")
    ordered = sorted(qids, key=lambda qid: qid_hash(seed, qid))
    train_ids, val_ids = set(ordered[:n_train]), set(ordered[n_train:])
    if train_ids & val_ids or (train_ids | val_ids) != set(qids):
        raise ValueError("split 防泄漏校验失败（同 query 跨集或未全覆盖）")
    train_q = [q for q in queries if q["id"] in train_ids]
    val_q = [q for q in queries if q["id"] in val_ids]
    for p, qs in ((train_path, train_q), (val_path, val_q)):
        with open(p, "w", encoding="utf-8") as f:
            json.dump(qs, f, ensure_ascii=False, indent=1)
    return len(train_q), len(val_q)


def load_golden_queries(path, label):
    """载入 golden split 的 queries：兼容列表结构（golden 条目列表）与旧 dict 结构（{"queries": [...]}）。"""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "queries" in data:
        return data["queries"]
    raise ValueError(f"{label} 结构未知（期望条目列表或 {{'queries': [...]}}）: {type(data)}")


def parse_current_weights(l3_path):
    """从 l3_retrieval.py L47 解析 CHANNEL_WEIGHTS。返回 dict 或 None。"""
    with open(l3_path, encoding="utf-8") as f:
        for line in f:
            m = re.search(r'CHANNEL_WEIGHTS\s*=\s*\{([^}]*)\}', line)
            if m:
                d = {}
                for k, v in re.findall(r'"(\w+)"\s*:\s*([\d.]+)', m.group(1)):
                    d[k] = float(v)
                return d
    return None


def update_l3_weights(l3_path, weights, log_path):
    """备份先行 + 文件更新 L47 CHANNEL_WEIGHTS 行（diff 仅目标行）。返回 (备份路径, 旧值)。"""
    date = datetime.date.today().strftime("%Y%m%d")
    bak = f"{l3_path}.bak-rrfweekly-{date}"
    old = parse_current_weights(l3_path)
    if not os.path.exists(bak):
        shutil.copy2(l3_path, bak)
    else:
        _log(f"[update] 备份已存在（同日幂等）: {bak}", log_path)
    with open(l3_path, encoding="utf-8") as f:
        content = f.read()
    new_line = (f'CHANNEL_WEIGHTS = {{"fts5": {weights["fts5"]}, "emb": {weights["emb"]}, '
                f'"graph": 1.0}}  # RRF 周度自动调参 {date}（备份 {os.path.basename(bak)}）\n')
    content2, n = WEIGHT_LINE_RE.subn(new_line, content, count=1)
    if n != 1:
        raise RuntimeError(f"l3 CHANNEL_WEIGHTS 行未匹配（n={n}），拒绝写入")
    with open(l3_path, "w", encoding="utf-8") as f:
        f.write(content2)
    return bak, old


def clone_db(workdir, log_path):
    """克隆生产库到工作目录（cp -c clonefile 秒级）。返回克隆库路径。"""
    os.makedirs(workdir, exist_ok=True)
    clone = os.path.join(workdir, "memory_store_weekly.db")
    if os.path.exists(clone):
        _log(f"[clone] 克隆库已存在，复用: {clone}", log_path)
        return clone
    if not os.path.exists(PROD_DB):
        raise RuntimeError(f"生产库不存在: {PROD_DB}")
    _log(f"[clone] cp -c 克隆生产库 → {clone}", log_path)
    shutil.copy2(PROD_DB, clone, follow_symlinks=False)
    return clone


def ndcg_at5(ranked_ids, relevant):
    dcg = 0.0
    for i, eid in enumerate(ranked_ids[:5]):
        if eid in relevant:
            dcg += 1.0 / (i + 2)
    k = min(5, len(relevant))
    idcg = sum(1.0 / (i + 2) for i in range(k))
    return dcg / idcg if idcg else 0.0


def run_phase(weights, queries, dep_ids, rel_ids, db_path, l3, label, log_path):
    """跑一组权重在 golden 子集上的当次跑测（冷启动：清缓存→预热→清缓存→逐 query）。"""
    l3.CHANNEL_WEIGHTS = {"fts5": weights["fts5"], "emb": weights["emb"], "graph": 1.0}
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM query_cache")
    conn.commit()
    conn.close()
    try:
        l3.search_memories("预热 检索 通道", mode="auto", top_k=5, tier="compact")
    except Exception as e:
        _log(f"[{label}] 预热异常(忽略): {e}", log_path)
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM query_cache")
    conn.commit()
    conn.close()

    phase = []
    for q in queries:
        qid, query = q["id"], q["query"]
        exp = q.get("expected_top5", [])
        second = q.get("second_answers", [])
        try:
            r = l3.search_memories(query, mode="auto", top_k=5, tier="compact")
            top5 = [x["id"] for x in r.get("results", [])]
            mode = r.get("search_mode", "?")
        except Exception as e:
            top5, mode = [], f"ERR:{e}"
        rel_valid = {e for e in exp if e not in dep_ids}
        second_ids = [s["id"] if isinstance(s, dict) else s for s in second]
        rel_multi = {e for e in (exp + second_ids) if e not in dep_ids}
        phase.append({
            "id": qid, "top5": top5, "expected": exp, "search_mode": mode,
            "ndcg_valid": round(ndcg_at5(top5, rel_valid), 6),
            "ndcg_multi": round(ndcg_at5(top5, rel_multi), 6),
            "relational": qid in rel_ids,
        })
    n = len(phase)
    valid = sum(p["ndcg_valid"] for p in phase) / n if n else 0.0
    multi = sum(p["ndcg_multi"] for p in phase) / n if n else 0.0
    rel_sub = [p for p in phase if p["relational"]]
    rel_valid = (sum(p["ndcg_valid"] for p in rel_sub) / len(rel_sub)) if rel_sub else None
    return {"weights": dict(weights), "n": n, "ndcg_valid": valid, "ndcg_multi": multi,
            "rel_ndcg_valid": rel_valid, "detail": phase}


def degraded_queries(cand_detail, base_detail):
    """候选相对基线的逐 query 退化列表（ndcg_valid 严格下降）。"""
    base_map = {p["id"]: p["ndcg_valid"] for p in base_detail}
    return [p["id"] for p in cand_detail
            if p["ndcg_valid"] < base_map.get(p["id"], 0.0) - 1e-9]


def create_notify_card(kanban_db, title, body):
    """建卡通知维护人（幂等：同标题非 complete 卡已存在则跳过）。"""
    conn = sqlite3.connect(kanban_db)
    try:
        row = conn.execute(
            "SELECT id FROM tasks WHERE title=? AND status NOT IN ('complete','archived') "
            "ORDER BY created_at DESC LIMIT 1", (title,)).fetchone()
        if row:
            return {"task_id": row[0], "created": False, "reason": "同标题卡已存在（幂等跳过）"}
        task_id = "rrf_weekly_" + time.strftime("%Y%m%d_%H%M%S")
        now = int(time.time())
        conn.execute(
            "INSERT INTO tasks (id, title, body, assignee, status, priority, created_by, "
            "created_at, workspace_kind) VALUES (?,?,?,?,?,?,?,?,'scratch')",
            (task_id, title, body, os.environ.get("SIKU_KANBAN_ASSIGNEE", "default"), "ready", 2, os.environ.get("SIKU_KANBAN_CREATED_BY", "siku-core"), now))
        conn.commit()
        return {"task_id": task_id, "created": True, "reason": "新建"}
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="RRF 周度网格重调（no_agent 0 token）")
    ap.add_argument("--dry-run", action="store_true", help="只跑不改（不备份不更新不建卡）")
    ap.add_argument("--force", action="store_true", help="忽略当周去重标记强制重跑")
    ap.add_argument("--fts5-grid", default="1.3,1.5,1.7", help="fts5 权重档位（逗号分隔）")
    ap.add_argument("--emb-grid", default="0.6,0.7,0.8", help="emb 权重档位（逗号分隔）")
    ap.add_argument("--no-baseline", action="store_true", help="不自动加 (1,1) 基线组合")
    ap.add_argument("--no-current-grid", action="store_true",
                    help="当前权重不强制加入网格（构造门禁失败测试用）")
    args = ap.parse_args()

    golden_dir = os.environ.get("SIKU_GOLDEN_DIR", GOLDEN_DIR_DEFAULT)
    golden_train = os.environ.get("SIKU_GOLDEN_TRAIN",
                                  os.path.join(golden_dir, "rrf_golden_split_train.json"))
    golden_val = os.environ.get("SIKU_GOLDEN_VAL",
                                os.path.join(golden_dir, "rrf_golden_split_val.json"))
    l3_path = os.environ.get("RRF_L3_PATH", L3_DEFAULT)
    workdir = os.environ.get("RRF_WORKDIR", WORKDIR_DEFAULT)
    kanban_db = os.environ.get("SIKU_KANBAN_DB", KANBAN_DEFAULT)
    cur_override = os.environ.get("RRF_CURRENT_WEIGHTS")
    monthly_log = os.environ.get("RRF_MONTHLY_LOG", MONTHLY_LOG_DEFAULT)

    os.makedirs(workdir, exist_ok=True)
    log_path = os.path.join(workdir, "logs", "rrf_weekly_tune.log")
    results_dir = os.path.join(workdir, "results")
    audit_path = os.path.join(workdir, "audit", "rrf_weekly_audit.jsonl")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(os.path.dirname(audit_path), exist_ok=True)

    # ── 当周去重（防止 cron 重复触发；--force 跳过）──
    today = datetime.date.today().strftime("%Y%m%d")
    results_path = os.path.join(results_dir, f"rrf_weekly_tune_{today}.json")
    if os.path.exists(results_path) and not args.force:
        _log(f"[skip] 今日已跑过（{results_path}），--force 可强制重跑", log_path)
        return 0

    # ── 硬化②：precision-monitor 翻回互斥（rrf 调参暂停；--force 人工显式放行）──
    block_mark = os.environ.get("RRF_BLOCK_MARK", BLOCK_MARK_DEFAULT)
    if os.path.exists(block_mark) and not args.force:
        _log(f"[skip] 互斥标记存在（precision-monitor 已紧急翻回）: {block_mark}，跳过本次调参"
             f"（人工处置后删除标记，或 --force 显式放行）", log_path)
        print(f"[rrf_weekly_tune] 跳过：检测到 precision-monitor 翻回互斥标记 {block_mark}，"
              f"本周不调参（防撤销翻回恢复）；人工确认后删除该标记即可恢复自动调参", file=sys.stderr)
        return 0

    # ── golden split 检查 + 内联 auto-split（方案②：缺/过期自动从最新 golden-set 重生成）──
    # SIKU_GOLDEN_TRAIN/VAL 显式指定 → 跳过 auto-split（调用方显式给 split，直接用）
    if "SIKU_GOLDEN_TRAIN" in os.environ or "SIKU_GOLDEN_VAL" in os.environ:
        _log(f"[split] env 显式指定 golden（跳过 auto-split）: train={golden_train} val={golden_val}",
             log_path)
    else:
        gset, gset_mtime = latest_golden_set(golden_dir)
        train_mtime = os.path.getmtime(golden_train) if os.path.exists(golden_train) else 0.0
        val_mtime = os.path.getmtime(golden_val) if os.path.exists(golden_val) else 0.0
        missing = not os.path.exists(golden_train) or not os.path.exists(golden_val)
        stale = bool(gset) and gset_mtime > max(train_mtime, val_mtime)
        if gset is None:
            _log(f"[split] 未找到 golden-set-*.json（auto-split 无源，走原缺失检查）: {golden_dir}",
                 log_path)
        elif missing or stale:
            reason = "split 缺失" if missing else "golden-set mtime 晚于 split（过期）"
            _log(f"[split] auto-split 触发（{reason}）: 源={gset} seed={AUTO_SPLIT_SEED} "
                 f"ratio={AUTO_SPLIT_RATIO}", log_path)
            try:
                n_train, n_val = auto_split_golden(gset, golden_train, golden_val)
                _log(f"[split] ✅ 重生成 train={n_train} val={n_val}（固定种子 {AUTO_SPLIT_SEED} 可复现）",
                     log_path)
            except Exception as e:
                _log(f"[split] ❌ auto-split 异常: {e}（回退原缺失检查）", log_path)
        else:
            _log(f"[split] split 最新（不缺失不过期），直接用（零改动路径）", log_path)

    if not os.path.exists(golden_train) or not os.path.exists(golden_val):
        _log(f"[error] golden split 缺失: train={os.path.exists(golden_train)} val={os.path.exists(golden_val)}", log_path)
        sys.exit(2)
    if not os.path.exists(l3_path):
        _log(f"[error] l3_retrieval.py 不存在: {l3_path}", log_path)
        sys.exit(2)

    # ── 当前权重（默认从 l3 解析，env 可覆盖）──
    if cur_override:
        wf0, we0 = (float(x) for x in cur_override.split(","))
        current = {"fts5": wf0, "emb": we0}
    else:
        current = parse_current_weights(l3_path)
        if not current:
            _log("[error] 无法从 l3 解析 CHANNEL_WEIGHTS", log_path)
            sys.exit(2)

    # ── DB：env 指定（测试）或自动克隆生产 ──
    db_path = os.environ.get("SIKU_DB_PATH")
    if not db_path:
        db_path = clone_db(workdir, log_path)
    if not os.path.exists(db_path):
        _log(f"[error] DB 不存在: {db_path}", log_path)
        sys.exit(2)

    # ── 延迟导入 l3（SIKU_ROUTER=off 直通主检索；图关=生产形态）──
    os.environ.setdefault("SIKU_ROUTER", "off")
    os.environ["SIKU_GRAPH_CHANNEL"] = "0"
    os.environ["SIKU_DB_PATH"] = db_path
    sys.path.insert(0, os.path.dirname(l3_path))
    sys.path.insert(0, os.path.join(_SIKU_ROOT, "scripts"))
    import l3_retrieval as l3
    l3.GRAPH_CHANNEL_ENABLED = False
    l3.GRAPH_GATE_ENABLED = True
    l3.GRAPH_PER_SEED = int(os.environ.get("SIKU_GRAPH_PER_SEED", "1"))

    # ── 载入 golden（兼容列表结构与旧 dict 结构）──
    train_q = load_golden_queries(golden_train, "golden_train")
    val_q = load_golden_queries(golden_val, "golden_val")
    rel_ids = {q["id"] for q in train_q + val_q if q.get("relational")}
    conn = sqlite3.connect(db_path)
    dep_ids = {r[0] for r in conn.execute("SELECT id FROM memory_store WHERE deprecated=1")}
    conn.close()
    _log(f"[init] train={len(train_q)} val={len(val_q)} relational={len(rel_ids)} "
         f"current={current} db={db_path}", log_path)

    # ── train 网格组合（基线 (1,1) + fts5×emb 叉积，含当前权重去重）──
    combos = []
    if not args.no_baseline:
        combos.append((1.0, 1.0))
    fts5_vals = [float(x) for x in args.fts5_grid.split(",")]
    emb_vals = [float(x) for x in args.emb_grid.split(",")]
    for wf in fts5_vals:
        for we in emb_vals:
            if (wf, we) not in combos:
                combos.append((wf, we))
    if (current["fts5"], current["emb"]) not in combos and not args.no_current_grid:
        combos.append((current["fts5"], current["emb"]))
    _log(f"[grid] {len(combos)} 组合: {combos}", log_path)

    # ── verifier 连续分增强（；RRF_VERIFIER=0 可关，失败不阻断主流程）──
    vrf = None
    if os.environ.get("RRF_VERIFIER", "1") == "1":
        try:
            sys.path.insert(0, os.path.join(_HERMES_HOME, "scripts"))
            import verifier as vrf
            vrf_ok, vrf_ev = vrf.probe_logprobs()
            _log(f"[verifier] logprobs 支持={vrf_ok}（证据 {vrf_ev[:80]}）", log_path)
        except Exception as e:
            _log(f"[verifier] 导入失败，跳过连续分增强: {e}", log_path)
            vrf = None

    # ── train 网格扫描（单进程循环，模型只加载一次）──
    train_matrix = []
    for wf, we in combos:
        w = {"fts5": wf, "emb": we}
        r = run_phase(w, train_q, dep_ids, rel_ids, db_path, l3,
                      f"train_f{wf}_e{we}", log_path)
        train_matrix.append({"weights": w, "ndcg_valid": r["ndcg_valid"],
                             "ndcg_multi": r["ndcg_multi"],
                             "rel_ndcg_valid": r["rel_ndcg_valid"]})
        if vrf is not None:  # 连续分选优增强列（抽样 20 正+5 负，参数组合横向可比）
            qc = vrf.calibrate(top_k=5, mode="top5", probe_first=False, pos_limit=20, neg_limit=5)
            train_matrix[-1]["vrf_diff"] = qc["diff"]
            _log(f"[train] ({wf},{we}) vrf_diff={qc['diff']}（抽样 20+5 连续分）", log_path)
        _log(f"[train] ({wf},{we}) valid={r['ndcg_valid']:.6f} multi={r['ndcg_multi']:.6f}", log_path)

    # train 最优候选（valid 口径，排除基线 (1,1) 当候选——基线只是锚点）
    if args.no_baseline:
        cands = list(train_matrix)
    else:
        cands = [m for m in train_matrix if not (m["weights"]["fts5"] == 1.0 and m["weights"]["emb"] == 1.0)]
    candidate = max(cands, key=lambda m: m["ndcg_valid"])
    cand_w = candidate["weights"]
    _log(f"[train] 最优候选: ({cand_w['fts5']},{cand_w['emb']}) "
         f"valid={candidate['ndcg_valid']:.6f}", log_path)

    # ── 无变化：候选==当前 → 静默（0 输出 0 建卡），日志留档 ──
    if cand_w["fts5"] == current["fts5"] and cand_w["emb"] == current["emb"]:
        _log("[gate] 候选==当前权重，无需更新（静默）", log_path)
        summary = {"time": _now_iso(), "decision": "no_change",
                   "current": current, "candidate": cand_w,
                   "reason": "train 最优 == 当前生产权重",
                   "train_matrix": train_matrix}
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=1)
        return 0

    # ── val 验证：候选 vs 当前（均当次跑测，冷启动公平）──
    cur_r = run_phase(current, val_q, dep_ids, rel_ids, db_path, l3,
                      "val_current", log_path)
    cand_r = run_phase(cand_w, val_q, dep_ids, rel_ids, db_path, l3,
                       f"val_f{cand_w['fts5']}_e{cand_w['emb']}", log_path)
    deg = degraded_queries(cand_r["detail"], cur_r["detail"])
    _log(f"[val] current valid={cur_r['ndcg_valid']:.6f} multi={cur_r['ndcg_multi']:.6f} "
         f"rel={cur_r['rel_ndcg_valid']}", log_path)
    _log(f"[val] cand({cand_w['fts5']},{cand_w['emb']}) valid={cand_r['ndcg_valid']:.6f} "
         f"multi={cand_r['ndcg_multi']:.6f} rel={cand_r['rel_ndcg_valid']} degraded={len(deg)} {deg}", log_path)

    # ── 历史回归用例集校验（防 8-28 val 样本不全假过重演）──
    # 回归 qid 常落在 train split（如 g009/g027/g038/g039/g051/g059 全在 train 91），val 22 覆盖不到
    # → 门禁必须独立跑回归集（候选 vs 当前当次跑测逐 qid 对比），dry_run 同受门禁。
    regress_gset, _ = latest_golden_set(golden_dir)
    regress_qids_all = load_history_regress_qids(monthly_log, log_path)
    regress_ev = {"source": monthly_log, "qids_total": len(regress_qids_all),
                  "matched": 0, "unmatched": [], "golden_set": regress_gset,
                  "current": None, "candidate": None, "degraded_queries": []}
    reg_deg = []
    if regress_gset is None:
        _log("[regress] 未找到 golden-set-*.json（回归集无法取 query 定义，跳过校验）", log_path)
    else:
        regress_queries, regress_unmatched = load_history_regress_queries(
            monthly_log, regress_gset, log_path, qids=regress_qids_all)
        regress_ev["unmatched"] = regress_unmatched
        regress_ev["matched"] = len(regress_queries)
        if regress_queries:
            cur_rr = run_phase(current, regress_queries, dep_ids, rel_ids, db_path, l3,
                               "regress_current", log_path)
            cand_rr = run_phase(cand_w, regress_queries, dep_ids, rel_ids, db_path, l3,
                                f"regress_f{cand_w['fts5']}_e{cand_w['emb']}", log_path)
            reg_deg = degraded_queries(cand_rr["detail"], cur_rr["detail"])
            regress_ev["current"] = {"ndcg_valid": cur_rr["ndcg_valid"],
                                     "ndcg_multi": cur_rr["ndcg_multi"],
                                     "detail": {p["id"]: p["ndcg_valid"] for p in cur_rr["detail"]}}
            regress_ev["candidate"] = {"ndcg_valid": cand_rr["ndcg_valid"],
                                       "ndcg_multi": cand_rr["ndcg_multi"],
                                       "detail": {p["id"]: p["ndcg_valid"] for p in cand_rr["detail"]}}
            regress_ev["degraded_queries"] = reg_deg
            _log(f"[regress] 候选({cand_w['fts5']},{cand_w['emb']}) vs 当前 回归集 "
                 f"({len(regress_queries)} 条): current valid={cur_rr['ndcg_valid']:.6f} "
                 f"cand valid={cand_rr['ndcg_valid']:.6f} degraded={len(reg_deg)} {reg_deg}", log_path)
        else:
            _log("[regress] 回归集为空（无历史回归记录），校验跳过（门禁不额外拦截）", log_path)
    gate_pass = ((cand_r["ndcg_valid"] >= cur_r["ndcg_valid"] - 1e-9) and len(deg) == 0
                 and len(reg_deg) == 0)

    # ── verifier 增强：候选权重全量校准（golden 正 113/负 18 区分度）──
    verifier_ev = None
    if vrf is not None:
        cand_cal = vrf.calibrate(top_k=5, mode="top5", probe_first=False)
        verifier_ev = {"mode": cand_cal["mode"], "threshold": cand_cal["threshold"],
                       "mean_pos": cand_cal["mean_pos"], "mean_neg": cand_cal["mean_neg"],
                       "diff": cand_cal["diff"], "pass": cand_cal["pass"],
                       "n_pos": len(cand_cal["pos"]), "n_neg": len(cand_cal["neg"])}
        _log(f"[verifier] 候选({cand_w['fts5']},{cand_w['emb']}) 全量校准: 区分度={cand_cal['diff']} "
             f"正均={cand_cal['mean_pos']} 负均={cand_cal['mean_neg']} 阈值≥{cand_cal['threshold']} "
             f"→ {'✅ 达标' if cand_cal['pass'] else '❌ 未达标（如实记录，不阻断原门禁）'}", log_path)

    evidence = {
        "time": _now_iso(), "decision": "gate_pass" if gate_pass else "gate_fail",
        "current": current, "candidate": cand_w,
        "train": {"best": candidate, "matrix": train_matrix},
        "val": {"current": {"ndcg_valid": cur_r["ndcg_valid"], "ndcg_multi": cur_r["ndcg_multi"],
                            "rel_ndcg_valid": cur_r["rel_ndcg_valid"]},
                "candidate": {"ndcg_valid": cand_r["ndcg_valid"], "ndcg_multi": cand_r["ndcg_multi"],
                              "rel_ndcg_valid": cand_r["rel_ndcg_valid"]},
                "degraded_queries": deg},
        "history_regress": regress_ev,
        "gate": {"pass": gate_pass,
                 "rule": "cand.val_ndcg >= cur.val_ndcg 且 val 退化条数=0 且历史回归用例集逐条零退化"},
        "golden": {"train": os.path.abspath(golden_train), "val": os.path.abspath(golden_val),
                   "train_n": len(train_q), "val_n": len(val_q)},
        "db": db_path, "l3": l3_path,
        "verifier": verifier_ev,
    }

    # ── 不达标 → 静默退出（stdout 空），证据落盘 + 日志 ──
    if not gate_pass:
        _log(f"[gate] 零退化门禁未通过（val 退化 {len(deg)} 条 / 回归集退化 {len(reg_deg)} 条），静默", log_path)
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(evidence, f, ensure_ascii=False, indent=1)
        return 0

    # ── 达标 → 备份 + 更新 + audit + 建卡通知维护者（--dry-run 只出证据不动手）──
    _log("[gate] ✅ 零退化门禁通过，执行更新", log_path)
    if not args.dry_run:
        bak, old = update_l3_weights(l3_path, cand_w, log_path)
        # 备份+改后 md5 双份核对
        md5_before = subprocess.run(["md5", "-q", bak], capture_output=True, text=True).stdout.strip()
        md5_after = subprocess.run(["md5", "-q", l3_path], capture_output=True, text=True).stdout.strip()
        _log(f"[update] 备份={bak} md5_before={md5_before}", log_path)
        _log(f"[update] 更新后 md5={md5_after}（{'✅ 变化可追踪' if md5_before != md5_after else '⚠️ md5 未变' }）", log_path)
        # 读回验证
        verify = parse_current_weights(l3_path)
        _log(f"[update] 读回验证: {verify}", log_path)
        # audit 痕迹（谁/何时/从什么到什么）
        audit_row = {"time": _now_iso(), "operator": "rrf_weekly_tune.py(cron)",
                     "from": current, "to": cand_w, "backup": bak,
                     "md5_before": md5_before, "md5_after": md5_after,
                     "val_ndcg_from": cur_r["ndcg_valid"], "val_ndcg_to": cand_r["ndcg_valid"],
                     "degraded": len(deg), "regress_degraded": len(reg_deg),
                     "regress_qids": regress_ev.get("matched", 0), "results": results_path}
        with open(audit_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_row, ensure_ascii=False) + "\n")
        # 通知维护者（建卡，幂等）
        body_lines = [
            f"【RRF 周度权重自动调参】零退化门禁通过，权重已更新，请复审",
            f"- 触发时间: {_now_iso()}",
            f"- 权重变更: {current} → {cand_w}",
            f"- val 证据: valid {cur_r['ndcg_valid']:.6f} → {cand_r['ndcg_valid']:.6f} "
            f"(退化 {len(deg)} 条) multi {cur_r['ndcg_multi']:.6f} → {cand_r['ndcg_multi']:.6f}",
            f"- 历史回归集校验: {regress_ev['matched']} 条用例逐条零退化"
            f"（退化 {len(reg_deg)} 条{reg_deg}；数据源 {os.path.basename(monthly_log)}）",
        ]
        if verifier_ev:
            body_lines.append(f"- verifier 连续分校准: 区分度 {verifier_ev['diff']} "
                              f"(正均 {verifier_ev['mean_pos']} / 负均 {verifier_ev['mean_neg']}, "
                              f"阈值≥{verifier_ev['threshold']}, {verifier_ev['n_pos']}正+{verifier_ev['n_neg']}负) "
                              f"{'✅' if verifier_ev['pass'] else '❌未达标'}")
        body_lines += [
            f"- 备份: {bak}（md5 {md5_before}）",
            f"- audit: {audit_path}",
            f"- 证据 JSON: {results_path}",
            f"- 来源: rrf_weekly_tune.py（no_agent 纯脚本 0 token，周度 cron 自动调参）",
            f"- 依据: RRF自动微调-方案-20260813 推荐主干（候选③ 周期网格重调）",
            f"- 后续: 维护者复审 audit 行确认；不满意可运行 rrf_weekly_tune_restore.py 还原",
        ]
        card = create_notify_card(kanban_db, NOTIFY_TITLE_DEFAULT, "\n".join(body_lines))
        evidence["card"] = card
        evidence["backup"] = bak
        evidence["md5_before"] = md5_before
        evidence["md5_after"] = md5_after
        _log(f"[notify] 建卡: {card}", log_path)
    else:
        _log("[dry-run] 达标但 --dry-run，不更新不建卡", log_path)
        evidence["dry_run"] = True

    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(evidence, f, ensure_ascii=False, indent=1)

    # 达标 → stdout JSON（cron deliver=local 可见触发证据）；不达标路径已提前 return（stdout 空）
    print(json.dumps(evidence, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
