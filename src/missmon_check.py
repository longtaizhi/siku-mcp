#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""未命中率监控检查器（命令行入口，纯规则零 LLM）。

信号源①（本卡落地）：l3_retrieval.py 每查询记录 hit/miss 到
  $SIKU_ROOT/scripts/siku_option/missmon_queries.jsonl（JSONL 追加）；
滚动窗口（默认近 7 天，--window-days / SIKU_MISSMON_WINDOW_DAYS 参数化）内
按 query 去重合并（同 query 任一次真实检索命中即算 hit）计算未命中率：
  > 20%（SIKU_MISSMON_THRESHOLD）→ 自动建卡「入库提示」（kanban INSERT 模板化，
    幂等防重复：同标题非 complete/archived 卡已存在即跳过）——只建卡提示，不自动写库
  ≤ 20% → stdout 空静默退出（EXIT=0），零建卡零打扰
同时登记监控状态（missmon_state.json）：状态/到期标记/信号记录。

退出码：0=正常（含静默）；2=配置/文件错误；3=达标建卡（供 cron 可见性）。
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

# ── 路径 ────────────────────────────────────────────────────────────
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
OPTION_DIR = os.path.join(SCRIPTS_DIR, "siku_option")
LOG_DEFAULT = os.path.join(OPTION_DIR, "missmon_queries.jsonl")
STATE_DEFAULT = os.path.join(OPTION_DIR, "missmon_state.json")
KANBAN_DEFAULT = os.path.join(_HERMES_HOME, "kanban", "boards", "default", "kanban.db")

# ── 参数（环境变量可覆盖）────────────────────────────────────────────
WINDOW_DAYS_DEFAULT = int(os.environ.get("SIKU_MISSMON_WINDOW_DAYS", "7"))
THRESHOLD_DEFAULT = float(os.environ.get("SIKU_MISSMON_THRESHOLD", "0.2"))

CARD_TITLE = "未命中率预警-入库提示"


def _now_iso():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def load_queries(log_path, window_days):
    """读 JSONL（容忍坏行），返回窗口内 [{'query','hit','ts',...}]。"""
    if not os.path.exists(log_path):
        return []
    cutoff = time.time() - window_days * 86400
    out = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                continue  # 坏行跳过，绝不让统计炸掉
            ts = e.get("ts", 0) or 0
            if ts < cutoff:
                continue
            out.append(e)
    return out


def compute_miss_rate(entries):
    """按 query 去重合并：窗口内任一次 hit=true 即算 hit；miss 数/去重 query 数。"""
    per_query = {}
    for e in entries:
        q = e.get("query", "")
        if not q:
            continue
        rec = per_query.setdefault(q, {"hit": False, "miss": False, "count": 0})
        rec["count"] += 1
        if e.get("hit"):
            rec["hit"] = True
        else:
            rec["miss"] = True
    queries = list(per_query.keys())
    if not queries:
        return None, 0, 0
    miss_q = [q for q, r in per_query.items() if r["hit"] is False]
    rate = len(miss_q) / len(queries)
    return rate, len(miss_q), len(queries)


def load_state(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # 首次运行/文件缺失：完整期权登记默认值（含 6 个月到期标记）
    now = _now_iso()
    return {
        "option_name": "多模态架构移植评估（arXiv:2608.08366）",
        "status": "挂账",
        "status_note": "半侧已落地：未命中率监控 >20% 自动建卡入库提示（R3-A）；整体架构不移植，6 个月无立项信号到期降级。",
        "statuses": ["active", "挂账", "到期降级"],
        "created_at": now,
        "expires_at": datetime.fromtimestamp(time.time() + 6 * 30 * 86400).astimezone().strftime(
            "%Y-%m-%dT%H:%M:%S%z"),
        "expiry_months": 6,
        "signals": {
            "miss_rate_monitor": {"enabled": True, "threshold": THRESHOLD_DEFAULT,
                                  "window_days": WINDOW_DAYS_DEFAULT,
                                  "last_check": None, "last_rate": None,
                                  "last_query_count": None, "last_triggered": None,
                                  "last_card_id": None},
            "manual_demand": {"enabled": True, "last_signal": None},
            "expiry_eval": {"enabled": True, "due_at": None, "last_eval": None},
        },
        "history": [{"ts": now, "event": "option_registered",
                     "note": "R3-A 落地：未命中率监控信号源①启用（默认阈值 20%/窗口 7 天）"}],
    }


def save_state(path, state):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def update_state(state, rate, miss_q, total_q, triggered, card_id, window_days, threshold):
    """登记信号：状态字段/到期标记不变，miss_rate_monitor 信号记录更新 + history。"""
    sig = state.setdefault("signals", {}).setdefault("miss_rate_monitor", {})
    now = _now_iso()
    sig.update({
        "enabled": True,
        "threshold": threshold,
        "window_days": window_days,
        "last_check": now,
        "last_rate": rate,
        "last_query_count": total_q,
        "last_triggered": now if triggered else sig.get("last_triggered"),
        "last_card_id": card_id if triggered else sig.get("last_card_id"),
    })
    state.setdefault("history", []).append({
        "ts": now,
        "event": "miss_rate_check",
        "rate": rate,
        "miss_queries": miss_q,
        "total_queries": total_q,
        "triggered": triggered,
        "card_id": card_id,
    })
    # 到期标记：expires_at 已过 → 状态自动转「到期降级」（信号源③到期评估兜底）
    exp = state.get("expires_at")
    if exp:
        try:
            exp_ts = datetime.strptime(exp, "%Y-%m-%dT%H:%M:%S%z").timestamp()
        except Exception:
            exp_ts = None
        if exp_ts is not None and time.time() > exp_ts:
            if state.get("status") != "到期降级":
                state["status"] = "到期降级"
                state.setdefault("history", []).append({"ts": now, "event": "status_auto_downgrade",
                                                        "note": "6 个月到期无立项信号，自动转到期降级"})
    return state


def build_card_body(rate, miss_q, total_q, window_days, threshold, miss_queries_sample):
    lines = [
        f"未命中率预警（信号源①：未命中率监控自动触发）",
        "",
        f"- 未命中率: {rate:.1%}（阈值 >{threshold:.0%}，{miss_q}/{total_q} 个去重 query 无检索结果）",
        f"- 滚动窗口: 近 {window_days} 天（missmon_queries.jsonl 每查询 hit/miss 记录，纯规则零 LLM）",
        f"- 触发时间: {_now_iso()}",
        "",
        "- 未命中 query 示例（前 10 条，供人工判断入库价值）:",
    ]
    for q in miss_queries_sample[:10]:
        lines.append(f"  - {q}")
    lines += [
        "",
        "- 动作: 仅建卡提示，不自动写库（研究收敛定稿——检索无结果的 query 主题需人工评估是否入库）",
        "- 处置: 评估上列未命中主题是否入库（新条目/同义词/图谱关系）；确认后置卡 complete 并回填",
        "- 依据: 三主题研究 R3-A 未命中监控+ 期权机制（未命中率监控信号源）",
    ]
    return "\n".join(lines)


def create_card(kanban_db, title, body):
    """建卡「未命中率预警-入库提示」。
    幂等：同标题且状态非 complete/archived 的卡已存在 → 不重复建，返回已有卡 id。
    """
    conn = sqlite3.connect(kanban_db)
    try:
        row = conn.execute(
            "SELECT id FROM tasks WHERE title=? AND status NOT IN ('complete','archived') "
            "ORDER BY created_at DESC LIMIT 1", (title,)).fetchone()
        if row:
            return {"task_id": row[0], "created": False, "reason": "同标题卡已存在（幂等跳过）"}
        task_id = "siku_missmon_" + time.strftime("%Y%m%d_%H%M%S")
        now = int(time.time())
        conn.execute(
            "INSERT INTO tasks (id, title, body, assignee, status, priority, created_by, "
            "created_at, workspace_kind) VALUES (?,?,?,?,?,?,?,?,'scratch')",
            (task_id, title, body, os.environ.get("SIKU_KANBAN_ASSIGNEE", "default"), "ready", 1, os.environ.get("SIKU_KANBAN_CREATED_BY", "siku-core"), now))
        conn.commit()
        return {"task_id": task_id, "created": True, "reason": "新建"}
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="未命中率监控检查器（纯规则零 LLM）")
    ap.add_argument("--window-days", type=int, default=WINDOW_DAYS_DEFAULT,
                    help="滚动窗口天数（默认 7）")
    ap.add_argument("--threshold", type=float, default=THRESHOLD_DEFAULT,
                    help="未命中率阈值（默认 0.2）")
    ap.add_argument("--log", default=os.environ.get("SIKU_MISSMON_LOG", LOG_DEFAULT))
    ap.add_argument("--state", default=os.environ.get("SIKU_MISSMON_STATE", STATE_DEFAULT))
    ap.add_argument("--kanban-db", default=os.environ.get("SIKU_KANBAN_DB", KANBAN_DEFAULT))
    ap.add_argument("--dry-run", action="store_true", help="只检查不建卡")
    args = ap.parse_args()

    if args.window_days <= 0 or not (0 < args.threshold < 1):
        sys.stderr.write("[missmon_check] 参数错误: window_days>0 且 0<threshold<1\n")
        return 2

    entries = load_queries(args.log, args.window_days)
    rate, miss_q, total_q = compute_miss_rate(entries)

    state = load_state(args.state)
    if rate is None:
        # 窗口内零查询：无信号，静默（登记 last_check 保持期权信号记录连续）
        update_state(state, None, 0, 0, False, None, args.window_days, args.threshold)
        save_state(args.state, state)
        return 0

    triggered = rate > args.threshold
    card = None
    if triggered:
        if args.dry_run:
            card = {"task_id": None, "created": False, "reason": "dry_run（只检查不建卡）"}
        else:
            if not os.path.exists(args.kanban_db):
                sys.stderr.write(f"[missmon_check] kanban 库不存在: {args.kanban_db}\n")
                return 2
            miss_queries_sample = sorted(
                (e for e in entries if not e.get("hit")),
                key=lambda e: e.get("ts", 0), reverse=True)
            sample = []
            seen = set()
            for e in miss_queries_sample:
                q = e.get("query", "")
                if q and q not in seen:
                    seen.add(q)
                    sample.append(q)
            body = build_card_body(rate, miss_q, total_q, args.window_days,
                                   args.threshold, sample)
            card = create_card(args.kanban_db, CARD_TITLE, body)
    update_state(state, rate, miss_q, total_q, triggered,
                 card["task_id"] if card else None, args.window_days, args.threshold)
    save_state(args.state, state)

    if not triggered:
        return 0  # 不达标静默（stdout 空）

    evidence = {
        "trigger_path": "miss_rate_monitor",
        "time": _now_iso(),
        "window_days": args.window_days,
        "threshold": args.threshold,
        "rate": rate,
        "miss_queries": miss_q,
        "total_queries": total_q,
        "card": card,
        "state_file": args.state,
    }
    print(json.dumps(evidence, ensure_ascii=False, indent=1))
    return 3


if __name__ == "__main__":
    sys.exit(main())
