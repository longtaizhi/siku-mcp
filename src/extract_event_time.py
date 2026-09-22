#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""extract_event_time.py — 事件+事件时间提取（阶段2）

核心（沙盒三臂报告数据驱动：预过滤结构性无效——29.3%→29.3% 带窗仅 4/133——
temporal 提升 = 事件时间提取，VEKTOR 四段式：写时 LLM 提取事件+日期锚定会话时间戳）：

  1. 规则优先：显式日期（ISO/中文/英文月/AMB 斜杠）+ 简单相对表达锚定解析（0token）
  2. LLM 兜底：仅规则未命中且路由预筛有时间信号的条目 → 本地 8081 qwen38-v4 提取
     {event, date}（结构化输出——事件+日期锚定会话时间戳）——云端零调用（红线）
  3. 四层红线：路由预筛（无时间信号→跳过不调 LLM）/ 触发率≤30%（LLM 调用占比上限）/
     频率限流（单并发+间隔）/ 低置信留空（confidence<0.6 不写——宁放过不误杀）
  4. 锚定：AMB 模式用 doc_ts，四库模式用 timestamp（会话时间戳=对话日期）
  5. 幂等 + 审计日志（本地 JSONL）+ 抽样核输出

用法：
  python3 extract_event_time.py --db ... --mode amb --limit 2000 --dry-run --sample 30   # 提取+抽样核（不写库）
  python3 extract_event_time.py --db ... --mode amb --limit 2000 --write                # 写 event_date（高置信）
  python3 extract_event_time.py --db ... --mode prod --limit 500 --write                # 四库试点 500 条
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timedelta

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

API_URL = os.environ.get("SIKU_LLM_URL", "http://127.0.0.1:8081/v1/chat/completions")
MODEL = os.environ.get("SIKU_EXTRACT_MODEL", os.path.join(_SIKU_ROOT, "models", "qwen38-v4"))
CONF_MIN = 0.6          # 低置信留空
LLM_MAX_RATIO = 0.30    # 触发率红线 ≤30%
LLM_INTERVAL = 0.05     # 频率限流：间隔秒（本地推理串行本身 ~1-2s/次，间隔为保险丝）
MAX_TOKENS = 160

# ── 规则：显式日期（严格校验 datetime）──────────────
_RE_ISO  = re.compile(r"(?<!\d)((?:19|20)\d{2})-(0?[1-9]|1[0-2])-([12]\d|3[01]|0?[1-9])(?![\dT])")
_RE_AMB  = re.compile(r"((?:19|20)\d{2})/(0?[1-9]|1[0-2])/([12]\d|3[01]|0?[1-9])(?!\d)")
_RE_CN   = re.compile(r"(?<!\d)((?:19|20)\d{2})\s*年\s*(0?[1-9]|1[0-2])\s*月\s*([12]\d|3[01]|0?[1-9])\s*日?")
_RE_EN   = re.compile(
    r"\b((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?)"
    r"\s+([12]?\d|3[01])(?:st|nd|rd|th)?(?:,?\s*((?:19|20)\d{2}))?\b")
# 2026-08-28 修复（.bak-enbug-20260828）：旧版 [a-z]*\.? 通配吞词——"decomposition. 2" /
# "Marigolds. 3" 等以月份三字母开头的单词 + 句点 + 列表序号被误判为 "December 2"（Dec+omposition+. 2
# → 2022-12-02 高频误提取 58 条根因）。改显式月份词枚举 + 首字母大写（英文日期规范），
# 小写 may/december 宁漏勿错（低置信留空原则）。
_MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}

# ── 路由预筛：时间信号（任一命中才有机会提取）────────
_RE_SIGNAL = re.compile(
    r"\b(?:19|20)\d{2}\b|(?:19|20)\d{2}[/\-年]|"
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b|"
    r"昨|今|昨|前天|上周|上个月|去年|前年|天后|天前|周后|周前|月后|月前|年前|次日|"
    r"yesterday|ago|later|today|last\s+(?:night|week|month|year|tuesday|wednesday|thursday|friday|saturday|sunday|monday)|"
    r"this\s+(?:morning|afternoon|week|month)|tonight|tomorrow|"
    r"\d+\s*(?:day|days|week|weeks|month|months|year|years)\b", re.I)

_DOW = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6}


def _last_weekday_days(m, anchor_date):
    """'last tuesday' → 锚点日期回推到最近的上一个周二（天数差为负）。"""
    wd = _DOW.get(m.group(1).lower())
    if wd is None:
        return None
    ad = _anchor_dt(anchor_date) if anchor_date else None
    if ad is None:
        return None
    delta = (ad.weekday() - wd) % 7
    if delta == 0:
        delta = 7
    return -delta


# ── 规则：简单相对表达（锚定 anchor_ts 解析）─────────
_RE_REL = [
    # (regex, fn(m, anchor_date)) —— 返回相对锚点天数差
    (re.compile(r"\btoday\b", re.I), lambda m, a: 0),
    (re.compile(r"\byesterday\b", re.I), lambda m, a: -1),
    (re.compile(r"\btomorrow\b", re.I), lambda m, a: 1),
    (re.compile(r"\btonight\b|\bthis\s+morning\b|\bthis\s+afternoon\b", re.I), lambda m, a: 0),
    (re.compile(r"\blast\s+night\b", re.I), lambda m, a: -1),
    (re.compile(r"\blast\s+(tuesday|wednesday|thursday|friday|saturday|sunday|monday)\b", re.I),
     _last_weekday_days),
    (re.compile(r"(\d+)\s*(?:days?|weeks?|months?|years?)\s+ago", re.I), lambda m, a: None),
    (re.compile(r"in\s+(\d+)\s*(?:days?|weeks?|months?|years?)\b", re.I), lambda m, a: None),
    (re.compile(r"(\d+)\s*(?:天|周|个月|年)\s*前"), lambda m, a: None),
    (re.compile(r"(\d+)\s*(?:天|周|个月|年)\s*(?:后|之后)"), lambda m, a: None),
    (re.compile(r"昨\s*天|昨天"), lambda m, a: -1),
    (re.compile(r"前天"), lambda m, a: -2),
    (re.compile(r"去年"), lambda m, a: -365),
    (re.compile(r"上个月"), lambda m, a: -30),
    (re.compile(r"上周"), lambda m, a: -7),
    (re.compile(r"今年"), lambda m, a: 0),
]
_UNIT_DAYS = {"day": 1, "week": 7, "month": 30, "year": 365,
              "天": 1, "周": 7, "个月": 30, "年": 365}


def _valid_date(y, m, d):
    try:
        dt = datetime(int(y), int(m), int(d))
        if 1970 <= dt.year <= 2100:
            return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    return None


def _parse_rel_days(text, anchor_date=None):
    """相对表达 → 相对锚点天数差（None=未命中）。显式规则部分。"""
    m = re.search(r"(\d+)\s*(days?|weeks?|months?|years?)\s+ago", text, re.I)
    if m:
        return -int(m.group(1)) * _UNIT_DAYS[m.group(2).lower().rstrip("s")]
    m = re.search(r"(?<![a-zA-Z])in\s+(\d+)\s*(days?|weeks?|months?|years?)\b", text, re.I)
    if m:
        return int(m.group(1)) * _UNIT_DAYS[m.group(2).lower().rstrip("s")]
    m = re.search(r"(\d+)\s*(天|周|个月|年)\s*前", text)
    if m:
        return -int(m.group(1)) * _UNIT_DAYS[m.group(2)]
    m = re.search(r"(\d+)\s*(天|周|个月|年)\s*(?:后|之后)", text)
    if m:
        return int(m.group(1)) * _UNIT_DAYS[m.group(2)]
    for pat, fn in _RE_REL:
        m = pat.search(text)
        if m:
            d = fn(m, anchor_date)
            if d is not None:
                return d
    return None


def _today_dow():
    return datetime.now().weekday()  # 0=Mon


def _parse_anchor(anchor_ts):
    """doc_ts 格式 2023/05/20 (Sat) 02:21 或 ISO timestamp → datetime。"""
    if not anchor_ts:
        return None
    m = re.search(r"((?:19|20)\d{2})/(\d{1,2})/(\d{1,2})", anchor_ts)
    if m:
        return _valid_date(m.group(1), m.group(2), m.group(3))
    m = re.search(r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})", anchor_ts)
    if m:
        return _valid_date(m.group(1), m.group(2), m.group(3))
    return None


def _anchor_dt(anchor_date):
    try:
        return datetime.strptime(anchor_date, "%Y-%m-%d")
    except Exception:
        return None


# ── 规则提取（0token）──────────────────────────────
def rule_extract(text, anchor_date):
    """显式日期/简单相对 → (event_date, confidence) 或 (None, 0)。"""
    # 1. 显式日期（优先级：ISO > AMB 斜杠 > 中文 > 英文月）
    for pat in (_RE_ISO, _RE_AMB, _RE_CN):
        m = pat.search(text or "")
        if m:
            d = _valid_date(m.group(1), m.group(2), m.group(3))
            if d:
                return d, 0.95
    m = _RE_EN.search(text or "")
    if m:
        mon = _MONTHS.get(m.group(1).lower().rstrip(".")[:3])
        day = int(re.sub(r"\D", "", m.group(2)))
        year = int(m.group(3)) if m.group(3) else None
        if mon:
            if year:
                d = _valid_date(year, mon, day)
                if d:
                    return d, 0.95
            elif anchor_date:
                ad = _anchor_dt(anchor_date)
                if ad:
                    # 无年份 → 锚定会话年（若月份晚于会话月则视为去年）
                    y = ad.year
                    if mon > ad.month:
                        y -= 1
                    d = _valid_date(y, mon, day)
                    if d:
                        return d, 0.85
    # 2. 简单相对表达（锚定）
    days = _parse_rel_days(text or "", anchor_date)
    if days is not None and anchor_date:
        ad = _anchor_dt(anchor_date)
        if ad:
            return (ad + timedelta(days=days)).strftime("%Y-%m-%d"), 0.85
    return None, 0.0


# ── LLM 兜底（本地 8081——云端零调用红线）────────────
def llm_extract(content, summary, anchor_date):
    """规则未命中 + 时间信号 → 本地 LLM 提取 {event, date}（结构化 JSON）。"""
    sys_prompt = (
        "You extract EVENT TIME from a conversation memory. "
        "The session timestamp (conversation date) is: %s. "
        "Find the EVENT described in the memory and the calendar DATE it happened, "
        "resolving relative expressions (yesterday, last week, 3 days ago, 上周, 去年 etc.) "
        "against the session timestamp. "
        'Reply with STRICT JSON only: {"event": "<short event>", "date": "YYYY-MM-DD" or null, "confidence": 0.0-1.0}. '
        "If no event has a resolvable time, reply {\"event\": \"\", \"date\": null, \"confidence\": 0}." % (anchor_date or "unknown")
    )
    user = ("MEMORY CONTENT:\n" + (content or "")[:1200] +
            ("\n\nSUMMARY:\n" + (summary or "")[:300] if summary else ""))
    payload = json.dumps({"model": MODEL, "messages": [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user}],
        "max_tokens": MAX_TOKENS, "temperature": 0.0}).encode()
    try:
        req = urllib.request.Request(API_URL, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            d = json.loads(resp.read().decode())
        content_out = d["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{.*\}", content_out, re.S)
        if not m:
            return None, 0.0
        obj = json.loads(m.group(0))
        date = obj.get("date")
        conf = float(obj.get("confidence", 0))
        if date:
            mm = re.search(r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})", str(date))
            if mm:
                d = _valid_date(mm.group(1), mm.group(2), mm.group(3))
                if d and conf >= CONF_MIN:
                    return d, conf
        return None, 0.0
    except Exception as e:
        return None, 0.0


# ── 主提取接口 ────────────────────────────────────
def extract_one(summary, content, anchor_ts, use_llm=True, stats=None):
    """单条提取 → (event_date, confidence, method)。method: rule|llm|skip。"""
    anchor_date = _parse_anchor(anchor_ts)
    text = (content or "") + "\n" + (summary or "")
    if not _RE_SIGNAL.search(text or ""):
        if stats is not None:
            stats["no_signal"] += 1
        return None, 0.0, "skip"
    d, conf = rule_extract(content or "", anchor_date)
    if d is None:
        d, conf = rule_extract(summary or "", anchor_date)
    if d:
        if stats is not None:
            stats["rule"] += 1
        return d, conf, "rule"
    if use_llm:
        if stats is not None:
            stats["llm"] += 1
        d, conf = llm_extract(content, summary, anchor_date)
        return d, conf, "llm"
    return None, 0.0, "skip"


def extract_batch(rows, use_llm=True, llm_max_ratio=LLM_MAX_RATIO, interval=LLM_INTERVAL,
                  progress_step=100, on_progress=None):
    """rows: [(id, summary, content, anchor_ts)] → [{id, event_date, conf, method}]。

    LLM 触发率红线：llm 调用数 / 总条数 ≤ llm_max_ratio（超限降级为不提取）。"""
    stats = {"no_signal": 0, "rule": 0, "llm": 0, "llm_called": 0, "written": 0}
    out = []
    llm_calls = 0
    llm_budget = max(1, int(len(rows) * llm_max_ratio))
    for i, (rid, summary, content, anchor_ts) in enumerate(rows):
        use_llm_i = use_llm and llm_calls < llm_budget
        d, conf, method = extract_one(summary, content, anchor_ts,
                                      use_llm=use_llm_i, stats=stats)
        if method == "llm":
            llm_calls += 1
            stats["llm_called"] += 1
            if d:
                stats["written"] += 1
                out.append({"id": rid, "event_date": d, "conf": conf, "method": method})
            else:
                out.append({"id": rid, "event_date": None, "conf": 0.0, "method": method})
        elif method == "rule":
            stats["written"] += 1
            out.append({"id": rid, "event_date": d, "conf": conf, "method": method})
        # skip → 不记录（无时间信号）
        if interval and method == "llm":
            time.sleep(interval)
        if progress_step and (i + 1) % progress_step == 0:
            if on_progress:
                on_progress(i + 1, len(rows), stats)
    return out, stats


# ── 库操作 ────────────────────────────────────────
def load_rows(conn, mode, limit=None, only_null=True, source_prefix=None, force=False):
    if mode == "amb":
        tbl, anchor_col = "memories", "doc_ts"
        sel = "id, summary, content, doc_ts"
    else:
        tbl, anchor_col = "memory_store", "timestamp"
        sel = "id, summary, content, timestamp"
    conds = []
    args = []
    if only_null and not force:
        conds.append("(event_date IS NULL OR event_date='')")
    if source_prefix:
        conds.append("source_ref LIKE ?")
        args.append(source_prefix + "%")
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    sql = f"SELECT {sel} FROM {tbl}{where}"
    if limit:
        sql += f" LIMIT {limit}"
    return conn.execute(sql, args).fetchall()


def write_event_dates(conn, mode, updates, audit_log_path):
    """updates: [(id, event_date, conf, method)] 或 [{id,event_date,conf,method}] 高置信写入（幂等）。

    2026-08-28 修复：前序版本元组解包 dict 列表 → 迭代键名（"id"/"event_date"）→
    UPDATE 写成 SET event_date='event_date' WHERE id='id' 匹配 0 行静默失败（done 虚增）。
    本版兼容两种形态，并校验 id/event_date 为真实值（形如 'id'/'event_date' 的占位符拒写）。"""
    tbl = "memories" if mode == "amb" else "memory_store"
    done = 0
    with open(audit_log_path, "a", encoding="utf-8") as f:
        for u in updates:
            if isinstance(u, dict):
                rid, ev, conf, method = u["id"], u["event_date"], u["conf"], u["method"]
            else:
                rid, ev, conf, method = u
            if not ev or ev == "event_date" or rid == "id":
                continue  # 占位符/空值拒写（宁缺勿滥）
            conn.execute(f"UPDATE {tbl} SET event_date=? WHERE id=?", (ev, rid))
            f.write(json.dumps({"op": "extract_write", "mode": mode, "id": rid,
                                "event_date": ev, "conf": conf, "method": method,
                                "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")},
                               ensure_ascii=False) + "\n")
            done += 1
        conn.commit()
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--mode", choices=["amb", "prod"], default="amb")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--source-prefix", default=None, help="限定 source_ref 前缀（如 amb:longmemeval:）")
    ap.add_argument("--force", action="store_true", help="全量重扫（含已有 event_date——覆盖写场景）")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--write", action="store_true", help="写库（默认 dry-run）")
    ap.add_argument("--sample", type=int, default=30, help="抽样核条数")
    ap.add_argument("--no-llm", action="store_true", help="纯规则（0token）")
    ap.add_argument("--audit", default=os.path.join(_SIKU_ROOT, "scripts/siku_option/extract_event_time_audit.jsonl"))
    args = ap.parse_args()

    if not os.path.exists(args.db):
        print("库不存在: %s" % args.db, file=sys.stderr)
        sys.exit(1)
    print("目标库: %s 模式: %s limit=%s prefix=%s force=%s llm=%s" % (
        args.db, args.mode, args.limit, args.source_prefix, args.force,
        "关" if args.no_llm else "开(本地)"))

    conn = sqlite3.connect(args.db, timeout=120)
    conn.execute("PRAGMA busy_timeout=120000")
    rows = load_rows(conn, args.mode, args.limit, only_null=True,
                     source_prefix=args.source_prefix, force=args.force)
    total = conn.execute("SELECT COUNT(*) FROM %s" % (
        "memories" if args.mode == "amb" else "memory_store")).fetchone()[0]
    print("总行数=%d 待提取(无 event_date)=%d" % (total, len(rows)))

    def prog(i, n, st):
        print("  进度 %d/%d 规则=%d LLM调用=%d 产出=%d" % (
            i, n, st["rule"], st["llm_called"], st["written"]), flush=True)

    t0 = time.time()
    out, stats = extract_batch(rows, use_llm=not args.no_llm,
                               llm_max_ratio=LLM_MAX_RATIO,
                               on_progress=prog)
    print("提取完成: 无信号=%d 规则=%d LLM调用=%d(触发率=%.1f%%≤30%%) 高置信产出=%d 耗时=%.0fs" % (
        stats["no_signal"], stats["rule"], stats["llm_called"],
        stats["llm_called"] / len(rows) * 100 if rows else 0,
        stats["written"], time.time() - t0))

    # 抽样核（规则+LLM 混合抽）
    n_written = [r for r in out if r["event_date"]]
    if not n_written:
        print("无可写条目。")
        return
    import random
    random.seed(20260827)
    sample = random.sample(n_written, min(args.sample, len(n_written)))
    print("\n=== 抽样核 %d 条（池 %d）===" % (len(sample), len(n_written)))
    idx_by_id = {r[0]: r for r in rows}
    for s in sample:
        src = idx_by_id.get(s["id"], (None, "", "", ""))
        snip = (src[1] or src[2] or "")[:90].replace("\n", " ")
        print("  %s | %s->%s [%s conf=%.2f] | %s" % (
            s["id"][:14], s["method"], s["event_date"], s["method"], s["conf"], snip))

    if args.write:
        n_upd = write_event_dates(conn, args.mode, n_written, args.audit)
        filled = conn.execute("SELECT COUNT(*) FROM %s WHERE event_date IS NOT NULL AND event_date!=''" % (
            "memories" if args.mode == "amb" else "memory_store")).fetchone()[0]
        print("\n已写 %d 条（审计: %s）→ event_date 非空 %d/%d (%.1f%%)" % (
            n_upd, args.audit, filled, total, filled / total * 100))
    else:
        print("\n[dry-run] 未写库。--write 才写。")
    conn.close()


if __name__ == "__main__":
    main()
