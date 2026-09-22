#!/usr/bin/env python3
# ── venv 守卫 ( 2026-08-17) ──
# 非 venv 解释器（系统 python 3.9 等）自动重执行为 venv python——fail-closed，不静默降级。
# 升级/venv 路径变化后系统 python 跑会缺 3.11 编译依赖；SIKU_VENV_PYTHON 可覆盖（运维/沙盒测试）。
import os as _os, sys as _sys

_SIKU_ROOT = _os.environ.get("SIKU_ROOT", _os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = _os.environ.get("HERMES_HOME", _os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
if _os.environ.get("SIKU_VENV_GUARDED") != "1":
    _vp = _os.environ.get("SIKU_VENV_PYTHON") or _os.path.join(_HERMES_HOME, "hermes-agent/venv/bin/python")
    if not _os.path.exists(_vp):
        _sys.stderr.write("WARN venv-guard: %s 不存在 → 运行于系统解释器 %s（降级已留痕）\n" % (_vp, _sys.executable))
    elif _os.path.realpath(_sys.executable) != _os.path.realpath(_vp):
        _os.environ["SIKU_VENV_GUARDED"] = "1"
        _os.execv(_vp, [_vp] + _sys.argv)

"""
RETA Pipeline — 自动晋升管道（P2.5）

L1→L2: 每日02:00 会话摘要→私有记忆
L2→L3: 每日03:00 符合条件的私记晋升大数据底座
L3→L4b: 周日22:00 全量蒸馏

日志: success/fail + 连续2次失败告警
v4.1: 已去掉训练数据引擎相关内容
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）
# : 类型枚举权威源（13 内容 + 8 资产）——校验/分支统一引此，禁硬编码清单
import siku_types
# : 跨库冲突判定（一套判定+双适配器——影子模式只记录不拦截）
# 导入失败 → 冲突检测降级跳过（不阻断管道写入）；SIKU_CONFLICT_MODE=off 可显式关闭（零 diff 回退）
try:
    import siku_conflict
except Exception as _conflict_import_err:
    siku_conflict = None
    try:
        _awd = os.environ.get("SIKU_AUDIT_DIR") or os.path.join(_SIKU_ROOT, "audit", "write")
        os.makedirs(_awd, exist_ok=True)
        with open(os.path.join(_awd, "audit-%s.yaml" % datetime.now().strftime("%Y-%m-%d")), "a") as _af:
            _af.write("---\nts: %s\nop: conflict-guard\nop_type: warn\neid: import-fail\ntype: guard\nsummary_a: siku_conflict 导入失败 → 跨库冲突检测降级跳过: %s\nalert: yes\n---\n"
                      % (datetime.now().isoformat(), _conflict_import_err))
    except Exception:
        pass
# : 摄入过滤（substance/冗余——影子模式只标记不拦截；SIKU_INGEST_FILTER=1 启用）
# 导入失败 → 检测降级跳过（不阻断管道写入）；默认关（生产零行为变化）
try:
    import ingest_filter
except Exception as _ingest_import_err:
    ingest_filter = None
    try:
        _awd = os.environ.get("SIKU_AUDIT_DIR") or os.path.join(_SIKU_ROOT, "audit", "write")
        os.makedirs(_awd, exist_ok=True)
        with open(os.path.join(_awd, "audit-%s.yaml" % datetime.now().strftime("%Y-%m-%d")), "a") as _af:
            _af.write("---\nts: %s\nop: ingest-filter\nts_op_type: warn\neid: import-fail\ntype: guard\nsummary_a: ingest_filter 导入失败 → 摄入过滤降级跳过: %s\nalert: yes\n---\n"
                      % (datetime.now().isoformat(), _ingest_import_err))
    except Exception:
        pass
# : 事件时间提取（规则优先+LLM 兜底——本地 8081，云端零调用红线）
# 写时默认纯规则（0token 零风险）；LLM 兜底需 SIKU_EVENT_LLM=1 显式开启（高频写场景默认关防成本失控）
try:
    import extract_event_time as eet
except Exception as _eet_err:
    eet = None
    try:
        _awd = os.environ.get("SIKU_AUDIT_DIR") or os.path.join(_SIKU_ROOT, "audit", "write")
        os.makedirs(_awd, exist_ok=True)
        with open(os.path.join(_awd, "audit-%s.yaml" % datetime.now().strftime("%Y-%m-%d")), "a") as _af:
            _af.write("---\nts: %s\nop: eventextract\nts_op_type: warn\neid: import-fail\ntype: guard\nsummary_a: extract_event_time 导入失败 → 写时事件提取降级跳过: %s\nalert: yes\n---\n"
                      % (datetime.now().isoformat(), _eet_err))
    except Exception:
        pass
# (2026-08-16): 蒸馏入库相似度去重守卫（增量防检索池膨胀；阈值/开关可配）
# 缺失/导入失败 → SimDedupGuard=None → 回退原行为（文件存在精确去重不变）
try:
    from distill_sim_dedup import SimDedupGuard
except Exception as _dedup_import_err:
    SimDedupGuard = None
    # (2026-08-17): 守卫绕过留痕——导入失败 → audit WARN（gate_check 通道），不静默降级
    try:
        _awd = os.environ.get("SIKU_AUDIT_DIR") or os.path.join(_SIKU_ROOT, "audit", "write")
        os.makedirs(_awd, exist_ok=True)
        with open(os.path.join(_awd, "audit-%s.yaml" % datetime.now().strftime("%Y-%m-%d")), "a") as _af:
            _af.write("---\nts: %s\nop: dedup-guard\nop_type: warn\neid: import-fail\ntype: guard\nsummary_a: SimDedupGuard 导入失败 → 相似度去重降级: %s\nalert: yes\n---\n"
                      % (datetime.now().isoformat(), _dedup_import_err))
    except Exception:
        pass

# ── 配置 ────────────────────────────────────────────

BASE = _SIKU_ROOT
L3_DB = os.path.join(BASE, "memory_store.db")
LOG_DIR = os.path.join(BASE, "logs")
ALERT_LOG = os.path.join(LOG_DIR, "reta_alerts.log")
PIPELINE_LOG = os.path.join(LOG_DIR, "reta_pipeline.log")
STATUS_FILE = os.path.join(LOG_DIR, "reta_status.json")

# 晋升条件
PROMOTE_CONFIDENCE_THRESHOLD = 0.8
PROMOTE_MIN_REPEAT = 2  # 重复≥2次晋升
PROMOTE_FORCE_KEYWORD = "记住"  # 用户说"记住"强制晋升

# A6-P2: 按 type 默认有效期（天）。未列出的类型视为永久（expires_at 留空）。
# 短期事实/临时记录到期后由衰减执行器标记 deprecated，避免陈旧信息长期污染检索。
DEFAULT_EXPIRES_DAYS = {
    "fact": 90,      # 事实类：90天
    "record": 30,    # 临时记录：30天
    "info": 30,      # 信息类：30天
    "context": 30,   # 上下文：30天
    "correction": 90,  # 修正类：90天
}
# 永久类型（明确不设默认有效期）
# : 8 系统资产类型显式入永久集（资产本体单源化 → 永久；原逻辑未列出类型
# 本就默认永久，此处显式化 + 与 siku_types 权威源对齐防漂移）
PERMANENT_TYPES = {"instruction", "lesson", "insight", "decision", "principle",
                   "pattern", "rule", "L4b"} | set(siku_types.ASSET_SET)

# ── G2/G3 标注配置（R1 根治 ：L2→L3 INSERT 补三字段）──
# type → data_type 映射，与 siku_g2_backfill.py / g2_label.py 保持一致（含补齐 5 类）
TYPE_MAP = {
    "lesson": "instruction", "instruction": "instruction", "decision": "preference",
    "result": "QA", "context": "instruction", "L4b": "preference",
    "principle": "instruction", "insight": "instruction", "idea": "instruction",
    "crash": "instruction", "preference": "preference", "pattern": "instruction",
    "fact": "QA", "correction": "instruction",
    "info": "info", "rule": "instruction", "record": "record",
    # : 8 系统资产类型 G2 标注映射（显式化，防落 DEFAULT_DATA_TYPE 兜底）：
    # 规范/技能/流程/门禁/脚本=操作类 instruction；评测基准=QA（可验证类）；训练资产/监控=info
    "spec": "instruction", "skill": "instruction", "cron": "instruction",
    "workflow": "instruction", "benchmark": "QA",
    "asset": "info", "monitor": "info",
    # : 5 类新资产 G2 标注映射（design/script=操作方法论 instruction；
    # research/config/reference=知识信息 info——值域无 insight，按语义归 info）
    "design": "instruction", "script": "instruction",
    "research": "info", "config": "info", "reference": "info",
}
DEFAULT_DATA_TYPE = "instruction"  # 兜底（当前全表 type 均已在 TYPE_MAP 内）
G2_LABELER = "g2_reta_pipeline"    # 晋升即完成 G2 规则标注（管道内零 LLM）
G3_REVIEWER = "g3_reta_pipeline"   # 晋升即标记已过管道复核（后续 g3_inject_verify 可批跑复核）


def default_expires_at(entry_type):
    """按 type 返回默认 expires_at（ISO 时间戳）；永久类型返回空字符串"""
    days = DEFAULT_EXPIRES_DAYS.get(entry_type, 0)
    if not days or entry_type in PERMANENT_TYPES:
        return ""
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()

os.makedirs(LOG_DIR, exist_ok=True)


# ── 日志工具 ────────────────────────────────────────

def log(level, stage, msg):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] [{level}] [{stage}] {msg}"
    with open(PIPELINE_LOG, "a") as f:
        f.write(line + "\n")
    print(line)


def get_fail_count():
    """读取连续失败次数"""
    try:
        with open(STATUS_FILE) as f:
            status = json.load(f)
        return status.get("consecutive_fails", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0


def record_success(stage):
    """记录成功，重置失败计数"""
    with open(STATUS_FILE, "w") as f:
        json.dump({
            "stage": stage,
            "last_success": datetime.now(timezone.utc).isoformat(),
            "consecutive_fails": 0
        }, f, indent=2)


def record_fail(stage, err):
    """记录失败，连续2次告警"""
    fails = get_fail_count() + 1
    with open(STATUS_FILE, "w") as f:
        json.dump({
            "stage": stage,
            "last_fail": datetime.now(timezone.utc).isoformat(),
            "consecutive_fails": fails,
            "error": str(err)
        }, f, indent=2)

    if fails >= 2:
        msg = f"[ALERT] RETA管道连续{fails}次失败 (stage={stage}): {err}"
        with open(ALERT_LOG, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} {msg}\n")
        log("ALERT", stage, msg)
    else:
        log("WARN", stage, f"失败第{fails}次 (stage={stage}): {err}")


# ── L3数据库连接 ────────────────────────────────────

def get_l3_conn():
    # (2026-08-17): busy_timeout=5000——并发写卡点不再 locked 崩溃
    conn = sqlite3.connect(L3_DB, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def l3_entry_exists(summary_hash):
    """检查L3中是否已存在相同摘要的条目（去重），返回完整行或 None"""
    conn = get_l3_conn()
    row = conn.execute(
        "SELECT * FROM memory_store WHERE summary_hash = ? LIMIT 1",
        (summary_hash,)
    ).fetchone()
    conn.close()
    return row


# ── L2私有记忆操作 ──────────────────────────────────

def get_l2_path(agent_id):
    path = os.path.join(BASE, "private", agent_id)
    os.makedirs(path, exist_ok=True)
    return path


def write_l2(agent_id, entry):
    """写入L2私有记忆（YAML格式文件）"""
    path = get_l2_path(agent_id)
    entry_id = entry.get("id", str(uuid.uuid4()))
    fpath = os.path.join(path, f"{entry_id}.yaml")
    with open(fpath, "w") as f:
        f.write(f"id: {entry_id}\n")
        f.write(f"type: {entry.get('type', 'fact')}\n")
        f.write(f"summary: {entry.get('summary', '')}\n")
        f.write(f"confidence: {entry.get('confidence', 0.7)}\n")
        f.write(f"timestamp: {entry.get('timestamp', datetime.now(timezone.utc).isoformat())}\n")
        f.write(f"source: {entry.get('source', '')}\n")
        if entry.get("expires_at"):
            f.write(f"expires_at: {entry['expires_at']}\n")
        if entry.get("content"):
            f.write(f"content: |\n  {entry['content'].replace(chr(10), chr(10)+'  ')}\n")
    return entry_id


def read_all_l2(agent_id=None):
    """读取所有L2条目（可筛选agent）"""
    if agent_id:
        search_dirs = [get_l2_path(agent_id)]
    else:
        search_dirs = [
            os.path.join(BASE, "private", d)
            for d in os.listdir(os.path.join(BASE, "private"))
            if os.path.isdir(os.path.join(BASE, "private", d))
        ]
    entries = []
    for d in search_dirs:
        if not os.path.isdir(d):
            continue
        for fname in os.listdir(d):
            fpath = os.path.join(d, fname)
            try:
                if fname.endswith(".yaml"):
                    ent = parse_yaml_file(fpath)
                elif fname.endswith(".md"):
                    ent = parse_md_file(fpath)
                    if ent is None:
                        continue  # 无 frontmatter 的 .md 跳过
                else:
                    continue
                ent["_file"] = fpath
                ent["_agent"] = os.path.basename(d)
                entries.append(ent)
            except Exception as e:
                log("WARN", "read_l2", f"解析失败 {fpath}: {e}")
    return entries


def parse_yaml_file(fpath):
    """简易YAML解析（只解析单层键值对，不引pyyaml）"""
    ent = {}
    with open(fpath) as f:
        content = f.read()
    for line in content.split("\n"):
        line = line.strip()
        if ":" in line and not line.startswith(" "):
            k, v = line.split(":", 1)
            ent[k.strip()] = v.strip()
    # 提取content块（| 之后的多行）
    in_content = False
    content_lines = []
    for line in content.split("\n"):
        if line.strip().startswith("content: |"):
            in_content = True
            continue
        if in_content:
            if line.startswith("  "):
                content_lines.append(line[2:])
            else:
                in_content = False
    if content_lines:
        ent["content"] = "\n".join(content_lines)
    return ent



def parse_md_file(fpath):
    """解析.md文件：优先读YAML frontmatter，无frontmatter则用文件名+全文"""
    with open(fpath) as f:
        raw = f.read()
    ent = {}

    if raw.startswith("---"):
        parts = raw.split("---", 2)
        if len(parts) >= 3:
            frontmatter_text = parts[1].strip()
            body = parts[2].strip()
            for line in frontmatter_text.split("\n"):
                line = line.strip()
                if ":" in line and not line.startswith(" "):
                    k, v = line.split(":", 1)
                    ent[k.strip()] = v.strip()
            ent["content"] = body
        else:
            ent["content"] = raw
    else:
        # 方案B: 无frontmatter的.md用文件名做标题，全文做内容
        ent["content"] = raw

    if "type" not in ent:
        ent["type"] = "record"
    if "summary" not in ent:
        body = ent.get("content", "")
        first_line = body.split("\n")[0].strip().lstrip("# ")
        base = os.path.basename(fpath).replace(".md", "")
        ent["summary"] = (first_line or base)[:200]
    if "confidence" not in ent:
        ent["confidence"] = "0.7"
    if "agent" not in ent:
        ent["agent"] = os.path.basename(os.path.dirname(fpath))
    if "timestamp" not in ent:
        from datetime import datetime, timezone
        ent["timestamp"] = datetime.now(timezone.utc).isoformat()
    if "source" not in ent:
        ent["source"] = "L2:markdown:" + ent.get("agent", "unknown")
    try:
        ent["confidence"] = float(ent["confidence"])
    except Exception:
        ent["confidence"] = 0.7
    return ent

# ── L1→L2：会话摘要 → 私有记忆 ────────────────────

def stage_l1_to_l2(session_data=None):
    """
    L1→L2 摘要晋升
    - 从会话内存提取有价值的片段
    - 去重后写入L2私有记忆
    - 模拟输入：session_data可以是JSON字符串或dict列表
    """
    log("INFO", "L1→L2", "开始摘要处理")

    if session_data is None:
        # 无输入时创建占位记录（cron调用时通过管道传session快照）
        # 2026-08-09 体检B卡：设计外——daily/ 为 md 文档无结构化输入源，实质由 gene-promote 旁路承担
        log("INFO", "L1→L2", "无会话数据输入，跳过（设计外：由 gene-promote 旁路承担）")
        record_success("L1→L2")
        return {"processed": 0, "skipped": True, "reason": "no_input"}

    if isinstance(session_data, str):
        session_data = json.loads(session_data)

    processed = 0
    skipped = 0
    for item in session_data:
        # 晋升检查
        summary = item.get("summary", "").strip()
        if not summary or len(summary) < 10:
            skipped += 1
            continue

        agent_id = item.get("agent_id", item.get("source", "unknown"))
        confidence = float(item.get("confidence", 0.7))
        summary_hash = hashlib.sha256(summary.encode('utf-8')).hexdigest()[:12]

        # 去重：检查是否已在L2中存在
        if l2_entry_exists(agent_id, summary_hash):
            skipped += 1
            continue

        # 写入L2
        entry_type = item.get("type", "fact")
        # : 类型枚举校验（L1→L2 入口，假类型拒收不写脏数据）
        try:
            siku_types.validate_type(entry_type, strict=True)
        except ValueError as _te:
            log("WARN", "L1→L2", f"类型校验拒收: type={entry_type!r} summary={summary[:40]!r}（{_te}）")
            skipped += 1
            continue
        entry = {
            "id": str(uuid.uuid4()),
            "type": entry_type,
            "summary": summary,
            "content": item.get("content", ""),
            "confidence": confidence,
            "timestamp": item.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "source": f"L1:{agent_id}",
            # A6-P2: 显式 expires_at 优先，否则按 type 默认有效期
            "expires_at": item.get("expires_at", "") or default_expires_at(entry_type),
        }
        write_l2(agent_id, entry)
        processed += 1

    record_success("L1→L2")
    log("INFO", "L1→L2", f"处理完成: {processed}条写入, {skipped}条跳过")
    return {"processed": processed, "skipped": skipped}


def l2_entry_exists(agent_id, summary_hash):
    """检查L2中是否已存在相同摘要"""
    path = get_l2_path(agent_id)
    for fname in os.listdir(path):
        if not (fname.endswith(".yaml") or fname.endswith(".md")):
            continue
        with open(os.path.join(path, fname)) as f:
            content = f.read()
        if summary_hash in content:
            return True
    return False


# ── L2→L3：符合条件的私记晋升大数据底座 ────────────

def stage_l2_to_l3():
    """
    L2→L3 自动晋升
    条件：置信度≥0.8 或 重复≥2次 或 用户说"记住"
    """
    log("INFO", "L2→L3", "开始晋升检查")

    entries = read_all_l2()
    promoted = 0
    skipped = 0
    conn = get_l3_conn()
    promoted_files = []

    for ent in entries:
        agent_id = ent.get("_agent", "unknown")
        confidence = float(ent.get("confidence", 0.7))
        summary = ent.get("summary", "")
        content = ent.get("content", "")
        entry_type = ent.get("type", "fact")
        # : 类型枚举校验（L2→L3 晋升入口，假类型拒晋升防脏数据入 L3）
        try:
            siku_types.validate_type(entry_type, strict=True)
        except ValueError as _te:
            log("WARN", "L2→L3", f"类型校验拒晋升: type={entry_type!r} summary={summary[:40]!r}（{_te}）")
            skipped += 1
            continue
        summary_hash = hashlib.sha256(summary.encode('utf-8')).hexdigest()[:12]

        # 晋升条件检查
        should_promote = False
        reason = None

        if confidence >= PROMOTE_CONFIDENCE_THRESHOLD:
            should_promote = True
            reason = f"高置信度({confidence})"

        if not should_promote:
            repeat_count = count_l2_pattern_repeats(agent_id, summary_hash)
            if repeat_count >= PROMOTE_MIN_REPEAT:
                should_promote = True
                reason = f"重复{repeat_count}次"

        if not should_promote:
            if PROMOTE_FORCE_KEYWORD in summary or PROMOTE_FORCE_KEYWORD in content:
                should_promote = True
                reason = "强制标记(记住)"

        if not should_promote:
            skipped += 1
            continue

        # A6-P2 排重链：同 summary_hash 已有条目 → 更新而非新增。
        # 语义同事实不同日期版本：保留最新写入，刷新有效期；过期版本由衰减执行器标记 deprecated。
        existing = l3_entry_exists(summary_hash)

        # 计算 expires_at：显式指定优先，否则按 type 默认有效期
        expires_at = ent.get("expires_at", "") or default_expires_at(entry_type)
        # 读取L2 YAML中的concern_id（如果有）
        concern_id = ent.get("concern_id", "unclassified")

        ts = datetime.now(timezone.utc).isoformat()

        if existing is not None:
            # 更新已有条目：刷新内容/有效期/来源，保留 id 与审计链
            old_audit = existing["audit_log"] or "[]"
            try:
                audit = json.loads(old_audit)
            except Exception:
                audit = []
            audit.append({
                "action": "l2_promote_update",
                "reason": reason,
                "promoted_at": ts,
                "prev_id": existing["id"],
            })
            # : 写时事件时间提取（规则优先；LLM 兜底需 SIKU_EVENT_LLM=1）
            ev_date = None
            if eet is not None:
                use_llm = os.environ.get("SIKU_EVENT_LLM") == "1"
                try:
                    ev_date, _conf, _m = eet.extract_one(
                        summary, content, ts, use_llm=use_llm)
                except Exception:
                    ev_date = None
            conn.execute("""
                UPDATE memory_store
                SET type = ?, summary = ?, content = ?, confidence = ?,
                    timestamp = ?, source_agent = ?, audit_log = ?,
                    concern_id = ?, industry = ?, source_ref = ?,
                    expires_at = ?, updated_at = ?, event_date = ?
                WHERE id = ?
            """, (entry_type, summary, content, confidence,
                  ts, agent_id, json.dumps(audit, ensure_ascii=False),
                  concern_id, ent.get("industry", ""), ent.get("source_ref", ""),
                  expires_at, ts, ev_date, existing["id"]))
            promoted_files.append(ent["_file"])
            promoted += 1
            log("INFO", "L2→L3", f"更新: {summary[:40]}... (summary_hash 已存在, 刷新有效期={expires_at or '永久'}, event_date={ev_date or '无'})")
            continue

        # 写入L3
        # [DEPRECATED-2026-08-27] g2_labeled_by/g3_reviewed_by 旧字段废弃，
        # 此处保留写入仅为存量兼容/审计，新判定以 gene_qc_trigger(review_state) 为准。
        # （沙盒发现：原 SQL 字符串内嵌 "#" 注释 → sqlite unrecognized token，注释外移修复）
        entry_id = str(uuid.uuid4())
        # : 跨库冲突检测（写入端逐条过 filter——影子模式只记录不拦截）
        # 四库适配器：memory_store 表；SIKU_CONFLICT_MODE=shadow（默认）/enforce/off；矛盾→标记不覆盖（双向 audit 留痕）
        conflict_type = ""
        conflict_audits = []
        conflict_low = False
        if siku_conflict is not None and os.environ.get("SIKU_CONFLICT_MODE", "shadow") != "off":
            try:
                _cres = siku_conflict.detect_for_write(
                    siku_conflict.SikuAdapter(L3_DB, "memory_store"),
                    conn,
                    {"summary": summary, "content": content, "type": entry_type},
                    mode=os.environ.get("SIKU_CONFLICT_MODE", "shadow"),
                    new_id=entry_id,
                )
                conflict_type = _cres["conflict_type"]
                conflict_audits = _cres["audits"]
                conflict_low = _cres.get("low_confidence", False)
            except Exception as _ce:
                log("WARN", "L2→L3", f"冲突检测异常（跳过，照常写入）: {_ce}")
        # : 写时事件时间提取（规则优先；LLM 兜底需 SIKU_EVENT_LLM=1）
        ev_date = None
        if eet is not None:
            use_llm = os.environ.get("SIKU_EVENT_LLM") == "1"
            try:
                ev_date, _conf, _m = eet.extract_one(summary, content, ts, use_llm=use_llm)
            except Exception:
                ev_date = None
        _base_audit = [{
            "action": "l2_promote",
            "reason": reason,
            "promoted_at": ts,
        }]
        if conflict_type:
            _base_audit.append({
                "action": "conflict_detect",
                "conflict_type": conflict_type,
                "subtype": conflict_audits[0]["subtype"] if conflict_audits else "",
                "conflict_with": conflict_audits[0]["matched_id"] if conflict_audits else None,
                "mode": "shadow",
                "low_confidence": conflict_low,
            })
        # : 摄入过滤（影子模式只标记不拦截——SIKU_INGEST_FILTER=1 启用，默认关）
        if ingest_filter is not None and os.environ.get("SIKU_INGEST_FILTER") == "1":
            try:
                _ires = ingest_filter.detect(
                    {"summary": summary, "content": content, "type": entry_type}, conn)
                if _ires["flagged"]:
                    _base_audit.append({
                        "action": "ingest_filter",
                        "state": _ires["state"],
                        "reason": _ires["reason"],
                        "redundant_of": _ires.get("redundant_of"),
                        "mode": "shadow",
                    })
                    log("INFO", "L2→L3", f"摄入过滤（影子）: {summary[:30]}... → {_ires['state']}（{_ires['reason']}）")
            except Exception as _ie:
                log("WARN", "L2→L3", f"摄入过滤异常（跳过，照常写入）: {_ie}")
        conn.execute("""
            INSERT INTO memory_store
                (id, version, timestamp, type, summary, content,
                 confidence, trust_score, importance, source_agent,
                 audit_log, summary_hash, concern_id,
                 industry, source_ref, expires_at,
                 g2_labeled_by, g3_reviewed_by, data_type, event_date, conflict_type)
            VALUES (?, '1.0', ?, ?, ?, ?, ?, 0.7, 5, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?)
        """, (entry_id, ts, entry_type, summary, content,
              confidence, agent_id, json.dumps(_base_audit, ensure_ascii=False), summary_hash, concern_id,
              ent.get("industry", ""), ent.get("source_ref", ""), expires_at,
              G2_LABELER, G3_REVIEWER, TYPE_MAP.get(entry_type, DEFAULT_DATA_TYPE), ev_date, conflict_type))
        if conflict_type:
            log("INFO", "L2→L3", f"冲突检测（影子）: {summary[:30]}... → {conflict_type}"
                + (f" 关联旧条目={conflict_audits[0]['matched_id']}" if conflict_audits else ""))
        promoted_files.append(ent["_file"])
        promoted += 1
        log("INFO", "L2→L3", f"晋升: {summary[:40]}... ({reason}, event_date={ev_date or '无'})")

    conn.commit()
    conn.close()

    # S2 M3 清缓存挂钩：L2→L3 晋升写入后全清 query_cache
    query_cache_invalidate.invalidate_query_cache()

    # 先commit成功，再删除L2源文件（防崩溃丢数据）
    for fpath in promoted_files:
        try:
            os.remove(fpath)
        except OSError:
            pass

    record_success("L2→L3")
    log("INFO", "L2→L3", f"晋升完成: {promoted}条晋升, {skipped}条跳过")
    return {"promoted": promoted, "skipped": skipped}


def count_l2_pattern_repeats(agent_id, summary_hash):
    """统计同一agent下相似摘要的出现次数"""
    path = get_l2_path(agent_id)
    if not os.path.isdir(path):
        return 0
    count = 0
    for fname in os.listdir(path):
        if summary_hash in fname:
            count += 1
    return count


# ── L3→L4b：全量蒸馏 ──────────────────────────────

def stage_l3_to_l4b():
    """
    L3→L4b 全量蒸馏
    - 从L3提取高价值条目
    - 归类为pattern/principles/methods/insights/avoid
    - 写入L4b策展层
    """
    log("INFO", "L3→L4b", "开始全量蒸馏")

    conn = get_l3_conn()
    rows = conn.execute("""
        SELECT id, type, summary, content, confidence, importance, timestamp, source_agent,
               industry, source_ref, expires_at
        FROM memory_store
        WHERE confidence >= 0.6 AND importance >= 3
        ORDER BY importance DESC, confidence DESC
    """).fetchall()
    conn.close()

    if not rows:
        log("INFO", "L3→L4b", "无可蒸馏条目")
        record_success("L3→L4b")
        return {"distilled": 0}

    # 分类映射
    type_map = {
        "lesson": "avoid",
        "decision": "principles",
        "preference": "patterns",
        "pattern": "patterns",
        "insight": "insights",
        "fact": "methods",
        "correction": "avoid",
    }

    # (2026-08-16): 相似度去重守卫（按目标目录懒建索引，同日多行复用）
    guard_cache = {}

    def get_guard(target_dir):
        """对目标目录下既有 curation yaml（剥 frontmatter 后 summary+body）建相似度索引；
        初始化失败 → None → 回退原行为（文件存在精确去重）"""
        if target_dir in guard_cache:
            return guard_cache[target_dir]
        g = None
        if SimDedupGuard is not None:
            try:
                g = SimDedupGuard()
                if not g.enabled:
                    # 开关禁用 → 不建索引，完全回退原行为（文件存在精确去重）
                    guard_cache[target_dir] = g
                    return g
                if os.path.isdir(target_dir):
                    for fname in os.listdir(target_dir):
                        if fname.endswith(".yaml"):
                            try:
                                with open(os.path.join(target_dir, fname), encoding="utf-8") as f:
                                    raw = f.read()
                                body = re.sub(r"^---.*?---", "", raw, flags=re.S)
                                g.add(body)
                            except OSError:
                                pass
            except Exception as e:
                log("WARN", "L3→L4b", f"相似度去重守卫初始化失败，回退原行为: {e}")
                g = None
        guard_cache[target_dir] = g
        return g

    distilled = 0
    sim_skipped = 0
    for row in rows:
        row = dict(row)
        l4b_type = type_map.get(row["type"], "insights")
        concept_slug = hashlib.sha256(row["id"].encode()).hexdigest()[:12]
        row_text = (row.get("summary") or "") + "\n" + (row.get("content") or "")

        # 行业路由：有industry字段则走industry/子目录
        industry_raw = str(row.get("industry", "") or "").strip()
        if industry_raw:
            summary = row.get('summary', '')
            # C级来源（博客/自媒体）不入L4b，保持在L2
            source_ref = str(row.get("source_ref", "") or "")
            if any(c in source_ref.lower() for c in ["blog", "weibo", "zhihu", "medium", "自媒体", "博客"]):
                continue
            # 取第一个行业
            import json as _json
            try:
                industries = _json.loads(industry_raw) if industry_raw.startswith("[") else [industry_raw.strip("[] \"'")]
            except Exception:
                industries = [industry_raw.strip("[] \"'")]
            first_industry = industries[0].strip()
            # 跨行业标注
            tags = ""
            if len(industries) > 1:
                tags = ", ".join(industries)
            fpath = os.path.join(BASE, "smart", "curation", "industry", first_industry, f"{concept_slug}.yaml")
            os.makedirs(os.path.dirname(fpath), exist_ok=True)
            if os.path.exists(fpath):
                continue
            # : 相似度去重（同目录既有 yaml 内容 > 阈值 → 跳过）
            guard = get_guard(os.path.dirname(fpath))
            if guard is not None:
                dup, sim, reason = guard.is_duplicate(row_text)
                if dup:
                    sim_skipped += 1
                    log("INFO", "L3→L4b", f"相似度去重跳过: sim={sim:.3f} ({reason}) {(row.get('summary') or '')[:40]}")
                    continue
            with open(fpath, "w") as f:
                f.write("---\n")
                f.write(f"id: {row['id']}\n")
                f.write(f"type: {l4b_type}\n")
                f.write(f"source_type: {row['type']}\n")
                f.write(f"industry: [{first_industry}]\n")
                f.write(f"source_ref: {source_ref}\n")
                if row.get("expires_at"):
                    f.write(f"expires_at: \"{row['expires_at']}\"\n")
                f.write(f"confidence: {row.get('confidence', 0.7)}\n")
                f.write(f"importance: {row.get('importance', 5)}\n")
                f.write(f"distilled_at: \"{datetime.now(timezone.utc).isoformat()}\"\n")
                f.write(f"source_agent: {row.get('source_agent', '')}\n")
                f.write(f"source_l3_id: {row['id']}\n")
                f.write(f"verification_status: pending\n")
                if "拿不准" in summary or not industry_raw:
                    f.write("review_needed: true\n")
                if tags:
                    f.write(f"tags: [{tags}]\n")
                f.write("---\n")
                summary = row.get('summary', '')
                if summary:
                    f.write(f"\n# {summary}\n")
                if row.get("content"):
                    f.write("\nbody: |\n")
                    for line in row['content'].split('\n'):
                        f.write(f"  {line}\n")
            distilled += 1
            if guard is not None:
                guard.add(row_text)  # : 注册本次入库内容，同批后续近义行可命中
            continue  # 已处理industry分支，跳过原逻辑

        # 原逻辑：无industry字段走原有type_map路径
        fpath = os.path.join(BASE, "smart", "curation", l4b_type, f"{concept_slug}.yaml")
        os.makedirs(os.path.dirname(fpath), exist_ok=True)

        # 去重：已存在则跳过
        if os.path.exists(fpath):
            continue
        # : 相似度去重（同目录既有 yaml 内容 > 阈值 → 跳过）
        guard = get_guard(os.path.dirname(fpath))
        if guard is not None:
            dup, sim, reason = guard.is_duplicate(row_text)
            if dup:
                sim_skipped += 1
                log("INFO", "L3→L4b", f"相似度去重跳过: sim={sim:.3f} ({reason}) {(row.get('summary') or '')[:40]}")
                continue

        with open(fpath, "w") as f:
            f.write("---\n")
            f.write(f"id: {row['id']}\n")
            f.write(f"type: {l4b_type}\n")
            f.write(f"source_type: {row['type']}\n")
            f.write(f"confidence: {row.get('confidence', 0.7)}\n")
            f.write(f"importance: {row.get('importance', 5)}\n")
            f.write(f"distilled_at: \"{datetime.now(timezone.utc).isoformat()}\"\n")
            f.write(f"source_agent: {row.get('source_agent', '')}\n")
            f.write(f"source_l3_id: {row['id']}\n")
            f.write(f"verification_status: pending\n")
            # L4b规范字段（2026-07-19）：industry/list + source_ref前缀 + expires_at
            ind_val = row.get('industry', '通用')
            if isinstance(ind_val, str):
                f.write(f"industry: [{ind_val}]\n")
            else:
                f.write(f"industry: {ind_val}\n")
            f.write(f"source_ref: internal:curation/{row.get('type', 'unknown')}\n")
            f.write(f"expires_at: \"{row.get('expires_at', '2027-01-01T00:00:00+00:00')}\"\n")
            summary = row.get('summary', '')
            if summary:
                f.write(f"\n# {summary}\n")
            if row.get("content"):
                f.write("\nbody: |\n")
                for line in row['content'].split('\n'):
                    f.write(f"  {line}\n")
        distilled += 1
        if guard is not None:
            guard.add(row_text)  # : 注册本次入库内容，同批后续近义行可命中

    record_success("L3→L4b")
    log("INFO", "L3→L4b", f"蒸馏完成: {distilled}条" + (f" (相似度去重跳过{sim_skipped}条)" if sim_skipped else ""))
    return {"distilled": distilled, "sim_skipped": sim_skipped}


# ── 主入口 ──────────────────────────────────────────

def run_all():
    """完整管道执行"""
    log("INFO", "RETA", "=== RETA管道启动 ===")
    results = {}

    # L1→L2（设计外空跑，2026-08-09 体检B卡确认）：daily/ 为 md 文档，无结构化 session_data
    # 输入源可接线；L1→L2 实质由 gene-promote 每60分钟旁路承担。此 stage 保持空跑占位不接线。
    results["L1→L2"] = stage_l1_to_l2(None)

    # L2→L3
    results["L2→L3"] = stage_l2_to_l3()

    # L3→L4b（周日全量）
    is_sunday = datetime.now().weekday() == 6
    if is_sunday:
        results["L3→L4b"] = stage_l3_to_l4b()
    else:
        log("INFO", "RETA", "非周日，跳过全量蒸馏")
        results["L3→L4b"] = {"skipped": True, "reason": "not_sunday"}

    log("INFO", "RETA", f"=== RETA管道完成: {json.dumps(results)} ===")
    return results


def perm_audit(layer):
    """P0-2 权限写路径接入(2026-08-03): audit模式只记录不拦截, 观察1周后转enforce"""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import check_permission as cp
    cp.check(os.environ.get("SIKU_AGENT", "用户"), "write", layer, mode="audit")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="RETA自动晋升管道")
    parser.add_argument("--stage", choices=["all", "l1-l2", "l2-l3", "l3-l4b"],
                        default="all", help="执行阶段")
    parser.add_argument("--session-data", help="L1会话数据JSON字符串")
    parser.add_argument("--dry-run", action="store_true",
                        help="dry-run模式：只输出计划不执行")

    args = parser.parse_args()

    # 在dry-run模式下，各阶段只输出计划不写入
    if args.dry_run:
        log("INFO", "RETA", "=== DRY RUN模式 — 仅预览，不执行写入 ===")
        results = {}
        if args.stage in ("all", "l1-l2"):
            log("INFO", "RETA-DRY", "[L1→L2] 将会处理会话摘要→私有记忆（输入数据决定条数）")
            results["L1→L2"] = {"dry_run": True, "would_process": "from session-data"}
        if args.stage in ("all", "l2-l3"):
            l2_entries = read_all_l2()
            promote_candidates = []
            for ent in l2_entries:
                confidence = float(ent.get("confidence", 0.7))
                summary = ent.get("summary", "")[:50]
                if confidence >= PROMOTE_CONFIDENCE_THRESHOLD:
                    promote_candidates.append(f"  {summary}... (置信度{confidence})")
            log("INFO", "RETA-DRY", f"[L2→L3] 将会检查{len(l2_entries)}条L2条目")
            for c in promote_candidates:
                log("INFO", "RETA-DRY", f"  候选晋升: {c}")
            results["L2→L3"] = {"dry_run": True, "l2_count": len(l2_entries), "promote_candidates": len(promote_candidates)}
        if args.stage in ("all", "l3-l4b"):
            is_sunday = datetime.now().weekday() == 6
            log("INFO", "RETA-DRY", f"[L3→L4b] {'今天周日，将会执行全量蒸馏' if is_sunday else '非周日，跳过全量蒸馏'}")
            results["L3→L4b"] = {"dry_run": True, "is_sunday": is_sunday}
        print(json.dumps(results, indent=2, ensure_ascii=False))
        sys.exit(0)

    perm_audit("L1-L4b")
    try:
        if args.stage == "all":
            results = run_all()
        elif args.stage == "l1-l2":
            data = json.loads(args.session_data) if args.session_data else None
            results = stage_l1_to_l2(data)
        elif args.stage == "l2-l3":
            results = stage_l2_to_l3()
        elif args.stage == "l3-l4b":
            results = stage_l3_to_l4b()

        print(json.dumps(results, indent=2, ensure_ascii=False))
        sys.exit(0)

    except Exception as e:
        log("ERROR", args.stage, str(e))
        record_fail(args.stage, str(e))
        print(json.dumps({"error": str(e)}, ensure_ascii=False))
        sys.exit(1)
