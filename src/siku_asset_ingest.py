#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
siku_asset_ingest.py —— 四库资产内容 0token 入库

扫描 13 类系统资产源 → 结构化提取（名称/描述/路径/关键信息）→
siku_types.validate_type 校验（必须 is_asset_type）→ 写 memory_store 新类型
（spec/skill/cron/workflow/rule/benchmark/asset/monitor +
 design/research/script/config/reference，扩展批次 2026-08-27 再补 5 类）
——幂等只增，不碰存量 13 内容类型（无 UPDATE/DELETE，仅 INSERT 资产类型）。

幂等：双键 —— summary_hash（sha256(summary)[:12]）已存在 或
(type, source_ref) 已存在 → 跳过（同源同条目不重复写入）。

维护指令（2026-08-27）：管道已扩展——资产内容入库（L1 声明，风险 L1）。
扩展指令（2026-08-27）：再补 5 类资产源（design/research/script/config/reference）。
"""
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, timezone

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

import siku_types  # noqa: E402  四库类型枚举权威源
# P2a②：SHACL 自检 0-token 校验器——增量写时钩子。
# SIKU_SHACL=on 时 error 违例拦截（拒写）；默认 off=只报不拦影子模式（生产零行为变化）。
import shacl_validate  # noqa: E402
SHACL_GATE = os.environ.get("SIKU_SHACL", "off").lower() not in ("off", "0", "false")

# ── 生产库（l3_retrieval.py 同源定位）──
DB_PATH = os.environ.get(
    "SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db")
)

HOME = os.path.expanduser("~")
ASSET_TYPES = set(siku_types.ASSET_TYPES)  # 13 资产类型
SRC_AGENT = os.environ.get("SIKU_SRC_AGENT", "siku-core")
INGEST_REASON = "0token 资产内容入库（维护指令：管道已扩展——资产内容入库；扩展批次再补 5 类）"

# 日志文件
LOG_PATH = os.path.join(SCRIPTS_DIR, "siku_asset_ingest_%s.log" % datetime.now().strftime("%Y%m%d"))

# ── 三库同步（2026-08-29）：基因双写 + reflex 注册 ──
GENE_WRITE_PY = os.environ.get("SIKU_GENE_WRITE_PY") or os.path.join(_HERMES_HOME, "scripts", "genes", "write_gene.py")  # 受控入口
GENE_TYPES = ("design", "research")  # 判断资料类型（关键判断资料才双写/注册）
GENE_WRITE_MAX = int(os.environ.get("SIKU_GENE_WRITE_MAX", "30"))      # 单次上限防批量污染
REFLEX_SHADOW_DIR = os.environ.get("SIKU_REFLEX_SHADOW_DIR") or os.path.join(_HERMES_HOME, "scripts", "reflex", "library", "shadow")
REFLEX_WRITE_MAX = int(os.environ.get("SIKU_REFLEX_WRITE_MAX", "100"))  # 单次上限


def log(msg):
    line = "[%s] %s" % (datetime.now().strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def clip(text, n=100):
    """summary ≤100 字（字符截断：n-1 内容 + 省略号 = n）"""
    text = (text or "").strip().replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)
    if len(text) > n:
        return text[: n - 1] + "…"
    return text


def read_head(path, n=8):
    """读文件头部 n 行（提取 docstring/注释摘要用）"""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return "".join(f.readlines()[:n])
    except Exception:
        return ""


def first_doc(path):
    """提取文件头注释/docstring 首段作为功能摘要"""
    head = read_head(path)
    lines = []
    for ln in head.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if ln.startswith("#") or ln.startswith("//") or ln.startswith('"""') or ln.startswith("'''"):
            lines.append(ln.lstrip("#/\"' ").strip())
        elif re.match(r"^[a-zA-Z_]+:", ln):  # yaml 键
            continue
        elif lines:
            break
    return " ".join(lines)[:200]


def parse_frontmatter(path):
    """解析 SKILL.md frontmatter 的 name/description"""
    name, desc = "", ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            head = f.read(4000)
        m = re.search(r"^---\s*\n(.*?)\n---", head, re.S)
        if m:
            fm = m.group(1)
            nm = re.search(r"^name:\s*(.+)$", fm, re.M)
            dm = re.search(r"^description:\s*(.+)$", fm, re.M)
            if nm:
                name = nm.group(1).strip().strip('"\'')
            if dm:
                desc = dm.group(1).strip().strip('"\'')
    except Exception:
        pass
    return name, desc


# ═════════════════════════ 8 类源扫描器 ═════════════════════════

def scan_skills():
    """skill 技能包：$HERMES_HOME/skills/*/SKILL.md → type=skill"""
    items = []
    base = os.path.join(_HERMES_HOME, "skills")
    if not os.path.isdir(base):
        return items
    for entry in sorted(os.listdir(base)):
        if entry.startswith(".") or entry.startswith("_") or ".bak" in entry:
            continue
        md = os.path.join(base, entry, "SKILL.md")
        if not os.path.isfile(md):
            continue
        name, desc = parse_frontmatter(md)
        name = name or entry
        desc = desc or (first_doc(md) or "Hermes 技能包")
        items.append({
            "type": "skill",
            "name": name,
            "summary": clip(f"{name}：{desc}"),
            "content": f"路径: {md}\n说明: {desc}",
            "source_ref": md,
        })
    return items


def scan_crons():
    """cron 定时任务：hermes cron list 解析 → type=cron"""
    items = []
    try:
        out = subprocess.run(["hermes", "cron", "list"], capture_output=True,
                             text=True, timeout=120).stdout
    except Exception as e:
        log("cron 源失败: %s" % e)
        return items
    cur = None
    for ln in out.splitlines():
        if "[active]" in ln:
            if cur:
                items.append(cur)
            m_id = re.match(r"\s*([\w]+)", ln)
            cur = {
                "type": "cron", "name": "", "schedule": "", "script": "",
                "source_ref": "hermes:cron:" + (m_id.group(1) if m_id else ""),
            }
        elif cur is not None:
            m = re.search(r"Name:\s+(\S+)", ln)
            if m:
                cur["name"] = m.group(1)
            m = re.search(r"Schedule:\s+(.+)$", ln)
            if m:
                cur["schedule"] = m.group(1).strip()
            m = re.search(r"Script:\s+(\S+)", ln)
            if m:
                cur["script"] = m.group(1)
    if cur:
        items.append(cur)
    out_items = []
    for it in items:
        name = it["name"] or "cron-task"
        desc = f"cron 定时任务（调度 {it['schedule']}，脚本 {it['script']}）"
        out_items.append({
            "type": "cron",
            "name": name,
            "summary": clip(f"{name}：{desc}"),
            "content": f"调度: {it['schedule']}\n脚本: {it['script']}\n源: hermes cron list",
            "source_ref": it["source_ref"] or f"hermes:cron:{name}",
        })
    return out_items


def scan_specs():
    """SOP 规范 + docs/规范 → type=spec（提取标题/章节要点）"""
    items = []
    specs = [
        (os.environ.get("SIKU_SOP_MD", os.path.join(_SIKU_ROOT, "docs", "SOP规范.md")), "SOP规范"),
        (os.path.join(_SIKU_ROOT, "docs", "规范", "README结构规范-宁多勿少-20260811.md"), "README结构规范"),
        (os.path.join(_SIKU_ROOT, "docs", "规范", "四库类型枚举-schema-20260827.yaml"), "四库类型枚举Schema"),
        (os.path.join(_SIKU_ROOT, "docs", "实验推广六要素-无缝连接标准.md"), "实验推广六要素无缝连接标准"),
        (os.path.join(_SIKU_ROOT, "docs", "真机验证-适用标准.md"), "真机验证适用标准"),
        (os.path.join(_SIKU_ROOT, "docs", "决策分级-schema.md"), "决策分级Schema"),
    ]
    for path, label in specs:
        if not os.path.isfile(path):
            continue
        head = read_head(path, 60)
        headings = re.findall(r"^#{1,2}\s+(.+)$", head, re.M)[:8]
        key = "；".join(h.strip() for h in headings) or first_doc(path)[:150]
        items.append({
            "type": "spec",
            "name": label,
            "summary": clip(f"{label}：四库规范文档（{os.path.basename(path)}）"),
            "content": f"路径: {path}\n章节要点: {key}",
            "source_ref": path,
        })
    return items


def scan_workflows():
    """协作流程文档 + docs/流程 → type=workflow"""
    items = []
    # 流程类技能文档源（目录名可经环境变量 SIKU_WORKFLOW_SKILLS 逗号分隔覆盖）
    _skill_names = [s.strip() for s in os.environ.get("SIKU_WORKFLOW_SKILLS", "workflow-guide,team-workflow").split(",") if s.strip()]
    skills = [(n, os.path.join(_HERMES_HOME, "skills", n, "SKILL.md")) for n in _skill_names]
    for label, path in skills:
        if not os.path.isfile(path):
            continue
        name, desc = parse_frontmatter(path)
        name = name or label
        desc = desc or "团队/团队协作流程"
        items.append({
            "type": "workflow",
            "name": name,
            "summary": clip(f"{name}：协作流程文档（{desc}）"),
            "content": f"路径: {path}\n说明: {desc}",
            "source_ref": path,
        })
    flow_dir = os.path.join(_SIKU_ROOT, "docs", "流程")
    if os.path.isdir(flow_dir):
        for fn in sorted(os.listdir(flow_dir)):
            if not fn.endswith(".md"):
                continue
            path = os.path.join(flow_dir, fn)
            name = fn[:-3]
            desc = first_doc(path) or "四库流程文档"
            items.append({
                "type": "workflow",
                "name": name,
                "summary": clip(f"{name}：{desc}"),
                "content": f"路径: {path}\n要点: {desc}",
                "source_ref": path,
            })
    return items


def scan_rules():
    """门禁规则：gate_check.py/flowhard_triggers.sql/role_matrix.json 等 → type=rule"""
    items = []
    rules = [
        ("gate_check.py", os.path.join(_HERMES_HOME, "scripts", "gate_check.py"), "门禁确定性函数（授权/通过判定）"),
        ("flowhard_triggers.sql", os.path.join(_HERMES_HOME, "scripts", "flowhard_triggers.sql"), "流程硬约束触发 SQL"),
        ("role_matrix.json", os.path.join(_HERMES_HOME, "scripts", "role_matrix.json"), "角色权限矩阵"),
        ("risk_grade.py", os.path.join(_HERMES_HOME, "scripts", "risk_grade.py"), "风险等级量化"),
        ("route-match.py", os.path.join(_HERMES_HOME, "scripts", "route-match.py"), "路由命中判定"),
        ("flow_templates.json", os.path.join(_HERMES_HOME, "scripts", "flow_templates.json"), "流程模板字段校验基准"),
    ]
    for name, path, hint in rules:
        if not os.path.isfile(path):
            continue
        doc = first_doc(path) or hint
        items.append({
            "type": "rule",
            "name": name,
            "summary": clip(f"{name}：门禁/流程规则资产（{hint}）"),
            "content": f"路径: {path}\n功能摘要: {doc}",
            "source_ref": path,
        })
    return items


def scan_benchmarks():
    """评测基准：eval_bank_v2.json/golden → type=benchmark"""
    items = []
    benchs = [
        ("eval_bank_v2.json", os.path.join(_SIKU_ROOT, "scripts", "siku_option", "eval_bank_v2.json"), "分级评测集（基础/多跳/时间推理/拒答场景）"),
        ("eval_bank_v1.json", os.path.join(_SIKU_ROOT, "scripts", "siku_option", "eval_bank_v1.json"), "评测集 v1"),
        ("rules.json", os.path.join(_SIKU_ROOT, "scripts", "siku_option", "rules.json"), "评测规则集"),
        ("rrf_golden_monitor.py", os.path.join(_SIKU_ROOT, "scripts", "rrf_golden_monitor.py"), "golden 基准监控"),
        ("eval_retrieval.py", os.path.join(_SIKU_ROOT, "scripts", "eval_retrieval.py"), "检索评测脚本"),
    ]
    for name, path, hint in benchs:
        if not os.path.isfile(path):
            continue
        extra = ""
        if name == "eval_bank_v2.json":
            try:
                d = json.load(open(path, encoding="utf-8"))
                cases = len(d.get("cases", [])) if isinstance(d, dict) else len(d)
                extra = f"；用例数 {cases}"
            except Exception:
                pass
        doc = first_doc(path) or hint
        items.append({
            "type": "benchmark",
            "name": name,
            "summary": clip(f"{name}：评测基准（{hint}{extra}）"),
            "content": f"路径: {path}\n说明: {hint}{extra}\n摘要: {doc}",
            "source_ref": path,
        })
    return items


def scan_monitors():
    """监控资产：看门狗脚本 → type=monitor"""
    items = []
    constr = os.path.join(_HERMES_HOME, "constraints")
    scripts = os.path.join(_HERMES_HOME, "scripts")
    monitors = [
        (os.path.join(constr, "watchdog-detect.sh"), "卡滞检测看门狗"),
        (os.path.join(constr, "watchdog-evidence-bridge.sh"), "证据桥接"),
        (os.path.join(constr, "watchdog-excuse-check.sh"), "借口核查"),
        (os.path.join(constr, "watchdog-gene-inject.sh"), "基因注入"),
        (os.path.join(constr, "watchdog-swarm-scout.sh"), "系统哨兵"),
        (os.path.join(constr, "watchdog-write.sh"), "写盘看门狗"),
        (os.path.join(scripts, "bge-watchdog.py"), "bge 模型监控"),
        (os.path.join(scripts, "hermes-health-check.py"), "Hermes 健康检查"),
        (os.path.join(scripts, "executions_monitor.py"), "执行监控"),
        (os.path.join(scripts, "nightly-train-watchdog.sh"), "夜间训练看门狗"),
    ]
    for path, hint in monitors:
        if not os.path.isfile(path):
            continue
        name = os.path.basename(path)
        doc = first_doc(path) or hint
        items.append({
            "type": "monitor",
            "name": name,
            "summary": clip(f"{name}：监控/看门狗资产（{hint}）"),
            "content": f"路径: {path}\n用途: {hint}\n摘要: {doc}",
            "source_ref": path,
        })
    return items


def scan_assets():
    """模型/训练资产：<SIKU_ROOT>/models/ → type=asset（目录级+顶层文档）"""
    items = []
    base = os.path.join(_SIKU_ROOT, "models")
    if not os.path.isdir(base):
        return items
    # 顶层关键文档/脚本
    for fn in sorted(os.listdir(base)):
        path = os.path.join(base, fn)
        if fn.startswith("."):
            continue
        if os.path.isfile(path) and (fn.endswith(".md") or fn.endswith(".sh")):
            name = fn[:-3] if fn.endswith(".md") else fn
            doc = first_doc(path) or "模型训练资产文档"
            items.append({
                "type": "asset",
                "name": name,
                "summary": clip(f"{name}：模型训练资产（{doc[:60]}）"),
                "content": f"路径: {path}\n说明: {doc}",
                "source_ref": path,
            })
        elif os.path.isdir(path):
            # 排除运行产物/备份类目录
            if any(k in fn for k in ("备份", "日志", "log", "审计")):
                continue
            doc = first_doc(os.path.join(path, "README.md")) if os.path.isfile(os.path.join(path, "README.md")) else "模型训练资产目录"
            items.append({
                "type": "asset",
                "name": fn,
                "summary": clip(f"{fn}：模型训练资产目录（{doc[:60]}）"),
                "content": f"路径: {path}\n说明: {doc}",
                "source_ref": path,
            })
    return items


# ═════════════════════════ 5 类新资产扫描器（扩展批次 2026-08-27）═════════════════════════

def scan_designs():
    """design 方案/设计文档：$SIKU_ROOT/docs/ 方案类目录 → type=design
    （目录级 + 关键方案文件条目；条目来自真实存在的目录/文件，不伪造）"""
    items = []
    docs = os.path.join(_SIKU_ROOT, "docs")
    # 目录级方案主题（按部署环境实际存在的 docs 子目录）
    design_dirs = [
        ("FDE前线部署工程师", os.path.join(docs, "FDE前线部署工程师"), "FDE 前线部署工程师方案目录"),
        ("Agent循环-三Agent架构", os.path.join(docs, "Agent循环-三Agent架构"), "三 Agent 自循环架构方案目录"),
        ("具身智能运动控制", os.path.join(docs, "具身智能运动控制"), "具身智能运动控制方案目录"),
        ("双库迁移", os.path.join(docs, "双库迁移"), "双库迁移方案目录（含方案设计/研究资料/脚本）"),
    ]
    for name, path, hint in design_dirs:
        if not os.path.isdir(path):
            continue
        items.append({
            "type": "design", "name": name,
            "summary": clip(f"{name}：{hint}（目录级）"),
            "content": f"路径: {path}\n说明: {hint}",
            "source_ref": path,
        })
    # 关键方案文件条目
    design_files = [
        ("具身运动控制方案", os.path.join(docs, "具身智能运动控制", "06-具身运动控制方案.md"), "具身智能运动控制正式定稿方案"),
        ("双库记忆系统设计方案", os.path.join(docs, "双库迁移", "方案设计", "设计说明书.md"), "双库记忆系统设计方案说明书"),
    ]
    for name, path, hint in design_files:
        if not os.path.isfile(path):
            continue
        doc = first_doc(path) or hint
        items.append({
            "type": "design", "name": name,
            "summary": clip(f"{name}：{hint}（{os.path.basename(path)}）"),
            "content": f"路径: {path}\n说明: {hint}\n摘要: {doc}",
            "source_ref": path,
        })
    return items


def scan_research():
    """research 调研成果：四库 docs 调研报告/调研目录 → type=research
    （有真实报告文件才条目化）"""
    items = []
    docs = os.path.join(_SIKU_ROOT, "docs")
    research_files = [
        ("具身智能外部调研报告", os.path.join(docs, "具身智能运动控制", "01-外部调研报告.md"), "具身智能外部调研报告（R0 调研 24 源）"),
        ("FDE核心结论提炼", os.path.join(docs, "FDE前线部署工程师", "01-FDE核心结论提炼-20260817.md"), "腾讯研究院 FDE 模式行业观察提炼"),
        ("Anthropic三Agent自循环调研", os.path.join(docs, "Agent循环-三Agent架构", "01-Anthropic三Agent自循环-最好的循环.txt"), "Anthropic 三 Agent 自循环模式调研文章"),
    ]
    for name, path, hint in research_files:
        if not os.path.isfile(path):
            continue
        doc = first_doc(path) or hint
        items.append({
            "type": "research", "name": name,
            "summary": clip(f"{name}：{hint}"),
            "content": f"路径: {path}\n说明: {hint}\n摘要: {doc}",
            "source_ref": path,
        })
    research_dirs = [
        ("本体驱动研究", os.path.join(docs, "本体驱动研究"), "本体驱动研究（Palantir/OWL/RDF 等调研资料）"),
        ("双库迁移研究资料", os.path.join(docs, "双库迁移", "研究资料"), "双库迁移研究资料目录"),
    ]
    for name, path, hint in research_dirs:
        if not os.path.isdir(path):
            continue
        files = [f for f in os.listdir(path) if not f.startswith(".")]
        extra = f"；含 {len(files)} 个文件" if files else ""
        items.append({
            "type": "research", "name": name,
            "summary": clip(f"{name}：{hint}{extra}（目录级）"),
            "content": f"路径: {path}\n说明: {hint}{extra}",
            "source_ref": path,
        })
    return items


def scan_scripts():
    """script 工具脚本（非定时）：四库 scripts + $HERMES_HOME/scripts 主要工具
    → type=script（siku_asset_ingest 自身除外；控制 ≤30 条主要工具；
    已在 rule/monitor/benchmark 类型的路径跳过防双类型）"""
    items = []
    siku_scripts = os.path.join(_SIKU_ROOT, "scripts")
    hermes_scripts = os.path.join(_HERMES_HOME, "scripts")
    # 已入其他资产类型的路径（rule/monitor/benchmark 已收——防同源双类型）
    occupied = {
        os.path.join(hermes_scripts, "gate_check.py"),
        os.path.join(hermes_scripts, "risk_grade.py"),
        os.path.join(hermes_scripts, "route-match.py"),
        os.path.join(hermes_scripts, "flowhard_triggers.sql"),
        os.path.join(hermes_scripts, "role_matrix.json"),
        os.path.join(hermes_scripts, "flow_templates.json"),
        os.path.join(hermes_scripts, "bge-watchdog.py"),
        os.path.join(hermes_scripts, "hermes-health-check.py"),
        os.path.join(hermes_scripts, "executions_monitor.py"),
        os.path.join(hermes_scripts, "nightly-train-watchdog.sh"),
        os.path.join(siku_scripts, "eval_retrieval.py"),
        os.path.join(siku_scripts, "rrf_golden_monitor.py"),
        os.path.join(siku_scripts, "siku_asset_ingest.py"),  # 自身除外
    }
    # 四库主要管道/工具脚本（20 条）
    siku_tools = [
        ("l3_retrieval", "l3_retrieval.py", "四库统一检索 CLI（FTS5+向量 RRF 融合）"),
        ("reta_pipeline", "reta_pipeline.py", "RETA 晋升管道（L1→L2/L2→L3）"),
        ("importance_scoring", "importance_scoring.py", "重要性评分（复杂度/新鲜度/稳定性）"),
        ("l4c_sync", "l4c_sync.py", "L4c 智慧层同步"),
        ("snapshot", "snapshot.py", "四库快照 create/list/rollback"),
        ("l1_harvest_all", "l1_harvest_all.py", "L1 全源收割"),
        ("l1_hermes_extractor", "l1_hermes_extractor.py", "Hermes 会话 L1 提取"),
        ("graph_builder", "graph_builder.py", "交互图构建"),
        ("graph_query", "graph_query.py", "图通道查询"),
        ("audit_logger", "audit_logger.py", "四库审计日志"),
        ("pipeline_runner", "pipeline_runner.py", "管道调度运行器"),
        ("usearch", "usearch.py", "三源聚合检索（siku/session/files）"),
        ("l4b_to_sft", "l4b_to_sft.py", "L4b→SFT 训练数据转换"),
        ("cleanup_expired", "cleanup_expired.py", "过期条目清理"),
        ("decay_stale", "decay_stale.py", "记忆衰减"),
        ("backfill_embeddings", "backfill_embeddings.py", "向量回填"),
        ("entity_archive", "entity_archive.py", "实体归档"),
        ("gene_promote", "gene_promote.py", "基因晋升管道"),
        ("missmon_check", "missmon_check.py", "missmon 选项监控检查"),
        ("l3_enhance", "l3_enhance.py", "L3 增强"),
    ]
    for name, fn, hint in siku_tools:
        path = os.path.join(siku_scripts, fn)
        if not os.path.isfile(path) or path in occupied:
            continue
        doc = first_doc(path) or hint
        items.append({
            "type": "script", "name": name,
            "summary": clip(f"{name}：{hint}（四库工具脚本）"),
            "content": f"路径: {path}\n功能: {hint}\n摘要: {doc}",
            "source_ref": path,
        })
    # $HERMES_HOME/scripts 主要工具（10 条）
    hermes_tools = [
        ("sop-validate", "sop-validate.sh", "SOP 文件标准化校验"),
        ("usearch_ss_wrap", "usearch_ss_wrap.py", "会话源检索 wrapper"),
        ("verifier", "verifier.py", "验收验证器"),
        ("skill-health-check", "skill-health-check.py", "技能健康检查"),
        ("audit-check", "audit-check.sh", "合规审计检查"),
        ("weekly-sync", "weekly-sync.sh", "每周同步"),
        ("system-health-push", "system-health-push.py", "系统健康推送"),
        ("stall-detector", "stall-detector.py", "卡滞检测"),
        ("write_gate_token", "write_gate_token.py", "写门 token 签发"),
        ("conflict-scan", "conflict-scan.sh", "冲突扫描"),
    ]
    for name, fn, hint in hermes_tools:
        path = os.path.join(hermes_scripts, fn)
        if not os.path.isfile(path) or path in occupied:
            continue
        doc = first_doc(path) or hint
        items.append({
            "type": "script", "name": name,
            "summary": clip(f"{name}：{hint}（Hermes 工具脚本）"),
            "content": f"路径: {path}\n功能: {hint}\n摘要: {doc}",
            "source_ref": path,
        })
    # 目录级条目（四库 scripts + $HERMES_HOME/scripts）
    for name, path, hint in [
        ("四库全书scripts", siku_scripts, "四库全书脚本目录（管道/工具/入库）"),
        ("hermes-scripts", hermes_scripts, "$HERMES_HOME/scripts 工具脚本目录"),
    ]:
        if os.path.isdir(path):
            items.append({
                "type": "script", "name": name,
                "summary": clip(f"{name}：{hint}（目录级）"),
                "content": f"路径: {path}\n说明: {hint}",
                "source_ref": path,
            })
    return items


def scan_configs():
    """config 配置资产：config.yaml + profile configs 目录级 → type=config
    （permissions.yaml 主文件不存在→跳过不伪造；role_matrix.json 已在 rule 类型→跳过防双类型）"""
    items = []
    cfg = os.path.join(_HERMES_HOME, "config.yaml")
    if os.path.isfile(cfg):
        items.append({
            "type": "config", "name": "hermes-config",
            "summary": clip("hermes-config：Hermes 主配置（模型/工具/网关/权限）"),
            "content": f"路径: {cfg}\n说明: Hermes 主配置文件",
            "source_ref": cfg,
        })
    profiles_dir = os.path.join(_HERMES_HOME, "profiles")
    if os.path.isdir(profiles_dir):
        pcfgs = sorted(
            p for p in os.listdir(profiles_dir)
            if os.path.isfile(os.path.join(profiles_dir, p, "config.yaml"))
        )
        extra = f"；含 {len(pcfgs)} 个 profile config" if pcfgs else ""
        items.append({
            "type": "config", "name": "hermes-profile-configs",
            "summary": clip(f"hermes-profile-configs：各 profile config.yaml{extra}（目录级）"),
            "content": f"路径: {profiles_dir}\n说明: 多 profile 配置目录{extra}",
            "source_ref": profiles_dir,
        })
    # role_matrix.json 已在 rule 类型（scan_rules 已收）——跳过防同源双类型，日志在 main 里记录
    return items


def scan_references():
    """reference 知识参考：$HERMES_HOME 下文档/README/schema 类 → type=reference（目录级+文档级）"""
    items = []
    hm = os.path.join(_HERMES_HOME)
    ref_files = [
        ("FTS5预计算索引与向量小模型原理分析", os.path.join(hm, "FTS5预计算索引与向量小模型原理分析.md"), "FTS5 预计算索引与向量小模型原理分析文档"),
    ]
    for name, path, hint in ref_files:
        if not os.path.isfile(path):
            continue
        doc = first_doc(path) or hint
        items.append({
            "type": "reference", "name": name,
            "summary": clip(f"{name}：{hint}"),
            "content": f"路径: {path}\n说明: {hint}\n摘要: {doc}",
            "source_ref": path,
        })
    ref_dirs = [
        ("hermes-docs", os.path.join(hm, "docs"), "Hermes 机制文档目录（mechanism-hardening 等）"),
        ("hermes-schemas", os.path.join(hm, "schemas"), "Hermes schema 参考目录（common/skill-specific/sop_templates）"),
        ("谷歌工程规范-checklists", os.path.join(_SIKU_ROOT, "docs", "谷歌工程规范-checklists"), "谷歌工程规范 checklists 参考（a11y/done/security 等）"),
    ]
    for name, path, hint in ref_dirs:
        if not os.path.isdir(path):
            continue
        files = [f for f in os.listdir(path) if not f.startswith(".")]
        extra = f"；含 {len(files)} 个文档" if files else ""
        items.append({
            "type": "reference", "name": name,
            "summary": clip(f"{name}：{hint}{extra}（目录级）"),
            "content": f"路径: {path}\n说明: {hint}{extra}",
            "source_ref": path,
        })
    return items


def _config_dirs():
    """配置目录清单（环境变量 SIKU_CONFIG_DIRS，逗号分隔；默认 <SIKU_ROOT>/config-dirs）。"""
    raw = os.environ.get("SIKU_CONFIG_DIRS", "")
    dirs = [p.strip() for p in raw.split(",") if p.strip()]
    return dirs or [os.path.join(_SIKU_ROOT, "config-dirs")]


def scan_config_dirs():
    """config-dirs 配置目录文档：SIKU_CONFIG_DIRS 指定目录的 md/sh/yaml
    → type=design/research/config/reference（0token 文件名关键词映射，
    siku_types.validate_type 严格校验—— 三库同步 2026-08-29）

    跳过：备份目录（backups/备份/审计/执行日志/archive/日志/承重墙/快照/
    snapshot/temp/tmp/.git/.learnings/.bak）+ .bak 文件 + 隐藏文件。
    幂等：source_ref=文件绝对路径 → (type, source_ref) 双键唯一，重跑 0 新增。
    """
    items = []
    roots = _config_dirs()
    skip_kw = ("backup", "备份", "审计", "执行日志", "archive", "日志",
               "承重墙", "快照", "snapshot", "temp", "tmp", ".git", ".learnings", ".bak")
    # .md/.html/.yaml/.json 为任务约定顶层文档（html=调研文章/报告，json=验证报告/配置）；
    # .sh 部署脚本同收；.txt 正文抽取临时件不收（e1-body 类，价值低）
    ext_ok = (".md", ".sh", ".yaml", ".yml", ".html", ".json")
    design_kw = ("方案", "设计", "计划", "规划", "落地", "路线", "架构", "v1", "v2", "v3", "v4", "v5", "v6", "版本")
    research_kw = ("调研", "研究", "报告", "分析", "总结", "对标", "盘点", "评估", "评测", "审计报告")

    def _classify(name):
        low = name.lower()
        if low.endswith((".yaml", ".yml")) or low.endswith(".sh"):
            return "config"  # yaml=配置；sh=部署/配置脚本
        if any(k in name for k in design_kw):
            return "design"
        if any(k in name for k in research_kw):
            return "research"
        return "reference"

    for root in roots:
        if not os.path.isdir(root):
            log("config-dirs 目录不存在跳过: %s" % root)
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if not any(k in d.lower() for k in skip_kw)]
            for fn in sorted(filenames):
                if fn.startswith(".") or ".bak" in fn.lower():
                    continue
                if not fn.lower().endswith(ext_ok):
                    continue
                path = os.path.join(dirpath, fn)
                if not os.path.isfile(path):
                    continue
                t = _classify(fn)
                if t not in ASSET_TYPES:
                    continue
                siku_types.validate_type(t, strict=True)  # 非法类型抛 ValueError 拒收
                name = os.path.splitext(fn)[0][:60]
                doc = first_doc(path) or "配置目录文档"
                items.append({
                    "type": t,
                    "name": name,
                    "summary": clip(f"{name}：{doc}"),
                    "content": f"路径: {path}\n说明: {doc}",
                    "source_ref": path,
                    "origin": "config_dirs",  # 三库同步标记（基因双写/reflex 注册范围）
                })
    return items


# ═════════════════════════ 入库 ═════════════════════════

def entry_exists(conn, summary_hash, type_, source_ref):
    """幂等双键：summary_hash 或 (type, source_ref) 已存在 → True"""
    row = conn.execute(
        "SELECT id FROM memory_store WHERE summary_hash = ? LIMIT 1",
        (summary_hash,),
    ).fetchone()
    if row:
        return True
    row = conn.execute(
        "SELECT id FROM memory_store WHERE type = ? AND source_ref = ? LIMIT 1",
        (type_, source_ref),
    ).fetchone()
    return row is not None


def ingest(conn, items):
    inserted = skipped = 0
    by_type = {}
    ts = datetime.now(timezone.utc).isoformat()
    for it in items:
        t = it["type"]
        if t not in ASSET_TYPES:
            log("WARN 非资产类型跳过: %s (%s)" % (t, it.get("source_ref", "")))
            continue
        siku_types.validate_type(t, strict=True)  # 非法类型抛 ValueError 拒收
        summary = clip(it["summary"])
        if not summary:
            continue
        summary_hash = hashlib.sha256(summary.encode("utf-8")).hexdigest()[:12]
        # P2a② SHACL 增量写时自检（SIKU_SHACL=on 时 error 违例拦截；默认影子只报不拦）
        if SHACL_GATE:
            _entry = {"id": "", "type": t, "summary": summary, "content": it.get("content", ""),
                      "source_agent": SRC_AGENT, "confidence": 0.9, "trust_score": 0.7,
                      "timestamp": ts, "memory_track": "semantic", "data_type": t,
                      "source_ref": it.get("source_ref", ""), "expires_at": "",
                      "summary_hash": summary_hash}
            _vs = shacl_validate.validate_entry(_entry)
            if any(v["severity"] == "error" for v in _vs):
                log("SHACL error 违例拦截: %s (%s) — %s" % (
                    t, it.get("source_ref", ""), _vs[0]["detail"]))
                continue
        if entry_exists(conn, summary_hash, t, it["source_ref"]):
            skipped += 1
            by_type[t] = by_type.get(t, {"inserted": 0, "skipped": 0})
            by_type[t]["skipped"] += 1
            continue
        entry_id = str(uuid.uuid4())
        audit = json.dumps([{
            "action": "asset_ingest",
            "reason": INGEST_REASON,
            "ingested_at": ts,
            "source_ref": it["source_ref"],
        }], ensure_ascii=False)
        conn.execute(
            """INSERT INTO memory_store
               (id, version, timestamp, type, summary, content,
                confidence, trust_score, importance, half_life,
                source_agent, audit_log, summary_hash, concern_id,
                industry, source_ref, expires_at, data_type, memory_track)
               VALUES (?, '1.0', ?, ?, ?, ?, 0.9, 0.7, 5, 'permanent',
                       ?, ?, ?, 'unclassified', '', ?, '', ?, 'semantic')""",
            (entry_id, ts, t, summary, it["content"],
             SRC_AGENT, audit, summary_hash,
             it["source_ref"], t),
        )
        inserted += 1
        by_type.setdefault(t, {"inserted": 0, "skipped": 0})["inserted"] += 1
    return inserted, skipped, by_type


# ═════════════════════════ 三库同步：基因双写 + reflex 注册 ═════════════════════════

def gene_double_write(items):
    """R2 基因双写（2026-08-29）：配置目录 判断资料（design/research）
    → write_gene.py 受控入口（$HERMES_HOME/scripts/genes）。

    SIKU_GENE_WRITE=off 显式关闭（默认 on）；write_gene 三要素验证 + 相似度去重幂等
    （重跑 0 重复）；单次上限 GENE_WRITE_MAX 防批量污染；失败只记日志不阻断入库主链。
    """
    if os.environ.get("SIKU_GENE_WRITE", "on").lower() in ("off", "0", "false"):
        log("gene 双写跳过（SIKU_GENE_WRITE=off）")
        return 0
    if not os.path.isfile(GENE_WRITE_PY):
        log("write_gene.py 不存在跳过: %s" % GENE_WRITE_PY)
        return 0
    written = 0
    for it in items:
        if it.get("origin") != "config_dirs" or it.get("type") not in GENE_TYPES:
            continue
        gene = {
            "summary": clip("[配置目录判断资料] %s" % it["name"], n=200),
            "content": ("location: %s\n说明: %s\nfix: 判断资料——方案/调研决策，检索命中时回源阅读"
                        "（三库同步基因双写）") % (it["source_ref"], it.get("content", "")),
            "proxy": SRC_AGENT,
            "project": "四库",
            "source": "siku_asset_ingest",
            "keywords": "配置目录,判断资料,%s" % it["type"],
            "task_id": os.environ.get("SIKU_GENE_TASK_ID", "siku-asset-ingest"),
            "confidence": 0.85,
        }
        try:
            p = subprocess.run([sys.executable, GENE_WRITE_PY],
                               input=json.dumps(gene, ensure_ascii=False),
                               capture_output=True, text=True, timeout=60)
            ok = p.returncode == 0
            tail = (p.stdout or p.stderr or "").strip().splitlines()
            log("gene 双写 %s: %s（%s）" % (
                "成功/去重跳过" if ok else "失败",
                it["source_ref"],
                tail[0][:60] if tail else "rc=%d" % p.returncode))
            if ok:
                written += 1
        except Exception as e:
            log("gene 双写异常: %s（%s）" % (it["source_ref"], e))
        if written >= GENE_WRITE_MAX:
            log("gene 双写达单次上限 %d，停止" % GENE_WRITE_MAX)
            break
    log("gene 双写完成：写入/确认 %d 条（上限 %d/次）" % (written, GENE_WRITE_MAX))
    return written


def _config_relkey(path):
    """reflex evidence 相对引用键（禁绝对路径——schema_validator _PRIVACY_PAT；
    空格→下划线对齐 EVIDENCE_RE \\S+ 约束）。"""
    for root, tag in ((d, os.path.basename(d)) for d in _config_dirs()):
        if path.startswith(root):
            sub = path[len(root):].lstrip("/")
            return "config:%s/%s" % (tag, sub.replace(" ", "_"))
    return "config:%s" % os.path.basename(path).replace(" ", "_")


def _config_hashkey(path):
    """reflex evidence 内容稳定键：sha256(文件内容)[:12]，格式 config:hash:<hex12>。

    与文件路径、目录结构、os.walk 顺序无关——同一文件内容不变即键不变
    （修复：幂等键从路径键升级为内容键，免疫目录整理/
    归档/路径表示漂移）。前缀必须为 schema_validator EVIDENCE_RE 白名单
    （session|kanban|report|memory|manual|gene|audit|config:）之一——config:hash:
    语义 = config: 引用键 + hash: 内容指纹子格式，校验通过；禁绝对路径、无空白。
    文件不可读 → None（调用方回退 relkey）。"""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return "config:hash:%s" % h.hexdigest()[:12]
    except OSError:
        return None


def reflex_register(items):
    """R2 reflex 注册（2026-08-29）：配置目录 判断资料（design/research）
    → reflex library shadow 影子状态（$HERMES_HOME/scripts/reflex/library/shadow）。

    零副作用不激活：status=shadow + action.enabled=false + tool=* 空触发（0 命中不晋级——
    activation_stats 晋级需激活率达标，惰性安全）。schema 对齐 cryst_* 并通过 schema_validator
    （ID_RE (cryst|manual)_YYYYMMDD_NNN / evidence≥2 且禁绝对路径 / version≥1 /
    permission_level=L1 / created_at +08:00 ISO）。幂等：evidence[0]=内容哈希键 +
    evidence[1]=路径相对键 双键索引（：历史 evidence[0]=路径键，
    双键兼容存量不重注册；内容键免疫 os.walk 顺序/目录整理/归档导致的键漂移）。
    写文件 O_EXCL 原子排他（防并发撞 id 覆写——历史同 rid 不同内容痕迹的根因防护）。
    SIKU_REFLEX_WRITE=off 可关闭（默认 on）；单次上限 REFLEX_WRITE_MAX。
    """
    if os.environ.get("SIKU_REFLEX_WRITE", "on").lower() in ("off", "0", "false"):
        log("reflex 注册跳过（SIKU_REFLEX_WRITE=off）")
        return 0
    if not os.path.isdir(REFLEX_SHADOW_DIR):
        log("reflex shadow 目录不存在跳过: %s" % REFLEX_SHADOW_DIR)
        return 0
    date8 = datetime.now().strftime("%Y%m%d")
    seq = 0
    mine = {}  # 稳定键（内容哈希 + 路径相对键）→ rid（本来源已注册索引，幂等键）
    try:
        for fn in os.listdir(REFLEX_SHADOW_DIR):
            m = re.match(r"^manual_%s_(\d{3})\.json$" % date8, fn)
            if not m:
                continue
            seq = max(seq, int(m.group(1)))
            try:
                with open(os.path.join(REFLEX_SHADOW_DIR, fn), encoding="utf-8") as f:
                    e = json.load(f)
                if e.get("source") == "siku_asset_ingest_%s" % date8 and e.get("evidence"):
                    for k in e["evidence"]:  # 双键兼容：evidence[0] 内容键 + [1] 路径键
                        mine.setdefault(k, e["id"])
            except (ValueError, OSError):
                pass
    except OSError:
        pass
    # 本地时区（CST +08:00）无微秒 ISO——对齐 schema_validator TS_RE（禁微秒）
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+08:00")
    written = 0
    candidates = sorted(
        (x for x in items if x.get("origin") == "config_dirs" and x.get("type") in GENE_TYPES),
        key=lambda x: x["source_ref"],
    )
    for it in candidates:
        relkey = _config_relkey(it["source_ref"])
        hashkey = _config_hashkey(it["source_ref"]) or relkey  # 内容键优先，文件不可读回退路径键
        if hashkey in mine or relkey in mine:  # 幂等：内容键或路径键任一命中即跳过
            continue
        seq += 1
        rid = "manual_%s_%03d" % (date8, seq)
        fp = os.path.join(REFLEX_SHADOW_DIR, rid + ".json")
        if os.path.exists(fp):  # 其他写者占用该 id → 下次再试（seq 已占位）
            continue
        name = it["name"]
        entry = {
            "id": rid,
            "title": "%s：%s（配置目录判断资料）" % (name, clip(it.get("summary", "").split("：", 1)[-1], n=80)),
            "description": ("Do not: 忽略 %s 涉及的方案/调研决策。IF 检索命中该判断资料 "
                            "THEN 回源阅读原文再执行（三库同步 reflex 注册，影子条目零副作用）") % name,
            "trigger": {
                "surface": "tool", "tool": "*",
                "args_regex": [], "text_regex": [], "regex_kind": None,
                "match_query": name[:40], "min_similarity": 0.9,
            },
            "action": {"type": "skip", "script": "", "params": {}, "enabled": False},
            "confidence": {
                "stats": {"precision": None, "hits": 0, "misses": 0, "unknown": 0, "window": None},
                "origin": {"count": 1, "period": date8,
                           "note": "siku_asset_ingest 入库资产注册（判断资料，零副作用不激活）"},
            },
            "status": "shadow",
            "permission_level": "L1",
            "created_at": ts,
            "promoted_at": None,
            "retired_at": None,
            "version": 1,
            "source": "siku_asset_ingest_%s" % date8,
            "evidence": [
                hashkey,
                relkey,
                "report:siku_asset_ingest_%s" % date8,
            ],
        }
        try:
            # O_EXCL 原子排他创建：并发/撞 id 时 FileExistsError → 跳过不覆写
            fd = os.open(fp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(entry, f, ensure_ascii=False, indent=2)
            written += 1
            mine[hashkey] = rid  # 本 run 内去重
            mine[relkey] = rid
            log("reflex 注册 %s: %s" % (rid, it["source_ref"]))
        except FileExistsError:
            log("reflex 注册跳过（并发占用 %s）: %s" % (rid, it["source_ref"]))
        except OSError as e:
            log("reflex 注册失败 %s（%s）" % (it["source_ref"], e))
        if written >= REFLEX_WRITE_MAX:
            log("reflex 注册达单次上限 %d，停止" % REFLEX_WRITE_MAX)
            break
    log("reflex 注册完成：新增 %d 条（上限 %d/次）" % (written, REFLEX_WRITE_MAX))
    return written


def main():
    log("==== siku_asset_ingest 启动（幂等只增，13 资产类型）====")
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    all_items = []
    for name, fn in [
        ("skill 技能包", scan_skills),
        ("cron 定时任务", scan_crons),
        ("spec 规范文档", scan_specs),
        ("workflow 流程文档", scan_workflows),
        ("rule 门禁规则", scan_rules),
        ("benchmark 评测基准", scan_benchmarks),
        ("monitor 监控资产", scan_monitors),
        ("asset 模型训练资产", scan_assets),
        ("design 方案设计", scan_designs),
        ("research 调研成果", scan_research),
        ("script 工具脚本", scan_scripts),
        ("config 配置资产", scan_configs),
        ("reference 知识参考", scan_references),
        ("config-dirs 配置目录文档", scan_config_dirs),
    ]:
        try:
            items = fn()
        except Exception as e:
            log("扫描源 %s 失败: %s" % (name, e))
            continue
        log("扫描 %s: %d 条" % (name, len(items)))
        all_items.extend(items)
    # role_matrix.json 已在 rule 类型（scan_rules 已收）——config 扫描跳过防同源双类型
    log("config 源注记: role_matrix.json 已在 rule 类型（scan_rules 已收），跳过防同源双类型；permissions.yaml 主文件不存在，跳过不伪造")
    inserted, skipped, by_type = ingest(conn, all_items)
    conn.commit()
    conn.close()
    log("==== 入库完成：新增 %d，幂等跳过 %d ====" % (inserted, skipped))
    for t in sorted(by_type):
        d = by_type[t]
        log("  类型 %-10s 新增 %3d  跳过 %3d" % (t, d["inserted"], d["skipped"]))
    # 三库同步：基因双写 + reflex 注册（配置目录判断资料；独立机制失败不阻断入库）
    n_gene = gene_double_write(all_items)
    n_reflex = reflex_register(all_items)
    # JSON 汇总（供验证脚本解析）
    print(json.dumps({
        "inserted": inserted,
        "skipped": skipped,
        "by_type": {k: v["inserted"] for k, v in by_type.items()},
        "skipped_by_type": {k: v["skipped"] for k, v in by_type.items()},
        "gene_written": n_gene,
        "reflex_registered": n_reflex,
        "db": DB_PATH,
        "log": LOG_PATH,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
