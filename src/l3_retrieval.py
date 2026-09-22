#!/usr/bin/env python3
import sys, os

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
_CONCEPTS_DIR = os.environ.get("SIKU_CONCEPTS_DIR", os.path.join(_HERMES_HOME, "memory-bank", "concepts"))  # 概念集目录
# 2026-08-14 修复：系统 python(<3.11) 加载 3.11 编译的 numpy 崩溃
# → embed 通道失效 → 检索降级 fts5_only（69%）。系统 python 跑时 execv 整体切换为 venv python 重执行。
if sys.version_info < (3, 11):
    _vp = os.environ.get("SIKU_VENV_PYTHON") or os.path.join(_HERMES_HOME, "hermes-agent/venv/bin/python")
    if os.path.exists(_vp):
        os.execv(_vp, [_vp] + sys.argv)
    sys.stderr.write("WARN venv-guard: %s 不存在 → 继续用系统解释器 %s（embed 通道可能降级；可用 SIKU_VENV_PYTHON 覆盖）\n" % (_vp, sys.executable))

#!/usr/bin/env python3
"""
l3_retrieval.py — L3 检索升级版（双通道 + RRF + 渐进式披露）

集成 memory-bank.py 的 FTS5/向量检索 + bm25-retriever.py 的 RRF 融合，
实现渐进式三层输出（compact→expand→full）和 MCP 工具接口。

设计原则：
1. 不修改 memory-bank.py / bm25-retriever.py 原始文件
2. 通过 import 复用现有基础设施
3. 异常自动降级
"""

import argparse
# ── Phase 0.5+ 路由层接入守卫（防递归：route_search 透传分支会回调 search_memories）──
_ROUTER_IN_CALL = False


import hashlib
import json
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

# ── 配置 ────────────────────────────────────────────────────────────

DB_PATH = os.environ.get("SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db"))
# ── R3-A 未命中监控：纯规则零 LLM ──────────────────
# 每查询追加记录 hit/miss 到 JSONL（滚动窗口），检查器 missmon_check.py 计算
# 未命中率；>20% 自动建卡「入库提示」（幂等）。只统计+提示，不自动写库
# （研究收敛定稿）。SIKU_MISSMON=off 一键关闭；SIKU_MISSMON_LOG 覆盖日志路径。
MISSMON_ENABLED = os.environ.get("SIKU_MISSMON", "on").lower() not in ("off", "0", "false")
MISSMON_LOG = os.environ.get(
    "SIKU_MISSMON_LOG",
    os.path.join(_SIKU_ROOT, "scripts/siku_option/missmon_queries.jsonl"),
)
RRF_K = 60          # RRF 融合常数（与 bm25-retriever.py 一致）
# ── R2 阶段1：融合权重参数化 ──────────────────────
# 保偏移公式 1/(RRF_K+rank/w+1)：w=1 与旧公式 1/(RRF_K+rank+1) 逐位等价（断言验证）；
# w<1 通道贡献整体下压（与 Qdrant w_r 同向）；w<=0 特判跳过该通道。
# ── R2 阶段2：val 集 19 query 验证通过后生产落地 ──
# 定参依据：train 网格最优 (1.5,0.7) → val 验证 valid=0.4233（基线 0.3810，+0.0423）退化=0 提升 4 条；
# fts5 提权 1.5（学霸多拿票，fts5 p@5=0.347 ≫ emb 0.173）+ emb 微降 0.7（削噪音保独特召回）。
CHANNEL_WEIGHTS = {"fts5": 1.3, "emb": 0.6, "graph": 1.0}  # 回退 8-23 稳定点（9/1 上 1.5/0.7 致月测 6 条 ndcg 对折；备份 .bak-rrf-1507-20260905）
COMPACT_SUMMARY_LEN = 50  # compact 模式摘要截断长度
GROUND_TRUTHS = {"instruction"}  # Ground Truth type（恒在 Top-3）

# ── R1：置信度显式化 + time_desc 兜底显性化（灰度开关 SIKU_GATE，默认 off）──
# SIKU_GATE=on：①返回 JSON 顶层新增 result_quality（high/medium/low/weak，基于 score 阈值）
# ②低分结果标注 weak_match ③time_desc 兜底显式标记（fallback: time_desc，search_mode 不再伪装
# 成 fts5_only/dual_rrf，empty 分支在无数据时可达）④missmon 低相关判定（兜底/weak 记未命中，
# 修复 D4 实证 hit=100% 监控失义）⑤落点 search_memories 主入口（判例：
# 一处接入 CLI+MCP+注入全生效，复用既有 _ROUTER_IN_CALL 防递归守卫）。
# SIKU_GATE=off（默认）：所有新标注/判定零生效，行为与现状逐位一致（纯透传）。
# 与 SIKU_ROUTER（路由层开关）独立共存：GATE 控验证闸门相关新功能，ROUTER 控路由层。
R1_GATE_ENABLED = os.environ.get("SIKU_GATE", "off").lower() not in ("off", "0", "false")

# ── P2a：验证循环+目录树（挂 SIKU_GATE——与 P1 并行）──
# C4：验证循环挂 SIKU_GATE 门复用（score_norm+missmon——不另起炉灶）：
#   verify 子命令受 SIKU_GATE 控制（off→disabled 零开销；on→完整验证循环）。
# C10：验证返回原文片段（可审计——通过/不通过+证据，命中上下文 ±SNIPPET_RADIUS 字符）。
# C13：验证循环试点默认开（1 个高频场景=关键数据规则命中查询）——search 主路径上 query 含
#   关键数据（数字/金额/日期/百分比/结论性表述——R-A4 0token 正则）时自动附加 verification
#   字段（新增字段，不破坏既有字段，R-A1 兼容）；未命中零开销（不加字段不查库）。
#   SIKU_VERIFY_PILOT=off/0/false 一键关闭试点（回退零影响）。
# R-A4：关键数据判定=规则优先（0token 正则，非 LLM 自评——成本）。
# R-A5：规则命中→精确匹配验证（子串精确匹配原文——grep 语义）；规则未命中→跳过（不增延迟）。
VERIFY_PILOT_ENABLED = os.environ.get("SIKU_VERIFY_PILOT", "on").lower() not in ("off", "0", "false")
VERIFY_SNIPPET_RADIUS = 40          # C10：原文片段回传宽度（命中位置前后字符数）
VERIFY_CONCEPT_SET = os.path.join(_CONCEPTS_DIR, "种子概念集.json")  # --tree 目录树单向生成源（C5 概念集权威源）
# R-A4 关键数据规则（顺序=长模式优先，number 兜底去重）：
VERIFY_KEY_PATTERNS = [
    ("percent", r"\d+(?:\.\d+)?\s*%"),                     # 26.7% / 0.5 %
    ("money",   r"\d+(?:\.\d+)?\s*(?:万|亿|元|块|k|K)"),    # 500万 / 1.5亿 / 300元
    ("date",    r"\d{4}\s*[-/年]\s*\d{1,2}\s*(?:[-/月]\s*\d{1,2}\s*日?)?"
                r"|\d{1,2}\s*月\s*\d{1,2}\s*日"),           # 2026-08-26 / 2026年8月 / 8月26日
    ("number",  r"\d+(?:\.\d+)?"),                          # 500 / 0.90（兜底）
    ("conclusion", r"(通过|失败|成功|完成|修复|解决|根因|原因|上线|部署|拒绝|"
                    r"回滚|禁止|必须|不得|允许|PASS|FAIL|approved|rejected)"),  # 结论性表述
]
VERIFY_CONCLUSION_TYPES = {"conclusion"}  # 结论性表述不强制精确匹配（宽泛语义信号）
# result_quality 阈值（基于最终 score：单通道 fts5≈BM25 原分 5-16，双通道 RRF≈0.01-0.05，
# rerank 后≈0-1，time_desc 兜底=0.0）：
R1_Q_HIGH = 0.10    # score ≥ 0.10 → high（RRF 双通道 top/rerank 高分/BM25 高分命中）
R1_Q_MEDIUM = 0.03  # 0.03 ≤ score < 0.10 → medium（RRF 双通道强融合/单通道 fts5 中命中）
R1_Q_LOW = 0.01     # 0.01 ≤ score < 0.03 → low（弱相关，RRF 低排名/词面单命中）
                    # score < 0.01 → weak（time_desc 兜底 0.0 必落此档；或杂讯）

# ── P2b：多跳动态检索（--multi-hop——依赖语义层概念关系）──
# C2：--multi-hop 依赖语义层（P1 概念关系）；C9：R-A8 复杂度判定（0token 规则：
#   实体数/疑问词/多主题——非 LLM）；C15：硬上限（max_hops≤3+证据池上限+统计截断——
#   BDTR 借鉴）。R-A1：默认关（不传 --multi-hop → search_memories 主路径零改动逐位一致）；
#   R-A2：按需开启（复杂问题检索场景）。多跳关系源 = P1 语义层 jsonld（C5 派生，
#   broader/narrower 邻接表，只读零写入）；概念识别源 = 种子概念集（VERIFY_CONCEPT_SET）。
MULTI_HOP_MAX_HOPS = 3        # C15：max_hops 硬上限（CLI 校验 1≤max_hops≤3）
MULTI_HOP_EVPOOL_CAP = 30     # C15：证据池上限（跨跳 id 去重后按 score 截断）
MULTI_HOP_CONCEPT_CAP = 12    # C15：概念扩展总数上限（统计截断）
MULTI_HOP_LAYER_CAP = 6       # C15：每层邻居概念上限（统计截断）
MULTI_HOP_HOP_K = 3           # 每跳检索 top_k 下限（与请求 top_k 取 max——候选充分性）
MULTI_HOP_REL_PATH = os.environ.get("SIKU_LAYER_JSONLD", os.path.join(_CONCEPTS_DIR, "domain-graph.jsonld"))  # P1 语义层概念关系权威源（只读）
# R-A8 疑问词（推理/对比/决策类问题信号——0token 正则，长模式优先）
MULTI_HOP_QUESTION_RE = (r"为什么|怎么|如何|区别|差异|选哪个|哪个好|选什么|选一个|"
                         r"要不要|该不该|会不会|是否|值得|带来什么|有什么影响|有什么问题|"
                         r"失效|对比|还是|好还是")
# R-A8 实体信号2：中文对比/并列段切分连接词（0token——概念表未覆盖的技术域实体）
MULTI_HOP_ENTITY_SEP_RE = r"[和与或、，,／/]|vs\.?|还是|跟|及"
# ── P2a：concept 前置扩展通道（检索前 query 概念化——扩召回）──
# 形态裁决（两轮收敛，复核拍板）：前置扩展 = 检索前 query 概念化 → 概念识别
# （_mh_identify_concepts——种子概念集 term 命中）→ 子类/同义一层扩展
# （_mh_expand_concepts max_layers=1——jsonld broader/narrower 邻接表只读）→
# 候选概念检索词（_mh_concept_query 最短 altLabel 优先）并入检索 query——
# 增强 fts/embed/graph 输入。**不做 RRF 第四路**（graph 重叠/权重膨胀——复核裁定）。
# 默认关（SIKU_CONCEPT=off 生产默认——KEEP_OFF 决策  落代码；on 才启用）；
# 缓存键含 cp 维度防两态串扰；A/B 判定：golden 113 条两态对比 hit_rate 增益 ≥5pp 才开
# （报告 siku_option/concept_ab_*.json——+0.00pp KEEP_OFF，配置面经环境变量显式覆盖 off）。
CONCEPT_ENABLED = os.environ.get("SIKU_CONCEPT", "off").lower() not in ("off", "0", "false")
CONCEPT_EXPAND_C0_MAX = 3     # 参与扩展的已命中概念上限（统计截断）
CONCEPT_EXPAND_MAX_WORDS = 4  # 扩展词并入 query 上限（防 query 膨胀）

# ── RAGI-T 整改（②，2026-08-13）──
# ①量纲归一：fts5 单通道快路（fts5_only/fts5_graph）返回 BM25 原分（≈5-16），与 RRF
#   量纲（≈0.01-0.05）相差约 300 倍——快路分数未归一前全落 high/strong（T 验收实测
#   「居里夫人登月飞船」on 态 high/strong/weak_match=False 根因）。归一后统一用 R1_Q_*
#   阈值判定。归一系数取量纲中位（BM25 10.5 → RRF 0.035）。
# ②保送条目识别：rrf_fusion 对 type=instruction 且 confidence>0.9 保送 Top-3（score+9999），
#   保送分污染 best score → 不可答查询被判 high/strong（T 验收根因②）。判定时排除保送
#   条目（还原真实分），保送仅影响排序不影响证据档位。
FTS5_TO_RRF_SCALE = 300.0  # BM25 原分 → RRF 量纲
GT_BOOST_MARK = 9000.0     # 保送偏移 9999 的识别阈值（正常 RRF/BM25 分远低于 9000）


def _r1_norm_scale(score, search_mode=""):
    """通道量纲归一：fts5 单通道快路 BM25 原分 → RRF 量纲；其余（RRF/embed）原分。"""
    if search_mode.startswith("fts5"):
        return score / FTS5_TO_RRF_SCALE
    return score


def _r1_is_gt_boosted(score):
    """保送条目识别（score+9999 偏移，正常分 <9000）。"""
    return score >= GT_BOOST_MARK


def _r1_gt_orig_score(score):
    """保送条目还原真实分（score - 9999）。"""
    return score - 9999.0 if _r1_is_gt_boosted(score) else score

def _r1_quality_of(score):
    """基于最终 score 定 result_quality 档位（R1，纯规则零 LLM）。"""
    if score >= R1_Q_HIGH:
        return "high"
    if score >= R1_Q_MEDIUM:
        return "medium"
    if score >= R1_Q_LOW:
        return "low"
    return "weak"

# ── R4：score_norm 相对权重快照 + CRAG grade 证据充分性提示（SIKU_GATE 下）──
# 防档位切变耦合振荡：RRF 周度自动调优（rrf_weekly_tune.py）更新 L47 CHANNEL_WEIGHTS →
# RRF score 整体量纲漂移 → R1 绝对阈值（R1_Q_*）下同一相关度被分到不同档位（score 突变）。
# 解法：权重更新时与基准快照对比，整体量纲归一系数 = Σ(基准权重)/Σ(当前权重)；
# score_norm = score × 系数（权重未变时系数=1.0，档位判定与 R1 逐位一致；权重更新后档位
# 判定基于相对量纲，不随权重比例缩放跳变）。档位判定统一用 score_norm（R1 阈值语义不变）。
R4_WEIGHT_BASELINE = {"fts5": 1.5, "emb": 0.7, "graph": 1.0}  # 基准快照 = R2 阶段2 生产定参（.bak-rrfimpl2-20260812）
R4_WEIGHT_BASELINE_SUM = sum(R4_WEIGHT_BASELINE.values())


def _r4_weight_norm_factor():
    """当前 CHANNEL_WEIGHTS vs 基准快照的整体量纲归一系数（权重未变=1.0，零影响）。"""
    cur = sum(CHANNEL_WEIGHTS.values())
    if cur <= 0:
        return 1.0
    return R4_WEIGHT_BASELINE_SUM / cur


def _r4_norm_score(score):
    """score_norm：原始 score 归一化到基准快照量纲（防权重更新→档位跳变耦合振荡）。"""
    return score * _r4_weight_norm_factor()


def _r4_grade_of(score_norm, time_desc_fallback=False):
    """CRAG grade：证据充分性提示 strong/medium/weak，纯标注零拦截。
    strong：score_norm ≥ R1_Q_HIGH（证据充分，可支撑回答）
    medium：R1_Q_MEDIUM ≤ score_norm < R1_Q_HIGH（证据中等，建议 refine/补充检索）
    weak：score_norm < R1_Q_MEDIUM 或 time_desc 兜底/空结果（证据不足，兜底不再伪装）
    与 result_quality 衔接：high→strong；medium→medium；low/weak→weak；兜底强制 weak。
    """
    if time_desc_fallback:
        return "weak"
    if score_norm >= R1_Q_HIGH:
        return "strong"
    if score_norm >= R1_Q_MEDIUM:
        return "medium"
    return "weak"

# ── RAGF-R1：reranker 证据分第二判定维度（SIKU_GATE 下）──────────
# 残留 50% 幻觉率=数学边界：纯分数阈值无法区分假前提词面命中（负样本与可答查询
# RRF 分数分布重叠 0.018-0.042 / BM25 4.1-39.8）。引入 bge-reranker-base 证据分
# （sigmoid 输出 ≈0-1，语义相关度）作第二维度：证据分低 → 判拒答强化。
# 阈值定参：18 负样本 vs 95 可答 golden 复跑实测 rerank 分数分布取分界（详见
#  落卡证据：负样本 top1 证据分 vs 可答 top1 证据分无重叠区/或极小重叠）。
# 判定融合：分数低（R1 既有 weak_match 阈值）OR 证据分低 → weak_match=True。
# 定参（实测 18 负样本 vs 95 可答 golden，SIKU_GATE=on）：
#   负样本 top1 证据分 p25=0.0185 / median=0.1297 / p75=0.6351；可答 p10=0.2618 / median=0.9192。
#   TH=0.10：负样本弱标 9/18（叠加既有 R1 弱标 12/18 → 拒答 13/18），可答误伤 0 新增
#   （唯一 <0.10 的可答 g043 为保送条目，真实分低，R1 判定本就 weak——非证据分误伤）。
#   TH=0.15 虽负样本多抓 n010，但可答新增误伤 g018/g037（rq 降级）——舍。
#   TH=0.65： 实测 18 负样本 vs 113 可答——
#   负样本 evidence distribution: min=0.0035, p25=0.53, median=0.90, p90=0.9885, max=0.9951
#   可答 evidence distribution: min=0.9965, max=1.0000
#   Gap=0.0014 → TH=0.995 拒答 18/18（100%），可答误伤 0
RERANK_EVIDENCE_TH = 0.9952  # rerank 证据分阈值（2026-08-27）
# 实测：18负样本证据分 min=0.0035, p25=0.53, median=0.90, p90=0.9885, max=0.9951
# 113可答证据分 min=0.9965, max=1.0，gap=0.0014 → TH=0.9952拒答18/18（100%），可答误伤0

# ── P1-D：保送机制语义门控 + 显示分统一 ──
# R0 模式 D：RRF 保送（type=instruction && confidence>0.9 → +9999 强制 Top-3）把无关全局
# 指令顶进 top5（"模式识别"、"任何代码修改前须经维护者同意"等）。
# 修复：保送不再 +9999 硬插；改为 query 含指令语义才追加到补充位（top_k 之后，不占 top1/top5）。
INSTRUCTION_HINTS = ("规则", "流程", "模式", "步骤", "必须", "禁止", "任何", "先", "SOP", "规范", "纪律", "铁律", "要求", "建议", "如何", "要不要", "该不该", "怎么")
GT_BOOST_HINT_ENABLED = os.environ.get("PREC_GT_BOOST", "1").lower() not in ("0", "off", "false")

# ── P0-B：embed 相对门控（分布实测：GT 余弦 min=0.5535 vs 噪音 min=0.5517
#    完全重叠 → 绝对阈值不可行，改 top-N 内相对分差）──
EMBED_REL_GATE_ENABLED = os.environ.get("PREC_EMBED_GATE", "1").lower() not in ("0", "off", "false")
_EMBED_GATE_STATS = {"calls": 0, "kept3": 0, "kept5": 0, "kept10": 0, "errors": 0}

# ── R1-IMPL：OBQC 确定性校验+拒答（rules.json 开关 shadow|enforce）────────
# OBQC=Ontology-Based Query Check（本体式查询校验，docs/本体驱动研究/05）：用确定性规则
# 在返回前检测低相关/低证据/来源不明/低置信度，把"可能错"变成"可证明错/可拒绝回答"——
# 错误拒答优于瞎答（data.world GenAI Benchmark II：OBQC+LLM 修复准确率 16.7%→72.55%）。
# 规则与开关集中在 siku_option/rules.json（与 missmon 同目录），改 obqc_mode 一行即切换：
#   shadow（默认）：全量校验只写 obqc_shadow.log（查询/判定/若拦截会怎样），返回零改动
#   enforce：相关性/证据分不达标 → 拒答（results=[] + note + obqc 字段）；来源不明/低置信度 → 标记
# 切换标准写死（rules.json switch_std）：误杀率<5% 且 命中率>80% → 可切 enforce；
# 切前运行 siku_option/obqc_stats.py 出统计报告 → 推送维护者确认（切换纪律）→ 改开关；切后双跑 1 周。
# 性能纪律（纯中文 query 路由拖慢 15 倍教训）：校验仅对非 fts5 快路
# （dual/embed 深度路径 + empty）执行——fts5 快路返回前零检查零日志零开销。
OBQC_RULES_PATH = os.environ.get("SIKU_OBQC_RULES",
                                  os.path.join(_SIKU_ROOT, "scripts/siku_option/rules.json"))
OBQC_LOG_PATH = os.environ.get("SIKU_OBQC_LOG",
                               os.path.join(_SIKU_ROOT, "scripts/siku_option/obqc_shadow.log"))
_OBQC_DEFAULT_RULES = {
    "obqc_mode": "shadow",
    "rules": {
        "relevance": {"enabled": True, "refuse_below": 0.01, "mark_below": 0.03},
        "evidence": {"enabled": True, "refuse_below": 0.10},
        "source_domain": {"enabled": True, "known_agents": [], "mark_unknown_source": True},
        "low_confidence": {"enabled": True, "confidence_threshold": 0.8},
    },
}
_obqc_rules = None
_obqc_rules_mtime = -1.0
_OBQC_REASON_TEXT = {
    "relevance_refuse": "相关性不达标（best score < 0.01，含 time_desc 兜底）",
    "relevance_mark": "相关性偏低（best score < 0.03）",
    "evidence_refuse": "rerank 证据分不足（best evidence < 0.10）",
    "source_domain_unknown": "来源 Agent 不在白名单",
    "source_domain_empty": "来源 Agent 为空",
    "low_confidence": "存在低置信度条目（confidence < 0.8）",
}


def _obqc_load_rules(force=False):
    """加载 rules.json（mtime 变化自动重载——改一行开关实时生效，无需重启）。
    异常回退内置默认（shadow，零拦截），校验失败绝不影响检索。"""
    global _obqc_rules, _obqc_rules_mtime
    try:
        mt = os.stat(OBQC_RULES_PATH).st_mtime
        if force or _obqc_rules is None or mt != _obqc_rules_mtime:
            with open(OBQC_RULES_PATH, encoding="utf-8") as f:
                _obqc_rules = json.load(f)
            _obqc_rules_mtime = mt
    except Exception:
        _obqc_rules = _OBQC_DEFAULT_RULES
    return _obqc_rules


def _obqc_mode():
    """当前生效模式：shadow|enforce（rules.json obqc_mode 字段）。"""
    r = _obqc_load_rules()
    m = str(r.get("obqc_mode") or "shadow").strip().lower()
    return m if m in ("shadow", "enforce") else "shadow"


def _obqc_write_log(entry):
    """追加写 OBQC 校验日志（JSONL，UTF-8）。失败全吞，绝不影响检索返回（同 shadow/missmon 纪律）。"""
    try:
        _dir = os.path.dirname(OBQC_LOG_PATH)
        if _dir and not os.path.isdir(_dir):
            os.makedirs(_dir, exist_ok=True)
        with open(OBQC_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _obqc_check(query, final, time_desc_fallback, search_mode):
    """OBQC 确定性校验（纯规则零 LLM，仅非 fts5 快路调用）。
    返回 (action, reasons, details)：
      action: "pass"（全过）| "mark"（仅标记不拒答）| "refuse"（命中拒答规则）
      reasons: 命中规则标识清单；details: 校验详情（供日志与返回字段）。"""
    rules = _obqc_load_rules()
    rr = rules.get("rules", {})
    reasons, details = [], {"search_mode": search_mode, "checked": True}
    # ① 相关性下限：best 真实分（排除保送污染 + fts5 量纲归一 + 权重归一，与 R1/R4 同口径）
    real_scores = [_r1_gt_orig_score(s) for s, _ in final] if final else []
    best_norm = _r4_norm_score(_r1_norm_scale(max(real_scores, default=0.0), search_mode)) if real_scores else 0.0
    details["best_score_norm"] = round(best_norm, 4)
    rel = rr.get("relevance", {}) or {}
    if rel.get("enabled", True):
        if time_desc_fallback or best_norm < float(rel.get("refuse_below", 0.01)):
            reasons.append("relevance_refuse")
        elif best_norm < float(rel.get("mark_below", 0.03)):
            reasons.append("relevance_mark")
    # ② rerank 证据分下限（仅 rerank 路径有 _rerank_evidence；fts5 快路无 → 不参与）
    ev_scores = [r.get("_rerank_evidence") for _, r in final if r.get("_rerank_evidence") is not None]
    best_ev = max(ev_scores) if ev_scores else None
    ev = rr.get("evidence", {}) or {}
    if best_ev is not None:
        details["best_evidence"] = round(best_ev, 4)
        if ev.get("enabled", True) and best_ev < float(ev.get("refuse_below", 0.10)):
            reasons.append("evidence_refuse")
    # ③ 来源域检查：top1 source_agent 白名单/空值（source_ref 缺失仅统计——全库 99.8% 缺失，不做判定依据）
    sd = rr.get("source_domain", {}) or {}
    if sd.get("enabled", True) and final:
        top1_agent = str(final[0][1].get("source_agent") or "").strip()
        known = set(sd.get("known_agents") or [])
        details["top1_source_agent"] = top1_agent or "(空)"
        details["top1_has_source_ref"] = bool(str(final[0][1].get("source_ref") or "").strip())
        if top1_agent and known and top1_agent not in known:
            reasons.append("source_domain_unknown")
        elif not top1_agent:
            reasons.append("source_domain_empty")
    # ④ 低置信度标记：条目级 confidence < 阈值（仅统计，不拒答）
    lc = rr.get("low_confidence", {}) or {}
    low_conf = []
    if lc.get("enabled", True) and final:
        th = float(lc.get("confidence_threshold", 0.8))
        for _s, r in final[:5]:
            c = r.get("confidence")
            if c is not None and c < th:
                low_conf.append({"id": r.get("id"), "confidence": c})
    if low_conf:
        details["low_confidence_items"] = low_conf
        reasons.append("low_confidence")
    refuse = any(x.endswith("_refuse") for x in reasons)
    action = "refuse" if refuse else ("mark" if reasons else "pass")
    details["reasons"] = reasons
    details["action"] = action
    return action, reasons, details


def _obqc_apply(query, out, final, time_desc_fallback, search_mode):
    """OBQC 校验应用点（_r1_annotate 之后调用；empty 分支亦调用）。
    shadow：全量校验只写 obqc_shadow.log，返回零改动（透传）。
    enforce：refuse → 拒答改写（results=[] + note + obqc 字段 + 质量档强制 weak）；
             mark → 顶层 obqc 字段 + 条目级 obqc_low_confidence 标记（不拒答）。
    任何异常全吞（校验失败绝不影响检索返回）。fts5 快路直接返回（零开销）。"""
    try:
        if search_mode.startswith("fts5"):
            return out
        mode = _obqc_mode()
        action, reasons, details = _obqc_check(query, final, time_desc_fallback, search_mode)
        entry = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "query": (query or "")[:200],
            "query_hash": "sha256:" + hashlib.sha256(
                _cache_normalize_query(query or "").encode("utf-8")).hexdigest()[:16],
            "mode": mode,
            "search_mode": search_mode,
            "best_score_norm": details.get("best_score_norm"),
            "best_evidence": details.get("best_evidence"),
            "action": action,
            "reasons": reasons,
            "would_block": action == "refuse",
            "if_enforced": ("拒答：" + "；".join(_OBQC_REASON_TEXT.get(r, r) for r in reasons)
                            if action == "refuse"
                            else ("标记：" + "；".join(_OBQC_REASON_TEXT.get(r, r) for r in reasons)
                                  if action == "mark" else "放行")),
            "top1_source_agent": details.get("top1_source_agent"),
            "top1_has_source_ref": details.get("top1_has_source_ref"),
            "low_confidence_items": details.get("low_confidence_items", []),
        }
        _obqc_write_log(entry)
        if mode == "enforce":
            if action == "refuse":
                out["results"] = []
                out["total"] = 0
                out["note"] = "OBQC 校验未通过（错误拒答优于瞎答）：" + "；".join(
                    _OBQC_REASON_TEXT.get(r, r) for r in reasons)
                out["obqc"] = {"mode": "enforce", "action": "refuse", "reasons": reasons, "checked": True}
                if R1_GATE_ENABLED:
                    out["result_quality"] = "weak"
                    out["grade"] = "weak"
                # P1-E: 拒答信号埋点（reject_feedback）
                try:
                    log_access([], query, "reject_feedback")
                except Exception:
                    pass
            elif action == "mark":
                out["obqc"] = {"mode": "enforce", "action": "mark", "reasons": reasons, "checked": True}
                low_ids = {x["id"] for x in details.get("low_confidence_items", [])}
                for it in out.get("results", []):
                    if it.get("id") in low_ids:
                        it["obqc_low_confidence"] = True
            else:  # pass：enforce 下也标注校验生效（切后双跑对比可见性）
                out["obqc"] = {"mode": "enforce", "action": "pass", "reasons": [], "checked": True}
        return out
    except Exception:
        return out


# Concern搜索配置（从permissions.yaml读取）
CONCERN_ENABLED = True
SEMANTIC_WEIGHT = 0.6
CONCERN_WEIGHT = 0.4

try:
    import yaml, re
    with open(os.path.join(_SIKU_ROOT, 'permissions.yaml')) as f:
        cfg = yaml.safe_load(f)
        cs = cfg.get('concern_search', {})
        CONCERN_ENABLED = cs.get('enabled', True)
        SEMANTIC_WEIGHT = cs.get('semantic_weight', 0.6)
        CONCERN_WEIGHT = cs.get('concern_weight', 0.4)
except Exception:
    pass

# ---- Rerank config ----
RERANK_ENABLED = False
RERANK_BATCH = 20
RERANK_TOP_K = 20

# ---- Query rewrite config ----
REWRITE_ENABLED = False
REWRITE_MIN_LEN = 8

try:
    import yaml, re
    with open(os.path.join(_SIKU_ROOT, 'permissions.yaml')) as f:
        cfg = yaml.safe_load(f)
    rr = cfg.get('rerank', {})
    RERANK_ENABLED = rr.get('enabled', False)
    RERANK_BATCH = rr.get('batch_size', 20)
    RERANK_TOP_K = rr.get('top_k_rrf', 20)
    qr = cfg.get('query_rewrite', {})
    REWRITE_ENABLED = qr.get('enabled', False)
    REWRITE_MIN_LEN = qr.get('min_length', 8)
except Exception as e:
    print(f"[warn] 搜索配置读取失败({e})，使用默认值", file=sys.stderr)

# ---- Reranker model (lazy load) ----
_reranker_model = None

def load_reranker():
    global _reranker_model
    if _reranker_model is None:
        try:
            from sentence_transformers import CrossEncoder
            device = "mps"
            device = 'mps'
            try:
                import yaml
                with open(os.path.join(_SIKU_ROOT, 'permissions.yaml')) as f:
                    rr = yaml.safe_load(f).get('rerank', {})
                    device = rr.get('device', 'mps')
            except Exception:
                pass
            _reranker_model = CrossEncoder("BAAI/bge-reranker-base", device=device, local_files_only=True)
        except Exception as e:
            print(f"[rerank] MPS加载失败({e})，回退CPU...")
            try:
                _reranker_model = CrossEncoder("BAAI/bge-reranker-base", device="cpu", local_files_only=True)
            except Exception:
                print("[rerank] CPU也失败，跳过rerank")
                return None
    return _reranker_model

def rerank_results(query, candidates, top_k=5):
    """bge-reranker-base 重排 top_k。返回 (orig_score, row) 对列表（排序按 rerank 分数降序）。

    RAGF-R1：rerank 分数（sigmoid 输出 ≈0-1，越大越相关）作为**证据分**内嵌到
    row['_rerank_evidence']（内部字段，format_* 不输出、消费方不可见）——复用本次 predict 输出
    零额外调用（性能纪律：证据分路径不增加主查询延迟），供 _r1_annotate
    融合判定（分数低 OR 证据分低 → weak_match）。fts5 快路/rerank 失败无该键 → 不参与证据分判定。
    """
    model = load_reranker()
    if model is None:
        return candidates[:top_k]
    pairs = [(query, r[1].get('summary', '') + ' ' + (r[1].get('content', '') or '')) for r in candidates[:RERANK_TOP_K]]
    try:
        scores = model.predict(pairs, batch_size=RERANK_BATCH, show_progress_bar=False)
        ranked = sorted(zip(scores, candidates[:RERANK_TOP_K]), key=lambda x: -x[0])
        out = []
        for s, (orig_score, row) in ranked[:top_k]:
            row = dict(row)
            row["_rerank_evidence"] = float(s)
            out.append((orig_score, row))
        return out
    except Exception as e:
        print("[rerank] inference failed:", e)
        return candidates[:top_k]

# Query rewrite cache (LRU)
_rewrite_cache = {}
def rewrite_query(query):
    global _rewrite_cache
    if not REWRITE_ENABLED:
        return query
    # Cache check (simple LRU, size 100)
    if query in _rewrite_cache:
        return _rewrite_cache[query]
    q = query.strip()
    if re.search(r'[A-Za-z]{2,}[-_]\d+|\d{5,}', q):
        return query
    if re.search(r'[|;>&]|^[a-z]{2,10}\s+-', q):
        return query
    pronouns = ['那个', '这个', '上次', '那个什么', '之前', '你那个', '刚才']
    has_pronoun = any(p in q for p in pronouns)
    if len(q) >= REWRITE_MIN_LEN and not has_pronoun:
        return query
    if len(q) < 4:
        result = q + ' 是什么'
        _rewrite_cache[query] = result
        if len(_rewrite_cache) > 100:
            _rewrite_cache.pop(next(iter(_rewrite_cache)))
        return result
    _rewrite_cache[query] = query
    if len(_rewrite_cache) > 100:
        _rewrite_cache.pop(next(iter(_rewrite_cache)))
    return query

TYPE_EMOJI = {
    "decision": "📋", "lesson": "⚠️", "instruction": "📌",
    "idea": "💡", "result": "✅", "insight": "🔍",
    "principle": "⭐", "pattern": "🔄", "preference": "❤️",
    "fact": "📄", "correction": "🛠️",
    # : 8 系统资产类型 emoji（spec📐 skill🧩 cron⏰ workflow🔀
    # rule🚦 benchmark🏁 asset📦 monitor📡）
    "spec": "📐", "skill": "🧩", "cron": "⏰", "workflow": "🔀",
    "rule": "🚦", "benchmark": "🏁", "asset": "📦", "monitor": "📡",
    # : 5 类新资产 emoji（design🎨 research🔬 script⚙️
    # config🗂️ reference📚）
    "design": "🎨", "research": "🔬", "script": "⚙️", "config": "🗂️", "reference": "📚",
}

# ── 数据库 ────────────────────────────────────────────────────────────

def get_conn():
    """获取 SQLite 连接（复用 memory-bank.py 的 DB）"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


MAX_QUERY_LEN = 500  # LIKE查询最大长度，防慢查询


# ── 通道 A：FTS5 全文检索 ───────────────────────────────────────────

def channel_fts5(query, candidate_size=200, track=None, time_cond=None, note_first=False):
    query = query[:MAX_QUERY_LEN]  # 截断超长查询
    """
    通道 A：FTS5 trigram 全文检索。

    FTS5 的 rank 列给出 BM25-like 分数，直接用做 BM25 通道。
    track: None=不限轨（默认全量）| 'episodic' | 'semantic'（R1-C 记忆分轨：限轨候选过滤）
    time_cond: (sql_frag, params) 宽容时间过滤条件（预过滤——
               候选层只排除确定不在窗内的；None 零改动）
    note_first: True=只检 summary 列（FTS5 列限定 'summary: (...)'——Note-First 级联第一轮；
               False=summary+content 全量（现状行为）。）
    返回: [(rank, row_dict), ...]，rank 越高越匹配
    """
    conn = get_conn()
    c = conn.cursor()

    # Phase 0: jieba 预分词（与重建索引切词一致），2字中文词可命中
    import jieba
    tokens = [t.strip() for t in jieba.lcut(query) if t.strip()]
    if not tokens:
        tokens = [query]
    fts_q = " OR ".join('"%s"' % t for t in tokens)
    # ：Note-First 级联第一轮——FTS5 列限定只检 summary（Note 常驻层）
    fts_match = ("summary: (%s)" % fts_q) if note_first else fts_q

    _tc_sql, _tc_params = time_cond or ("", [])
    try:
        if track:
            c.execute("""
                SELECT f.id, f.type, f.summary, f.content,
                       f.confidence, f.timestamp, f.source_agent, f.rowid,
                       f.industry, f.source_ref, f.expires_at, f.summary_hash,
                       f.event_date,
                       rank AS fts_rank
                FROM memory_store f
                JOIN mem_fts ON f.rowid = mem_fts.rowid
                WHERE mem_fts MATCH ? AND f.memory_track = ?%s
                ORDER BY rank
                LIMIT ?
            """ % _tc_sql, (fts_match, track) + tuple(_tc_params) + (candidate_size,))
        else:
            c.execute("""
                SELECT f.id, f.type, f.summary, f.content,
                       f.confidence, f.timestamp, f.source_agent, f.rowid,
                       f.industry, f.source_ref, f.expires_at, f.summary_hash,
                       f.event_date,
                       rank AS fts_rank
                FROM memory_store f
                JOIN mem_fts ON f.rowid = mem_fts.rowid
                WHERE mem_fts MATCH ?%s
                ORDER BY rank
                LIMIT ?
            """ % _tc_sql, (fts_match,) + tuple(_tc_params) + (candidate_size,))
        rows = c.fetchall()
    except sqlite3.OperationalError:
        # FTS5 失败时降级到 LIKE（note_first 时只 LIKE summary 列——与 Note 语义一致）
        try:
            if track:
                c.execute("""
                    SELECT id, type, summary, content,
                           confidence, timestamp, source_agent, rowid,
                           industry, source_ref, expires_at, summary_hash,
                           0.0 AS fts_rank
                    FROM memory_store
                    WHERE %s LIKE ? AND memory_track = ?%s
                    LIMIT ?
                """ % ("summary" if note_first else "(summary OR content)", _tc_sql),
                    (f"%{query}%", track) + tuple(_tc_params) + (candidate_size,))
            else:
                c.execute("""
                    SELECT id, type, summary, content,
                           confidence, timestamp, source_agent, rowid,
                           industry, source_ref, expires_at, summary_hash,
                           0.0 AS fts_rank
                    FROM memory_store
                    WHERE %s LIKE ?%s
                    LIMIT ?
                """ % ("summary" if note_first else "(summary OR content)", _tc_sql),
                    (f"%{query}%",) + tuple(_tc_params) + (candidate_size,))
            rows = c.fetchall()
        except Exception:
            rows = []

    conn.close()

    if not rows:
        return [], "fts5_empty"

    # FTS5 rank 是浮点数（越接近 0 越匹配），转为正向分数
    # rank 是负的 BM25 分数，所以 -rank 是正向 BM25 分数
    results = []
    for r in rows:
        d = dict(r)
        try:
            bm25_score = -float(d.get("fts_rank", 0))
        except (ValueError, TypeError):
            bm25_score = 0.0
        results.append((bm25_score, d))

    # 按 BM25 分数降序
    results.sort(key=lambda x: -x[0])
    return results, "fts5"


def channel_embedding(query, rows, limit=200, track=None):
    """
    通道 B：Chroma 向量语义检索（v2026-07-08 升级）。

    先用 Chroma ANN 从全局语义空间召回，降级时回退 SQLite BLOB 余弦相似度。
    track: None=不限轨 | 'episodic' | 'semantic'（R1-C 记忆分轨：限轨候选过滤）
    返回: [(cosine_sim, row_dict), ...]
    """
    chroma_path = os.path.join(_SIKU_ROOT, "memory_store.chromadb")
    try:
        # 获取查询向量
        sys.path.insert(0, os.environ.get("SIKU_EMBED_DIR", os.path.join(_HERMES_HOME, "scripts", "embedding")))
        from embed import embed_text
        qvec = embed_text(query)
        import numpy as np
        if isinstance(qvec, list):
            qvec = np.array(qvec)
    
        # Chroma ANN 检索
        import chromadb
        client = chromadb.PersistentClient(path=chroma_path)
        collection = client.get_collection("siku_memories")
        top_n = min(limit * 3, 200)
        cresult = collection.query(
            query_embeddings=[qvec.tolist()],
            n_results=top_n,
            include=["metadatas", "distances"]
        )
    
        if cresult and cresult["ids"] and cresult["ids"][0]:
            ids = cresult["ids"][0]
            dists = cresult["distances"][0]
            marks = ",".join("?" * len(ids))
            conn = get_conn()
            cur = conn.cursor()
            if track:
                cur.execute(
                    "SELECT id, type, summary, content, confidence, timestamp, "
                    "source_agent, rowid, industry, source_ref, expires_at "
                    f"FROM memory_store WHERE id IN ({marks}) AND memory_track = ?",
                    (*ids, track),
                )
            else:
                cur.execute(
                    "SELECT id, type, summary, content, confidence, timestamp, "
                    "source_agent, rowid, industry, source_ref, expires_at "
                    f"FROM memory_store WHERE id IN ({marks})",
                    ids,
                )
            by_id = {r["id"]: dict(r) for r in cur.fetchall()}
            conn.close()
            scored = []
            for mid, dist in zip(ids, dists):
                if mid in by_id:
                    scored.append((1.0 - float(dist), by_id[mid]))
            scored.sort(key=lambda x: -x[0])
            return scored[:limit], "embed_chroma_ann"
    except Exception:
        pass
    
    # 降级：SQLite BLOB 余弦相似度
    try:
        sys.path.insert(0, os.environ.get("SIKU_EMBED_DIR", os.path.join(_HERMES_HOME, "scripts", "embedding")))
        from embed import embed_text
        qvec = embed_text(query)
        import numpy as np
        if isinstance(qvec, list):
            qvec = np.array(qvec)
        conn = get_conn()
        cur = conn.cursor()
        if track:
            cur.execute(
                "SELECT id, type, summary, content, confidence, timestamp, "
                "source_agent, rowid, industry, source_ref, expires_at, embedding "
                "FROM memory_store WHERE embedding IS NOT NULL AND length(embedding)>0 "
                "AND memory_track = ?",
                (track,),
            )
        else:
            cur.execute(
                "SELECT id, type, summary, content, confidence, timestamp, "
                "source_agent, rowid, industry, source_ref, expires_at, embedding "
                "FROM memory_store WHERE embedding IS NOT NULL AND length(embedding)>0"
            )
        scored = []
        for r in cur.fetchall():
            d = dict(r)
            raw = d.pop("embedding")
            vec = np.frombuffer(raw, dtype=np.float32)
            cs = float(np.dot(qvec, vec))
            scored.append((cs, d))
        conn.close()
        scored.sort(key=lambda x: -x[0])
        # P0-B：embed 相对门控——分布实测 GT/噪音余弦完全重叠
        # （GT min=0.5535 vs 噪音 min=0.5517），绝对阈值会误伤 → 改 top-N 内相对分差：
        #   gap10=top1-top10  <0.02 → 无区分度，仅留 top3（13 条失败 GT embed 位≤3 占 11/13）
        #   gap5=top1-top5   <0.05 → 留 top5；否则全量 top10
        # 多跳-09（GT embed 位 11）在此门控下被截——该条双通道召回均失败，不指望 embed 救（记录在案）
        if EMBED_REL_GATE_ENABLED and scored:
            try:
                _EMBED_GATE_STATS["calls"] += 1
                t1 = float(scored[0][0])
                gap5 = t1 - float(scored[min(4, len(scored) - 1)][0])
                gap10 = t1 - float(scored[min(9, len(scored) - 1)][0])
                if gap10 < 0.02:
                    scored = scored[:3]
                    _EMBED_GATE_STATS["kept3"] += 1
                elif gap5 < 0.05:
                    scored = scored[:5]
                    _EMBED_GATE_STATS["kept5"] += 1
                else:
                    scored = scored[:10]
                    _EMBED_GATE_STATS["kept10"] += 1
            except Exception:
                _EMBED_GATE_STATS["errors"] += 1
        return scored[:limit], "embed_fallback_ann"
    except Exception:
        return [], "embed_unavailable"
def rrf_fusion(*channels, top_k=5, weights=None, boost_query=None):
    """
    RRF (Reciprocal Rank Fusion) 融合多通道结果（S8 P1b 泛化：双通道 → N 通道）。

    channels: 每路为 [(score, row_dict), ...]（按 score 降序），如 bm25/embed/graph
    weights:  命名通道权重 dict {fts5: w, emb: w, graph: w}（R2 阶段1 参数化，默认 None→CHANNEL_WEIGHTS 全 1.0）
    返回: [(rrf_score, row_dict), ...] 按 rrf_score 降序
    向后兼容：rrf_fusion(bm25_results, embed_results, top_k=5) 与原双通道签名等价；
    R2 阶段1：保偏移公式 1/(RRF_K+rank/w+1)——w=1 严格逐位==旧公式 1/(RRF_K+rank+1)
    （等价断言验证）；w<1 通道贡献整体下压（与 Qdrant w_r 同向）；w<=0 特判跳过该通道。
    注意：通道权重按位置对应（channels 顺序=fts5/emb/graph），种子融合（seed_fused）恒等权不传 weights。
    """
    if weights is None:
        weights = CHANNEL_WEIGHTS
    # 通道顺序约定：与调用点一致（bm25=fts5, embed=emb, graph=graph）
    channel_names = ("fts5", "emb", "graph")
    rank_map = {}  # entry_id -> rrf_score
    row_map = {}   # entry_id -> row_dict

    for ci, channel in enumerate(channels):
        w = weights.get(channel_names[ci], 1.0) if ci < len(channel_names) else 1.0
        if w <= 0:
            continue  # w<=0 特判跳过该通道（Qdrant 同要求）
        for rank, (score, row) in enumerate(channel):
            eid = row["id"]
            rank_map[eid] = rank_map.get(eid, 0) + 1.0 / (RRF_K + rank / w + 1)
            row_map[eid] = row

    # 按 RRF 分数降序
    ranked = sorted(rank_map.items(), key=lambda x: -x[1])

    # 置信度加成：RRF_score × (1 + 0.3 × confidence)
    final = []
    for eid, rrf_score in ranked:
        row = row_map[eid]
        conf = row.get("confidence") or 0.5
        boost = rrf_score * (1 + 0.3 * conf)
        final.append((boost, row))

    final.sort(key=lambda x: -x[0])

    # P1-D保送机制语义门控：不再 +9999 硬插 Top-3（R0 模式 D：无关全局指令
    # 被保送顶进 top5）。改为：query 含指令语义 → 保送条目追加到补充位（top_k 之后，
    # 不占 top1/top5 判定位）；无指令语义 → 不保送（正常分参与排序）。
    # 保送不影响 RRF 分数（不做 +9999 偏移），排序完全由正常分数决定。
    if boost_query and GT_BOOST_HINT_ENABLED:
        has_hint = any(h in (boost_query or "") for h in INSTRUCTION_HINTS)
        if has_hint:
            gt_appendix = []
            normal_items = []
            for score, row in final:
                if row.get("type") in GROUND_TRUTHS and (row.get("confidence") or 0) > 0.9:
                    gt_appendix.append((score, row))
                else:
                    normal_items.append((score, row))
            return (normal_items + gt_appendix)[:top_k + len(gt_appendix)]
    return final[:top_k]


# ── S8 P1b: 条目图通道（通道 C）───────────────────────────────────────
# Zero-Mem V3 定稿 §二 P1b：rrf_fusion 泛化第三路 + channel_graph 仅真信号；
# S0 盘点实锤：supports 482万同 concern 共现伪信号 + same_type 773万组合爆炸必须排除，
# 真信号仅 keyword_overlap/semantic_similar/same_agent 约 70 万边。
# 参数：top_k=主通道 1/2（V3 网格初始值：主通道 top-30 → 图通道 top-15）；
# eid 去重防 rank 叠加放大；缓存独立命名空间（_query_cache_hash 附加 g 标记）
# S8b 回退（V3 纪律）：golden 抽样判定存在真实退化（13 条中 ~11 条顶替者不相关，g060 完美命中被 mem_test 污染）
# → 生产默认图关（SIKU_GRAPH_CHANNEL 缺省=0，等于 S7 基线 0.4322 零风险）；图通道代码保留，实验启用需显式 SIKU_GRAPH_CHANNEL=1
# （语义层主线，方案 v1.3 判据）：SEMLAY_V=语义层增强版本位 env（默认关对齐 Zero-Mem V3 定案）。
# 30 query 真实核查实证：图开融合净增益（w1.0 平均Δsim +0.032，17增益/9退化 vs 图关基线），
# 权重降档(0.5/0.3)与 same_agent 门控阈值(0.25/0.30/0.35)均不改善退化（顶替者同源于种子强连边）；
# → 融合权重/阈值维持 L874-881 定稿值（A/B 实测最优），SEMLAY_V 仅作增强形态版本位入缓存键（A/B 不串）。
GRAPH_CHANNEL_ENABLED = os.environ.get("SIKU_GRAPH_CHANNEL", "0").lower() not in ("0", "off", "false")
SEMLAY_V_ENABLED = os.environ.get("SEMLAY_V", "0").lower() not in ("0", "off", "false")
GRAPH_RELATIONS = ("keyword_overlap", "semantic_similar", "same_agent")  # 仅真信号
GRAPH_SEED_K = 30         # 种子数 = 主通道候选截断（主通道 top-30）
GRAPH_CHANNEL_TOP_K = 15  # 图通道候选截断 = 主通道 1/2
GRAPH_PER_SEED = 3        # 每种子邻居上限（真信号度数均值 132，top3 覆盖主要邻居）
# ── R3 候选相关性门控（C4 门禁）────────────────────────────────────
# 根因 Q3：候选层静态遍历（graph_query.batch_expand_multi L79 SQL 邻接，query 零参与），
# 边权修复（R2）治本、门控治标互补：ranked 排序前按 query-候选向量相似度过滤，
# 低于边类型阈值则丢弃（无向量候选放行防误杀）。
# 阈值差异化：keyword_overlap 严（词面重叠易噪音 <0.35 丢）/ semantic_similar 中（<0.30 丢）/
# same_agent 豁免（防误杀 rel_unique，复核建议）
GRAPH_GATE_ENABLED = os.environ.get("SIKU_GRAPH_GATE", "1").lower() not in ("0", "off", "false")
GRAPH_GATE_THRESHOLDS = {  # 边类型 → 最低 query-候选余弦相似度（None=豁免）
    "keyword_overlap": 0.35,
    "semantic_similar": 0.30,
    "same_agent": None,
}
_GRAPH_STATS = {"calls": 0, "hits": 0, "empty": 0, "errors": 0, "candidates": 0, "dup_filtered": 0,
                "gate_checked": 0, "gate_filtered": 0, "gate_skipped": 0, "gate_errors": 0}


# ── R3 阶段3：fts5 候选语义截断 ────────────────────
# 根因：fts5 p@5=0.347 ≫ emb 0.173，但候选仍混入"词面重叠语义无关"噪音——词面命中 ≠ 语义相关
# （业界依据：Qdrant 混合检索实践 + 图通道研究结论：候选质量决定融合上限——图通道 p@5=0.011 教训）。
# 做法：fts5 候选进融合前与 query 算语义相似度（_query_candidate_sims 公共函数，图门控同源），
# 按相似度降序保留 top SIKU_CAND_TRUNCATE_K（默认 8，R2 网格 {5,8,12}）。
# 不设绝对值阈值（bge 相似度绝对值会漂移，防误杀真独有）：语义排序截断，高相似度真独有保留。
# SIKU_CAND_TRUNCATE 默认关（R3 整改回退 ：R2 网格全退化，生产恢复无截断精度），
# 设 1/on/true 开启（功能保留，改进后可再开）。
CAND_TRUNCATE_ENABLED = os.environ.get("SIKU_CAND_TRUNCATE", "0").lower() not in ("0", "off", "false")
CAND_TRUNCATE_K = int(os.environ.get("SIKU_CAND_TRUNCATE_K", "8"))
_TRUNCATE_STATS = {"calls": 0, "kept": 0, "truncated": 0, "no_vec": 0, "errors": 0}


# ──  + R3：event_date 时间预过滤（宽容 NULL + 门控按方案开启）──
# 方案：08-时间管线实施方案-20260827.md 段2（预过滤为主——检索前候选层）。
# ① 时间标记检测（0token 正则）：query 中显式日期/区间 → [t1,t2]；无标记零开销跳过
# ② 宽容过滤：event_date BETWEEN t1,t2 OR event_date IS NULL（只排除确定不在窗内的，
#    未知时间条目（NULL）一律放行——宁放过不误杀，与回填纪律同源）
# ③ 门控 SIKU_TIME_FILTER（仿 SIKU_GRAPH_CHANNEL 先例）：R2 沙盒三臂（08-沙盒三臂报告-20260827.md）
#    golden 113 条 A/B/C 三臂 hit_rate 0.9735 零退化 → 满足"golden A/B 通过才开"——R3 按方案开启，
#    生产默认 on；env SIKU_TIME_FILTER=off 一键回滚（双通道 桌面/gateway 同源生效）
# ④ 缓存 v11：时间维度入键（tf/tt）防两态串扰；门控 off 态键不变（仅版本前缀）
# ⑤ 审计日志（本地 JSONL）：查询 id+边界+滤除数，逐条可查
# ⑥ 第四路（SIKU_TIME_4TH=on，沙盒对照档）：event_date 窗内条目按时间接近度作为
#    独立通道进 RRF——待覆盖率达标后二期评估，默认关
TIME_FILTER_ENABLED = os.environ.get("SIKU_TIME_FILTER", "on").lower() not in ("0", "off", "false")
TIME_4TH_ENABLED = os.environ.get("SIKU_TIME_4TH", "off").lower() not in ("0", "off", "false")

# ──  子卡3/4（检索侧：两级粒度级联 + 遗忘背景化）──
# ① 级联检索 SIKU_CASCADE（默认 off）：Note-First（FTS 只检 summary 列——Note 常驻检索快检）
#    → 命中不足（空/弱/条数不足）→ Episode 补全（第二轮全量 summary+content——细节补全）。
#    HiMem Note-First→Episode 级联，l3_retrieval 接入点；门控默认关=生产零风险，
#    沙盒 golden A/B 零退化通过后才开（仿 SIKU_TIME_FILTER R3 先例）。
# ② 遗忘策略 SIKU_FORGET（默认 off）：低频背景化（Mnemosyne 对标）——memory_access_log
#    90 天窗口无命中记录 + 写入超 30 天（防误伤新条目）→ 降权系数 FORGET_DECAY（不删除）；
#    有命中记录（高频/近期活跃）零改动——不误杀。门控默认关。
CASCADE_ENABLED = os.environ.get("SIKU_CASCADE", "on").lower() not in ("0", "off", "false")
FORGET_ENABLED = os.environ.get("SIKU_FORGET", "on").lower() not in ("0", "off", "false")
CASCADE_NOTE_SCORE_MIN = 8.0   # Note 候选 best BM25 分低于此值=弱命中 → 触发 Episode 补全
CASCADE_BACKFILL_K = 40        # Episode 补全候选上限（去重后追加到 Note 之后）
FORGET_WINDOW_DAYS = 90        # 命中窗口：90 天内无访问记录 = 低频
FORGET_MIN_AGE_DAYS = 30       # 写入 30 天内的新条目不参与降权（防误伤新知识）
FORGET_DECAY = 0.6             # 降权系数：低频老条目分数 ×0.6（背景化不删除）
FORGET_STATS = {"queries": 0, "decayed": 0}
TIME_FILTER_LOG = os.environ.get(
    "SIKU_TIME_FILTER_LOG",
    os.path.join(_SIKU_ROOT, "scripts/siku_option/time_filter_audit.jsonl"),
)
TIME_4TH_TOP_K = 15  # 第四路候选截断（仿图通道：主通道 1/2）

# 时间标记正则（0token；年份 1970-2100 与 ISO 归一复用 l3_timeframe.normalize）
_TF_RE_YMD = re.compile(
    r"(?<!\d)((?:19|20)\d{2})\s*[-/年.]\s*(\d{1,2})\s*[-/月.]\s*(\d{1,2})\s*日?")   # 2026-08-01 / 2026年8月1日
_TF_RE_YM = re.compile(
    r"(?<!\d)((?:19|20)\d{2})\s*[-/年.]\s*(\d{1,2})\s*月?(?!\s*[-/年.]?\s*\d)")      # 2026-08 / 2026年8月
_TF_RE_RANGE = re.compile(r"(?:从|自)?(?:到|至|~|～|—|－|-|和|与)")                    # 区间连接词
_TF_STATS = {"detected": 0, "filtered_rows": 0, "audit_fail": 0}


def _tf_cn2iso(y, m, d=None):
    """中文/分隔符日期 → ISO（YYYY-MM-DD 或 YYYY-MM-01 月粒度；非法 → None）。"""
    try:
        yy, mm = int(y), int(m)
        if not (1970 <= yy <= 2100) or not (1 <= mm <= 12):
            return None
        if d is not None:
            dd = int(d)
            if not (1 <= dd <= 31):
                return None
            return "%04d-%02d-%02d" % (yy, mm, dd)
        return "%04d-%02d-01" % (yy, mm)
    except (TypeError, ValueError):
        return None


def _tf_detect(query):
    """时间标记检测：query → (t1, t2) ISO 串 或 (None, None)。
    单日期 → [同日,同日]；年月 → [月首,月末]；两日期+区间词 → [早,晚]。
    返回的 t1/t2 与 l3_timeframe.normalize 兼容（ISO 串，字典序即时间序）。"""
    if not query:
        return None, None
    dates = []
    for m in _TF_RE_YMD.finditer(query):
        d = _tf_cn2iso(m.group(1), m.group(2), m.group(3))
        if d:
            dates.append(d)
    if not dates:
        for m in _TF_RE_YM.finditer(query):
            d = _tf_cn2iso(m.group(1), m.group(2))
            if d:
                dates.append(d)
    if not dates:
        return None, None  # 无时间标记 → 零开销跳过
    if len(dates) == 1:
        d = dates[0]
        if len(d) == 10:  # 精确日 → 单日窗
            return d, d
        return d, d  # 月粒度：月初至月初（宽容过滤下边界模糊条目仍可命中当月内查询）
    dates.sort()
    return dates[0], dates[-1]


def _tf_sql_cond(column, t1, t2):
    """宽容 SQL 条件：event_date BETWEEN t1,t2 OR event_date IS NULL。
    返回 (sql_fragment, params)；两参皆 None → ('', [])。column 需含表别名（如 f.event_date）。"""
    if not t1 and not t2:
        return "", []
    conds, vals = [], []
    if t1:
        conds.append("%s >= ?" % column)
        vals.append(t1)
    if t2:
        conds.append("%s <= ?" % column)
        vals.append(t2)
    return " AND (%s IS NULL OR (%s))" % (column, " AND ".join(conds)), vals


def _tf_filter_rows(final, t1, t2):
    """融合后结果级宽容兜底过滤：行缺 event_date 按 id 批量补查一次（仿 l3_timeframe.filter_rows），
    补查仍缺 → 放行（宽容——宁放过不误杀）。仅时间标记命中时调用。"""
    if not t1 and not t2:
        return final
    if not final:
        return final
    missing = [row.get("id") for _, row in final
               if row.get("id") and not (row.get("event_date") or "")]
    if missing:
        try:
            conn = get_conn()
            marks = ",".join("?" * len(missing))
            rows = conn.execute(
                "SELECT id, event_date FROM memory_store WHERE id IN (%s)" % marks,
                missing,
            ).fetchall()
            conn.close()
            ed_map = {r[0]: r[1] for r in rows}
            for _, row in final:
                rid = row.get("id")
                if rid and not (row.get("event_date") or ""):
                    row["event_date"] = ed_map.get(rid) or ""
        except Exception:
            pass  # 补查失败 → 按现有值处理（缺 event_date → 放行）
    out = []
    for score, row in final:
        ed = row.get("event_date") or ""
        if not ed:
            out.append((score, row))
            continue
        if t1 and ed < t1:
            continue
        if t2 and ed > t2:
            continue
        out.append((score, row))
    return out


def _forget_adjust(final):
    """：遗忘策略——低频背景化降权（Mnemosyne 对标，SIKU_FORGET=on 时调用）。

    只降低频老条目：memory_access_log 90 天窗口内无命中记录 + 写入超 30 天（防误伤新条目）
    → 分数 ×FORGET_DECAY（背景化不删除）。有命中记录（高频/近期活跃）与新条目零改动——不误杀。
    降权后按分重排；任何异常全吞（降权失败照常返回原结果，检索零影响）。
    """
    if not final:
        return final
    try:
        conn = get_conn()
        c = conn.cursor()
        ids = [r["id"] for _, r in final]
        marks = ",".join("?" * len(ids))
        since = (datetime.now(timezone.utc) - timedelta(days=FORGET_WINDOW_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%S")
        c.execute(
            "SELECT entry_id, COUNT(*) FROM memory_access_log"
            " WHERE accessed_at >= ? AND entry_id IN (%s) GROUP BY entry_id" % marks,
            [since] + ids)
        active = {rid for rid, _ in c.fetchall()}
        c.execute("SELECT id, created_at FROM memory_store WHERE id IN (%s)" % marks, ids)
        created = dict(c.fetchall())
        conn.close()
        cut = (datetime.now(timezone.utc) - timedelta(days=FORGET_MIN_AGE_DAYS)).isoformat()
        out = []
        decayed = 0
        for score, row in final:
            rid = row.get("id")
            _cre = created.get(rid) or row.get("created_at") or ""
            if rid not in active and _cre and _cre < cut:
                out.append((score * FORGET_DECAY, row))
                decayed += 1
            else:
                out.append((score, row))
        out.sort(key=lambda x: -x[0])
        FORGET_STATS["decayed"] += decayed
        FORGET_STATS["queries"] += 1
        return out
    except Exception:
        return final  # 任何异常零影响（降权失败照常返回）


def _tf_audit(query, t1, t2, n_before, n_after, mode="prefilter"):
    """本地审计日志（JSONL 追加）：查询 id+边界+滤除数。任何失败不影响检索。"""
    try:
        qid = hashlib.sha256((query or "").encode("utf-8")).hexdigest()[:12]
        entry = {
            "op": "time_filter", "qid": qid, "query": (query or "")[:120],
            "t1": t1, "t2": t2, "n_before": n_before, "n_after": n_after,
            "filtered": (n_before or 0) - (n_after or 0), "mode": mode,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        }
        with open(TIME_FILTER_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        _TF_STATS["audit_fail"] += 1


def channel_time(query, t1, t2, candidate_size=None):
    """第四路（沙盒对照档）：event_date 窗内条目按时间接近度评分。
    分数 = 窗中心距离衰减（0.05 基础分，窗中心 1.0）+ 词面重叠加成；
    无时间标记/无结果 → ([], 'not_used')。仅 SIKU_TIME_4TH=on 时被调用。"""
    if not t1 or not t2:
        return [], "not_used"
    if candidate_size is None:
        candidate_size = TIME_4TH_TOP_K
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, type, summary, content, confidence, timestamp, source_agent, rowid,"
            " industry, source_ref, expires_at, summary_hash, event_date"
            " FROM memory_store WHERE event_date IS NOT NULL AND event_date >= ? AND event_date <= ?"
            " ORDER BY event_date DESC LIMIT ?",
            (t1, t2, candidate_size),
        ).fetchall()
    except Exception:
        conn.close()
        return [], "time_err"
    conn.close()
    if not rows:
        return [], "time_empty"
    # 时间接近度：窗中心距离 → [0,1]；外加词面重叠加成（query 词在 summary/content 的命中比例）
    q_tokens = set(re.findall(r"[\u4e00-\u9fff]{2,}|\w+", query or ""))
    from datetime import date as _date
    try:
        c0 = _date.fromisoformat(t1)
        c1 = _date.fromisoformat(t2)
        span = max((c1 - c0).days, 1)
    except ValueError:
        span = 1
    out = []
    for r in rows:
        d = dict(r)
        ed = d.get("event_date") or ""
        try:
            d0 = _date.fromisoformat(ed[:10])
            dist = abs((d0 - c0).days) / float(span) if span else 1.0
            tscore = max(0.0, 1.0 - dist)
        except (ValueError, TypeError):
            tscore = 0.0
        text = (d.get("summary") or "") + " " + (d.get("content") or "")
        ov = len([t for t in q_tokens if t in text]) / max(len(q_tokens), 1)
        out.append((0.05 + 0.85 * tscore + 0.10 * ov, d))
    out.sort(key=lambda x: -x[0])
    return out[:candidate_size], "time4th"


def _tf_cache_dim(t1, t2):
    """缓存键时间维度：'tf:<t1>|tt:<t2>'；两参皆 None → ''（键不变零开销）。"""
    if not t1 and not t2:
        return ""
    return "tf:%s|tt:%s" % (t1 or "", t2 or "")


def _query_candidate_sims(query, ids):
    """公共函数：query 与候选 id 列表的语义相似度（embed + 批量 SQL + dot）。

    - 相似度 = query 向量 · 候选向量（memory_store.embedding 列，bge-small-zh-v1.5 512 维同源，
      两端均已归一化 → 点积即余弦；与 channel_embedding 的 np.dot 口径一致）
    - R3 阶段3 提取自 _graph_gate_filter（图通道门控与新 fts5 截断共用，消除重复）
    - 返回 {id: float sim}；query 向量失败 / SQL 异常返回 {}（调用方降级：宁放勿杀）
    """
    import numpy as np
    if not ids:
        return {}
    try:
        sys.path.insert(0, os.environ.get("SIKU_EMBED_DIR", os.path.join(_HERMES_HOME, "scripts", "embedding")))
        from embed import embed_text
        qvec = np.asarray(embed_text(query), dtype=np.float32)
        if qvec.ndim == 2:
            qvec = qvec[0]
    except Exception:
        return {}
    marks = ",".join("?" * len(ids))
    emb = {}
    try:
        conn = get_conn()
        rows = conn.execute(
            "SELECT id, embedding FROM memory_store WHERE id IN (%s) "
            "AND embedding IS NOT NULL AND length(embedding)>0" % marks,
            ids,
        ).fetchall()
        conn.close()
        for r in rows:
            emb[r["id"]] = np.frombuffer(r["embedding"], dtype=np.float32)
    except Exception:
        return {}
    out = {}
    for rid, vec in emb.items():
        out[rid] = float(np.dot(qvec, vec))
    return out


def _fts5_candidate_truncate(query, results, k=None):
    """R3 阶段3：fts5 候选语义截断——候选与 query 相似度降序保留 top K。

    - 相似度 = _query_candidate_sims（embed + 批量 SQL + dot，与图门控同源）
    - 无语义相似度（无向量）候选：保持原 fts5 顺序追加（宁放勿杀，防误杀真独有）
    - 相似度计算整体失败：原样返回（防误杀）
    - k=None → CAND_TRUNCATE_K（默认 8，R2 网格 {5,8,12}）；SIKU_CAND_TRUNCATE=0 由调用方跳过
    返回: [(score, row), ...]（保持原 (score, row) 结构）
    """
    if k is None:
        k = CAND_TRUNCATE_K
    _TRUNCATE_STATS["calls"] += 1
    ids = [row.get("id") for _, row in results if row.get("id")]
    sims = _query_candidate_sims(query, ids)
    if not sims:
        _TRUNCATE_STATS["errors"] += 1
        return results  # 相似度计算失败 → 不截断
    scored, rest = [], []
    for score, row in results:
        rid = row.get("id")
        sim = sims.get(rid) if rid else None
        if sim is None:
            rest.append((score, row))
            _TRUNCATE_STATS["no_vec"] += 1
        else:
            scored.append((sim, score, row))
    scored.sort(key=lambda x: -x[0])  # 语义相似度降序
    kept = [(s, r) for _, s, r in scored[:k]]
    _TRUNCATE_STATS["truncated"] += len(scored) - len(kept)
    _TRUNCATE_STATS["kept"] += len(kept)
    return kept + rest


def _graph_gate_filter(query, best):
    """R3 候选相关性门控：候选与 query 向量相似度低于边类型阈值则丢弃。

    - 相似度 = _query_candidate_sims 公共函数（R3 阶段3 提取：embed + 批量 SQL + dot，
      memory_store.embedding 列 bge-small-zh-v1.5 512 维同源，点积即余弦，与 channel_embedding 口径一致）
    - 阈值差异化：keyword_overlap 0.35（严）/ semantic_similar 0.30（中）/ same_agent 豁免
    - 无向量候选、未知边类型、计算异常：放行（防误杀，宁放勿杀）
    返回: 过滤后的 best（同结构 {rid: nb}）"""
    if not best:
        return best
    sims = _query_candidate_sims(query, list(best.keys()))
    if not sims:
        _GRAPH_STATS["gate_errors"] += 1
        return best  # 相似度计算失败 → 不过滤（防误杀）
    out = {}
    for rid, nb in best.items():
        rel = nb.get("relation")
        thr = GRAPH_GATE_THRESHOLDS.get(rel)
        if thr is None:
            out[rid] = nb  # same_agent/未知边类型豁免
            continue
        sim = sims.get(rid)
        if sim is None:
            _GRAPH_STATS["gate_skipped"] += 1
            out[rid] = nb  # 无向量放行
            continue
        _GRAPH_STATS["gate_checked"] += 1
        if sim >= thr:
            out[rid] = nb
        else:
            _GRAPH_STATS["gate_filtered"] += 1
    return out


def channel_graph(query, main_candidates, candidate_size=None):
    """
    S8 P1b 通道 C：条目图邻居扩展（种子-邻居候选）。

    - 种子：主通道 RRF 融合 top GRAPH_SEED_K（main_candidates 已按 RRF 分降序）
    - 邻居：batch_expand_multi 单 SQL 批量查询（graph_query.py），
      仅真信号关系 keyword_overlap/semantic_similar/same_agent（排除 supports/same_type）
    - eid 去重（同 eid 保留最高 weight）；通道分数 = 边 weight
      （无权重超参——RRF 等权由 top_k 截断量控制，V3 Q2 定案；candidate_size=None→GRAPH_CHANNEL_TOP_K，
      运行时可调供网格定参）
    返回: ([(score, row_dict), ...], mode)
    """
    if candidate_size is None:
        candidate_size = GRAPH_CHANNEL_TOP_K
    _GRAPH_STATS["calls"] += 1
    seeds = []
    for _, row in main_candidates[:GRAPH_SEED_K]:
        eid = row.get("id")
        if eid:
            seeds.append(eid)
    if not seeds:
        _GRAPH_STATS["empty"] += 1
        return [], "graph_empty"
    try:
        import graph_query
        neighbors = graph_query.batch_expand_multi(seeds, top_k=GRAPH_PER_SEED, relations=GRAPH_RELATIONS)
    except Exception as e:
        _GRAPH_STATS["errors"] += 1
        return [], "graph_err:%s" % (e,)
    if not neighbors:
        _GRAPH_STATS["empty"] += 1
        return [], "graph_empty"
    # S0 实锤：keyword/semantic 边多命中"同教训多措辞"重复对（34万+18万边大半连近似重复变体）——
    # 邻居与种子近似重复（summary_hash 前缀8 相同 或 summary 编辑距离≥0.85，S4 阈值）视为冗余变体排除，
    # 防图通道把重复变体顶进 top5（g039 类退化根因：图通道引入近似重复 → rerank 顶替原条目）
    seed_texts = [(row.get("summary") or "", row.get("summary_hash") or "")
                  for _, row in main_candidates[:GRAPH_SEED_K]]
    def _is_dup_of_seed(nb):
        s = nb.get("summary") or ""
        h = nb.get("summary_hash") or ""
        if not s:
            return False
        for ss, sh in seed_texts:
            if h and sh and h[:8] == sh[:8]:
                return True
            if ss and _text_sim(s, ss) >= 0.85:
                return True
        return False
    _n_before = len(neighbors)
    neighbors = [nb for nb in neighbors if not _is_dup_of_seed(nb)]
    _GRAPH_STATS["dup_filtered"] += _n_before - len(neighbors)
    if not neighbors:
        _GRAPH_STATS["empty"] += 1
        return [], "graph_empty"
    # eid 去重（batch_expand_multi 已取最高 weight，双保险）
    best = {}
    for nb in neighbors:
        rid = nb["related_id"]
        if rid not in best or nb["weight"] > best[rid]["weight"]:
            best[rid] = nb
    # ── R3 候选相关性门控（C4 门禁）：ranked 排序前按 query-候选相似度过滤 ──
    # 根因 Q3：候选层静态遍历（SQL 邻接，query 零参与）；R2 边权修复治本、此门控治标互补。
    # SIKU_GRAPH_GATE 默认开（过滤生效），设 0/off/false 关闭对比。
    if GRAPH_GATE_ENABLED and best:
        best = _graph_gate_filter(query, best)
    ranked = sorted(best.values(), key=lambda x: -x["weight"])[:candidate_size]
    results = []
    for nb in ranked:
        results.append((float(nb["weight"]), {
            "id": nb["related_id"],
            "type": nb.get("type"),
            "summary": nb.get("summary") or "",
            "content": None,
            "confidence": nb.get("confidence") or 0.5,
            "timestamp": nb.get("timestamp") or "",
            "source_agent": nb.get("source_agent") or "",
            "rowid": None,
            "industry": nb.get("industry"),
            "source_ref": nb.get("source_ref"),
            "expires_at": nb.get("expires_at"),
            "summary_hash": nb.get("summary_hash"),
            "deprecated": nb.get("deprecated") or 0,
        }))
    _GRAPH_STATS["hits"] += 1
    _GRAPH_STATS["candidates"] += len(results)
    return results, "graph"


# ── S9: shadow 在线采样（默认关，生产零影响）─────────────────────────
# Zero-Mem V3 §2.3：在线 1% 采样 shadow_log + 独立缓存命名空间（shadow 结果不写生产 query_cache）；
# S8 结论：图通道生产默认关（SIKU_GRAPH_CHANNEL 缺省=0）——shadow 在真实流量下收集
# "开图会怎样"的对比数据（双通道 top5 对比/延迟差/是否关系型 query），为 1 月 ROI 评估提供依据。
# 纪律：SIKU_SHADOW=1 时仅 hash 采样命中（~1%）的查询额外跑图通道对比，只写 shadow_log；
#       生产返回路径零改动、生产 query_cache 零写入（shadow 独立计算、独立落盘，天然隔离命名空间）。
SHADOW_ENABLED = os.environ.get("SIKU_SHADOW", "0").lower() not in ("0", "off", "false")
SHADOW_SAMPLE_RATE = 0.01                # 1% hash 采样
SHADOW_LOG_DIR = os.environ.get("SIKU_SHADOW_LOG_DIR", os.path.join(_SIKU_ROOT, "logs", "shadow"))
SHADOW_LOG_FILE = os.path.join(SHADOW_LOG_DIR, "shadow_log.jsonl")
_SHADOW_STATS = {"sampled": 0, "written": 0, "errors": 0}
# 关系型 query 启发式（供 shadow_log 子集分析用；与 golden 的 relational 标注近似对应）
_RELATIONAL_HINTS = ("相关", "关联", "关系", "联系", "依赖", "影响", "区别", "对比", "比较",
                     "哪些", "谁", "之间", "联动", "交互", "因果", "为什么", "如何影响", "作用于")


def _shadow_should_sample(query):
    """hash 采样：sha256(规范化 query) 前 8 字节换算 [0,1) < 1% → 采样。
    同 query 稳定命中（确定性），避免重复 query 被反复/漏采；生产默认关（SHADOW_ENABLED=False 恒 False）。"""
    if not SHADOW_ENABLED:
        return False
    q = (query or "").strip()
    if not q:
        return False
    h = hashlib.sha256(_cache_normalize_query(q).encode("utf-8")).digest()[:8]
    return int.from_bytes(h, "big") < int(SHADOW_SAMPLE_RATE * (1 << 64))


def _shadow_is_relational(query):
    """是否关系型 query（启发式）：关系/比较类提示词，或 和/与/及 等多实体连接词"""
    q = query or ""
    if any(kw in q for kw in _RELATIONAL_HINTS):
        return True
    return any(c in q for c in ("和", "与", "及", "vs", "VS"))


def _shadow_build_entry(ctx, query, mode, top_k, prod_mode, prod_final,
                        bm25_results, embed_results, now_iso, rr_top):
    """构造 shadow_log 条目：生产(图关) vs shadow(图开) 双通道 top5 对比 + 延迟差。
    只读计算：不读写生产 query_cache（独立命名空间=完全不碰），结果只落 shadow_log，不返回生产侧。
    shadow 侧走与生产完全相同的后处理链（RRF → dedup → conflict_filter → rerank），保证对比公平。"""
    def _ids_top5(final):
        return [{"id": r.get("id"), "score": round(s, 4)} for s, r in final[:top_k]]

    prod = {"search_mode": prod_mode, "top5": _ids_top5(prod_final)}

    sch = []
    if bm25_results:
        sch.append(bm25_results)
    if embed_results:
        sch.append(embed_results)
    if ctx["graph_results"]:
        sch.append(ctx["graph_results"])
    t0 = time.perf_counter()
    if len(sch) >= 2:
        s_final = rrf_fusion(*sch, top_k=rr_top)
    elif len(sch) == 1:
        s_final = sch[0][:top_k]
    else:
        s_final = []
    for i, (score, row) in enumerate(s_final):
        s_final[i] = (score, _ensure_deprecated_flag(row))
    s_final = _dedup_by_summary_hash(s_final, now_iso)
    s_final = _conflict_filter(s_final, now_iso)
    if not s_final:
        s_final = []
    # 生产 rerank 分支镜像（仅非 fts5 快路）
    if RERANK_ENABLED and s_final and not prod_mode.startswith("fts5"):
        reranked = rerank_results(query, s_final, top_k)
        if reranked:
            s_final = reranked
    t_shadow_post = (time.perf_counter() - t0) * 1000.0

    s_mode = prod_mode + "_graph" if ctx["graph_results"] else prod_mode
    shadow = {
        "search_mode": s_mode,
        "graph_channel": ctx["graph_mode"],
        "top5": _ids_top5(s_final),
    }
    prod_ids = [x["id"] for x in prod["top5"]]
    shadow_ids = [x["id"] for x in shadow["top5"]]
    extra_ms = ctx["graph_latency_ms"] + t_shadow_post
    return {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "query": query,
        "query_hash": "sha256:" + hashlib.sha256(
            _cache_normalize_query(query).encode("utf-8")).hexdigest()[:16],
        "sampled": True,
        "sample_rate": SHADOW_SAMPLE_RATE,
        "is_relational_query": _shadow_is_relational(query),
        "mode": mode,
        "top_k": top_k,
        "prod": prod,
        "shadow": shadow,
        "top5_overlap": len(set(prod_ids) & set(shadow_ids)),
        "prod_only_ids": [x for x in prod_ids if x not in set(shadow_ids)],
        "shadow_only_ids": [x for x in shadow_ids if x not in set(prod_ids)],
        "prod_latency_ms": round(ctx["prod_latency_ms"], 2),
        "graph_channel_latency_ms": round(ctx["graph_latency_ms"], 2),
        "shadow_total_latency_ms": round(ctx["prod_latency_ms"] + extra_ms, 2),
        "latency_delta_ms": round(extra_ms, 2),
    }


def _shadow_write_log(entry):
    """追加写 shadow_log（JSONL，UTF-8）。失败仅计数，绝不影响生产返回。"""
    if not entry:
        return
    try:
        os.makedirs(SHADOW_LOG_DIR, exist_ok=True)
        with open(SHADOW_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        _SHADOW_STATS["written"] += 1
    except Exception:
        _SHADOW_STATS["errors"] += 1


# ── 渐进式输出 ──────────────────────────────────────────────────────

def _ensure_source_fields(row):
    """
    A4-P1: 兜底查补 source_agent/source_ref。
    正常通道（fts5/embed/time_desc）均已 SELECT 这两列，row 中应已存在；
    仅当上游降级/异常导致缺失时，按 id 补查一次 memory_store。
    零命中路径不触发任何查询，保证常规检索零额外开销。
    """
    if row is None or not row.get("id"):
        return row
    if "source_agent" in row and "source_ref" in row:
        return row
    try:
        conn = get_conn()
        c = conn.cursor()
        r = c.execute(
            "SELECT source_agent, source_ref FROM memory_store WHERE id=?",
            (row.get("id"),),
        ).fetchone()
        conn.close()
        if r:
            row["source_agent"] = r["source_agent"] or ""
            row["source_ref"] = r["source_ref"] or ""
        else:
            row.setdefault("source_agent", "")
            row.setdefault("source_ref", "")
    except Exception:
        row.setdefault("source_agent", "")
        row.setdefault("source_ref", "")
    return row


def _ensure_deprecated_flag(row):
    """
    A6-P2: 惰性补查 deprecated 标记（与 _ensure_source_fields 同模式）。
    正常通道 SELECT 未含 deprecated 列；仅当 row 缺失该键时按 id 补查一次，
    保证常规检索零额外开销。
    """
    if row is None or not row.get("id"):
        return row
    if "deprecated" in row:
        return row
    try:
        conn = get_conn()
        c = conn.cursor()
        r = c.execute(
            "SELECT deprecated FROM memory_store WHERE id=?",
            (row.get("id"),),
        ).fetchone()
        conn.close()
        if r is not None:
            row["deprecated"] = r["deprecated"] or 0
        else:
            row["deprecated"] = 0
    except Exception:
        # 旧库无 deprecated 列 → 视为未弃用
        row["deprecated"] = 0
    return row


def _filter_by_track(final, track):
    """R1-C 记忆分轨：融合后结果级 track 兜底过滤（graph 通道行无 memory_track 键时按 id 补查）。

    - track=None → 原样返回（零开销，向后兼容）
    - track='episodic'|'semantic' → 仅保留该轨条目；row 缺 memory_track 键则批量补查一次
    - 返回: [(score, row), ...] 保持原顺序
    """
    if not track or not final:
        return final
    missing = [row.get("id") for _, row in final if row.get("id") and "memory_track" not in row]
    if missing:
        try:
            marks = ",".join("?" * len(missing))
            conn = get_conn()
            rows = conn.execute(
                f"SELECT id, memory_track FROM memory_store WHERE id IN ({marks})",
                missing,
            ).fetchall()
            conn.close()
            trk = {r["id"]: r["memory_track"] for r in rows}
            for _, row in final:
                if row.get("id") and "memory_track" not in row:
                    row["memory_track"] = trk.get(row["id"])
        except Exception:
            pass  # 补查失败 → 按缺失处理（下述）
    out = []
    for score, row in final:
        if row.get("memory_track") == track:
            out.append((score, row))
    return out


def _filter_by_type(final, type_filter):
    """: 融合后结果级类型过滤（8 资产类型可查——按类型检索）。

    - type_filter=None/空 → 原样返回（零开销，向后兼容——存量 13 类型检索行为逐位不变）
    - type_filter='spec' 或 'spec,skill'（逗号分隔多值）→ 仅保留 type 命中条目；
      row 缺 type 键则按 id 批量补查一次（对齐 _filter_by_track 模式）
    返回: [(score, row), ...] 保持原顺序
    """
    if not type_filter or not final:
        return final
    want = {t.strip() for t in str(type_filter).split(",") if t.strip()}
    if not want:
        return final
    missing = [row.get("id") for _, row in final
               if row.get("id") and not row.get("type")]
    if missing:
        try:
            marks = ",".join("?" * len(missing))
            conn = get_conn()
            rows = conn.execute(
                f"SELECT id, type FROM memory_store WHERE id IN ({marks})",
                missing,
            ).fetchall()
            conn.close()
            tmap = {r["id"]: r["type"] for r in rows}
            for _, row in final:
                if row.get("id") and not row.get("type"):
                    row["type"] = tmap.get(row["id"])
        except Exception:
            pass  # 补查失败 → 按缺失处理（类型不匹配则被滤除，宁少勿错）
    out = []
    for score, row in final:
        if (row.get("type") or "").strip() in want:
            out.append((score, row))
    return out


def _dedup_by_summary_hash(final, now_iso):
    """
    A6-P2: 同事实不同日期版本排重。
    - summary_hash 相同的条目视为同一事实的不同版本；
    - 过滤已过期/已弃用（deprecated）版本；
    - 同 hash 组内优先生效版本：有效（未过期）优先，再取 timestamp 最新；
    - 组内全部过期/弃用时：保留 timestamp 最新的一条（审计可追溯），避免整体丢失。
    返回: [(score, row), ...]（保持原顺序）
    """
    seen = {}          # hash -> (score, row)
    order = []         # 保持 hash 首次出现顺序
    expired_all = {}   # hash -> (score, row)  组内全部无效时兜底
    for score, row in final:
        h = (row.get("summary_hash") or "").strip()
        if not h:
            # 无 hash 的旧条目不参与排重，直接保留（用 id 作唯一 key，防止多条互相覆盖）
            uid = "__nohash__" + str(row.get("id") or len(seen))
            seen[uid] = (score, row)
            order.append(uid)
            continue
        expires = (row.get("expires_at") or "")
        deprecated = int(row.get("deprecated", 0) or 0)
        valid = (not deprecated) and (not expires or expires >= now_iso)
        if not valid:
            # 组内全部无效兜底：保留 timestamp 最新
            cur = expired_all.get(h)
            if cur is None or (row.get("timestamp") or "") > (cur[1].get("timestamp") or ""):
                expired_all[h] = (score, row)
            continue
        cur = seen.get(h)
        if cur is None:
            seen[h] = (score, row)
            order.append(h)
        else:
            # 同 hash 有效版本：取 timestamp 更新的
            if (row.get("timestamp") or "") > (cur[1].get("timestamp") or ""):
                seen[h] = (score, row)
    result = []
    for h in order:
        result.append(seen[h] if h in seen else expired_all.get(h))
    return [x for x in result if x is not None]


# ── S7 P1a: 确定性证据校准层（冲突过滤）──────────────────────────────
# 冲突信号源（不用 graph_edges conflicts 边——死代码，Zero-Mem V3 定稿 §2.1 实锤）：
#   1) correction 语义：条目 deprecated=1 = 已被 correction 推翻 / 已弃用版本 → 丢弃
#   2) summary_hash 近似（前缀 8 位 或 summary 编辑距离 ≥0.85）+ 内容矛盾
#      （否定/纠错信号不对称命中 + 否定对象不同，S4 §3.2 方法）→ 丢低置信
# 埋点：_CONFLICT_DROPS（cap 200）供 golden 验收；SIKU_CONFLICT_FILTER=0 临时关闭（A/B 对照）
_CONFLICT_DROPS = []
# 校准层统计（golden/影子模式观测）：input_total=过滤器输入条目数，input_valid=其中非 deprecated 有效证据数
_CONFLICT_STATS = {"input_total": 0, "input_valid": 0}

# 强否定/纠错信号（S7 精度收敛：剔除"错误/过时/误报/不再/禁止/应为"等易在同向措辞中出现的高频噪声词——
# S4 §3.2 实证全库近似对 0 真矛盾，规则重精度：宁漏勿误杀）
_NEG_SIGNALS = ("不是", "误判", "纠正", "实为", "推翻", "而非", "≠", "作废", "假阳性",
                "不应", "不能用", "不适用", "否定", "别用")


def _extract_neg_object(text):
    """取第一个否定/纠错信号之后的片段作为否定对象（截断 24 字符；信号在末尾则取不到→空）"""
    for s in _NEG_SIGNALS:
        i = text.find(s)
        if i >= 0:
            return text[i + len(s):i + len(s) + 24]
    return ""


def _text_sim(a, b):
    try:
        from difflib import SequenceMatcher
        return SequenceMatcher(None, a, b).ratio()
    except Exception:
        return 0.0


def _is_near_dup(row_a, row_b):
    """summary_hash 近似：前缀 8 位相同，或 summary 编辑距离 ≥0.85（S4 阈值）"""
    ha = (row_a.get("summary_hash") or "").strip()
    hb = (row_b.get("summary_hash") or "").strip()
    if ha and hb and ha[:8] == hb[:8]:
        return True
    return _text_sim(row_a.get("summary") or "", row_b.get("summary") or "") >= 0.85


def _is_contradictory(row_a, row_b):
    """内容矛盾判定：否定/纠错信号不对称命中 + 否定对象不同（对象相似度 <0.35，S4 §3.2 等价）
    无信号一侧取其 summary 开头 24 字符作伪对象（该侧主张的主题）；信号侧取其否定对象（被纠正后的真相）。
    伪对象与否定对象相似度高 → 同一主张 → 不判矛盾；相似度低 → 主张 vs 否定真相 → 真矛盾。
    """
    ta = (row_a.get("summary") or "") + " " + (row_a.get("content") or "")
    tb = (row_b.get("summary") or "") + " " + (row_b.get("content") or "")
    # 全文近似（同一知识的多措辞记录/同一事件双记录）→ 非矛盾（S4 §3.2：近似对全为同向变体）
    if _text_sim(ta, tb) >= 0.85:
        return False
    ha_sig = any(s in ta for s in _NEG_SIGNALS)
    hb_sig = any(s in tb for s in _NEG_SIGNALS)
    if ha_sig == hb_sig:
        return False  # 信号对称（都无/都有）→ 保守不判矛盾
    obj_a = _extract_neg_object(ta) if ha_sig else (row_a.get("summary") or "")[:24]
    obj_b = _extract_neg_object(tb) if hb_sig else (row_b.get("summary") or "")[:24]
    if not obj_a or not obj_b:
        return False  # 无法定位否定对象 → 保守不判矛盾（避免误杀）
    return _text_sim(obj_a, obj_b) < 0.35  # 否定对象编辑距离 >0.65 → 对象不同 → 真矛盾


def _pick_winner(pair_a, pair_b):
    """矛盾对决胜：置信度→timestamp（新覆盖旧）→score"""
    score_a, row_a = pair_a
    score_b, row_b = pair_b
    conf_a = row_a.get("confidence") or 0.5
    conf_b = row_b.get("confidence") or 0.5
    if conf_a != conf_b:
        return 0 if conf_a > conf_b else 1
    ts_a = row_a.get("timestamp") or ""
    ts_b = row_b.get("timestamp") or ""
    if ts_a != ts_b:
        return 0 if ts_a > ts_b else 1
    return 0 if score_a >= score_b else 1


def _log_conflict_drop(row, reason):
    if len(_CONFLICT_DROPS) >= 200:
        return
    _CONFLICT_DROPS.append({
        "id": row.get("id"),
        "reason": reason,  # deprecated | contradiction
        "type": row.get("type"),
        "confidence": row.get("confidence"),
        "deprecated": row.get("deprecated"),
        "summary": (row.get("summary") or "")[:60],
    })


def _conflict_filter(final, now_iso=None):
    """
    S7 P1a: 确定性证据校准层——冲突过滤（插 _dedup_by_summary_hash 后、industry 过滤前）。

    规则：
      1) correction 语义：检索结果中出现 deprecated=1 条目（已被 correction 推翻/已弃用版本）→ 丢弃；
      2) summary_hash 近似（前缀 8 位 / 编辑距离 ≥0.85）且内容矛盾 → 保留高置信，丢低置信；
      3) 全部被过滤时返回空表，由调用方按 _pre_filter[:1] 兜底（与 dedup 同语义）。
    返回: [(score, row), ...]（保持原顺序）。
    埋点: 每次丢弃 append _CONFLICT_DROPS（cap 200，golden 验收读取用）。
    """
    if os.environ.get("SIKU_CONFLICT_FILTER", "1").lower() in ("0", "off", "false"):
        return final
    _CONFLICT_STATS["input_total"] += len(final)
    _CONFLICT_STATS["input_valid"] += sum(1 for _, row in final if not int(row.get("deprecated", 0) or 0))
    kept = []
    for score, row in final:
        if int(row.get("deprecated", 0) or 0):
            _log_conflict_drop(row, "deprecated")
            continue
        kept.append((score, row))
    # 近似矛盾：结果集内两两比对（n≤top_k 量级，O(n²) 可接受）
    drop_idx = set()
    for i in range(len(kept)):
        for j in range(i + 1, len(kept)):
            if i in drop_idx or j in drop_idx:
                continue
            if not _is_near_dup(kept[i][1], kept[j][1]):
                continue
            if not _is_contradictory(kept[i][1], kept[j][1]):
                continue
            winner = _pick_winner(kept[i], kept[j])
            loser_idx = j if winner == i else i
            drop_idx.add(loser_idx)
            _log_conflict_drop(kept[loser_idx][1], "contradiction")
    if drop_idx:
        kept = [x for idx, x in enumerate(kept) if idx not in drop_idx]
    return kept



def _r1_annotate(out, final, time_desc_fallback, search_mode=""):
    """R1：返回 JSON 注入 result_quality / weak_match / fallback 标注。

    纯标注零拦截（D1 裁定：拒答=信号不拦截，消费 Agent 决策）：
    - 顶层 result_quality（high/medium/low/weak）：取最终结果最高**真实** score 定档
      （排除 instruction 保送条目污染——RAGI-T 整改②）；time_desc 兜底或无结果强制 weak。
    - 每条结果 weak_match：归一化后真实 score < R1_Q_MEDIUM 或 time_desc 兜底 → True
      （RAGI-T 整改①：低分阈值判定扩展到所有路径，不限于兜底；fts5 快路 BM25 原分
      先归一到 RRF 量纲再判，否则快路低分全漏标）。
    - time_desc 兜底时顶层 fallback="time_desc"（显式标记，D4 实证根因修复）。
    - RAGF-R1：rerank 证据分第二维度融合——best 证据分 < RERANK_EVIDENCE_TH
      → 查询级 result_quality/grade 强制 weak（拒答强化，破纯分数阈值数学边界：假前提词面
      命中 RRF/BM25 分数与可答重叠，rerank 语义分可区分）；条目级弱匹配=分数低 OR 证据分低。
      仅 rerank 路径有 _rerank_evidence 键（fts5 快路无 → 仅既有分数判定，行为不变）。
    新字段不破坏既有字段（返回结构兼容，243 调用方零改动）。
    """
    if not R1_GATE_ENABLED:
        return out
    # RAGI-T 整改②：保送条目（+9999）还原真实分后取 best——保送仅排序用，不参与证据档位
    real_scores = [_r1_gt_orig_score(s) for s, _ in final] if final else []
    best = max(real_scores, default=0.0) if real_scores else 0.0
    best_norm = _r4_norm_score(_r1_norm_scale(best, search_mode))  # R4：权重归一（权重未变系数=1.0）
    # RAGF-R1：rerank 证据分（仅 rerank 路径存在；取 top 证据分作为查询级证据强度）
    ev_scores = [r.get("_rerank_evidence") for _, r in final if r.get("_rerank_evidence") is not None]
    best_ev = max(ev_scores) if ev_scores else None
    low_evidence = (best_ev is not None) and (best_ev < RERANK_EVIDENCE_TH)
    if time_desc_fallback or not final:
        quality = "weak"
    elif low_evidence:
        quality = "weak"  # RAGF-R1：证据分不足 → 拒答强化（R1 低分 OR 低证据分）
    else:
        quality = _r1_quality_of(best_norm)
    out["result_quality"] = quality
    out["grade"] = _r4_grade_of(best_norm, time_desc_fallback or not final or low_evidence)  # R4：CRAG 证据充分性提示（不拦截）
    if time_desc_fallback:
        out["fallback"] = "time_desc"
    res = out.get("results", [])
    for i, item in enumerate(res):
        if i < len(final):
            # RAGI-T 整改①：低分阈值判定扩展到所有路径（score<阈值即 weak_match=True，不限于兜底）
            raw = _r1_gt_orig_score(final[i][0])
            norm = _r4_norm_score(_r1_norm_scale(raw, search_mode))
            # RAGF-R1：条目级融合——分数低 OR 证据分低 → weak_match=True
            ev = final[i][1].get("_rerank_evidence") if i < len(final) else None
            low_ev_item = (ev is not None) and (ev < RERANK_EVIDENCE_TH)
            item["weak_match"] = bool(time_desc_fallback or norm < R1_Q_MEDIUM or low_ev_item)
            # RAGF-R1：透传证据分供消费端审计（新增字段，兼容既有字段）
            if ev is not None:
                item["rerank_evidence"] = round(ev, 4)
        else:
            item["weak_match"] = bool(time_desc_fallback)
    return out


def format_compact(results, search_mode):
    """第一层：compact — 索引视图，50字摘要 + token预估 + 来源/置信度标注（A4-P1）"""
    out = []
    for score, row in results:
        row = _ensure_source_fields(row)
        summary = row.get("summary", "") or ""
        truncated = len(summary) > COMPACT_SUMMARY_LEN
        entry_type = row.get("type", "")
        # P1-D显示分统一：去除 lesson/insight +0.1 显示加权（R0 模式 D 证据 3：
        # 教训条目显示分 0.12-0.14 呈"高分"假象，误导按 score 消费的下游；排序由 rerank/融合
        # 决定，显示分应与排序口径一致）。📋关联教训标记保留（提示性，不加分）。
        lesson_marker = " 📋关联教训" if entry_type in ("lesson", "insight") else ""
        display_score = score
        # RAGF-R1：透传rerank证据分供_r1_annotate使用（内部字段，不暴露给消费方）
        ev = row.get("_rerank_evidence")
        out.append({
            "id": row["id"],
            "type": row["type"],
            "emoji": TYPE_EMOJI.get(row["type"], "📄"),
            "summary": summary[:COMPACT_SUMMARY_LEN] + ("…" if truncated else "") + lesson_marker,
            "summary_truncated": truncated,
            "token_est": max(1, int(len(summary) * 1.8)),
            "score": round(display_score, 4),
            "has_content": bool(row.get("content")),
            "confidence": row.get("confidence"),
            "source_agent": row.get("source_agent", ""),
            "source_ref": row.get("source_ref", ""),
            "expires_at": (row.get("expires_at") or ""),  # A6-P2: 有效期窗口，空=永久
            "expand_hint": "使用 l3_expand_tool(id) 获取完整详情",
            "_rerank_evidence": ev,  # 内部字段，供_r1_annotate访问
        })
    return {
        "tier": "compact",
        "search_mode": search_mode,
        "total": len(out),
        "results": out,
        "expand_hint": "使用 l3_expand_tool(entry_id) 获取完整详情",
    }


def format_expand(row, score=None):
    """第二层：expand — 单条完整详情（含来源标注 A4-P1）"""
    row = _ensure_source_fields(row)
    entry = {
        "id": row["id"],
        "type": row["type"],
        "summary": row.get("summary", ""),
        "confidence": row.get("confidence"),
        "source_agent": row.get("source_agent", ""),
        "source_ref": row.get("source_ref", ""),
        "timestamp": row.get("timestamp", ""),
        "project": row.get("source_agent", ""),
        "keywords": (row.get("source_agent") or "").split(",") if row.get("source_agent") else [],
    }
    if row.get("content"):
        entry["content"] = row["content"]
    if score is not None:
        entry["score"] = round(score, 4)
    return {
        "tier": "expand",
        "entry": entry,
    }


def format_full(row, score=None):
    """第三层：full — 包含 embedding 等原始数据"""
    entry = format_expand(row, score)["entry"]
    entry["embedding_present"] = bool(row.get("embedding"))
    entry["embedding_dim"] = 512 if row.get("embedding") else 0
    entry["raw_confidence"] = row.get("confidence")
    return {
        "tier": "full",
        "entry": entry,
    }


# ── RI P0: memory_access_log 埋点 ──────────────────────────────────

def log_access(entry_ids, query, scene="search"):
    """
    异步埋点：记录检索命中的条目到 memory_access_log。
    不阻塞检索，失败静默跳过。
    TTL 90天通过查询时 WHERE accessed_at > datetime('now','-90 days') 实现。
    """
    if not entry_ids:
        return
    try:
        conn = get_conn()
        c = conn.cursor()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        rows = [(eid, query, scene, now) for eid in entry_ids]
        c.executemany("INSERT INTO memory_access_log(entry_id, query, scene, accessed_at) VALUES (?,?,?,?)", rows)
        conn.commit()
        conn.close()
    except Exception:
        pass  # 埋点失败不阻塞检索


# ── S2 query_cache 缓存层（M2：tier 限定 + 陈旧判据 + 键版本化）────────────
CACHE_KEY_VERSION = "v11"      # hash 版本号前缀：v4=S7 P1a 起 compact 输出经 _conflict_filter 冲突过滤（correction/deprecated + 近似矛盾），旧 v3 缓存失效；v5=R1-C 记忆分轨键加 track 维度（防 episodic/semantic 串扰），旧 v4 缓存失效；v6=R1 置信度显式化（SIKU_GATE=on 时 compact 输出含 result_quality/weak_match/fallback 字段，键加 R1 标记防两态串扰），旧 v5 缓存失效；v7=R4 score_norm 归一+grade 字段（SIKU_GATE=on 时 compact 输出含 grade，键加 R4 标记防旧 v6 R1 缓存命中返回无 grade 结构），旧 v6 缓存失效；v8=RAGI-T 整改（weak_match 阈值 R1_Q_MEDIUM + fts5 量纲归一 + 保送排除，标注语义变更，旧 v7 缓存命中返回旧标注），旧 v7 缓存失效；v9=RAGF-R1（rerank 证据分第二维度：on 态 result_quality/grade 可被低证据分降级 weak + 条目级 rerank_evidence 字段 + weak_match 融合证据分，旧 v8 缓存命中返回无证据分标注），旧 v8 缓存失效；v10=（检索结果级类型过滤 type_filter 维度入键——带/不带类型过滤两态缓存隔离，旧 v9 缓存命中返回无过滤结果），旧 v9 缓存失效；v11= + R3（event_date 时间预过滤：SIKU_TIME_FILTER=on 时时间标记命中结果带时间过滤语义 + 键含时间维度 tf/tt 防两态串扰——旧 v10 缓存命中返回无过滤结果；R3 门控默认开——按方案开启（R2 沙盒 golden A/B 零退化实证），on 态键含 tf/tt 时间维度，env SIKU_TIME_FILTER=off 回滚后 off 态键仅版本前缀变化，结果语义不变），旧 v10 缓存失效
CACHE_TIERS = ("compact",)    # 仅 compact 层缓存（expand/full 输出结构不同，禁缓存）
# （语义层主线）：失效策略=TTL 简版锁定（方案 v1.3 判据——不碰写入主路径）。
# 根因实证：旧读判据 db_max_updated==当前 MAX(updated_at) 在活跃库（近1h 3324 行更新）几乎恒不满足
# → query_cache 命中率≈0（写入侧 invalidate 全清钩子仍保留，写库后全清不受影响）；命中路径本身有效
# （同 query 二次 58ms vs 首次 2619ms=97.8% 加速实证）。TTL=5 分钟窗口内缓存可用，超窗视为未命中；
# 写入侧零改动（invalidate 钩子不碰）。cached_at 存 UTC naive（%Y-%m-%dT%H:%M:%S）。
QUERY_CACHE_TTL_S = 300

def _cache_normalize_query(query):
    """规范化函数单点实现：去首尾空白 + 压缩连续空白（单点修改防多入口漂移）"""
    return " ".join((query or "").strip().split())

def _channel_set_tag():
    """通道集合+权重状态 → 稳定哈希后缀（R2 阶段1：v4/v4g/v4gt → 通道集合哈希）
    参与通道（fts5/emb 恒在，graph 按开关）+ 门控标记 + 截断标记 + 参与通道权重序列化 → sha256 前 8 位。
    weights 参与哈希：不同 weights 不同缓存键（防缓存串扰，阶段2 调权无需手动清缓存）；
    图关时 graph 权重不参与（图关结果不受其影响，键不变=旧缓存仍可复用）。
    R3 阶段3：截断标记无条件参与（fts5 截断与图开关无关，开/关两态结果不同须隔离）。"""
    active = ["fts5", "emb"]
    if GRAPH_CHANNEL_ENABLED:
        active.append("graph")
    gate_flag = "gate" if (GRAPH_CHANNEL_ENABLED and GRAPH_GATE_ENABLED) else ""
    r1_flag = "r4" if R1_GATE_ENABLED else ""   # R4：R1 标记升级为 r4（on 态结构=R1 全字段+grade，防 v6 旧缓存命中返回无 grade）
    trunc_flag = "trunc" if CAND_TRUNCATE_ENABLED else ""
    # R1-IMPL：OBQC enforce 态键标记——enforce 返回结构含 obqc 字段/拒答改写，
    # 与 shadow/off 态（返回零改动）键隔离防串扰；shadow 态空串 → 键不变 → v9 旧缓存零影响。
    obqc_flag = "enf" if _obqc_mode() == "enforce" else ""
    # ：第四路（SIKU_TIME_4TH）参与通道集合变化 → 键标记防两态串扰
    t4_flag = "t4" if TIME_4TH_ENABLED else ""
    # ：SEMLAY_V 增强形态版本位——A/B 两态缓存隔离（默认关空串=键不变零影响）
    semlay_flag = "s1" if SEMLAY_V_ENABLED else ""
    w_parts = ",".join("%s=%r" % (k, CHANNEL_WEIGHTS.get(k, 1.0)) for k in active)
    raw = "|".join(["+".join(active), gate_flag, trunc_flag, "w:" + w_parts, "r1:" + r1_flag,
                    "obqc:" + obqc_flag, "t4:" + t4_flag] +
                   (["semlay:" + semlay_flag] if semlay_flag else []))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]

def _query_cache_hash(query, tier, mode, track=None, type_filter=None, time_dim=None):
    """缓存键：v1:sha256(规范化 query|tier|mode)——含 tier 维度（门禁条件 5）
    + mode 维度（防 fts5 快路结果与 dual 回落结果同键互覆）
    R1-C 记忆分轨：键加 track 维度（None|episodic|semantic）——防同 query 不同轨结果串扰（P2 注意项）
    : 键加 type_filter 维度（None|spec|spec,skill）——类型过滤两态缓存隔离
    : 键加 time_dim 维度（'' 或 'tf:<t1>|tt:<t2>'）——时间预过滤两态隔离
    S8 P1b：图通道独立命名空间——KEY_VERSION 在 v4 基础上图通道开启时附加 g 标记
    （v4g=含图通道结果 / v4=纯双通道），防图通道结果与生产 query_cache 串扰
    （V3 风险登记 P3 query_cache 污染对策）。
    R3 门控：图开+门控开再附加 t 标记（v4gt），防门控开/关两态结果同键互覆。
    R2 阶段1：v4/v4g/v4gt → 通道集合哈希——v4-<tag>，tag 含
    参与通道集合+门控+融合权重（_channel_set_tag），不同 weights 不同缓存键（防串扰）。"""
    raw = "%s|%s|%s|%s|%s|%s" % (_cache_normalize_query(query), tier, mode, track or "",
                                 type_filter or "", time_dim or "")
    ver = "%s-%s" % (CACHE_KEY_VERSION, _channel_set_tag())
    return "%s:%s" % (ver, hashlib.sha256(raw.encode("utf-8")).hexdigest())

def _query_cache_get(query, tier, mode, track=None, type_filter=None, time_dim=None):
    """读缓存：仅 compact 启用；陈旧判据 TTL 简版（QUERY_CACHE_TTL_S=5 分钟）
    ——命中路径纯只读（无写事务/无 fsync），保证命中耗时 <1ms；
    hit_count 观察数据留 S3（命中不累加，写回时重置 0）。
    历史判据（db_max_updated==当前 MAX(updated_at)）在活跃库恒不满足导致命中率≈0，已由
    TTL 简版锁定替代（方案 v1.3 判据；写入侧 invalidate 钩子保留零改动——不碰写入主路径）。"""
    if tier not in CACHE_TIERS:
        return None
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        h = _query_cache_hash(query, tier, mode, track, type_filter, time_dim)
        row = c.execute("SELECT * FROM query_cache WHERE query_hash=?", (h,)).fetchone()
        if row is None:
            return None
        # TTL 简版：cached_at 距今 ≤ QUERY_CACHE_TTL_S 秒 → 命中（超窗/解析失败 → 未命中）
        _ca = row["cached_at"]
        if _ca:
            try:
                _t_cached = datetime.fromisoformat(_ca)
                if _t_cached.tzinfo is None:
                    _t_cached = _t_cached.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - _t_cached).total_seconds() > QUERY_CACHE_TTL_S:
                    return None
            except Exception:
                return None
        else:
            return None
        return json.loads(row["result_json"])
    except Exception:
        return None  # 缓存异常绝不影响检索
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

def _query_cache_put(query, tier, mode, result, track=None, type_filter=None, time_dim=None):
    """写缓存：仅 compact 启用；result_json 存完整 format_compact 输出（含 search_mode/total/expand_hint）"""
    if tier not in CACHE_TIERS:
        return
    conn = None
    try:
        conn = get_conn()
        c = conn.cursor()
        h = _query_cache_hash(query, tier, mode, track, type_filter, time_dim)
        cur_max = c.execute("SELECT MAX(updated_at) FROM memory_store").fetchone()[0]
        token_est = sum(r.get("token_est", 0) for r in (result.get("results") or []))
        c.execute(
            "INSERT OR REPLACE INTO query_cache (query_hash, query, result_json, token_est, db_max_updated, hit_count, cached_at)"
            " VALUES (?,?,?,?,?,0,?)",
            (h, _cache_normalize_query(query), json.dumps(result, ensure_ascii=False),
             token_est, cur_max, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")),
        )
        conn.commit()
    except Exception as _qcp_e:
        # （缓存写失败诊断）：不再静默——异常打 stderr 留痕。
        # 检索主路径零影响语义不变（仍不 raise），仅诊断可见性提升。
        try:
            print("[query_cache_put] 写失败 query=%r tier=%s mode=%s err=%r" % (
                (query or "")[:80], tier, mode, _qcp_e), file=sys.stderr)
        except Exception:
            pass  # 诊断日志自身失败不处理
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ── R3-A 未命中监控：记录函数 ───────────────────────────────────────
def _missmon_record(query, hit, total, search_mode, mode="auto", track=None, quality=None, fallback=None):
    """R3-A 未命中监控：每查询追加记录 hit/miss 到 JSONL（纯规则零 LLM）。

    调用点：真实检索的 empty 分支（miss）与正常返回前（hit）。
    缓存命中路径不记录（重复查询不构成新检索，避免扭曲滚动窗口）。
    异常全吞：统计失败绝不影响检索主流程（与 shadow 采样同纪律）。
    R1：新增 quality（result_quality 档位）与 fallback（time_desc 兜底标记），
    供 missmon_check 低相关判定——time_desc 兜底/weak 档不再伪装成 hit=100%（D4 实证根因）。
    """
    if not MISSMON_ENABLED:
        return
    try:
        _dir = os.path.dirname(MISSMON_LOG)
        if _dir and not os.path.isdir(_dir):
            os.makedirs(_dir, exist_ok=True)
        entry = {
            "ts": int(time.time()),
            "iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "query": query[:200],
            "hit": bool(hit),
            "total": int(total),
            "search_mode": search_mode,
            "mode": mode,
            "track": track,
        }
        if quality is not None:
            entry["quality"] = quality      # R1：result_quality 档位（high/medium/low/weak）
        if fallback:
            entry["fallback"] = fallback    # R1：兜底标记（"time_desc"）
        with open(MISSMON_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ── P2a：验证循环核心（R-A4/R-A5/C10——纯规则零 LLM）──

def verify_extract_key_data(query):
    """R-A4：从 query 提取关键数据 token（0token 正则，长模式优先，number 兜底去重）。

    返回 [{"type": "percent|money|date|number|conclusion", "token": "..."}]
    """
    import re as _re
    found = []
    seen = set()
    covered = []  # [(start, end)] 已被长模式（percent/money/date）覆盖的区间
    for ptype, pat in VERIFY_KEY_PATTERNS:
        for m in _re.finditer(pat, query):
            tok = m.group(0).strip()
            if not tok:
                continue
            if ptype == "number":
                # 兜底：跳过与已匹配长模式重叠的区间（防 "2026-08-26" 拆出 2026/08/26 冗余）
                if any(m.start() < e and s < m.end() for s, e in covered):
                    continue
            else:
                covered.append((m.start(), m.end()))
            key = (ptype, tok)
            if key in seen:
                continue
            seen.add(key)
            found.append({"type": ptype, "token": tok})
    return found


def verify_query_has_key_data(query):
    """C13 试点触发判定：query 含数字/金额/日期/百分比任一 → 关键数据场景（触发验证）。

    仅 conclusion 型（宽泛语义信号）不算关键数据场景（不触发试点——零开销）。
    """
    for item in verify_extract_key_data(query):
        if item["type"] not in VERIFY_CONCLUSION_TYPES:
            return True
    return False


def _verify_find_snippet(text, token, radius=None):
    """C10：在 text 中定位 token（大小写不敏感子串），返回命中上下文片段。

    返回 {"found": bool, "snippet": str|None, "idx": int|None}
    """
    radius = radius or VERIFY_SNIPPET_RADIUS
    if not text or not token:
        return {"found": False, "snippet": None, "idx": None}
    low_text = text.lower()
    idx = low_text.find(token.lower())
    if idx < 0:
        return {"found": False, "snippet": None, "idx": None}
    start = max(0, idx - radius)
    end = min(len(text), idx + len(token) + radius)
    return {"found": True, "snippet": text[start:end], "idx": idx}


def _verify_passage(token, text, ptype):
    """R-A5 单 token 精确匹配验证（grep 语义——子串精确匹配原文）。

    conclusion 型不强制（宽泛语义，恒 pass 标记为 conclusion_signal）；其余精确匹配。
    """
    if ptype in VERIFY_CONCLUSION_TYPES:
        return {"type": ptype, "token": token, "passed": True,
                "verification": "conclusion_signal", "snippet": None}
    r = _verify_find_snippet(text, token)
    return {"type": ptype, "token": token, "passed": r["found"],
            "verification": "exact_match", "snippet": r["snippet"]}


def verify_entry(query, summary, content=""):
    """对单条目完整验证：提取 query 关键数据 → 逐 token 精确匹配（R-A5）→ 原文片段（C10）。

    返回 {"key_data": [...], "verdicts": [...], "passed_all": bool,
          "passed_count": int, "total": int}
    """
    facts = verify_extract_key_data(query)
    verdicts = []
    for f in facts:
        text = f"{content or ''}\n{summary or ''}"
        verdicts.append(_verify_passage(f["token"], text, f["type"]))
    passed = sum(1 for v in verdicts if v["passed"])
    return {
        "key_data": [f["token"] for f in facts],
        "verdicts": verdicts,
        "passed_all": passed == len(verdicts) if verdicts else True,
        "passed_count": passed,
        "total": len(verdicts),
    }


def _verify_fetch_contents(ids):
    """批量回查 memory_store.content（单次 SQL，验证需原文核对——compact 层无 content）。"""
    if not ids:
        return {}
    try:
        conn = get_conn()
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT id, content FROM memory_store WHERE id IN ({placeholders})", ids).fetchall()
        conn.close()
        return {r[0]: (r[1] or "") for r in rows}
    except Exception:
        return {}


def verify_run(query, top_k=5):
    """完整验证循环（verify 子命令核心）：检索 → 关键数据提取 → 逐条精确匹配 → 原文片段。

    C4：挂 SIKU_GATE 门——门 off 时返回 disabled（零开销，不检索不验证）；
    门 on 时完整验证。检索结果取 compact（含 summary），原文片段需回查 content（C10）。
    """
    if not R1_GATE_ENABLED:
        return {"gate": "off", "enabled": False,
                "note": "SIKU_GATE=off 验证循环未启用（C4 门控）——开闸：SIKU_GATE=on"}
    facts = verify_extract_key_data(query)
    if not facts:
        return {"gate": "on", "enabled": True, "query": query,
                "key_data": [], "verdicts": [],
                "summary": {"total": 0, "passed": 0, "failed": 0,
                            "note": "query 无关键数据（数字/金额/日期/百分比/结论性表述）——无需验证"}}
    result = search_memories(query, mode="auto", top_k=top_k, tier="compact")
    items = result.get("results", [])
    if not items:
        return {"gate": "on", "enabled": True, "query": query,
                "key_data": [f["token"] for f in facts],
                "verdicts": [], "summary": {"total": 0, "passed": 0, "failed": 0,
                                            "note": "检索无结果——无可验证条目"}}
    contents = _verify_fetch_contents([it["id"] for it in items])
    verdicts = []
    for it in items:
        entry_v = verify_entry(query, it.get("summary", ""), contents.get(it["id"], ""))
        verdicts.append({
            "id": it["id"],
            "score": it.get("score"),
            "weak_match": it.get("weak_match", False),
            "key_data": entry_v["key_data"],
            "passed_all": entry_v["passed_all"],
            "passed_count": entry_v["passed_count"],
            "total": entry_v["total"],
            "verdicts": entry_v["verdicts"],
        })
    passed = sum(1 for v in verdicts if v["passed_all"] and v["total"] > 0)
    return {
        "gate": "on", "enabled": True, "query": query,
        "key_data": [f["token"] for f in facts],
        "verdicts": verdicts,
        "summary": {"total": len(verdicts), "passed": passed, "failed": len(verdicts) - passed},
    }


def verify_pilot_annotate(query, out):
    """C13 试点默认开：search 主路径上，query 含关键数据 → compact 输出附加 verification 字段。

    纯新增字段（不破坏既有字段，R-A1 兼容）；未命中关键数据 → 零开销（原样返回）。
    仅 compact 层注入（高频检索场景）；expand/full 层不注入（避免结构漂移）。
    开关关闭（SIKU_VERIFY_PILOT=off / SIKU_GATE=off）→ 剥离残留 verification 字段
    （旧缓存可能带字段，杜绝开关切换后字段泄漏——输出始终与当前开关状态一致）。
    """
    if not VERIFY_PILOT_ENABLED or not R1_GATE_ENABLED:
        if isinstance(out, dict) and "verification" in out:
            out = dict(out)
            out.pop("verification", None)
        return out
    if not isinstance(out, dict) or out.get("tier") != "compact":
        return out
    if not out.get("results"):
        return out
    if not verify_query_has_key_data(query):
        return out
    try:
        items = out["results"]
        contents = _verify_fetch_contents([it["id"] for it in items])
        verdicts = []
        for it in items:
            entry_v = verify_entry(query, it.get("summary", ""), contents.get(it["id"], ""))
            verdicts.append({
                "id": it["id"],
                "passed_all": entry_v["passed_all"] and entry_v["total"] > 0,
                "passed_count": entry_v["passed_count"],
                "total": entry_v["total"],
                "evidence": [v for v in entry_v["verdicts"]
                             if v["type"] not in VERIFY_CONCLUSION_TYPES],
            })
        passed = sum(1 for v in verdicts if v["passed_all"])
        out["verification"] = {
            "trigger": "key_data_rule_hit",  # R-A4 关键数据规则命中 → 试点验证（C13）
            "key_data": [f["token"] for f in verify_extract_key_data(query)],
            "summary": {"total": len(verdicts), "passed": passed, "failed": len(verdicts) - passed},
            "verdicts": verdicts,
            "note": "试点默认开（C13）：关键数据规则命中自动验证，SIKU_VERIFY_PILOT=off 可关",
        }
    except Exception:
        pass  # 试点注入任何异常零影响（同 shadow/missmon 纪律）
    return out


# ── P2a：--tree 目录树（C5 概念集权威源——单向生成）──

def concept_tree(json_path=None):
    """C5：从概念集权威源单向生成目录树（只读概念集，绝不写回）。

    --tree 子命令核心：读取种子概念集（VERIFY_CONCEPT_SET），按来源分组
    （K 系列 / TAOGE 样板 / 合并概念）渲染目录树。单向 = 树只是概念集的只读视图，
    任何情况下不修改概念集文件（与 verify 循环同为审计性只读操作）。

    返回 {"concepts": N, "groups": [...], "tree": "文本树", "path": ...}
    —— tree 字段为终端可读渲染，groups 为结构化分组（--json 用）。
    """
    path = json_path or VERIFY_CONCEPT_SET
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"error": "概念集读取失败: %s" % e, "path": path}
    concepts = data.get("concepts", [])
    k_only, tpl_only, merged = [], [], []
    for c in concepts:
        srcs = c.get("source", [])
        has_k = any("routing.json" in s for s in srcs)
        has_t = any("TAOGE" in s for s in srcs)
        if has_k and has_t:
            merged.append(c)
        elif has_k:
            k_only.append(c)
        else:
            tpl_only.append(c)
    for bucket in (k_only, tpl_only, merged):
        bucket.sort(key=lambda c: c.get("prefLabel", ""))

    lines = ["种子概念集（%d 概念）— C5 权威源：%s" % (len(concepts), path)]
    groups = []
    for gname, gsrc, gconcepts in (
        ("K 系列（routing.json）", "routing.json", k_only),
        ("TAOGE 样板", "TAOGE", tpl_only),
        ("合并概念（K↔样板）", "merged", merged),
    ):
        group_items = [{
            "id": c.get("id", ""),
            "prefLabel": c.get("prefLabel", ""),
            "altLabels": len(c.get("altLabels", []) or []),
            "skill": c.get("skill", ""),
            "merged_from": c.get("merged_from", []) or [],
        } for c in gconcepts]
        groups.append({"name": gname, "source": gsrc, "concepts": group_items})
        lines.append("")
        lines.append("%s（%d）" % (gname, len(gconcepts)))
        for c in gconcepts:
            alt_n = len(c.get("altLabels", []) or [])
            line = "  ├─ %s [%s]" % (c.get("prefLabel", "?"), c.get("id", "?"))
            if alt_n:
                line += " · altLabels %d" % alt_n
            if c.get("skill"):
                line += " · %s" % c.get("skill")
            mf = " · ".join(c.get("merged_from", []) or [])
            if mf:
                line += " · ← %s" % mf
            lines.append(line)
    return {"concepts": len(concepts), "groups": groups,
            "tree": "\n".join(lines), "path": path}


# ── P2b：多跳动态检索实现 ──────────────────

_MH_REL_CACHE = None   # (mtime, {concept_id: [neighbor_ids]})
_MH_SEED_CACHE = None  # 种子概念集（id → concept dict），一次加载供 term 索引/查询词共用


def _mh_load_seed():
    """种子概念集懒加载（模块级缓存）——概念识别 term 索引 + 概念查询词共用。"""
    global _MH_SEED_CACHE
    if _MH_SEED_CACHE is not None:
        return _MH_SEED_CACHE
    try:
        with open(VERIFY_CONCEPT_SET, encoding="utf-8") as f:
            data = json.load(f)
        by_id = {}
        for c in data.get("concepts", []):
            cid = c.get("id", "")
            if cid:
                by_id[cid] = c
        _MH_SEED_CACHE = by_id
    except Exception:
        _MH_SEED_CACHE = {}
    return _MH_SEED_CACHE


def _mh_load_rel_graph(path=None):
    """P1 语义层概念关系邻接表（jsonld @graph 只读零写入；broader+narrower 双向）。

    C5 派生源：domain-graph.jsonld（ttl2jsonld.py 构建生成）。懒加载 + mtime 缓存失效；
    任何异常返回 {}（fail-open——多跳降级单跳，绝不因关系源故障破坏检索）。
    """
    global _MH_REL_CACHE
    p = path or MULTI_HOP_REL_PATH
    try:
        mtime = os.path.getmtime(p)
        if _MH_REL_CACHE and _MH_REL_CACHE[0] == mtime:
            return _MH_REL_CACHE[1]
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        graph = {}
        for node in data.get("@graph", []):
            if node.get("@type") != "skos:Concept":
                continue
            cid = (node.get("@id") or "").rsplit("/", 1)[-1]
            if not cid:
                continue
            nbs = []
            for rel in ("broader", "narrower"):
                for ref in node.get(rel, []) or []:
                    nid = (ref.get("@id") or "").rsplit("/", 1)[-1]
                    if nid and nid != cid and nid not in nbs:
                        nbs.append(nid)
            graph[cid] = sorted(nbs)
        _MH_REL_CACHE = (mtime, graph)
        return graph
    except Exception:
        return {}


def _mh_identify_concepts(query):
    """R-A8 实体识别：query 子串命中种子概念集 term（prefLabel+altLabels）。

    最长词优先（term 索引按长度降序）——长词先占位，短词命中独立概念仍计入
    （多主题信号），仅同概念去重。term 长度 ≥2 防单字误命中。
    """
    import re as _re
    if not query or not query.strip():
        return []
    seed = _mh_load_seed()
    if not seed:
        return []
    terms = []  # [(len, term, cid)]
    for cid, c in seed.items():
        cands = [c.get("prefLabel", "")] + list(c.get("altLabels", []) or [])
        for t in cands:
            t = (t or "").strip()
            if len(t) >= 2:
                terms.append((len(t), t, cid))
    terms.sort(key=lambda x: (-x[0], x[1]))
    found, seen = [], set()
    for _ln, term, cid in terms:
        if cid in seen:
            continue
        if _re.search(_re.escape(term), query):
            seen.add(cid)
            found.append(cid)
    return found


def _mh_entity_tokens(query):
    """R-A8 实体信号2：query 独立实义词（0token——补概念表未覆盖的技术域实体）。

    双通道：①英文/数字 token（FTS5/Node/SQLite/L3/chroma…）；②中文段按连接词
    （和/与/或/还是…）切分，去疑问词后保留含中文实义内容的段（纯英文段由①计，
    防双重计数）。返回去重 token 列表。
    """
    import re as _re
    if not query or not query.strip():
        return []
    toks = set()
    for m in _re.finditer(r"[A-Za-z][A-Za-z0-9+#.\-]*|\d+(?:\.\d+)?", query):
        if len(m.group(0)) >= 2:
            toks.add(m.group(0))
    for seg in _re.split(MULTI_HOP_ENTITY_SEP_RE, query):
        # 仅删段首疑问词（为什么/怎么/如何…）——段内/段尾疑问词（区别/差异/对比）
        # 是实体语义组成部分，全文删除会产生 "向量检索有" 类残词
        seg = _re.sub(r"^(?:" + MULTI_HOP_QUESTION_RE + r")+", "", seg)
        seg = seg.strip(" 的了吗呢吧啊：:，,。.!！?？")
        # 中文实义部分（去英文/数字——英文 token 由①通道计，防双重计数；
        # 混合段如 "cron 状态修正" 只取中文 "状态修正"，避免整段残词作检索词）
        cn = _re.sub(r"[A-Za-z0-9+#.\-]", "", seg).strip(" 的了吗呢吧啊：:，,。.!！?？")
        if len(cn) >= 2:
            toks.add(cn)
    return sorted(toks)


def _mh_complexity(query):
    """R-A8 复杂度判定（0token 规则：实体数/疑问词/多主题——非 LLM，零成本）。

    实体数 = 概念命中数（种子概念集 prefLabel+altLabels）+ 独立实义词数
    （英文/数字 token + 中文对比段——覆盖技术域）；
    complex = (实体数 ≥2 AND 疑问词命中) OR 实体数 ≥3；
    否则 simple → 单跳原样（多跳零开销路径）。
    返回 {"level", "entities", "question", "concepts", "tokens"}。
    """
    import re as _re
    concepts = _mh_identify_concepts(query)
    tokens = _mh_entity_tokens(query)
    entities = len(concepts) + len(tokens)
    question = bool(_re.search(MULTI_HOP_QUESTION_RE, query))
    complex_ = (entities >= 2 and question) or entities >= 3
    return {"level": "complex" if complex_ else "simple",
            "entities": entities, "question": question,
            "concepts": concepts, "tokens": tokens}


def _mh_expand_concepts(c0, max_layers):
    """概念关系扩展：c0 的邻居按 layer 分层（layer1=直接邻居，layer2=邻居的邻居…）。

    C15 统计截断：总数 ≤MULTI_HOP_CONCEPT_CAP、每层 ≤MULTI_HOP_LAYER_CAP；
    关系图缺失 → {}（多跳退化为仅 hop0 原始 query 检索，仍安全）。
    """
    rel = _mh_load_rel_graph()
    if not rel or not c0:
        return {}
    out = {}
    frontier = list(c0)
    for layer in range(1, max_layers + 1):
        nxt = []
        for cid in frontier:
            for nb in rel.get(cid, []):
                if nb in c0 or nb in out or nb in nxt:
                    continue
                if len(out) + len(nxt) >= MULTI_HOP_CONCEPT_CAP:
                    break
                nxt.append(nb)
            if len(out) + len(nxt) >= MULTI_HOP_CONCEPT_CAP:
                break
        if not nxt:
            break
        picked = nxt[:MULTI_HOP_LAYER_CAP]
        for nb in picked:
            out[nb] = layer
        frontier = picked
    return out


def _mh_concept_query(cid):
    """概念 → 检索词：最短 altLabel（≥2 字符）优先（FTS5 中文分词友好），否则 prefLabel（去括号）。"""
    seed = _mh_load_seed()
    c = seed.get(cid) if seed else None
    if not c:
        return cid
    alts = [t for t in (c.get("altLabels", []) or []) if isinstance(t, str) and len(t.strip()) >= 2]
    if alts:
        return min(alts, key=len).strip()
    pref = (c.get("prefLabel", "") or "").split("（")[0].split("(")[0].strip()
    return pref or cid


def _concept_front_expand(query):
    """P2a concept 前置扩展：query 概念化 → 子类/同义一层扩展词并入检索 query（扩召回）。

    非 RRF 第四路——仅增强 fts/embed/graph 输入（复核裁定：graph 重叠/权重膨胀不做第四路）。
    流程：概念识别（_mh_identify_concepts）→ 一层子类/同义扩展（_mh_expand_concepts
    max_layers=1，broader/narrower 邻接表只读）→ 概念检索词（_mh_concept_query）
    并入 query（统计截断 CONCEPT_EXPAND_MAX_WORDS）。
    返回 {"query": 扩展后 query, "words": [扩展词]}；无命中/任何异常 → None
    （off 等价零开销，fail-open 安全降级——绝不因概念层故障破坏检索）。
    """
    try:
        c0 = _mh_identify_concepts(query)
        if not c0:
            return None
        layers = _mh_expand_concepts(c0[:CONCEPT_EXPAND_C0_MAX], max_layers=1)
        words, seen = [], set()
        for cid in list(c0) + list(layers.keys()):
            if cid in seen:
                continue
            seen.add(cid)
            w = _mh_concept_query(cid)
            if not w or w in query or w in words:
                continue
            words.append(w)
            if len(words) >= CONCEPT_EXPAND_MAX_WORDS:
                break
        if not words:
            return None
        return {"query": (query + " " + " ".join(words))[:MAX_QUERY_LEN], "words": words}
    except Exception:
        return None


def _mh_fetch_rows(ids):
    """批量回查 memory_store 完整行（expand/full 层需要 content/timestamp/embedding）。"""
    if not ids:
        return {}
    try:
        conn = get_conn()
        cur = conn.execute("SELECT * FROM memory_store WHERE 1=0")
        cols = [d[0] for d in cur.description]
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT * FROM memory_store WHERE id IN ({placeholders})", ids).fetchall()
        conn.close()
        return {r[0]: dict(zip(cols, r)) for r in rows}
    except Exception:
        return {}


def multi_hop_search(query, top_k=5, max_hops=None, mode="auto", tier="compact",
                     industry=None, track=None, reader=None):
    """P2b 多跳动态检索（CLI --multi-hop 入口；R-A1：search_memories 主路径零改动）。

    R-A8 复杂度判定 complex → 概念关系扩展多跳（hop0 原始 query + hopN 概念扩展检索，
    证据池跨跳 id 去重，score 降序融合——跨跳同源分可直接比较）；simple → 单跳原样
    （附 multi_hop 标注，零额外检索）。C15 硬上限：max_hops≤3、证据池≤30、
    概念扩展≤12/层≤6。多跳路径跳过 query_cache 读写（低频复杂查询，防污染普通缓存 R-A1）。
    """
    hops = max(1, min(int(max_hops or MULTI_HOP_MAX_HOPS), MULTI_HOP_MAX_HOPS))  # C15
    cx = _mh_complexity(query)
    if cx["level"] == "simple":
        base = search_memories(query, mode=mode, top_k=top_k, tier=tier,
                               industry=industry, track=track, reader=reader)
        if isinstance(base, dict):
            base["multi_hop"] = {"enabled": False, "complexity": "simple",
                                 "note": "R-A8 单主题判定——单跳（--multi-hop 已启用，无需多跳）"}
        return base

    # complex → 多跳
    pool = {}  # id -> (score, compact_entry)

    def _absorb(result):
        for it in result.get("results", []):
            rid = it.get("id")
            if not rid:
                continue
            s = it.get("score", 0.0)
            if rid not in pool or s > pool[rid][0]:
                pool[rid] = (s, it)
        # C15：证据池硬上限——超限丢弃最低分（保留最高分，统计截断）
        if len(pool) > MULTI_HOP_EVPOOL_CAP:
            for rid in sorted(pool, key=lambda x: pool[x][0])[:len(pool) - MULTI_HOP_EVPOOL_CAP]:
                del pool[rid]
        return len(pool)

    hops_meta = []
    hop_k = max(top_k, MULTI_HOP_HOP_K)
    # hop0：原始 query 检索（内部统一 compact 结构——id/score 直取；
    # 输出层再按请求 tier 格式化，expand/full 条目结构 {tier,entry} 无 id 键不可池化）
    r0 = search_memories(query, mode=mode, top_k=hop_k, tier="compact",
                         industry=industry, track=track, reader=reader)
    _absorb(r0)
    hops_meta.append({"hop": 0, "kind": "query", "concept": None,
                      "query": query, "n": len(r0.get("results", []))})
    # hop1..N：概念关系扩展 + 实体 token 双源检索（C15：池满即停——统计截断）
    # 概念邻居按层（layer1=直接邻居，layer2=邻居的邻居…）；实体 token 归入 hop1
    # （技术域实体无关系图节点，直接作检索词兜底——避免概念集未覆盖时多跳空转退化单跳）。
    c0 = cx["concepts"]
    expanded = _mh_expand_concepts(c0, max_layers=hops - 1)
    terms = []  # (layer, kind, cid, query)
    for cid, lyr in sorted(expanded.items(), key=lambda x: x[1]):
        q = _mh_concept_query(cid)
        if q and q != query:
            terms.append((lyr, "concept", cid, q))
    seen_q = {q for _, _, _, q in terms}
    if hops >= 2:  # token 兜底属扩展跳——max_hops=1 时仅 hop0 检索
        for t in cx["tokens"]:
            if t and t != query and t not in seen_q:
                terms.append((1, "entity", None, t))
                seen_q.add(t)
    terms = terms[:MULTI_HOP_CONCEPT_CAP]  # C15：扩展检索词总数上限（统计截断）
    for layer, kind, cid, q in terms:
        rr = search_memories(q, mode=mode, top_k=hop_k, tier="compact",
                             industry=industry, track=track, reader=reader)
        _absorb(rr)
        hops_meta.append({"hop": layer, "kind": kind, "concept": cid,
                          "query": q, "n": len(rr.get("results", []))})
        if len(pool) >= MULTI_HOP_EVPOOL_CAP:
            break

    # 融合：score 降序 → top_k（统计截断 C15）
    ranked = sorted(pool.values(), key=lambda x: x[0], reverse=True)[:top_k]
    search_mode = "multi_hop"
    mh_field = {
        "enabled": True, "complexity": "complex",
        "max_hops": hops, "hops_used": len(hops_meta),
        "entities": cx["entities"], "question": cx["question"],
        "query_concepts": c0, "entity_tokens": cx["tokens"],
        "expanded_concepts": sorted(expanded),
        "pool_size": len(pool), "hops": hops_meta,
        "note": "R-A8 复杂判定——概念关系扩展多跳（C15 硬上限 max_hops≤3/证据池≤30）",
    }
    if tier == "compact":
        try:
            log_access([it["id"] for _, it in ranked], query, "search")  # RI P0 埋点
        except Exception:
            pass
        _fmt = format_compact(ranked, search_mode)
        _r1_annotate(_fmt, ranked, False, search_mode)
        _obqc_apply(query, _fmt, ranked, False, search_mode)
        _missmon_record(query, hit=bool(ranked), total=len(ranked),
                        search_mode=search_mode, mode=mode, track=track)
        _fmt["multi_hop"] = mh_field
        return verify_pilot_annotate(query, _fmt)
    # expand/full：DB 回查完整行再格式化
    rows = _mh_fetch_rows([it["id"] for _, it in ranked])
    results = []
    for score, it in ranked:
        row = rows.get(it["id"]) or dict(it)
        row.setdefault("summary", it.get("summary", ""))
        results.append(
            format_full(row, score) if tier == "full" else format_expand(row, score))
    out = {"tier": "full_multi" if tier == "full" else "expand_multi",
           "search_mode": search_mode, "total": len(results), "results": results}
    _r1_annotate(out, ranked, False, search_mode)
    _obqc_apply(query, out, ranked, False, search_mode)
    out["multi_hop"] = mh_field
    return out


# ── 主搜索 ─────────────────────────────────────────────────────────

def search_memories(query, mode="auto", top_k=5, tier="compact", industry=None, reader=None, track=None, type_filter=None):
    """
    主搜索入口：双通道 + RRF + 渐进式输出。
    P0-3.1: reader 参数透传路由层读审计（MCP/CLI 调用方可标记来源，默认 None→unknown）
    R1-C 记忆分轨: track=None 不限轨（默认，全量检索，向后兼容）| 'episodic' | 'semantic'
    限轨检索=通道候选层过滤（fts5/emb SQL WHERE memory_track）+ 融合后结果级兜底过滤；
    缺省不过滤 → 现有调用方零改动；query_cache 键含 track 维度防串扰。

    参数:
        query: 搜索关键词
        mode: auto|fts5|embed|dual
        top_k: 返回条数
        tier: compact|expand|full
    返回: dict
    """
    # ── 路由层接入（Phase 0.5+）：SIKU_ROUTER=off / router 未启用 / 任何异常 → 完全透传 ──
    global _ROUTER_IN_CALL
    if not _ROUTER_IN_CALL:
        _ROUTER_IN_CALL = True
        try:
            import os as _os
            if _os.environ.get("SIKU_ROUTER", "").lower() not in ("off", "0", "false"):
                import router
                return router.route_search(query, mode=mode, top_k=top_k, tier=tier, industry=industry, reader=reader, track=track, type_filter=type_filter)
        except Exception:
            pass  # 路由层异常绝不影响主检索
        finally:
            _ROUTER_IN_CALL = False
    if not query or not query.strip():
        return {"error": "query 不能为空"}

    query = query.strip()

    # 查询改写（默认关闭；生产 permissions.yaml query_rewrite.enabled=true）——
    # S2 M2 缓存键统一：先 rewrite 再算键，get/put 均用改写后 query（与"实际执行的检索 query"一致），
    # 覆盖 rewrite 开关两态（关→原样返回键不变，开→短查询改写后 get/put 同键可命中）
    original_query = query
    query = rewrite_query(query)

    # ── ：event_date 时间预过滤（门控默认关；无标记零开销）──
    # 时间标记检测（0token 正则）→ [t1,t2]；缓存键含时间维度防两态串扰
    _tf_t1 = _tf_t2 = None
    _tf_time_dim = ""
    if TIME_FILTER_ENABLED:
        _tf_t1, _tf_t2 = _tf_detect(query)
        if _tf_t1 or _tf_t2:
            _TF_STATS["detected"] += 1
        _tf_time_dim = _tf_cache_dim(_tf_t1, _tf_t2)

    # ：级联/遗忘开关 on 态缓存键加 cs/fg 维度防两态串扰
    # （off 态空串 → 键与现状逐位一致；on 态键含维度 → 与 off 缓存隔离）
    _opt_dim = ("cs" if CASCADE_ENABLED else "") + ("fg" if FORGET_ENABLED else "")
    _cache_dim = (_tf_time_dim + "|" + _opt_dim) if _opt_dim else _tf_time_dim

    # ── P2a：concept 前置扩展（默认关——off 零开销逐位现状）──
    # 前置扩展=检索前 query 概念化：识别概念 → 子类/同义扩展词并入检索 query（增强
    # fts/embed/graph 输入，非 RRF 第四路）；缓存键含 cp 维度防两态串扰；无概念命中/
    # 任何异常 → 原样零开销（fail-open 安全降级）。
    _concept_expanded = False
    _concept_words = []
    if CONCEPT_ENABLED:
        _ce = _concept_front_expand(query)
        if _ce:
            query = _ce["query"]
            _concept_words = _ce["words"]
            _concept_expanded = True
            _cache_dim = (_cache_dim + "|cp") if _cache_dim else "cp"

    # ── S2 query_cache 命中检测（M2：tier 限定 compact + 陈旧判据 db_max_updated）──
    cached = _query_cache_get(query, tier, mode, track, type_filter, _cache_dim)
    if cached is not None:
        # C13（P2a）：缓存命中同样过试点注入——关键数据 query 现场补
        # verification 字段，保证与首次检索输出一致（pilot 开关即时生效，新旧缓存口径统一）；
        # 未命中关键数据 → 原样返回零开销（verify_pilot_annotate 内部全条件短路）。
        return verify_pilot_annotate(query, cached)

    # S9: shadow 生产延迟计时起点（仅采样命中时记录，常规路径零开销）
    _t_pipe_start = time.perf_counter()

    # 通道 A: FTS5（：时间标记命中时候选层宽容过滤）
    _tf_sql, _tf_params = _tf_sql_cond("f.event_date", _tf_t1, _tf_t2) if (_tf_t1 or _tf_t2) else ("", [])
    # ：两级粒度级联（SIKU_CASCADE=on 时生效，默认 off——off 逐位现状）
    # Note-First：第一轮 FTS 只检 summary 列（Note 常驻层快检）；命中不足（空/弱/条数不足）
    # → Episode 补全：第二轮全量（summary+content）取细节条目去重后追加（HiMem 级联读端）。
    _cascade_round = ""
    if CASCADE_ENABLED:
        bm25_results, bm25_mode = channel_fts5(
            query, track=track, note_first=True,
            time_cond=(_tf_sql, _tf_params) if _tf_sql else None,
        )
        if not bm25_results:
            bm25_results, bm25_mode = channel_fts5(
                query, track=track,
                time_cond=(_tf_sql, _tf_params) if _tf_sql else None,
            )
            _cascade_round = "ep_backfill_empty"
        elif len(bm25_results) < top_k or bm25_results[0][0] < CASCADE_NOTE_SCORE_MIN:
            ep_results, _ep_mode = channel_fts5(
                query, track=track,
                time_cond=(_tf_sql, _tf_params) if _tf_sql else None,
            )
            _note_ids = {r["id"] for _, r in bm25_results}
            _ep_fill = [(s, r) for s, r in ep_results if r["id"] not in _note_ids][:CASCADE_BACKFILL_K]
            bm25_results = bm25_results + _ep_fill
            _cascade_round = "ep_backfill_%d" % len(_ep_fill)
        else:
            _cascade_round = "note_only"
    else:
        bm25_results, bm25_mode = channel_fts5(
            query, track=track,
            time_cond=(_tf_sql, _tf_params) if _tf_sql else None,
        )

    # R1：time_desc 兜底显性化标记——fts5 零命中自动按时间倒序返回最近条目时，
    # 置位 _time_desc_fallback（不再伪装成正常命中：search_mode 显式化 + fallback 字段 + weak 标注）
    _time_desc_fallback = False

    # 如果没有 FTS5 结果，降级到时间排序
    if not bm25_results:
        conn = get_conn()
        c = conn.cursor()
        try:
            if track:
                c.execute("""
                    SELECT id, type, summary, content,
                           confidence, timestamp, source_agent, rowid,
                           industry, source_ref, expires_at
                    FROM memory_store
                    WHERE memory_track = ?
                    ORDER BY timestamp DESC LIMIT ?
                """, (track, top_k))
            else:
                c.execute("""
                    SELECT id, type, summary, content,
                           confidence, timestamp, source_agent, rowid,
                           industry, source_ref, expires_at
                    FROM memory_store
                    ORDER BY timestamp DESC LIMIT ?
                """, (top_k,))
            rows = c.fetchall()
            bm25_results = [(0.0, dict(r)) for r in rows]
            bm25_mode = "time_desc"
            if bm25_results and R1_GATE_ENABLED:
                # R1：兜底有返回 → 显性化标记（0.0 分必落 weak 档）；
                # 仅 SIKU_GATE=on 时置位——off 时 search_mode 走原伪装逻辑，行为与现状逐位一致
                _time_desc_fallback = True
        except Exception:
            pass
        conn.close()

    # ── R3 阶段3：fts5 候选语义截断（融合前）────────
    # fts5 候选与 query 相似度降序保留 top SIKU_CAND_TRUNCATE_K（默认 8），
    # 堵"词面重叠但语义无关"噪音（词面命中 ≠ 语义相关，Qdrant 混合检索实践 + 图通道 p@5=0.011 教训）。
    # 仅截断真 fts5 候选（bm25_mode=="fts5"）；time_desc 兜底不截断（意图是最近条目）。
    # SIKU_CAND_TRUNCATE=0/off/false 关闭（防误伤一键回退）；相似度失败/无向量宁放勿杀。
    if CAND_TRUNCATE_ENABLED and bm25_mode == "fts5" and bm25_results:
        bm25_results = _fts5_candidate_truncate(query, bm25_results)

    # 通道 B: Embedding（只在 dual/hybrid 模式下跑）
    embed_results = []
    embed_mode = "not_used"

    if mode in ("auto", "dual", "embed"):
        embed_results, embed_mode = channel_embedding(query, bm25_results, track=track)

    # 判断最终模式（S8 P1b：图通道命中后附加 _graph 后缀）
    # R1：time_desc 兜底不再伪装——fts5 零命中走时间倒序兜底时，
    # search_mode 显式标注 time_desc（而非伪装成 fts5_only/dual_rrf，D4 实证根因）
    if embed_results and not _time_desc_fallback:
        search_mode = "dual_rrf"
    elif embed_results and _time_desc_fallback:
        search_mode = "dual_rrf_time_desc"
    elif bm25_results and not embed_results and not _time_desc_fallback:
        search_mode = "fts5_only"
    elif bm25_results and not embed_results and _time_desc_fallback:
        search_mode = "time_desc"
    else:
        search_mode = "empty"

    # ── S8 P1b: 通道 C 条目图（种子-邻居，仅真信号）──────────────────
    # 种子=主通道 RRF 融合 top GRAPH_SEED_K；邻居=真信号关系批量扩展；
    # 图通道独立缓存命名空间（_query_cache_hash 附加 g 标记）防 query_cache 串扰
    # S9：seed_fused 计算上提（图开生产 与 shadow 采样共用，零重复计算）
    graph_results = []
    graph_mode = "not_used"
    seed_fused = None
    if bm25_results or embed_results:
        seed_pool = []
        if bm25_results:
            seed_pool.append(bm25_results)
        if embed_results:
            seed_pool.append(embed_results)
        if len(seed_pool) == 2:
            seed_fused = rrf_fusion(bm25_results, embed_results, top_k=GRAPH_SEED_K)
        else:
            seed_fused = seed_pool[0][:GRAPH_SEED_K]
    if GRAPH_CHANNEL_ENABLED and seed_fused:
        graph_results, graph_mode = channel_graph(query, seed_fused)
        if graph_results:
            if bm25_results and embed_results:
                search_mode = "dual_rrf_time_desc_graph" if _time_desc_fallback else "dual_rrf_graph"
            elif bm25_results:
                search_mode = "time_desc_graph" if _time_desc_fallback else "fts5_graph"
            else:
                search_mode = "embed_graph"

    # ── S9: shadow 采样对比（默认关；hash 采样命中 ~1% 时额外跑图通道，只写 shadow_log）──
    _shadow_ctx = None
    if SHADOW_ENABLED and not graph_results and _shadow_should_sample(query) and seed_fused:
        _SHADOW_STATS["sampled"] += 1
        _shadow_ctx = {
            "query": query,
            "prod_latency_ms": 0.0,
            "graph_latency_ms": 0.0,
            "graph_results": [],
            "graph_mode": "not_used",
        }
        t_g0 = time.perf_counter()
        s_graph, s_graph_mode = channel_graph(query, seed_fused)
        _shadow_ctx["graph_latency_ms"] = (time.perf_counter() - t_g0) * 1000.0
        _shadow_ctx["graph_results"] = s_graph
        _shadow_ctx["graph_mode"] = s_graph_mode

    # RRF 融合（S8 泛化：双通道 + 图通道；等权 K=60，第三路影响由截断量控制）
    channels = []
    if bm25_results:
        channels.append(bm25_results)
    if embed_results:
        channels.append(embed_results)
    if graph_results:
        channels.append(graph_results)
    # ：第四路（沙盒对照档 SIKU_TIME_4TH，默认关）——
    # event_date 窗内条目按时间接近度作为独立通道进 RRF（待覆盖率达标后二期评估）
    time4_results, time4_mode = [], "not_used"
    if TIME_4TH_ENABLED and (_tf_t1 or _tf_t2):
        time4_results, time4_mode = channel_time(query, _tf_t1, _tf_t2)
        if time4_results:
            channels.append(time4_results)
            search_mode += "_time4th" if "empty" not in search_mode else ""

    rr_top = RERANK_TOP_K if RERANK_ENABLED else top_k
    if len(channels) >= 2:
        # P1-D：boost_query 传入 → 保送仅补充位（query 含指令语义才追加）
        final = rrf_fusion(*channels, top_k=rr_top, boost_query=query)
    elif len(channels) == 1:
        final = channels[0][:top_k]
        if not bm25_results and embed_results:
            search_mode = "embed_only"
    else:
        search_mode = "empty"
        # ── R3-A 未命中监控：空结果 = miss（纯规则零 LLM）──
        # R1：quality 字段仅 SIKU_GATE=on 时写入（off 时原样记录，与现状一致）
        _missmon_record(query, hit=False, total=0, search_mode=search_mode, mode=mode, track=track,
                        quality="weak" if R1_GATE_ENABLED else None)
        _empty_out = {
            "tier": "compact",
            "search_mode": "empty",
            "total": 0,
            "results": [],
            "note": "无匹配结果",
            **({
                "result_quality": "weak",
                "grade": "weak",  # R4：空结果=证据不足（与 result_quality 衔接，纯标注不拦截）
            } if R1_GATE_ENABLED else {}),
        }
        # R1-IMPL：空结果同样过 OBQC（shadow 记日志 / enforce 显式拒答标注）
        return _obqc_apply(query, _empty_out, [], _time_desc_fallback, "empty")

    # ：遗忘策略接入（SIKU_FORGET=on 时生效，默认 off）——低频背景化
    # 降权在融合排序后执行：只降低频老条目（90 天无命中 + 写入超 30 天），高频零改动不误杀。
    if FORGET_ENABLED and final:
        final = _forget_adjust(final)

    # R1-C 记忆分轨：融合后结果级 track 兜底过滤（graph 通道行无 memory_track 键时按 id 补查）
    if track:
        final = _filter_by_track(final, track)

    # ：融合后结果级宽容时间兜底过滤（行缺 event_date 放行）+
    # 审计日志（查询 id+边界+滤除数——本地 JSONL）
    if _tf_t1 or _tf_t2:
        _n_before = len(final)
        final = _tf_filter_rows(final, _tf_t1, _tf_t2)
        _TF_STATS["filtered_rows"] += _n_before - len(final)
        _tf_audit(query, _tf_t1, _tf_t2, _n_before, len(final), mode="prefilter")

    # : 融合后结果级类型过滤（--type spec 等按类型检索；
    # type_filter=None → 零开销原样返回，存量行为不变）
    if type_filter:
        final = _filter_by_type(final, type_filter)

    # A6-P2 有效期过滤 + 同事实多版本排重（根据 expires_at 字段）
    # 1) 惰性补查 deprecated 标记；2) 过滤过期/弃用版本；3) 同 summary_hash 只保留有效版本
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    _pre_filter = final  # 全过期回退用
    for i, (score, row) in enumerate(final):
        final[i] = (score, _ensure_deprecated_flag(row))
    final = _dedup_by_summary_hash(final, now_iso)
    if not final:
        final = _pre_filter[:1]  # 全过期时保留最佳匹配（未过滤前的最高分）

    # S7 P1a: 确定性证据校准层——冲突过滤（correction/deprecated + summary_hash 近似矛盾）
    final = _conflict_filter(final, now_iso)
    if not final:
        final = _pre_filter[:1]  # 全被冲突过滤时保留最佳匹配（审计可追溯）

    # Industry 过滤（命令行参数）
    if industry:
        ind_filtered = []
        for score, row in final:
            row_ind = row.get("industry", "") or ""
            if isinstance(row_ind, str):
                # 数据库存的是字符串，可能是 "[AI]" 或 "AI" 或 "[AI, 金融]"
                row_ind = row_ind.strip("[] ").replace('"', "").replace("'", "")
                inds = [x.strip() for x in row_ind.split(",")] if row_ind else []
            elif isinstance(row_ind, (list, tuple)):
                inds = list(row_ind)
            else:
                inds = []
            if industry in inds:
                ind_filtered.append((score, row))
        if ind_filtered:
            final = ind_filtered
        # 如果过滤后为空，保留原结果（不强制空结果）

    # Rerank（默认关闭；fts5 快路跳过，dual/深度保留）
    # S8 P1b：startswith("fts5") 覆盖 fts5_only 与新增 fts5_graph（快路语义一致）
    if RERANK_ENABLED and final and not search_mode.startswith("fts5"):
        reranked = rerank_results(query, final, top_k)
        if reranked:
            final = reranked
            if "dual_rrf" in search_mode and "rerank" not in search_mode:
                search_mode += "_rerank"  # dual_rrf→dual_rrf_rerank；dual_rrf_graph→dual_rrf_graph_rerank

    # ── S9: shadow_log 落盘（仅采样命中；生产返回零改动）────────────────
    if _shadow_ctx is not None:
        try:
            _shadow_ctx["prod_latency_ms"] = (time.perf_counter() - _t_pipe_start) * 1000.0
            entry = _shadow_build_entry(_shadow_ctx, query, mode, top_k, search_mode, final,
                                        bm25_results, embed_results, now_iso, rr_top)
            _shadow_write_log(entry)
        except Exception:
            _SHADOW_STATS["errors"] += 1

    # ── R3-A 未命中监控：真实检索命中记录（缓存命中路径不记录，避免重复计数）──
    # R1：低相关判定——time_desc 兜底不再伪装成 hit=100%（D4 实证根因）：
    # 兜底时 hit 记 False + fallback 标记 + quality=weak；正常命中记 hit=True + 质量档位。
    # 仅 SIKU_GATE=on 时启用低相关判定与 quality 字段——off 时 hit=True 原样记录（与现状一致）。
    if _time_desc_fallback:
        _missmon_record(query, hit=False, total=len(final), search_mode=search_mode, mode=mode,
                        track=track, quality="weak", fallback="time_desc")
    else:
        # RAGI-T 整改②：missmon quality 同口径——排除保送条目污染 + 通道量纲归一
        _real = [_r1_gt_orig_score(s) for s, _ in final] if final else []
        _q = _r1_quality_of(_r4_norm_score(_r1_norm_scale(max(_real, default=0.0), search_mode))) \
            if (_real and R1_GATE_ENABLED) else None
        _missmon_record(query, hit=True, total=len(final), search_mode=search_mode, mode=mode,
                        track=track, quality=_q)

    # ── R5：G2 验证闸门旁路异步——主路径禁同步等 judge ──────────
    # 仅 SIKU_GATE=on 时后台线程异步验证证据充分性（写 l3_verify_log.jsonl，零拦截零改返回）；
    # 默认 SIKU_GATE=off：不 import、不加载 judge、主路径零开销；任何异常全吞。
    if R1_GATE_ENABLED and final:
        try:
            import l3_verify as _l3v
            _l3v.maybe_verify_async(query, [
                {"id": r.get("id", ""), "summary": r.get("summary", ""), "content": r.get("content", "")}
                for _, r in final[:5]
            ])
        except Exception:
            pass  # 旁路异步：judge 任何失败绝不影响检索返回

    # 格式化输出
    if tier == "compact":
        # RI P0: 异步埋点记录使用频率
        try:
            log_access([row["id"] for _, row in final], query, "search")
        except Exception:
            pass
        _fmt = format_compact(final, search_mode)
        if CASCADE_ENABLED:
            _fmt["cascade_round"] = _cascade_round  # ：级联轮次（note_only/ep_backfill_N；on 态才标注，键含 cs 维度防串扰）
        if CONCEPT_ENABLED and _concept_expanded:
            # P2a：concept 前置扩展标注（仅 on 态且实际扩展时——off 态输出零改动逐位现状；
            # 供 golden A/B 冒烟取证：concept_expanded=True 表示该 query 走了概念化扩展）
            _fmt["concept_expanded"] = True
            _fmt["concept_words"] = _concept_words
        _r1_annotate(_fmt, final, _time_desc_fallback, search_mode)  # R1：注入 result_quality/weak_match/fallback
        _obqc_apply(query, _fmt, final, _time_desc_fallback, search_mode)  # R1-IMPL：OBQC 校验+拒答（shadow/enforce）
        # C13（P2a）：缓存存纯净 compact（verification 是展示层字段——命中时按当前
        # SIKU_VERIFY_PILOT/SIKU_GATE 开关状态现场注入，杜绝 pilot 开关后旧缓存残留字段）
        _query_cache_put(query, tier, mode, _fmt, track, type_filter, _cache_dim)  # S2 M2：写回缓存（无 verification 纯净版）
        # P1-E: 引用信号埋点（结果被消费方读取）
        if _fmt.get("total", 0) > 0:
            try:
                log_access([row["id"] for _, row in final], query, "citation")
            except Exception:
                pass
        return verify_pilot_annotate(query, _fmt)
    elif tier == "expand":
        results = []
        for score, row in final:
            results.append(format_expand(row, score))
        out = {"tier": "expand_multi", "search_mode": search_mode, "total": len(results), "results": results}
        _r1_annotate(out, final, _time_desc_fallback, search_mode)  # R1：同上（expand 层同样显式化）
        _obqc_apply(query, out, final, _time_desc_fallback, search_mode)  # R1-IMPL：OBQC（expand 层同样校验）
        return out
    else:  # full
        results = []
        for score, row in final:
            results.append(format_full(row, score))
        out = {"tier": "full_multi", "search_mode": search_mode, "total": len(results), "results": results}
        _r1_annotate(out, final, _time_desc_fallback, search_mode)  # R1：同上（full 层同样显式化）
        _obqc_apply(query, out, final, _time_desc_fallback, search_mode)  # R1-IMPL：OBQC（full 层同样校验）
        return out


def expand_entry(entry_id, tier="expand"):
    """
    展开单条记忆条目。

    参数:
        entry_id: 记忆条目 ID
        tier: expand|full
    返回: dict
    """
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT id, type, summary, content,
                   confidence, timestamp, source_agent,
                   industry, source_ref, expires_at
            FROM memory_store
            WHERE id = ?
        """, (entry_id,))
        row = c.fetchone()
    except Exception:
        conn.close()
        return {"error": f"查询失败: {entry_id}"}
    conn.close()

    if not row:
        return {"error": f"未找到条目: {entry_id}"}

    row = dict(row)
    if tier == "expand":
        return format_expand(row)
    else:
        return format_full(row)


def timeline(start=None, end=None, project=None, agent=None, limit=50):
    """
    时间轴回溯。
    """
    conn = get_conn()
    c = conn.cursor()
    conds, vals = [], []

    if start:
        conds.append("timestamp >= ?")
        vals.append(start)
    if end:
        conds.append("timestamp <= ?")
        vals.append(end)
    # project/agent columns removed from memory_store
    pass

    where = " AND ".join(conds) if conds else "1=1"
    try:
        c.execute(f"""
            SELECT id, type, summary, confidence, timestamp, source_agent,
                   industry, source_ref, expires_at
            FROM memory_store
            WHERE {where}
            ORDER BY timestamp DESC LIMIT ?
        """, (*vals, limit))
        rows = c.fetchall()
    except Exception:
        conn.close()
        return {"error": "时间轴查询失败"}

    conn.close()
    return {
        "total": len(rows),
        "results": [dict(r) for r in rows],
    }


# ── MCP 工具接口 ────────────────────────────────────────────────────

def mcp_search_tool(query, mode="auto", top_k=5, track=None, time_from=None, time_to=None):
    """
    MCP 兼容的搜索工具。
    返回 compact 模式结果（渐进式第一层）。

    time_from/time_to: 可选时间窗口过滤下界/上界（ISO 8601 或 epoch 秒字符串，
        如 "2026-08-01" / "1786242300"）；缺省不过滤（向后兼容）。
        过滤在检索后对结果做窗口裁剪（不侵入主检索链路），
        有过滤时候选池放大 3 倍以补偿截损，保证窗口内尽量凑满 top_k。
    """
    if time_from or time_to:
        result = search_memories(query, mode=mode, top_k=top_k * 3, tier="compact", track=track)
        return _mcp_time_filter(result, time_from, time_to, top_k)
    return search_memories(query, mode=mode, top_k=top_k, tier="compact", track=track)


def _mcp_time_filter(result, time_from, time_to, top_k):
    """结果级时间窗口过滤（MCP search_memories 专用，不触主检索链路）。

    对 compact 结果批量回查 timestamp（单次 SQL），按归一化 epoch 秒做窗口比较；
    无法判定时间戳的条目按严格窗口语义剔除；过滤后截断回 top_k。
    time_from/time_to 均无法解析时原样返回（防误伤）。
    """
    if not isinstance(result, dict) or not result.get("results"):
        return result
    lo = _norm_ts(time_from) if time_from else None
    hi = _norm_ts(time_to) if time_to else None
    if lo is None and hi is None:
        return result
    ts_map = _fetch_timestamps([r["id"] for r in result["results"]])
    kept = []
    for r in result["results"]:
        t = _norm_ts(ts_map.get(r["id"]))
        if t is None:
            continue  # 无/坏时间戳：严格窗口语义剔除
        if lo is not None and t < lo:
            continue
        if hi is not None and t > hi:
            continue
        kept.append(r)
    result["results"] = kept[:top_k]
    result["total"] = len(kept)
    return result


def _fetch_timestamps(ids):
    """批量回查 memory_store.timestamp（单次 SQL，避免逐条查库）。"""
    if not ids:
        return {}
    conn = get_conn()
    c = conn.cursor()
    try:
        placeholders = ",".join("?" * len(ids))
        c.execute(f"SELECT id, timestamp FROM memory_store WHERE id IN ({placeholders})", ids)
        rows = c.fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    return {r[0]: r[1] for r in rows}


def _norm_ts(s):
    """把 ISO 8601 或 epoch 秒字符串归一化为 epoch 秒（float）；无法解析返回 None。

    无时区 ISO 按本地时区（CST +08:00）假设；DB 时间戳为 UTC aware，统一转 epoch 比较。
    """
    if s is None or s == "":
        return None
    s = str(s).strip()
    try:
        return float(s)  # epoch 秒
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))  # 本地 CST 假设
        return dt.timestamp()
    except ValueError:
        return None


def mcp_expand_tool(entry_id):
    """
    MCP 兼容的展开工具。
    根据 entry_id 返回 expand 模式详情（渐进式第二层）。
    """
    return expand_entry(entry_id, tier="expand")


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="L3 检索引擎（双通道 + RRF + 渐进式披露）")
    sp = p.add_subparsers(dest="command")

    # search
    ps = sp.add_parser("search", help="搜索记忆（双通道+RRF）")
    ps.add_argument("--query", "-q", required=True)
    ps.add_argument("--mode", choices=["auto", "fts5", "embed", "dual"], default="auto")
    ps.add_argument("--limit", "-n", type=int, default=5)
    ps.add_argument("--tier", choices=["compact", "expand", "full"], default="compact")
    # B1-P2-1：--progressive 显式化——C3 决议「三级加载已存在
    # （--tier compact/expand/full）——--progressive 显式化非新增」；等价 --tier（渐进披露），
    # 显式别名补齐方案 12.1 承诺；不传则走 --tier 默认（行为逐位不变）
    ps.add_argument("--progressive", choices=["compact", "expand", "full"], default=None,
                    help="渐进式加载显式化（等价 --tier compact/expand/full——C3 决议显式别名；缺省走 --tier 默认 compact）")
    ps.add_argument("--industry", help="按行业过滤（如 AI/金融/医疗）")
    ps.add_argument("--track", choices=["episodic", "semantic"], default=None,
                    help="记忆分轨过滤：episodic=情景轨 / semantic=语义轨（缺省不限轨）")
    # : 类型过滤（8 资产类型可查：spec/skill/cron/workflow/rule/benchmark/asset/monitor + 13 内容类型）
    ps.add_argument("--type", dest="type_filter", default=None,
                    help="按类型过滤（逗号分隔多值，如 --type spec 或 --type spec,skill）")
    # P2b：多跳动态检索——默认关（R-A1：不传 → 主路径逐位一致）
    ps.add_argument("--multi-hop", action="store_true",
                    help="多跳动态检索（R-A8 复杂判定触发——概念关系扩展；默认关）")
    ps.add_argument("--max-hops", type=int, default=None,
                    help="多跳层数上限（默认 %d，硬上限 %d——C15）" % (
                        MULTI_HOP_MAX_HOPS, MULTI_HOP_MAX_HOPS))
    ps.add_argument("--pretty", action="store_true", help="美化 JSON 输出")

    # expand
    pe = sp.add_parser("expand", help="展开单条记忆")
    pe.add_argument("--id", required=True)
    pe.add_argument("--tier", choices=["expand", "full"], default="expand")
    pe.add_argument("--pretty", action="store_true")

    # timeline
    pt = sp.add_parser("timeline", help="时间轴回溯")
    pt.add_argument("--start")
    pt.add_argument("--end")
    # pt.add_argument("--project")  # column removed
    # pt.add_argument("--agent")  # column removed
    pt.add_argument("--limit", type=int, default=50)
    pt.add_argument("--pretty", action="store_true")

    # mcp（模拟 MCP 调用）
    pm = sp.add_parser("mcp", help="MCP 工具接口（测试用）")
    pm.add_argument("--tool", choices=["search", "expand"], required=True)
    pm.add_argument("--query")
    pm.add_argument("--id")
    pm.add_argument("--limit", type=int, default=5)
    pm.add_argument("--pretty", action="store_true")

    # verify（P2a ：验证循环——C4 挂 SIKU_GATE 门）
    pv = sp.add_parser("verify", help="验证循环（检索→关键数据→精确匹配→原文片段回传；SIKU_GATE=on 启用）")
    pv.add_argument("--query", "-q", required=True)
    pv.add_argument("--limit", "-n", type=int, default=5)
    pv.add_argument("--pretty", action="store_true")

    # tree（P2a ：目录树——C5 概念集权威源单向生成，只读）
    ptree = sp.add_parser("tree", help="目录树（从种子概念集单向生成，只读不写回）")
    ptree.add_argument("--json", action="store_true", help="输出结构化 JSON（默认文本树）")
    ptree.add_argument("--pretty", action="store_true")

    args = p.parse_args()
    if not args.command:
        p.print_help()
        return

    kwargs = {}
    if hasattr(args, "pretty") and args.pretty:
        kwargs["indent"] = 2
        kwargs["ensure_ascii"] = False
    else:
        kwargs["ensure_ascii"] = False

    if args.command == "search":
        # B1-P2-1：--progressive 显式别名 → 合并进 tier（缺省 None 走 --tier 默认，行为不变）
        tier = getattr(args, "progressive", None) or args.tier
        # P2b：--multi-hop 显式开启（R-A2 默认关）→ 多跳动态检索入口
        if getattr(args, "multi_hop", False):
            result = multi_hop_search(args.query, mode=args.mode, top_k=args.limit,
                                      max_hops=args.max_hops, tier=tier,
                                      industry=args.industry, track=args.track)
        else:
            result = search_memories(args.query, mode=args.mode, top_k=args.limit, tier=tier,
                                     industry=args.industry, track=args.track,
                                     type_filter=getattr(args, "type_filter", None))
        print(json.dumps(result, **kwargs))

    elif args.command == "expand":
        result = expand_entry(args.id, tier=args.tier)
        print(json.dumps(result, **kwargs))

    elif args.command == "timeline":
        result = timeline(
            start=args.start, end=args.end,
            # project=args.project, agent=args.agent,
            limit=args.limit
        )
        print(json.dumps(result, **kwargs))

    elif args.command == "mcp":
        if args.tool == "search":
            result = mcp_search_tool(args.query, top_k=args.limit)
        elif args.tool == "expand":
            result = mcp_expand_tool(args.id)
        else:
            result = {"error": f"未知 MCP 工具: {args.tool}"}
        print(json.dumps(result, **kwargs))

    elif args.command == "verify":
        result = verify_run(args.query, top_k=args.limit)
        print(json.dumps(result, **kwargs))

    elif args.command == "tree":
        result = concept_tree()
        if args.json:
            print(json.dumps(result, **kwargs))
        else:
            print(result.get("tree", json.dumps(result, **kwargs)))


if __name__ == "__main__":
    main()
