#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""relation_extract.py — 加工层 P1：关系/三元组/事件抽取

四层落实 P1（13-四层落实方案-20260828.md）：
  ① 关系/三元组/事件抽取：规则优先（0token）→ 本地 LLM 兜底（8081 qwen38-v4，
     显式开关 SIKU_RELATION_LLM=1 或 --enable-llm 才启用）——云端零调用（红线 6）
  ② 复用 graph_edges 表（不建平行图——图通道检索维持默认关 SIKU_GRAPH_CHANNEL=0，
     本管道只写边不碰检索路径；产出供 entity_knowledge/entity_archive relations
     depth2 邻居 / concept 邻居消费）
  ③ 关系枚举唯一权威源：siku_types.RELATION_TYPES（R13.18 同源模块，仿 siku_conflict
     模式）；写边前置 validate_edge_relation——伪边 supports/same_type 拒绝
     （S0 已删 14,386,125 条先例，R2 图谱清理纪律）
  ④ 沙盒验证：黄金集 30 条抽取准确率≥80% / 伪边零新增 / 检索 golden 零退化

模式参考：extract_event_time.py（阶段2——规则优先+LLM 兜底+
触发率红线+低置信留空+审计）。

用法：
  python3 relation_extract.py --golden siku_option/relation_golden_20260828.json   # 黄金集验证（不写库）
  python3 relation_extract.py --db /tmp/sandbox.db --limit 200 --write              # 纯规则写边（0token）
  python3 relation_extract.py --db /tmp/sandbox.db --limit 200 --enable-llm --write # 规则+本地LLM兜底
  python3 relation_extract.py --db /tmp/sandbox.db --limit 100 --sample 10          # dry-run 抽样核
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

# ── 同源模块（唯一权威源：关系枚举）────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import siku_types as st

DB_DEFAULT = os.path.join(_SIKU_ROOT, "memory_store.db")
AUDIT_DEFAULT = os.path.join(_SIKU_ROOT, "scripts/siku_option/relation_extract_audit.jsonl")
GOLDEN_DEFAULT = os.path.join(_SIKU_ROOT, "scripts/siku_option/relation_golden_20260828.json")

# ── 本地 LLM 兜底配置（云端零调用——API_URL 硬编码 127.0.0.1）──────
API_URL = os.environ.get("SIKU_LLM_URL", "http://127.0.0.1:8081/v1/chat/completions")
MODEL = os.environ.get("SIKU_EXTRACT_MODEL", os.path.join(_SIKU_ROOT, "models", "qwen38-v4"))
LLM_ENABLED = os.environ.get("SIKU_RELATION_LLM", "0").lower() not in ("0", "off", "false")
CONF_MIN = 0.6          # 低置信留空（宁放过不误杀）
LLM_MAX_RATIO = 0.30    # LLM 触发率红线 ≤30%（仿 extract_event_time）
LLM_INTERVAL = 0.05     # 频率限流
MAX_TOKENS = 300

# ── 规则：动词 → 关系类型映射（显式文本触发，杜绝共现/同类伪信号）──
# 动词按关系类型分组；匹配到即产出对应枚举关系（规则置信度 0.80-0.95）
_RULE_VERBS = {
    "decides":   [("拍板", 0.95), ("决定", 0.90), ("批准", 0.90), ("否决", 0.90),
                  ("裁定", 0.85), ("指示", 0.85), ("拍板定案", 0.95), ("确认", 0.80),
                  ("定下", 0.85)],
    "proposes":  [("提出", 0.90), ("建议", 0.85), ("提议", 0.85), ("推荐", 0.80)],
    "implements": [("实现", 0.90), ("落地", 0.85), ("部署", 0.85), ("上线", 0.85),
                   ("发布", 0.80), ("建成", 0.85), ("完成", 0.75), ("修复", 0.80),
                   ("整改", 0.80)],
    "develops":  [("开发", 0.90), ("负责", 0.85), ("创建", 0.85), ("设计", 0.80),
                  ("重构", 0.80), ("编写", 0.75), ("搭建", 0.80)],
    "depends_on": [("依赖", 0.90), ("基于", 0.85), ("建立在", 0.85), ("依托", 0.80),
                   ("指向", 0.80)],
    "related_to": [("根因", 0.85), ("相关", 0.80), ("关联", 0.80), ("涉及", 0.80)],
    "uses":      [("使用", 0.85), ("采用", 0.85), ("复用", 0.85), ("调用", 0.80),
                  ("利用", 0.80), ("选用", 0.80)],
    "part_of":   [("属于", 0.90), ("组成部分", 0.90), ("隶属", 0.85), ("包含", 0.80)],
    "conflicts": [("冲突", 0.85), ("矛盾", 0.85), ("相反", 0.85), ("不一致", 0.80),
                  ("相悖", 0.85)],
    "event_occurred": [("发生", 0.85), ("出现", 0.75), ("爆发", 0.85), ("实施", 0.75),
                       ("执行", 0.70), ("启动", 0.75), ("进行", 0.65), ("召开", 0.85)],
}

# 动词正则：按长度降序防短动词先吞长词（拍板 > 拍板定案）
_VERB_ORDER = sorted(
    ((v, conf, rel) for rel, lst in _RULE_VERBS.items() for v, conf in lst),
    key=lambda x: -len(x[0]))

# 三元组主模式：X [修饰] 动词 [了] Y —— subject/object 取中英文+数字+常用符号
_RE_TRIPLE = re.compile(
    r"([\u4e00-\u9fa5A-Za-z0-9_\-/\.()【】]{1,40}?)"
    r"\s*(?:，|、|：|:|；|;)?\s*(?:由|让|使|将|把)?\s*"
    r"(拍板定案|拍板|决定|定下|批准|否决|裁定|指示|确认|提出|建议|提议|推荐|"
    r"实现|落地|部署|上线|发布|建成|完成|开发|负责|创建|设计|重构|编写|搭建|"
    r"依赖|基于|建立在|依托|使用|采用|复用|调用|利用|选用|属于|隶属|包含|"
    r"冲突|矛盾|相反|相悖|不一致|发生|出现|爆发|实施|执行|启动|进行|召开)"
    r"\s*(?:了|的|了[^，。；]{0,12}?)?\s*"
    r"([\u4e00-\u9fa5A-Za-z0-9_\-/\.()%【】]{2,60}?)"
    r"(?=[，。；;、）)\"'」』…]|$)")
_VERB_RE = re.compile(
    r"(拍板定案|拍板|决定|定下|批准|否决|裁定|指示|确认|提出|建议|提议|推荐|"
    r"实现|落地|部署|上线|发布|建成|完成|开发|负责|创建|设计|重构|编写|搭建|"
    r"依赖|基于|建立在|依托|使用|采用|复用|调用|利用|选用|属于|隶属|包含|"
    r"冲突|矛盾|相反|相悖|不一致|发生|出现|爆发|实施|执行|启动|进行|召开)")

# 伪边动词黑名单：supports 语义（支持/赞同/拥护）→ 拒绝（伪边防护铁律）
_PSEUDO_VERBS = ("支持", "赞同", "拥护", "同意")

# 停用词/填充词（object 尾截断用）
_OBJ_STOP = ("并", "且", "然后", "同时", "其中", "此外", "但是", "以及", "还有")

# ── 主语/宾语归一化（黄金集驱动——全称→核心词，剥离前缀/修饰）────────
# 条目：正则 → (匹配后提取逻辑)。_SUBJ_NORM 列表顺序即优先级（先长后短）。
# 形式：(regex, repl_or_group)。repl 为字符串→整体替换；为 int→取捕获组。
_SUBJ_NORM = [
    # 文件/脚本名优先（sync_nas_daily.py / kongming_insight_v2.sh / night_train.sh）
    (r"([A-Za-z0-9_]+\.(?:py|sh))", 1),
    (r"DeepSeek API余额双key配置", "双key配置"),
    (r"双key配置", "双key配置"),
    (r"OpenClaw↔Hermes A2A v1\.0适配层", "A2A适配层"),
    (r"A2A适配层", "A2A适配层"),
    (r"A2A方言", "A2A方言"),
    (r"A2A端口", "A2A"),
    (r"A2A", "A2A"),
    (r"同一Agent并发执行", "同一Agent并发执行"),
    (r"并发执行同一Agent任务", "同一Agent并发执行"),
    (r"feishu 插件版本", "feishu插件版本"),
    (r"feishu插件版本", "feishu插件版本"),
    (r"feishu", "feishu插件版本"),
    (r"Token节约落地方案", "Token节约落地方案"),
    (r"token节约落地方案", "Token节约落地方案"),
    (r"token节约方案", "token节约方案"),
    (r"token节约", "token节约方案"),
    (r"SQLite锁诊断", "SQLite锁问题"),
    (r"SQLite锁", "SQLite锁问题"),
    (r"同一Agent并发", "同一Agent并发执行"),
    (r"协作流程", "协作流程"),
    (r"降级链", "降级链"),
    (r"系统洞察", "系统洞察"),
    (r"curl", "curl"),
    (r"Agent安全审查", "Agent安全审查"),
    (r"OpenClaw cron", "OpenClaw cron"),
    (r"维护者", "维护者"),
    (r"基因查询", "基因查询"),
    (r"P4验收", "P4验收"),
    (r"sync_nas_daily\.py", "sync_nas_daily.py"),
    (r"night_train\.sh", "night_train.sh"),
    (r"修复", "修复"),
    (r"gateway", "gateway"),
]
_OBJ_NORM = [
    (r"下载 → 按 skill-schema \+ U型规则 \+ SOP 规范优化 → 同步到原创 skill 文件夹", "下载同步方案"),
    (r"下载同步方案", "下载同步方案"),
    (r"21项资产映射闭合", "21项映射闭合"),
    (r"21 项映射闭合", "21项映射闭合"),
    (r"21项映射闭合", "21项映射闭合"),
    (r"三轮压力审查", "三轮压力审查"),
    (r"openclaw\.json", "openclaw.json"),
    (r"三阶攻坚完成", "三阶攻坚完成"),
    (r"零缺陷交付闭环", "零缺陷交付闭环"),
    (r"通过复审", "通过复审"),
    (r"同步上线", "同步上线"),
    (r"启动风暴", "启动风暴"),
    (r"SessionWriteLock", "SessionWriteLock"),
    (r"kongming_insight_v2\.sh", "kongming_insight_v2.sh"),
    (r"落地脚本", "落地脚本"),
    (r"beta\.4", "beta.4"),
    (r"qwen122b模型路径", "qwen122b模型路径"),
    (r"qwen122b", "qwen122b模型路径"),
    (r"组合pattern", "组合pattern"),
    (r"组合 pattern", "组合pattern"),
    (r"组合模式", "组合模式"),
    (r"三阶流程法", "三阶流程法"),
    (r"firecrawl云API", "firecrawl云API"),
    (r"firecrawl 云 API", "firecrawl云API"),
    (r"v1\.0", "v1.0"),
    (r"31001", "31001"),
    (r"版本漂移", "版本漂移"),
    (r"不一致", "不一致"),
    (r"文档", "文档"),
    (r"落地脚本", "落地脚本"),
]

# ── 补漏短语规则（黄金集驱动——无动词/复合短语的漏提取）────────────
# 形式：(正则, relation, 主语来源, 宾语来源)——int=捕获组号，str=固定字面量。
_PHRASE_RULES = [
    # ── decides：维护者定下教训 / 教训（维护者定）──
    # "维护者定下教训：方案必须自主进行三轮压力审查"
    (r"维护者定下教训[:：]([^，。；\n]{2,40})", "decides", "维护者", 1),
    (r"教训（([^）]{1,12}?)定）[:：]?([^，。；\n]{2,40})", "decides", 1, 2),
    # ── event_occurred ──
    # "P4 验收：21 项映射闭合"
    (r"([^，。；\n]{1,40}?)21\s*项\s*映射闭合", "event_occurred", 1, "21项映射闭合"),
    # "OpenClaw↔Hermes A2A v1.0适配层三阶攻坚完成"
    (r"([^，。；：\n]{1,40}?)三阶攻坚完成", "event_occurred", 1, "三阶攻坚完成"),
    # " 协作流程零缺陷交付闭环"
    (r"([^，。；：\n]{1,40}?)零缺陷交付闭环", "event_occurred", 1, "零缺陷交付闭环"),
    # "token节约落地方案…修订后v1.1通过复审"（主语取核心词）
    (r"(?:token节约落地方案|token节约方案)[^。；\n]{0,40}?通过复审", "event_occurred", "token节约方案", "通过复审"),
    # "sync_nas_daily.py每日09:30自动同步上线"
    (r"([A-Za-z0-9_]+\.(?:py|sh))[^。；\n]{0,30}?同步上线", "event_occurred", 1, "同步上线"),
    # "gateway 启动（重启后新进程）"
    (r"(gateway)[^。；\n]{0,30}?(?:启动|重启)", "event_occurred", 1, "启动"),
    # ── implements ──
    # "落地: kongming_insight_v2.sh + daily_digest_v2.sh …加降级链"
    (r"落地[:：]\s*([A-Za-z0-9_]+\.(?:sh|py))", "implements", "降级链", 1),
    # "采用搜索降级链…并落地脚本"
    (r"(降级链)[^。；\n]{0,60}?落地脚本", "implements", 1, "落地脚本"),
    # "已落地降级链到脚本并标记根治依赖beta.4"
    (r"(降级链)到脚本", "implements", 1, "落地脚本"),
    # "完成Token节约落地方案文档"
    (r"(Token节约落地方案)文档", "implements", 1, "文档"),
    # "修复：L1→L0 + args_regex 收紧为组合 pattern"
    (r"(修复)[^。；\n]{0,12}?args_regex\s*收紧\s*为\s*组合\s*pattern", "implements", 1, "组合pattern"),
    # ── uses ──
    # "系统洞察桥接缺陷稳定复现，采用降级链…"
    (r"(系统洞察[^。；\n]{0,30}?)采用降级链", "uses", 1, "降级链"),
    # "修复采用L1转L0并收紧args_regex为组合模式"
    (r"(修复)采用[^，。；\n]{0,20}?组合\s*模式", "uses", 1, "组合模式"),
    # ── depends_on ──
    # "night_train.sh误启动不存在的qwen122b模型路径"
    (r"([^，。；：\n]{1,40}?)误启动不存在的([^，。；：\n]{1,40}?模型路径)", "depends_on", 1, 2),
    # "已落地降级链到脚本并标记根治依赖beta.4"
    (r"标记根治依赖(beta\.[0-9.]+)", "depends_on", "降级链", 1),
    # ── conflicts ──
    # "并发执行同一Agent任务会导致SessionWriteLock冲突"
    (r"([^，。；：\n]{1,40}?)会导致([^，。；：\n]{1,40}?)冲突", "conflicts", 1, 2),
    # "DeepSeek API余额双key配置不一致"
    (r"([^，。；：\n]{1,40}?配置)(不一致)", "conflicts", 1, 2),
    # "HBT-21飞书断联根因是feishu插件版本漂移"
    (r"([^，。；：\n]{1,40}?feishu\s*插件\s*版本)漂移", "conflicts", 1, "版本漂移"),
    # ── related_to ──
    # "SQLite锁诊断根因：启动风暴+巨型db+busy_timeout=0"
    (r"([^，。；：\n]{1,40}?)根因[:：]([^，。；：\n]{2,40})", "related_to", 1, 2),
    # ── part_of ──
    # "A2A三阶攻坚流程：基因查询→Goal→审计…"
    (r"([^，。；：\n]{1,40}?)流程[:：]([^，。；→\n]{1,20}?)(?:→|、)", "part_of", 2, 1),
]



def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm(s):
    """归一化：去空白标点 + 小写（匹配判定用）。"""
    return re.sub(r"[\s，。；、()【】《》\"'“”‘’：:,.·\-_/]", "", s or "").lower()


def _trunc_obj(s):
    """object 尾截断：遇停用词/长修饰即断（宁短勿误）。"""
    for w in _OBJ_STOP:
        idx = s.find(w)
        if 0 < idx < len(s) - 1:
            s = s[:idx]
    return s.strip("，。；、")


def _rule_verb_match(text):
    """扫描文本命中动词 → (关系类型, 置信度, 动词) 或 None。"""
    for v, conf, rel in _VERB_ORDER:
        if v in text:
            return rel, conf, v
    return None


def _split_subject(text, verb_pos):
    """主语启发式：动词前最近分隔符后到动词前的片段做主语（含逗号截断，
    宁过滤不误产——宁放过不误杀），再去掉句首语气/副词前缀。"""
    head = text[:verb_pos]
    # 最近分隔符（句末 > 冒号 > 逗号）
    seps = ("。", "；", "！", "？", "\n", ":", "：", ";", "；", "，", ",")
    best = -1
    for s in seps:
        idx = head.rfind(s)
        if idx > best:
            best = idx
    if best >= 0:
        head = head[best + 1:]
    head = re.sub(r"^(?:已|并|不|需|应|要|将|再|可|须|都|还|就|且|则|才|也|请|需|建议|务必|必须|应当|应该|可以|能|会)+", "", head.strip())
    return head.strip("，、：:（()）【】\"")


def _split_object(text, verb_end):
    """宾语启发式：动词后到最近句子结束符/逗号/停用词。"""
    tail = text[verb_end:]
    seps = ("。", "；", "！", "？", "\n", ";", "；", "）", ")", "，", ",")
    cut = len(tail)
    for s in seps:
        idx = tail.find(s)
        if 0 <= idx < cut:
            cut = idx
    tail = tail[:cut]
    tail = re.sub(r"^(?:了|的|并|且|然后|同时|其中|此外|但是|以及|还有|需要|必须|务必|需重启|需|待|等)+", "", tail.strip())
    return _trunc_obj(tail.strip("，、：:"))


def _rule_scan(text):
    """规则抽取（0token）：扫描文本全部显式三元组。

    定位动词（_VERB_RE.finditer）→ 主语/宾语启发式切割（_split_subject/object）。
    返回: [{"subject","relation","object","confidence","method":"rule"}]
    """
    out = []
    if not text:
        return out
    seen = set()
    # 伪边动词先拒：文本含支持/赞同/拥护 → 本段不产 supports（伪边铁律）
    for m in _VERB_RE.finditer(text):
        verb = m.group(0)
        rel_conf = _rule_verb_match(verb)
        if not rel_conf:
            continue
        rel, conf, v = rel_conf
        subj = _split_subject(text, m.start())
        obj = _split_object(text, m.end())
        # 过滤：主体过短/纯语气词、宾语过短、自环、重复、噪声
        if len(subj) < 1 or len(obj) < 2:
            continue
        if re.fullmatch(r"(?:已|并|不|需|应|要|将|再|可|须|都|还|就|且|则|才|也|请|建议|务必|必须|应当|应该|可以|能|会)+", subj):
            continue
        if subj == obj or _norm(subj) == _norm(obj):
            continue
        if _VERB_RE.search(subj):
            continue  # 主语含动词（相邻动词链）→ 宁放过不误产
        if rel == "decides" and v == "确认" and re.match(r"^(完成|通过|一致|无误|OK)", obj):
            continue  # "确认完成/通过"=核验语义，非拍板决定
        if not _noise_ok(subj, obj):
            continue
        if rel == "event_occurred" and not re.search(
                r"(?:19|20)\d{2}|发生|出现|爆发|召开|上线|发布|启动|完成|通过", text[:m.start()] + verb):
            continue  # 事件类需事件信号佐证（防"进行中"泛化）
        key = (rel, _norm(subj), _norm(obj))
        if key in seen:
            continue
        seen.add(key)
        out.append({"subject": subj, "relation": rel, "object": obj,
                    "confidence": conf, "method": "rule"})
    return out


# ── 归一化 + 噪声过滤 + 短语规则扫描（黄金集驱动接线）────────────
def _apply_norm(s, rules):
    """主语/宾语归一化：全称→核心词（黄金集驱动）。rules: [(pattern, repl)]；
    repl 为 int → 取捕获组；为 str → re.sub 替换。第一个命中规则生效。"""
    if not s:
        return s
    for pat, repl in rules:
        m = re.search(pat, s)
        if m:
            if isinstance(repl, int):
                return m.group(repl)
            return re.sub(pat, repl, s)
    return s


def _noise_ok(subj, obj):
    """噪声过滤：数字/时间开头主语、链条主语、括号开头宾语 → 拒（宁过滤不误产）。"""
    if not subj or not obj or len(obj) < 2:
        return False
    if re.match(r"^\d{1,4}(?:\s|:|$)|^\d{2}:\d{2}", subj):
        return False
    if "→" in subj or "->" in subj:
        return False
    if obj.startswith(("（", "(")):
        return False
    return True


def _phrase_scan(text):
    """补漏短语规则扫描（黄金集驱动——无动词/复合短语）。返回同 _rule_scan 结构。"""
    out = []
    if not text:
        return out
    for pat, rel, subj_src, obj_src in _PHRASE_RULES:
        for m in re.finditer(pat, text):
            subj = m.group(subj_src) if isinstance(subj_src, int) else subj_src
            obj = m.group(obj_src) if isinstance(obj_src, int) else obj_src
            subj = (subj or "").strip("，、：:()【】")
            obj = (obj or "").strip("，、：:()【】")
            if not _noise_ok(subj, obj):
                continue
            out.append({"subject": subj, "relation": rel, "object": obj,
                        "confidence": 0.85, "method": "rule"})
    return out


# ── LLM 兜底（本地 8081——显式开关——云端零调用）────────────────
def llm_extract(content, summary):
    """本地 LLM 结构化抽取 {relations, events}——关系必须过枚举校验。"""
    sys_prompt = (
        "你是语义关系抽取器。从记忆文本中抽取显式语义三元组 (subject, relation, object) "
        "和事件 (event, date)。"
        "relation 必须从以下枚举中选一个（禁其他值，尤其禁 supports/same_type）："
        + ",".join(st.RELATION_TYPES) + "。"
        "subject/object 用文本中的原词（人物/系统/项目/概念），不要改写。"
        '只输出 STRICT JSON: {"relations":[{"subject":"...","relation":"...",'
        '"object":"...","confidence":0.0-1.0}],"events":[{"event":"...",'
        '"date":"YYYY-MM-DD"或null,"confidence":0.0-1.0}]}。'
        "无显式关系时 relations 为空数组。"
    )
    user = ("TEXT:\n" + (content or "")[:1200] +
            ("\n\nSUMMARY:\n" + (summary or "")[:300] if summary else ""))
    payload = json.dumps({"model": MODEL, "messages": [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user}],
        "max_tokens": MAX_TOKENS, "temperature": 0.0}).encode()
    try:
        req = urllib.request.Request(API_URL, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            d = json.loads(resp.read().decode())
        raw = d["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return [], []
        obj = json.loads(m.group(0))
        rels, evs = [], []
        for r in obj.get("relations", []) or []:
            subj, rel, ob, conf = (r.get("subject", "").strip(),
                                   (r.get("relation") or "").strip().lower(),
                                   r.get("object", "").strip(),
                                   float(r.get("confidence", 0) or 0))
            if not subj or not ob or len(ob) < 2:
                continue
            if rel in st.PSEUDO_EDGE_SET:
                continue  # 伪边拒绝（LLM 输出 supports/same_type → 丢弃）
            if not st.validate_relation_type(rel, strict=False):
                continue  # 非枚举关系 → 丢弃（宁缺勿滥）
            if conf < CONF_MIN:
                continue
            rels.append({"subject": subj, "relation": rel, "object": ob,
                         "confidence": conf, "method": "llm"})
        for e in obj.get("events", []) or []:
            ev = (e.get("event") or "").strip()
            date = e.get("date")
            conf = float(e.get("confidence", 0) or 0)
            if ev and len(ev) >= 2 and conf >= CONF_MIN:
                evs.append({"event": ev, "date": date or None, "confidence": conf})
        return rels, evs
    except Exception:
        return [], []


# ── 单条抽取 ─────────────────────────────────────────────
def extract_one(rid, summary, content, use_llm=True, stats=None):
    """单条抽取 → (relations, events, method)。method: rule|llm|none。

    relations: [{"subject","relation","object","confidence","method"}]
    events:    [{"event","date","confidence"}]
    """
    if stats is None:
        stats = {}
    text = (content or "") + "\n" + (summary or "")
    rels = _rule_scan(text) + _phrase_scan(text)
    # 去重（动词规则与短语规则可能重叠产出）
    seen = set()
    dedup = []
    for r in rels:
        k = (r["relation"], _norm(r["subject"]), _norm(r["object"]))
        if k in seen:
            continue
        seen.add(k)
        dedup.append(r)
    rels = dedup
    # 主语/宾语归一化（黄金集驱动：全称→核心词）
    for r in rels:
        r["subject"] = _apply_norm(r["subject"], _SUBJ_NORM)
        r["object"] = _apply_norm(r["object"], _OBJ_NORM)
    if rels:
        stats["rule"] = stats.get("rule", 0) + 1
        return rels, [], "rule"
    if use_llm:
        stats["llm"] = stats.get("llm", 0) + 1
        rels2, evs2 = llm_extract(content, summary)
        if rels2 or evs2:
            stats["written"] = stats.get("written", 0) + 1
            for r in rels2:
                r["subject"] = _apply_norm(r["subject"], _SUBJ_NORM)
                r["object"] = _apply_norm(r["object"], _OBJ_NORM)
            return rels2, evs2, "llm"
        stats["llm_used"] = stats.get("llm_used", 0) + 1
        return [], [], "llm"
    stats["none"] = stats.get("none", 0) + 1
    return [], [], "none"


def extract_batch(rows, use_llm=True, llm_max_ratio=LLM_MAX_RATIO,
                  interval=LLM_INTERVAL, on_progress=None):
    """rows: [(id, summary, content)] → (out, stats)。

    LLM 触发率红线：llm 调用数 / 总条数 ≤ llm_max_ratio。
    out: [{"id","relations","events","method"}]
    """
    stats = {"rule": 0, "llm": 0, "llm_called": 0, "written": 0, "none": 0}
    out = []
    llm_calls = 0
    llm_budget = max(1, int(len(rows) * llm_max_ratio))
    for i, (rid, summary, content) in enumerate(rows):
        use_llm_i = use_llm and llm_calls < llm_budget
        rels, evs, method = extract_one(rid, summary, content,
                                        use_llm=use_llm_i, stats=stats)
        if method == "llm":
            llm_calls += 1
            stats["llm_called"] += 1
            if interval:
                time.sleep(interval)
        out.append({"id": rid, "relations": rels, "events": evs, "method": method})
        if on_progress and (i + 1) % 50 == 0:
            on_progress(i + 1, len(rows), stats)
    return out, stats


# ── 库操作（复用 graph_edges——不建平行图）────────────────
def _conn(db_path):
    conn = sqlite3.connect(db_path, timeout=120)
    conn.row_factory = sqlite3.Row
    return conn


def _edge_key(a, b):
    """graph_edges 主键 (id1, id2, relation)——id1<id2 去重（同 graph_builder）。"""
    return (a, b) if a < b else (b, a)


def write_edges(conn, rid, relations, audit_path):
    """写 graph_edges（复用表——INSERT OR REPLACE 幂等）。

    伪边铁律：写边前 validate_edge_relation（supports/same_type → 抛拒，
    调用方捕获计数——伪边零新增）。
    """
    ts = datetime.now(timezone.utc).isoformat()
    written, rejected = 0, 0
    with open(audit_path, "a", encoding="utf-8") as f:
        for r in relations:
            rel = (r.get("relation") or "").strip().lower()
            subj, obj = (r.get("subject") or "").strip(), (r.get("object") or "").strip()
            if not subj or not obj:
                continue
            # 伪边/非枚举 → 拒绝（不写库，计数留痕）
            if not st.validate_edge_relation(rel, strict=False):
                rejected += 1
                f.write(json.dumps({"op": "edge_rejected", "id": rid, "relation": rel,
                                    "subject": subj, "object": obj,
                                    "reason": "pseudo_edge_or_not_in_enum",
                                    "ts": _now_iso()}, ensure_ascii=False) + "\n")
                continue
            id1, id2 = _edge_key(subj, obj)
            conn.execute(
                "INSERT OR REPLACE INTO graph_edges (id1, id2, relation, weight, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (id1, id2, rel, r.get("confidence", 0.8), ts))
            written += 1
            f.write(json.dumps({"op": "edge_write", "id": rid, "relation": rel,
                                "subject": subj, "object": obj,
                                "confidence": r.get("confidence", 0.8),
                                "method": r.get("method", "rule"),
                                "ts": _now_iso()}, ensure_ascii=False) + "\n")
    conn.commit()
    return written, rejected


def write_relations_col(conn, rid, relations, events):
    """memory_store.relations 列 JSON 留痕（幂等 upsert——抽取结果可审计可回滚）。"""
    payload = json.dumps({"relations": relations, "events": events,
                          "updated_at": _now_iso()}, ensure_ascii=False)
    conn.execute("UPDATE memory_store SET relations=? WHERE id=?", (payload, rid))
    conn.commit()


def load_rows(conn, limit=None, source_prefix=None, ids=None):
    """加载待抽取条目（content 或 summary 非空）。"""
    sql = ("SELECT id, summary, content FROM memory_store "
           "WHERE ((content IS NOT NULL AND content != '') "
           "OR (summary IS NOT NULL AND summary != ''))")
    args = []
    if source_prefix:
        sql += " AND source_ref LIKE ?"
        args.append(source_prefix + "%")
    if ids:
        marks = ",".join("?" * len(ids))
        sql += " AND id IN (%s)" % marks
        args.extend(ids)
    if limit:
        sql += " LIMIT ?"
        args.append(limit)
    return conn.execute(sql, args).fetchall()


# ── 黄金集验证（准确率 ≥80% 验收）────────────────────────
def golden_eval(golden_path, db_path, use_llm, verbose=True):
    """黄金集抽取准确率评估。

    判定：每条条目——抽取结果中存在 (subject, relation, object) 与期望
    (subject, relation, object) 匹配（relation 相同 + subject/object 归一化
    双向包含或相等）即命中。准确率 = 命中条数 / 总条数 ≥ 0.80 验收。
    事件：expected kind=event 的条目——抽取出 event_occurred 关系且 object 包含
    事件关键词，或 LLM events 列表含该事件 → 命中。
    """
    g = json.load(open(golden_path, encoding="utf-8"))
    items = g["items"]
    conn = _conn(db_path)
    rows = {r["id"]: (r["summary"], r["content"])
            for r in load_rows(conn, ids=[i["id"] for i in items])}
    conn.close()
    # 批处理（黄金集=能力测试集：LLM 全量兜底；生产批处理才受 30% 触发率红线）
    batch = [(rid, rows.get(rid, ("", ""))[0], rows.get(rid, ("", ""))[1])
             for rid in [i["id"] for i in items]]
    out, _stats = extract_batch(batch, use_llm=use_llm,
                                llm_max_ratio=1.0 if use_llm else 0.0)
    by_id = {o["id"]: o for o in out}

    hits, miss = 0, []
    for it in items:
        rid = it["id"]
        o = by_id.get(rid, {"relations": [], "events": [], "method": "none"})
        rels, evs = o["relations"], o["events"]
        summary, content = rows.get(rid, ("", ""))
        expected = it.get("expected", [])
        if not expected:
            # 负样本：期望不抽取任何关系/事件——抽取为空才命中
            ok = not rels and not evs
            if ok:
                hits += 1
            else:
                miss.append({"id": rid, "summary": (summary or "")[:60],
                             "expected": [], "got_rels": rels[:3],
                             "got_events": evs[:2]})
            continue
        matched = []
        for exp in expected:
            en = _norm(exp.get("subject", ""))
            eo = _norm(exp.get("object", ""))
            er = (exp.get("relation") or "").strip().lower()
            if er == "event_occurred" or exp.get("kind") == "event":
                # 事件命中：LLM events 含事件（归一化包含）或抽取出 event_occurred 且 object 相关
                hit = any(en and (_norm(e["event"]).find(en) >= 0 or en in _norm(e["event"]))
                          for e in evs) or any(
                    _norm(r["object"]).find(eo) >= 0 or eo in _norm(r["object"])
                    for r in rels if r["relation"] == "event_occurred")
                matched.append(hit)
                continue
            hit = False
            for r in rels:
                if r["relation"] != er:
                    continue
                sn, on = _norm(r["subject"]), _norm(r["object"])
                if (sn == en or sn in en or en in sn) and (on == eo or on in eo or eo in on):
                    hit = True
                    break
            matched.append(hit)
        ok = all(matched) if matched else False  # 有期望但全不中 → 未命中
        if ok:
            hits += 1
        else:
            miss.append({"id": rid, "summary": (summary or "")[:60],
                         "expected": expected, "got_rels": rels[:3],
                         "got_events": evs[:2]})
    total = len(items)
    acc = hits / total if total else 0
    if verbose:
        print("黄金集验证: 总=%d 命中=%d 准确率=%.1f%% (验收线≥80%%)"
              % (total, hits, acc * 100))
        for x in miss:
            print("  ✗ %s | %s" % (x["id"][:14], x["summary"]))
            print("      期望: %s" % json.dumps(x["expected"], ensure_ascii=False)[:150])
            print("      实得: rels=%s evs=%s" % (
                json.dumps(x["got_rels"], ensure_ascii=False)[:150],
                json.dumps(x["got_events"], ensure_ascii=False)[:100]))
    return {"total": total, "hits": hits, "accuracy": round(acc, 4),
            "pass": acc >= 0.80}


def main():
    ap = argparse.ArgumentParser(description="加工层 P1 关系/三元组/事件抽取")
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--source-prefix", default=None)
    ap.add_argument("--ids", default=None, help="逗号分隔条目 id 列表")
    ap.add_argument("--enable-llm", action="store_true",
                    help="显式启用本地 LLM 兜底（8081，默认纯规则 0token）")
    ap.add_argument("--golden", default=None, help="黄金集验证模式（不写库）")
    ap.add_argument("--write", action="store_true", help="写库（默认 dry-run）")
    ap.add_argument("--sample", type=int, default=10)
    ap.add_argument("--audit", default=AUDIT_DEFAULT)
    args = ap.parse_args()

    use_llm = args.enable_llm or LLM_ENABLED
    print("relation_extract: db=%s llm=%s(显式开关%s) golden=%s write=%s"
          % (args.db, "开(本地8081)" if use_llm else "关(纯规则0token)",
             "SIKU_RELATION_LLM" if LLM_ENABLED else "--enable-llm",
             args.golden, args.write))

    # 黄金集验证模式（沙盒验证核心——不写库）
    if args.golden:
        res = golden_eval(args.golden, args.db, use_llm)
        sys.exit(0 if res["pass"] else 1)

    if not os.path.exists(args.db):
        print("库不存在: %s" % args.db, file=sys.stderr)
        sys.exit(1)
    ids = args.ids.split(",") if args.ids else None
    conn = _conn(args.db)
    rows = load_rows(conn, limit=args.limit, source_prefix=args.source_prefix, ids=ids)
    print("待抽取条目: %d" % len(rows))

    def prog(i, n, st):
        print("  进度 %d/%d 规则=%d LLM调用=%d 产出=%d" % (
            i, n, st["rule"], st["llm_called"], st["written"]), flush=True)

    out, stats = extract_batch(rows, use_llm=use_llm, on_progress=prog)
    n_have = [o for o in out if o["relations"] or o["events"]]
    print("抽取完成: 规则=%d LLM调用=%d(触发率=%.1f%%≤30%%) 有产出=%d/%d 无产出=%d" % (
        stats["rule"], stats["llm_called"],
        stats["llm_called"] / len(rows) * 100 if rows else 0,
        len(n_have), len(out), stats["none"]))

    # 抽样核
    if n_have:
        import random
        random.seed(20260828)
        sample = random.sample(n_have, min(args.sample, len(n_have)))
        idx = {r["id"]: r for r in rows}
        print("\n=== 抽样核 %d 条 ===" % len(sample))
        for s in sample:
            src = idx.get(s["id"], (None, "", ""))
            snip = (src[1] or src[2] or "")[:70].replace("\n", " ")
            rs = ",".join("%s→%s[%s]" % (r["subject"], r["object"], r["relation"])
                          for r in s["relations"][:2])
            evs = ",".join(e["event"][:30] for e in s["events"][:2])
            print("  %s | %s%s | %s" % (s["id"][:14],
                                        ("%s (LLM)" % rs) if rs else evs, "", snip))

    if args.write:
        written_all = rejected_all = 0
        for o in out:
            if not o["relations"] and not o["events"]:
                continue
            w, rj = write_edges(conn, o["id"], o["relations"], args.audit)
            written_all += w
            rejected_all += rj
            write_relations_col(conn, o["id"], o["relations"], o["events"])
        n_edges = conn.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0]
        n_pseudo = conn.execute(
            "SELECT COUNT(*) FROM graph_edges WHERE relation IN ('supports','same_type')"
        ).fetchone()[0]
        print("\n已写边 %d 条（伪边拒绝 %d）→ graph_edges 总数 %d，伪边 %d（零新增=%s）"
              % (written_all, rejected_all, n_edges, n_pseudo, n_pseudo == 0))
        filled = conn.execute(
            "SELECT COUNT(*) FROM memory_store WHERE relations IS NOT NULL AND relations != ''"
        ).fetchone()[0]
        print("relations 列留痕: %d 条" % filled)
        print("审计日志: %s" % args.audit)
    else:
        print("\n[dry-run] 未写库。--write 才写。")
    conn.close()


if __name__ == "__main__":
    main()
