#!/usr/bin/env python3
"""
router.py — L3 检索两级路由层 v0.2（l3_retrieval.py 零改动）

设计原则：
1. 不改 l3_retrieval.py 任何一行：通过 import 复用 search_memories 原签名
2. 返回格式：原字段全保留 + route meta 附加（浅拷贝，不污染原 dict）
3. SIKU_ROUTER=off 一键回滚：完全透传原逻辑（零记录零干预）
4. shadow 模式：纯观察记录 query+决策+结果，不改变实际执行路径
5. 强制 fallback：规则未命中 → mode=dual（L3 全量向量）
6. busy_timeout>=5000ms + 写代理串行化（threading.Lock 串行写 shadow 日志/DB）
7. LLM 兜底预留接口（长尾<10%场景，v0.2 暂不实现，返回 None 不介入）

配置：permissions.yaml 的 router 段（默认全关）
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import threading
import time
from collections import OrderedDict

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import l3_retrieval  # 只 import，永不修改
import audit_logger  # P0-3 读审计落盘（log_read）

__version__ = "v0.2"

# ── 配置 ────────────────────────────────────────────────────────────
CFG_PATH = os.path.join(_SIKU_ROOT, "permissions.yaml")
DEFAULT_CFG = {
    "enabled": False,
    "mode": "rule",
    "shadow": True,
    "cache_size": 256,
    "fallback": "dual",
    "llm_fallback": False,
    "log_path": os.path.join(_SIKU_ROOT, "logs", "router_shadow.jsonl"),
}

def load_cfg():
    cfg = dict(DEFAULT_CFG)
    try:
        import yaml
        with open(CFG_PATH, encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}
        rc = y.get("router", {}) or {}
        cfg.update({k: v for k, v in rc.items() if k in DEFAULT_CFG})
    except Exception:
        pass  # 配置读取失败 → 默认全关（安全侧）
    # 环境变量优先：SIKU_ROUTER=off|rule|semantic
    env = os.environ.get("SIKU_ROUTER", "").strip().lower()
    if env in ("off", "0", "false"):
        cfg["enabled"] = False
    elif env in ("rule", "on", "1", "true"):
        cfg["enabled"] = True
        cfg["mode"] = "rule"
    elif env == "semantic":
        cfg["enabled"] = True
        cfg["mode"] = "semantic"
    return cfg

CFG = load_cfg()
LOG_PATH = os.path.expanduser(CFG["log_path"])

# ── v3 词序感知规则 ──────────────────────────────────────────────
# 强标识符（具体实体）：ID/版本号/文件名/路径 → 快路信号
RE_STRONG_ID = re.compile(
    r"[A-Za-z]{2,}[-_][A-Za-z0-9]+"      # 如 l3_retrieval / bge-reranker
    r"|\d{4,}"                            # 如 20260731 / 9000
    r"|[\w.-]+\.(?:py|json|yaml|yml|sh|db|md|log|conf|plist|toml|bak)\b"  # 文件名
)
# 英文强特征词（自身即可触发快路——本身就是强标识符性质）
EN_FAST_WORDS = {
    "cron", "launchd", "brew", "sqlite", "python", "node", "npm", "nginx",
    "docker", "redis", "kafka", "mysql", "error", "debug", "fix", "api",
    "sdk", "cli", "yaml", "json",
}
# 中文工具词（P4修复：仅当 query 含英文锚点（ID/文件名/数字/英文词）时才计入强特征）
CH_FAST_WORDS = {
    "报错", "崩溃", "修复", "补丁", "版本", "配置", "日志", "权限", "升级",
    "安装", "删除", "备份", "恢复", "迁移", "脚本", "命令", "进程", "端口",
    "文件", "目录", "失败",
}
# 模糊/概念/疑问词 → 深路信号
FUZZY_WORDS = {
    "什么", "怎么", "为什么", "如何", "哪些", "哪个", "有没有", "是否",
    "多少", "那个", "上次", "之前", "记得", "关于", "所有", "全部",
    "总结", "概述", "是什么", "什么是", "怎么办", "介绍", "区别", "对比",
}

def tokenize(query):
    """中英文混合分词（保留给未来语义路由扩展用）"""
    return [t for t in re.split(r'([一-鿿　-〿]|[A-Za-z0-9._-]+)', query) if t.strip()]


def rule_decision(query):
    """v3.1 词序感知规则（P4 修复版）。

    P4 修复点（验收复核：黑盒命中 78%<80%）：
    ① 中文工具词触发 fts5 需同时含英文锚点（ID/文件名/数字/英文词）
    ② 纯中文 query（无任何 ASCII 字母/数字）最低降级 dual

    词序逻辑：比较「强特征词」与「模糊词」最早出现位置
    - 强特征在前（实体主导）→ fts5 快路
    - 模糊词在前或只有模糊词（概念主导）→ dual 深路
    - 均无信号 → 返回 None（调用方强制 fallback dual）
    返回: (mode|None, confidence, [reasons])
    """
    q = (query or "").strip()
    if not q:
        return None, 0.0, ["空查询"]
    n = max(len(q), 1)
    reasons = []

    # ── 锚点检测（P4 修复点①）──
    m = RE_STRONG_ID.search(q)
    has_anchor = m is not None  # 英文 ID/文件名/数字
    if m:
        reasons.append(f"强标识:{m.group(0)}")
    en_hits = [w for w in EN_FAST_WORDS if w in q]
    if en_hits:
        has_anchor = True
        reasons.append(f"英文词:{','.join(en_hits)}")
    pure_cn = not re.search(r"[A-Za-z0-9]", q)  # 纯中文（P4 修复点②）

    # ── 强特征最早位置（词序）──
    strong_pos = None
    if m:
        strong_pos = m.start() / n
    for w in en_hits:
        p = q.find(w) / n
        if strong_pos is None or p < strong_pos:
            strong_pos = p
    # 中文工具词：仅在有英文锚点时计入强特征
    for w in CH_FAST_WORDS:
        i = q.find(w)
        if i >= 0:
            if has_anchor:
                p = i / n
                if strong_pos is None or p < strong_pos:
                    strong_pos = p
                reasons.append(f"中文词(有锚点):{w}")
            else:
                reasons.append(f"中文词(无锚点,不计):{w}")

    # ── 模糊词最早位置 ──
    fuzzy_pos = None
    for w in FUZZY_WORDS:
        i = q.find(w)
        if i >= 0:
            p = i / n
            if fuzzy_pos is None or p < fuzzy_pos:
                fuzzy_pos = p
                reasons.append(f"模糊词:{w}")

    # S2 快路（门禁条件 2 口径）：纯中文不再强制降级 dual，改判 fts5 快路
    # 复用现有 mode 枚举 "fts5"（零新枚举）：l3_retrieval L454 已有 fts5、
    # L507 embed 白名单不含 fts5=不跑 embedding、L519 mode!=fts5 跳过 RRF=纯 FTS5 语义天然成立；
    # 低质命中由 L289 后回落重查层兜底（shadow 门控 + 软线 4.6329 判定）
    if pure_cn:
        reasons.append("纯中文→fts5快路")
        return "fts5", 0.80, reasons

    # 词序决策
    if strong_pos is not None and (fuzzy_pos is None or strong_pos < fuzzy_pos):
        return "fts5", 0.90, reasons or ["强特征主导"]
    if fuzzy_pos is not None and (strong_pos is None or fuzzy_pos <= strong_pos):
        return "dual", 0.85, reasons or ["模糊词主导"]
    return None, 0.0, reasons or ["无信号"]


def llm_fallback(query, decision):
    """LLM 兜底预留接口（长尾<10%场景，v0.2 暂不实现）。

    未来实现：对规则未命中/低置信 query 调用 LLM 判别路由。
    当前返回 None = 不介入 → 走强制 fallback（mode=dual）。
    签名已固定，后续实现无需改动调用点。
    """
    return None

# ── LRU 缓存（线程安全）──────────────────────────────────────────
class LRUCache:
    def __init__(self, cap):
        self.cap = max(cap, 1)
        self._d = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
                return self._d[key]
            return None

    def put(self, key, value):
        with self._lock:
            self._d[key] = value
            self._d.move_to_end(key)
            while len(self._d) > self.cap:
                self._d.popitem(last=False)

    def size(self):
        with self._lock:
            return len(self._d)

CACHE = LRUCache(CFG["cache_size"])

# ── 写代理：串行化 shadow 日志 + busy_timeout DB 连接 ─────────────
_write_lock = threading.Lock()

def shadow_log(entry):
    """shadow 记录（JSONL 追加）。写代理串行化：threading.Lock 保证并发安全。"""
    if not CFG.get("shadow", True):
        return
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        line = json.dumps(entry, ensure_ascii=False)
        with _write_lock:
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        pass  # shadow 失败不阻塞检索

def router_get_conn(timeout_ms=5000):
    """router 自有 DB 连接：busy_timeout >= 5000ms（写代理串行化配套）。"""
    timeout_ms = max(timeout_ms, 5000)
    conn = sqlite3.connect(l3_retrieval.DB_PATH, timeout=timeout_ms / 1000.0)
    try:
        conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn

# ── 路由主入口 ────────────────────────────────────────────────────
def route_search(query, mode="auto", top_k=5, tier="compact", industry=None, reader=None, track=None,
                 time_from=None, time_to=None, type_filter=None):
    """路由版主搜索。返回原结果全字段 + route meta 附加。
    P0-3: reader 参数（读审计记录者），默认取环境变量 SIKU_READER 或 "unknown"
    R1-C 记忆分轨: track=None|episodic|semantic 透传 l3_retrieval.search_memories（限轨检索）
    : time_from/time_to 时间过滤透传（None 不过滤 → 向后兼容零破坏）
    : type_filter 类型过滤透传（新类型可路由可查）

    - SIKU_ROUTER=off / router.enabled=false → 完全透传 l3_retrieval（零差异）
    - shadow=true → 决策只记录，实际执行用调用方 mode（纯观察）
    - shadow=false → 实际执行用决策 mode（规则生效）
    """
    # P0-3 修复（问题定位）：CLI 直调 route_search → 内部 search_memories 回调 route_search
    # 造成二次 log_read（reader=None→unknown）。复用 l3_retrieval 的 _ROUTER_IN_CALL 守卫：
    # 已处于路由递归中 → 直接透传 l3_retrieval 原始检索，不再审计、不再嵌套。
    if getattr(l3_retrieval, "_ROUTER_IN_CALL", False):
        return l3_retrieval.search_memories(query, mode=mode, top_k=top_k, tier=tier, industry=industry, track=track,
                                            time_from=time_from, time_to=time_to, type_filter=type_filter)
    t0 = time.time()
    # P0-3.1 修复：reader=None 时标记来源（避免 unknown 持续增长）
    # - 显式 reader → 用显式值
    # - SIKU_READER 环境变量 → 用 env
    # - 均未设（search_memories 内部调用）→ 标记 source:mcp/search_memories
    try:
        _reader = reader or os.environ.get("SIKU_READER") or "source:mcp/search_memories"
        audit_logger.log_read(_reader, "L3", "search", query[:100], "route_search")
    except Exception as _ae:
        print("P0-3 read-audit warn: %s" % _ae)
    if not CFG.get("enabled", False):
        return l3_retrieval.search_memories(query, mode=mode, top_k=top_k, tier=tier, industry=industry, track=track,
                                            time_from=time_from, time_to=time_to, type_filter=type_filter)

    cache_key = f"{query}|{mode}|{industry}|{track or ''}|{time_from or ''}|{time_to or ''}|{type_filter or ''}"
    cached = CACHE.get(cache_key)
    cache_hit = cached is not None
    if cached is None:
        if CFG.get("mode") == "semantic":
            # 语义路由预留：v0.2 未实现 → 走强制 fallback
            decision, conf, reasons = None, 0.0, ["semantic未实现→fallback"]
        else:
            decision, conf, reasons = rule_decision(query)
        # LLM 兜底预留（默认关）
        if decision is None and CFG.get("llm_fallback", False):
            llm_mode = llm_fallback(query, None)
            if llm_mode:
                decision, conf, reasons = llm_mode, 0.6, ["llm_fallback"]
        # 强制 fallback：规则未命中 → L3 全量向量
        if decision is None:
            decision = CFG.get("fallback", "dual")
            conf = 0.5
            reasons = reasons or ["未命中→强制fallback"]
        cached = (decision, conf, reasons)
        CACHE.put(cache_key, cached)
    decision, conf, reasons = cached

    # shadow：纯观察（实际走调用方 mode）；生效：走决策 mode
    eff_mode = mode if CFG.get("shadow", True) else decision
    result = l3_retrieval.search_memories(query, mode=eff_mode, top_k=top_k, tier=tier, industry=industry, track=track,
                                          time_from=time_from, time_to=time_to, type_filter=type_filter)

    # ── S2 快路回落重查层（门禁条件 2/3：shadow 门控 + 判定口径）────────────
    # 启用条件：shadow=false（决策生效态）且实际执行 mode=fts5 且 tier=compact；
    # shadow=true（默认/对比期）只记录 would_fallback 标志，不实际重查（保持纯观察）。
    # 判定口径：format_compact 输出 results[0].score（L375，含 lesson/insight +0.1 加权 L367，
    #           与 threshold.json 采样口径一致）；软线 4.6329 为主判据
    #           （time_desc 兜底 score=0.0 属预期触发回落），硬线 total==0 为兜底。
    # dual/auto 结果不做回落判定（防双倍耗时）。
    FALLBACK_SOFT_LINE = 4.6329
    would_fallback = False
    fallback_triggered = False
    if tier == "compact" and eff_mode == "fts5":
        try:
            _res0 = result.get("results") or []
            if result.get("total", 0) == 0:
                would_fallback = True  # 硬线：零结果返回
            elif _res0 and (float(_res0[0].get("score") or 0.0)) < FALLBACK_SOFT_LINE:
                would_fallback = True  # 软线：top1 低质命中
            if would_fallback and not CFG.get("shadow", True):
                # 重查一次 dual（fallback_triggered 标记防循环；route_search 内再调
                # search_memories 无自递归——l3_retrieval._ROUTER_IN_CALL 守卫已覆盖）
                fallback_result = l3_retrieval.search_memories(query, mode="dual", top_k=top_k, tier=tier, industry=industry, track=track,
                                                               time_from=time_from, time_to=time_to, type_filter=type_filter)
                if not fallback_result.get("error"):
                    result = fallback_result
                    fallback_triggered = True
        except Exception:
            pass  # 回落判定异常绝不影响主流程
    latency_ms = int((time.time() - t0) * 1000)

    # shadow 记录（query+决策+结果概要）
    shadow_log({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
        "query": query,
        "decision": decision,
        "confidence": round(conf, 3),
        "reasons": reasons,
        "eff_mode": eff_mode,
        "search_mode": result.get("search_mode", ""),
        "total": result.get("total", 0),
        "latency_ms": latency_ms,
        "would_fallback": would_fallback,
        "fallback_triggered": fallback_triggered,
    })

    # 原字段全保留 + route meta 附加（浅拷贝，不改 l3_retrieval 返回的 dict）
    out = dict(result)
    out["route"] = {
        "version": __version__,
        "enabled": True,
        "shadow": CFG.get("shadow", True),
        "decision": decision,
        "confidence": round(conf, 3),
        "reasons": reasons,
        "cache_hit": cache_hit,
        "eff_mode": eff_mode,
        "search_mode": result.get("search_mode", ""),
        "total": result.get("total", 0),
        "latency_ms": latency_ms,
        "would_fallback": would_fallback,
        "fallback_triggered": fallback_triggered,
    }
    return out

# ── CLI（与 l3_retrieval 同风格；expand/timeline 原样透传）────────
def main():
    p = argparse.ArgumentParser(description=f"L3 路由层 CLI（{__version__}，l3_retrieval 零改动）")
    sp = p.add_subparsers(dest="command")

    ps = sp.add_parser("search", help="路由搜索（原字段+route meta）")
    ps.add_argument("--reader", help="读取者标识（读审计用），默认环境变量 SIKU_READER")
    ps.add_argument("--query", "-q", required=True)
    ps.add_argument("--mode", choices=["auto", "fts5", "embed", "dual"], default="auto")
    ps.add_argument("--limit", "-n", type=int, default=5)
    ps.add_argument("--tier", choices=["compact", "expand", "full"], default="compact")
    ps.add_argument("--industry", help="按行业过滤")
    ps.add_argument("--track", choices=["episodic", "semantic"], default=None,
                    help="记忆分轨过滤（R1-C）：episodic=情景轨 / semantic=语义轨（缺省不限轨）")
    ps.add_argument("--since", help="时间过滤下界（ISO 或 epoch）")
    ps.add_argument("--until", help="时间过滤上界（ISO 或 epoch）")
    ps.add_argument("--pretty", action="store_true")

    pe = sp.add_parser("expand", help="展开单条（透传）")
    pe.add_argument("--id", required=True)
    pe.add_argument("--tier", choices=["expand", "full"], default="expand")
    pe.add_argument("--pretty", action="store_true")

    pt = sp.add_parser("timeline", help="时间轴（透传）")
    pt.add_argument("--start")
    pt.add_argument("--end")
    pt.add_argument("--limit", type=int, default=50)
    pt.add_argument("--pretty", action="store_true")

    pst = sp.add_parser("status", help="路由配置状态")
    pst.add_argument("--pretty", action="store_true")

    args = p.parse_args()
    if not args.command:
        p.print_help()
        return

    kw = {"ensure_ascii": False}
    if getattr(args, "pretty", False):
        kw["indent"] = 2

    if args.command == "search":
        result = route_search(args.query, mode=args.mode, top_k=args.limit,
                          reader=getattr(args, "reader", None),
                              tier=args.tier, industry=args.industry, track=getattr(args, "track", None),
                              time_from=getattr(args, "since", None), time_to=getattr(args, "until", None))
        print(json.dumps(result, **kw))
    elif args.command == "expand":
        print(json.dumps(l3_retrieval.expand_entry(args.id, tier=args.tier), **kw))
    elif args.command == "timeline":
        print(json.dumps(l3_retrieval.timeline(start=args.start, end=args.end, limit=args.limit), **kw))
    elif args.command == "status":
        print(json.dumps({
            "version": __version__,
            "enabled": CFG.get("enabled", False),
            "mode": CFG.get("mode"),
            "shadow": CFG.get("shadow", True),
            "fallback": CFG.get("fallback"),
            "llm_fallback": CFG.get("llm_fallback", False),
            "cache_size": CFG.get("cache_size"),
            "cache_current": CACHE.size(),
            "log_path": LOG_PATH,
            "env_override": os.environ.get("SIKU_ROUTER", "(unset)"),
        }, **kw))

if __name__ == "__main__":
    main()
