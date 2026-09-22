#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
蒸馏入库相似度去重守卫 (, 2026-08-16 维护者)

背景: 数据漂移避免①——入库相似度去重防检索池膨胀
  memory_store 实测近义重复 14,230 行 / content 精确重复 1,622 组（可回收约 60%）。
  蒸馏入口 (l4_smart.store / reta_pipeline.stage_l3_to_l4b) 原有 INSERT OR IGNORE
  （按 id）与文件存在（按 concept_slug）的【精确】去重——本模块为【相似度】去重增量。

相似度度量（对齐 text-similarity-clustering 技能实证）:
  sim = max(jieba 词集 Dice, 字符 bigram Dice)
  短文本 jieba 分词错位由字符 bigram 兜底；长文本以词级 Dice 为主。
  阈值可配（默认 0.9）：> 阈值 → 判为重复（跳过入库）。

误伤边界保护（高相似但真新知识 → 正常入库）:
  1. 度量本身: 单词翻转（打开/关闭、开启/停止）token Dice 通常 0.7~0.85 < 0.9 → 自然放行。
  2. 否定守卫: 若两文本差异 token 含否定/对比词（不/没/非/勿/无/别/禁/否/停/拒…）
     → 判为新知识（否定句/纠错句与原句语义相反，绝不能丢）→ 放行。
     方向保守: 宁可多留几条，绝不误杀真知识（安全可靠优先）。

性能（14k+ 条目）:
  - 字符 bigram 倒排索引做候选过滤（必要下界: Dice≥T ⇒ |A∩B| ≥ T·min(|A|,|B|)），
    只对候选计算精确 Dice；批量 add() 供同批内增量比较；索引构建实测 ~6.4s/23k 条。
  - 分词结果与 bigram 集合按条目缓存，重复比较零重算。

配置（阈值+开关，可禁用回退原行为）:
  - 配置文件: <本文件同目录>/distill_sim_dedup_config.json
      {"enabled": true, "threshold": 0.9, "min_len": 10, "negation_guard": true, "max_candidates": 500}
  - 环境变量覆盖: SIKU_DEDUP_ENABLED (0/1) / SIKU_DEDUP_THRESHOLD (float)
  - enabled=false 或异常 → is_duplicate 恒 False → 调用方走原逻辑（精确去重不变）。

纯 Python 实现（stdlib + jieba 可选，jieba 缺失自动降级字符 bigram）。
"""
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime  # : audit_warn 时间戳

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

try:
    import jieba
    jieba.setLogLevel(60)  # 静默初始化日志
    _HAS_JIEBA = True
except Exception:
    _HAS_JIEBA = False

_CFG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "distill_sim_dedup_config.json")

# (2026-08-17): 守卫绕过留痕——enabled=false / 导入失败 → audit WARN（gate_check 通道）
_AUDIT_DIR = os.environ.get("SIKU_AUDIT_DIR") or os.path.join(_SIKU_ROOT, "audit", "write")
_warned = set()

def audit_warn(tag, detail):
    """审计 WARN 写入（gate_check 通道: audit/write/audit-YYYY-MM-DD.yaml, alert: yes）。
    每进程每 tag 仅写一次（防刷屏）。"""
    if tag in _warned:
        return
    _warned.add(tag)
    try:
        os.makedirs(_AUDIT_DIR, exist_ok=True)
        af = os.path.join(_AUDIT_DIR, "audit-%s.yaml" % datetime.now().strftime("%Y-%m-%d"))
        with open(af, "a") as f:
            f.write("---\nts: %s\nop: %s\nop_type: warn\neid: %s\ntype: guard\nsummary_a: %s\nalert: yes\n---\n"
                    % (datetime.now().isoformat(), tag, tag, detail))
    except Exception:
        pass

_DEFAULT_CFG = {
    "enabled": True,
    "threshold": 0.9,
    "min_len": 8,           # 归一化后短于此长度的文本不建索引（精确匹配仍生效）
    "negation_guard": True,
    "max_candidates": 500,  # 候选过滤后的计算上限（防极端长尾拖慢）
}

# 否定/对比/纠错 关键词——差异 token 命中任一（含子串）→ 判为新知识（放行）
_NEG_CHARS = "不没非勿无别禁否停拒免莫未休"
_NEG_TOKENS = ("取消", "删除", "撤销", "关闭", "停止", "禁止", "拒绝", "无法",
               "不能", "不会", "不要", "无需", "不得", "不可", "不准", "尚未")

_PUNCT_RE = re.compile(r"^[\s\W_]+$", re.UNICODE)
_WS_RE = re.compile(r"\s+")


def load_config():
    """读取配置: 文件 + 环境变量覆盖。文件缺失/损坏 → 默认值（enabled=true, 0.9）。"""
    cfg = dict(_DEFAULT_CFG)
    try:
        with open(_CFG_PATH, encoding="utf-8") as f:
            file_cfg = json.load(f)
        if isinstance(file_cfg, dict):
            cfg.update(file_cfg)
    except Exception:
        pass
    # 环境变量覆盖（运行时开关，无需改文件）
    env_enabled = os.environ.get("SIKU_DEDUP_ENABLED")
    if env_enabled is not None:
        cfg["enabled"] = env_enabled.strip().lower() in ("1", "true", "yes", "on")
    env_threshold = os.environ.get("SIKU_DEDUP_THRESHOLD")
    if env_threshold is not None:
        try:
            cfg["threshold"] = float(env_threshold)
        except ValueError:
            pass
    try:
        cfg["threshold"] = float(cfg.get("threshold", 0.9))
    except (TypeError, ValueError):
        cfg["threshold"] = 0.9
    return cfg


def normalize(text):
    """归一化: 去全部空白（中文分词前不留空格噪音）"""
    if not text:
        return ""
    return _WS_RE.sub("", str(text)).strip()


def tokenize(text):
    """jieba 分词（缺 jieba 降级单字）；过滤纯标点 token"""
    if _HAS_JIEBA:
        toks = [t for t in jieba.lcut(text) if t.strip() and not _PUNCT_RE.match(t)]
    else:
        toks = [c for c in text if c.strip() and not _PUNCT_RE.match(c)]
    return toks


def char_bigrams(text):
    """字符 bigram 集合（长度<2 返回空集）"""
    return {text[i:i + 2] for i in range(len(text) - 1)}


def _dice(set_a, set_b):
    if not set_a or not set_b:
        return 0.0
    return 2.0 * len(set_a & set_b) / (len(set_a) + len(set_b))


def _is_negation_diff(diff_tokens):
    """差异 token 是否含否定/对比语义（含子串匹配，覆盖 jieba 复合词）"""
    for tok in diff_tokens:
        if any(c in tok for c in _NEG_CHARS):
            return True
        if any(w in tok for w in _NEG_TOKENS):
            return True
    return False


class SimDedupGuard:
    """蒸馏入库相似度去重守卫。

    用法:
        guard = SimDedupGuard()            # 读取配置（阈值/开关）
        guard.load_from_db(conn)           # 用既有 memory_store 条目建索引（14k+ 可接受）
        # 或: guard.add(entry_text) 逐条喂入
        dup, sim, reason = guard.is_duplicate(new_content)
        if not dup:
            ...入库...
            guard.add(new_content)         # 注册，供同批后续条目比较
    """

    def __init__(self, threshold=None, enabled=None, negation_guard=None):
        cfg = load_config()
        self.threshold = float(threshold) if threshold is not None else cfg["threshold"]
        self.enabled = bool(enabled) if enabled is not None else cfg["enabled"]
        self.negation_guard = bool(negation_guard) if negation_guard is not None else cfg.get("negation_guard", True)
        self.min_len = int(cfg.get("min_len", 10))
        self.max_candidates = int(cfg.get("max_candidates", 500))
        # (2026-08-17): 守卫绕过留痕——被禁用不静默
        if not self.enabled:
            audit_warn("dedup-guard-disabled",
                       "distill_sim_dedup enabled=false → 相似度去重关闭（精确去重/INSERT OR IGNORE 不变）")
        self._entries = []                # [{norm, tokens, bigrams}]
        self._idx = defaultdict(list)     # bigram -> [entry_idx]
        self._tok_idx = defaultdict(list)  # token -> [entry_idx]（词级候选过滤，防 max 度量漏检）
        self.stats = {"indexed": 0, "checked": 0, "duplicate": 0,
                      "negation_kept": 0, "exact": 0}

    # ── 索引构建 ──────────────────────────────────────────

    def add(self, text):
        """注册一条既有/新入库文本到索引（归一化后 <min_len 只留精确匹配，不建索引）"""
        norm = normalize(text)
        if not norm:
            return
        entry = {"norm": norm, "tokens": set(), "bigrams": set()}
        if len(norm) >= self.min_len:
            entry["tokens"] = set(tokenize(norm))
            entry["bigrams"] = char_bigrams(norm)
            for b in entry["bigrams"]:
                self._idx[b].append(len(self._entries))
            for t in entry["tokens"]:
                self._tok_idx[t].append(len(self._entries))
        self._entries.append(entry)
        self.stats["indexed"] += 1

    def load_from_db(self, conn, where="deprecated IS NULL OR deprecated != 1"):
        """从 memory_store 批量建索引（summary+content 拼接）。返回索引条数。"""
        try:
            rows = conn.execute(
                "SELECT summary, content FROM memory_store WHERE %s" % where
            ).fetchall()
        except Exception:
            rows = conn.execute("SELECT summary, content FROM memory_store").fetchall()
        for summary, content in rows:
            self.add((summary or "") + "\n" + (content or ""))
        return self.stats["indexed"]

    # ── 判定 ──────────────────────────────────────────────

    def is_duplicate(self, text):
        """返回 (is_dup: bool, sim: float, reason: str)。
        开关禁用/索引为空 → (False, 0.0, 'disabled'/'no_index')，调用方走原行为。"""
        if not self.enabled:
            return False, 0.0, "disabled"
        self.stats["checked"] += 1
        norm = normalize(text)
        if not norm:
            return False, 0.0, "empty"
        # 1) 精确匹配快路径
        for e in self._entries:
            if e["norm"] == norm:
                self.stats["exact"] += 1
                self.stats["duplicate"] += 1
                return True, 1.0, "exact"
        # 2) 候选过滤（双倒排索引）: 度量=max(词级Dice, bigram Dice)，
        #    必要下界（各度量独立）: Dice≥T ⇒ |∩| ≥ T·min(|A|,|B|)
        #    词级/字符级任一满足即可能达标 → 并集候选，不漏检
        my_bigrams = char_bigrams(norm)
        my_tokens = set(tokenize(norm))
        if not my_bigrams:
            return False, 0.0, "too_short"
        bg_overlap = defaultdict(int)
        for b in my_bigrams:
            for idx in self._idx[b]:
                bg_overlap[idx] += 1
        tok_overlap = defaultdict(int)
        for t in my_tokens:
            for idx in self._tok_idx[t]:
                tok_overlap[idx] += 1
        my_bg_len = len(my_bigrams)
        my_tok_len = len(my_tokens)
        candidate_set = set()
        for idx, cnt in bg_overlap.items():
            e_len = len(self._entries[idx]["bigrams"])
            if cnt >= self.threshold * min(my_bg_len, e_len):
                candidate_set.add(idx)
        for idx, cnt in tok_overlap.items():
            e_tok_len = len(self._entries[idx]["tokens"])
            if cnt >= self.threshold * min(my_tok_len, e_tok_len):
                candidate_set.add(idx)
        candidates = sorted(
            candidate_set,
            key=lambda i: -max(bg_overlap.get(i, 0), tok_overlap.get(i, 0)))
        if len(candidates) > self.max_candidates:
            candidates = candidates[:self.max_candidates]
        # 3) 精确度量（只对候选）
        best_sim, best_reason, best_diff = 0.0, None, None
        for idx in candidates:
            e = self._entries[idx]
            sim = max(_dice(my_tokens, e["tokens"]), _dice(my_bigrams, e["bigrams"]))
            if sim > best_sim:
                best_sim = sim
                best_diff = (my_tokens - e["tokens"]) | (e["tokens"] - my_tokens)
                best_reason = e
        if best_sim < self.threshold:
            return False, best_sim, "below_threshold"
        # 4) 误伤边界: 否定/对比差异 → 真新知识放行
        if self.negation_guard and best_diff and _is_negation_diff(best_diff):
            self.stats["negation_kept"] += 1
            return False, best_sim, "negation_kept"
        self.stats["duplicate"] += 1
        return True, best_sim, "sim:%.3f" % best_sim


def make_guard(conn=None):
    """便捷工厂: 可选从 DB 建索引；任何异常 → 返回禁用守卫（回退原行为，安全兜底）。"""
    try:
        g = SimDedupGuard()
        if conn is not None:
            g.load_from_db(conn)
        return g
    except Exception:
        try:
            return SimDedupGuard(enabled=False)
        except Exception:
            return _DisabledGuard()


class _DisabledGuard:
    """绝对兜底：任何情况都不拦截（回退原行为）"""

    def __init__(self):
        self.stats = {"checked": 0, "duplicate": 0, "negation_kept": 0, "exact": 0}

    def add(self, text):
        pass

    def is_duplicate(self, text):
        return False, 0.0, "disabled"


if __name__ == "__main__":
    # 自检: python3 distill_sim_dedup.py
    g = SimDedupGuard()
    cfg = load_config()
    print("config: %s" % json.dumps(cfg, ensure_ascii=False))
    print("jieba: %s | threshold=%.2f | enabled=%s" % (_HAS_JIEBA, g.threshold, g.enabled))
    pairs = [
        ("看板任务状态的查看方法", "看板任务状态查看方法", True),
        # 误伤边界①: 插入否定词 → 语义相反 → 必须放行（否定守卫）
        ("系统每周六上午10点自动执行备份任务", "系统每周六上午10点不自动执行备份任务", False),
        # 误伤边界②: 反义词翻转 → 语义相反 → 度量自然低于阈值 → 放行
        ("禁止在雨天打开服务器机柜", "禁止在雨天关闭服务器机柜", False),
        ("服务器机柜温度监控方案", "看板任务状态查看方法", False),
    ]
    any_fail = False
    for a, b, expect in pairs:
        g = SimDedupGuard()
        g.add(a)
        dup, sim, reason = g.is_duplicate(b)
        mark = "PASS" if dup == expect else "FAIL"
        if dup != expect:
            any_fail = True
        print("[%s] sim=%.3f reason=%-16s %r vs %r" % (mark, sim, reason, a, b))
    if any_fail:
        sys.exit(1)
