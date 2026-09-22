#!/usr/bin/env python3
"""
siku_conflict.py — 跨库冲突判定：一套判定 + 双适配器

三库优化第二批子卡 2/4（跨库冲突机制——独立/扩展/矛盾三分类，不静默覆盖）。

架构（方案 10-三库优化方案-20260828.md 第二轮收敛）：
- 唯一判定逻辑：归一化 → 主题相似对比 → 三分类（independent/extension/conflict），
  矛盾再分三子类（同 summary 异结论 / 同事件异事实 / 同规则异语义）。
- 双适配器：gene（memory_fragments 表）/ 四库（memory_store 表）——
  库路径 + 表名 + 字段映射全部参数化，一套判定两端复用（R13.18 唯一权威源）。
- 分类枚举唯一权威源：siku_types.py（本模块只引用，禁本地硬编码清单，防漂移）。

影子模式（默认，G3 转正前）：
- mode="shadow"：只记录 conflict_type/conflict_state + audit 关联（矛盾双向留痕），
  任何情况 verdict="ok" 照常写入——不拦截。
- mode="enforce"：矛盾 → verdict="blocked"（真拦截，G3 门禁达标后启用）。
- mode="off"：零 diff 完全跳过（回退开关）。
- 误判低置信：边缘相似判 independent，仅留 audit（三问必答盲区：误判拒合法更新）。

用法：
  from siku_conflict import SikuAdapter, detect_for_write
  adapter = SikuAdapter("/path/memory_store.db", "memory_store")   # 四库
  adapter = GeneAdapter("/path/memory.db", "memory_fragments")     # 基因
  res = detect_for_write(adapter, conn, new_entry, mode="shadow")

纯 stdlib（0token）——无 LLM 调用，写入端 cron 场景零成本。
"""

import os
import re
import sqlite3
from difflib import SequenceMatcher

# ── 分类枚举唯一权威源：siku_types.py（R13.18——禁本地硬编码）────────
try:
    import siku_types
except ImportError:
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import siku_types

# 三分类（引用权威源，保证同源）
INDEPENDENT = siku_types.CONFLICT_TYPES[0]   # "independent"
EXTENSION = siku_types.CONFLICT_TYPES[1]     # "extension"
CONFLICT = siku_types.CONFLICT_TYPES[2]      # "conflict"
# 矛盾子类（引用权威源）
SUB_SUMMARY_CONCLUSION = siku_types.CONFLICT_SUBTYPES[0]
SUB_EVENT_FACT = siku_types.CONFLICT_SUBTYPES[1]
SUB_RULE_SEMANTIC = siku_types.CONFLICT_SUBTYPES[2]

# ── 判定阈值（保守取向：宁可判独立/扩展，不误判矛盾——盲区=误判拒合法更新）──
SIM_RELATED = 0.30      # 归一化相似度 < 此值且主题词重叠 < 阈值 → independent
SIM_HIGH = 0.50         # 高相似（否定翻转判定门）
TOPIC_RELATED = 0.16    # 主题词 Jaccard 重叠门（CJK bigram + 英文词）
ANT_SIM_MIN = 0.30      # 反义词对命中的最低相似度门
NUM_SIM_MIN = 0.42      # 数字冲突的最低相似度门
NUM_CAND_LIMIT = 5      # 候选旧条目上限
NUM_PRESCAN_LIMIT = 200 # 预筛扫描上限（LIKE 粗筛后精算）

# ── 停用字/词（归一化+关键词提取用；保守清单，仅去除纯虚词）────────
STOP_CHARS = set("的了是在与和及或就都也很于这那之而以对其为因由被把")
STOP_WORDS = {
    "一个", "这个", "那个", "可以", "需要", "应该", "必须", "进行", "通过",
    "我们", "你们", "他们", "相关", "以及", "并且", "因为", "所以", "如果",
    "但是", "还是", "或者", "对于", "关于", "同时", "目前", "已经", "正在",
    "the", "a", "an", "of", "to", "in", "on", "for", "and", "or", "is",
    "are", "was", "were", "be", "been", "with", "that", "this", "it",
}

# ── 否定词（结论方向翻转信号）──────────────────────────────
NEGATION_WORDS = [
    "不", "未", "无", "非", "勿", "别", "莫", "禁", "禁止", "避免",
    "不可", "不能", "不得", "不要", "无法", "不应该", "不需要", "没必要",
    "never", "not", "no", "don't", "dont", "cannot", "can't", "cant",
    "shouldn't", "shouldnt", "mustn't", "mustnt", "won't", "wont",
]

# ── 规则词（规则语义类信号）────────────────────────────────
RULE_WORDS = [
    "规则", "应该", "必须", "禁止", "不要", "允许", "需要", "务必",
    "always", "never", "must", "should", "shall", "required", "禁止",
    "do not", "don't", "dont",
]

# ── 反义词对（同主题相反结论信号；对内两词互斥方向）──────────────
ANTONYM_PAIRS = [
    ("提高", "降低"), ("提升", "下降"), ("增加", "减少"), ("上升", "下降"),
    ("上涨", "下跌"), ("成功", "失败"), ("正确", "错误"), ("有效", "无效"),
    ("启用", "禁用"), ("开启", "关闭"), ("允许", "禁止"), ("支持", "反对"),
    ("增强", "削弱"), ("加速", "减速"), ("扩张", "收缩"), ("买入", "卖出"),
    ("应该", "不应该"), ("必须", "禁止"), ("快", "慢"), ("多", "少"),
    ("高", "低"), ("大", "小"), ("好", "坏"), ("安全", "危险"),
    ("涨", "跌"), ("开", "关"), ("新", "旧"), ("强", "弱"),
]

# ── 数字量词上下文（数字冲突须共享量词才成立——防误伤时间/序号）──
NUM_UNIT_CONTEXT = ("万", "元", "人", "日", "年", "月", "天", "小时", "分钟",
                    "秒", "%", "％", "倍", "次", "个", "台", "家", "公里",
                    "米", "斤", "吨", "度", "层", "版", "期", "号", "点")


# ═════════════════════ 归一化与相似度 ═════════════════════

def normalize_text(text):
    """归一化：小写、全半角统一、去标点空白、压缩空白。返回规范化串。"""
    if not text:
        return ""
    t = str(text)
    # 全角 → 半角
    out = []
    for ch in t:
        code = ord(ch)
        if code == 0x3000:
            code = 0x20
        elif 0xFF01 <= code <= 0xFF5E:
            code -= 0xFEE0
        out.append(chr(code))
    t = "".join(out).lower()
    # 去标点/空白（保留 CJK 字与 ASCII 字母数字）
    t = re.sub(r"[^\w\u4e00-\u9fff]+", "", t)
    return t


def _cjk_bigrams(text):
    """CJK 字符 bigram（去停用字）——中文主题词"""
    grams = set()
    chars = [c for c in text if "\u4e00" <= c <= "\u9fff"]
    for i in range(len(chars) - 1):
        a, b = chars[i], chars[i + 1]
        if a not in STOP_CHARS and b not in STOP_CHARS:
            grams.add(a + b)
    return grams


def _en_words(text):
    """英文词（去停用词）"""
    words = re.findall(r"[a-z][a-z0-9]+", text)
    return {w for w in words if w not in STOP_WORDS and len(w) >= 2}


def _char_ngram_jaccard(a, b, n=3):
    """字符 n-gram Jaccard（归一化串）"""
    if not a or not b:
        return 0.0
    if len(a) < n or len(b) < n:
        # 短串回退到字符集 Jaccard
        sa, sb = set(a), set(b)
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)
    ga = {a[i:i + n] for i in range(len(a) - n + 1)}
    gb = {b[i:i + n] for i in range(len(b) - n + 1)}
    return len(ga & gb) / len(ga | gb)


def similarity(a, b):
    """组合相似度：max(字符 3-gram Jaccard, SequenceMatcher ratio) ∈ [0,1]"""
    na, nb = normalize_text(a), normalize_text(b)
    if not na or not nb:
        return 0.0
    jac = _char_ngram_jaccard(na, nb)
    sm = SequenceMatcher(None, na, nb).ratio()
    return max(jac, sm)


def topic_overlap(a, b):
    """主题词重叠度：CJK bigram ∪ 英文词 的 Jaccard ∈ [0,1]"""
    na, nb = normalize_text(a), normalize_text(b)
    ta, tb = _cjk_bigrams(na) | _en_words(na), _cjk_bigrams(nb) | _en_words(nb)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def shared_grams(a, b):
    """共享主题词绝对数（CJK bigram ∪ 英文词 交集大小）——短文本强主题信号。

     沙盒发现：比例阈值对短句过严（如"冲突检测…归一化"
    vs "冲突检测…n-gram" 共享 3 词但比例仅 0.13）——共享 ≥2 个双字词即主题相关。
    """
    na, nb = normalize_text(a), normalize_text(b)
    ta, tb = _cjk_bigrams(na) | _en_words(na), _cjk_bigrams(nb) | _en_words(nb)
    return len(ta & tb)


def _keywords(text, max_kw=5):
    """候选预筛关键词：中文 bigram 配额 3 + 英文词配额 2（中英混合防英文长词挤掉中文主题词）。

     沙盒发现：单按长度取 top3 会把中文核心词挤出
    （如"冲突检测使用字符n-gram计算相似度"→ 取到 ngram/似度/使用，丢"冲突"）——召回不足。
    """
    nt = normalize_text(text)
    cjk = sorted(_cjk_bigrams(nt), key=lambda g: (-len(g), g))[:3]
    en = sorted(_en_words(nt), key=lambda g: (-len(g), g))[:2]
    kws = cjk + en
    return kws[:max_kw]


# ═════════════════════ 反向信号（矛盾证据） ═════════════════════

def _negation_score(text):
    """否定词命中数"""
    if not text:
        return 0
    t = str(text).lower()
    return sum(t.count(w) for w in NEGATION_WORDS)


def _rule_style(text):
    """规则词命中数（规则语义类信号）"""
    if not text:
        return 0
    t = str(text).lower()
    return sum(t.count(w) for w in RULE_WORDS)


def _antonym_hit(text_a, text_b):
    """反义词对检测：返回 (hit, pair)——a 含 pair[0] 且 b 含 pair[1]，或反之"""
    ta, tb = str(text_a).lower(), str(text_b).lower()
    for w1, w2 in ANTONYM_PAIRS:
        if (w1 in ta and w2 in tb) or (w2 in ta and w1 in tb):
            return True, (w1, w2)
    return False, None


def _extract_numbers(text):
    """提取数字及相邻 2 字符上下文（量词）：返回 [(num, ctx)]"""
    found = []
    for m in re.finditer(r"\d+(?:\.\d+)?", str(text)):
        start, end = m.span()
        ctx = text[max(0, start - 2):start] + text[end:end + 2]
        found.append((m.group(), ctx))
    return found


def _num_conflict(new_text, old_text, sim):
    """数字冲突：sim 达标 + 双方都有数字 + 共享量词上下文 + 数字不一致"""
    if sim < NUM_SIM_MIN:
        return False
    n_new = _extract_numbers(new_text)
    n_old = _extract_numbers(old_text)
    if not n_new or not n_old:
        return False
    ctx_new = {c for _, c in n_new if any(u in c for u in NUM_UNIT_CONTEXT)}
    ctx_old = {c for _, c in n_old if any(u in c for u in NUM_UNIT_CONTEXT)}
    if not (ctx_new & ctx_old):
        return False
    nums_new = {n for n, _ in n_new}
    nums_old = {n for n, _ in n_old}
    if nums_new == nums_old:
        return False  # 数字一致（如版本号/编号引用）不算冲突
    return True


# ═════════════════════ 核心三分类判定 ═════════════════════

def classify(new_text, existing_text, new_type="", existing_type=""):
    """单对判定（唯一判定逻辑）。

    返回 (conflict_type, subtype, confidence, matched)——三分类 + 矛盾子类 + 置信度 + 是否匹配到旧条目。
    规则（保守取向）：
      1. 相似/主题都不达标           → independent（低置信仅留 audit）
      2. 规则型 且（反义 或 否定翻转）→ conflict_rule_semantic
      3. 反义词对命中（sim≥门）      → conflict（规则型→rule_semantic，否则 summary_conclusion）
      4. sim 高 且 否定极性翻转       → conflict_summary_conclusion
      5. 数字冲突（sim≥门+共享量词）  → conflict_event_fact
      6. 其余相关                     → extension
    """
    sim = similarity(new_text, existing_text)
    topic = topic_overlap(new_text, existing_text)
    shared = shared_grams(new_text, existing_text)

    # ① 独立：主题无关（相似/比例/绝对共享数 全不达标）
    if sim < SIM_RELATED and topic < TOPIC_RELATED and shared < 2:
        return INDEPENDENT, "", round(0.55 + 0.3 * sim, 2), False

    rule_new, rule_old = _rule_style(new_text), _rule_style(existing_text)
    ant_hit, ant_pair = _antonym_hit(new_text, existing_text)
    neg_new, neg_old = _negation_score(new_text), _negation_score(existing_text)
    neg_flip = (neg_new == 0) != (neg_old == 0) or (neg_new > 0 and neg_old > 0 and neg_new != neg_old)
    is_rule = (rule_new > 0 or rule_old > 0)

    # ② 规则语义相反（规则型 + 反向信号）
    if is_rule and (ant_hit or neg_flip):
        return CONFLICT, SUB_RULE_SEMANTIC, round(0.6 + 0.25 * sim, 2), True

    # ③ 反义词对（方向相反，含明确反义对——强信号）
    if ant_hit and sim >= ANT_SIM_MIN:
        sub = SUB_RULE_SEMANTIC if is_rule else SUB_SUMMARY_CONCLUSION
        return CONFLICT, sub, round(0.6 + 0.25 * sim, 2), True

    # ④ 高相似 + 否定极性翻转（同 summary 异结论）
    if sim >= SIM_HIGH and neg_flip:
        return CONFLICT, SUB_SUMMARY_CONCLUSION, round(0.6 + 0.3 * sim, 2), True

    # ⑤ 数字事实冲突（同事件异事实）
    if _num_conflict(new_text, existing_text, sim):
        return CONFLICT, SUB_EVENT_FACT, round(0.6 + 0.2 * sim, 2), True

    # ⑥ 扩展：相关但无反向信号
    return EXTENSION, "", round(0.5 + 0.3 * sim, 2), True


# ═════════════════════ 双适配器（参数化库路径+表名+字段映射） ═════════════════════

class BaseAdapter:
    """适配器基类——库路径 + 表名 + 字段映射全参数化，一套判定两端复用。

    默认字段映射（memory_store 风格）；GeneAdapter 覆写差异字段。
    """

    TABLE = ""
    FIELDS = {
        "id": "id",
        "type": "type",
        "summary": "summary",
        "content": "content",
        "conflict_col": "conflict_type",   # 分类结果写入列
        "audit_col": "audit_log",          # audit JSON 列（无则 None）
        "ts_col": "timestamp",
    }

    def __init__(self, db_path, table=None, fields=None):
        self.db_path = db_path
        if table:
            self.TABLE = table
        if fields:
            f = dict(self.FIELDS)
            f.update(fields)
            self.FIELDS = f

    def _cols(self):
        f = self.FIELDS
        cols = [f["id"], f["summary"], f["content"]]
        if f.get("type"):
            cols.append(f["type"])
        if f.get("ts_col"):
            cols.append(f["ts_col"])
        return list(dict.fromkeys(cols))

    def candidates(self, conn, summary, limit=NUM_CAND_LIMIT):
        """候选旧条目：主题词 LIKE 粗筛（限量）→ 返回 [{id, summary, content, type, ts}]"""
        kws = _keywords(summary)
        if not kws:
            return []
        f = self.FIELDS
        cols = ", ".join('"%s"' % c for c in self._cols())
        where = " OR ".join(['"%s" LIKE ?' % f["summary"]] * len(kws))
        sql = ('SELECT %s FROM "%s" WHERE %s ORDER BY "%s" DESC LIMIT ?'
               % (cols, self.TABLE, where, f.get("ts_col") or f["id"]))
        try:
            rows = conn.execute(sql, *([] if False else [tuple("%%%s%%" % k for k in kws) + (limit,)])).fetchall()
        except sqlite3.Error:
            return []
        out = []
        for r in rows:
            d = dict(zip(self._cols(), r))
            out.append({
                "id": d.get(f["id"]),
                "summary": d.get(f["summary"], "") or "",
                "content": d.get(f["content"], "") or "",
                "type": d.get(f["type"], "") or "",
                "ts": d.get(f["ts_col"], "") or "",
            })
        return out

    def ensure_conflict_column(self, conn):
        """幂等 ADD COLUMN（零迁移——大库不重写）"""
        f = self.FIELDS
        col = f.get("conflict_col")
        if not col:
            return
        existing = {r[1] for r in conn.execute("PRAGMA table_info(%s)" % self.TABLE)}
        if col not in existing:
            conn.execute('ALTER TABLE "%s" ADD COLUMN "%s" TEXT DEFAULT \'\'' % (self.TABLE, col))

    def mark(self, conn, row_id, conflict_type, subtype="", conflict_with=None, mode="shadow", low_confidence=False):
        """写入分类结果 + audit 留痕（影子模式不拦截）。返回 True=已标记。

        row_id = 被标记条目；conflict_with = 关联的另一方条目（可选，audit 留痕粒度）。
        双向留痕由调用方完成（新条目 INSERT 时写列+audit；旧条目在此标记+audit）。
        """
        f = self.FIELDS
        col = f.get("conflict_col")
        if col:
            conn.execute('UPDATE "%s" SET "%s"=? WHERE "%s"=?' % (self.TABLE, col, f["id"]),
                         (conflict_type, row_id))
        acol = f.get("audit_col")
        if acol:
            existing = conn.execute('SELECT "%s" FROM "%s" WHERE "%s"=?' % (acol, self.TABLE, f["id"]),
                                    (row_id,)).fetchone()
            try:
                audit = json_loads(existing[0]) if existing and existing[0] else []
            except Exception:
                audit = []
            rec = {
                "action": "conflict_detect",
                "conflict_type": conflict_type,
                "mode": mode,
            }
            if subtype:
                rec["subtype"] = subtype
            if conflict_with:
                rec["conflict_with"] = conflict_with
            if low_confidence:
                rec["low_confidence"] = True
            audit.append(rec)
            conn.execute('UPDATE "%s" SET "%s"=? WHERE "%s"=?' % (self.TABLE, acol, f["id"]),
                         (json_dumps(audit), row_id))


class GeneAdapter(BaseAdapter):
    """基因适配器：memory_fragments 表（conflict_state 列；无 audit_log 列）"""

    FIELDS = {
        "id": "id",
        "type": "type",
        "summary": "summary",
        "content": "content",
        "conflict_col": "conflict_state",   # 第一批已加事务字段
        "audit_col": None,                   # 基因表无 audit_log——仅列标记留痕
        "ts_col": "timestamp",
    }

    def mark(self, conn, row_id, conflict_type, subtype="", conflict_with=None, mode="shadow", low_confidence=False):
        """基因侧：仅写 conflict_state（independent 不写保持空=无冲突，与第一批语义兼容）。
        矛盾双向：旧条目 conflict_state 也标 conflict（双向可见——基因表无 audit 列，状态列留痕）。"""
        f = self.FIELDS
        col = f.get("conflict_col")
        if not col:
            return False
        if conflict_type == INDEPENDENT:
            return False  # 独立不标记（空=无冲突，存量零干扰）
        conn.execute('UPDATE "%s" SET "%s"=? WHERE "%s"=?' % (self.TABLE, col, f["id"]),
                     (conflict_type, row_id))
        # 双向留痕：关联的另一方也标记（矛盾零静默覆盖的基因侧证据）
        if conflict_type == CONFLICT and conflict_with:
            try:
                conn.execute('UPDATE "%s" SET "%s"=? WHERE "%s"=?' % (self.TABLE, col, f["id"]),
                             (CONFLICT, conflict_with))
            except sqlite3.Error:
                pass
        return True


class SikuAdapter(BaseAdapter):
    """四库适配器：memory_store 表（conflict_type 列 + audit_log 双向留痕）"""

    FIELDS = {
        "id": "id",
        "type": "type",
        "summary": "summary",
        "content": "content",
        "conflict_col": "conflict_type",
        "audit_col": "audit_log",
        "ts_col": "timestamp",
    }

    def mark(self, conn, row_id, conflict_type, subtype="", conflict_with=None, mode="shadow", low_confidence=False):
        """四库侧：写 conflict_type + audit_log 追加 conflict_detect 记录。

        矛盾双向：对关联方（旧条目）也标记 conflict + audit（矛盾零静默覆盖证据——
        旧条目不被覆盖，且双向留痕）。调用方保证两方均已存在（旧条目在库，新条目 INSERT 后补标）。
        """
        super().mark(conn, row_id, conflict_type, subtype, conflict_with, mode, low_confidence)
        if conflict_type == CONFLICT and conflict_with:
            try:
                super().mark(conn, conflict_with, CONFLICT, subtype=subtype,
                             conflict_with=row_id, mode=mode, low_confidence=low_confidence)
            except sqlite3.Error:
                pass
        return True


# ═════════════════════ 主入口（写入端挂接） ═════════════════════

def detect_for_write(adapter, conn, new_entry, mode="shadow", new_id=None, max_conflicts=3):
    """写入前冲突检测——写入端统一挂接点（reta_pipeline / gene_writer 双端复用）。

    参数：
      adapter    — GeneAdapter 或 SikuAdapter（库路径/表名/字段映射参数化）
      conn       — 已连接的目标库连接
      new_entry  — {"summary":..., "content":..., "type":...}
      mode       — "shadow"（默认：只记录不拦截）/ "enforce"（矛盾拦截）/ "off"（零 diff 跳过）
      new_id     — 新条目 id（可选；矛盾时对旧条目做双向关联留痕需要）
    返回 dict：
      verdict        — "ok"（照常写入）| "blocked"（仅 enforce 下矛盾时）
      conflict_type  — ""（off/无候选）| independent | extension | conflict
      subtype        — 矛盾子类（非矛盾为空串）
      conflict_with  — 关联旧条目 id（矛盾/扩展命中时）
      matched        — bool 是否命中相关旧条目
      low_confidence — bool 边缘判定（仅留 audit）
      audits         — [audit dict]（调用方合并进新条目 audit_log；矛盾含 conflict_with）
    """
    result = {
        "verdict": "ok", "conflict_type": "", "subtype": "",
        "conflict_with": None, "matched": False,
        "low_confidence": False, "audits": [],
    }
    if mode == "off":
        return result
    summary = (new_entry.get("summary") or "").strip()
    if not summary:
        return result
    try:
        adapter.ensure_conflict_column(conn)
    except sqlite3.Error:
        pass

    cands = adapter.candidates(conn, summary)
    if not cands:
        # 无相关候选 = 最强独立证据（主题无任何关联）——返回 independent 判定（matched=False）
        result["conflict_type"] = INDEPENDENT
        return result

    # 最强命中：矛盾(rank0) > 扩展(rank1) > 独立(rank2)；同 rank 取先命中者
    best = None  # (rank, ctype, subtype, matched_id, confidence)
    for c in cands:
        ctype, subtype, conf, matched = classify(
            summary, c["summary"],
            new_entry.get("type", ""), c.get("type", ""),
        )
        if not matched:
            continue
        rank = 0 if ctype == CONFLICT else (1 if ctype == EXTENSION else 2)
        if best is None or rank < best[0]:
            best = (rank, ctype, subtype, c["id"], conf)
        if best[0] == 0 and rank == 0:
            break  # 已命中矛盾——无需再扫（标记一对即可）

    if best is None:
        return result

    _, ctype, subtype, matched_id, conf = best
    result["conflict_type"] = ctype
    result["subtype"] = subtype
    result["conflict_with"] = matched_id
    result["matched"] = True
    result["audits"] = [{
        "action": "conflict_detect",
        "conflict_type": ctype,
        "subtype": subtype,
        "matched_id": matched_id,
        "confidence": conf,
    }]

    # 低置信边缘判定（sim 恰在阈值边缘且无强反向信号）→ 仅留 audit
    if ctype == EXTENSION:
        sim0 = similarity(summary, next(
            (c["summary"] for c in cands if c["id"] == matched_id), ""))
        if SIM_RELATED <= sim0 < SIM_RELATED + 0.06:
            result["low_confidence"] = True

    # enforce：矛盾 → 拦截（G3 转正后启用；影子阶段永不触发）
    if mode == "enforce" and ctype == CONFLICT:
        result["verdict"] = "blocked"
        return result

    # 影子模式：旧条目标记（不拦截——照常写入由调用方继续）
    try:
        adapter.mark(conn, matched_id, ctype, subtype=subtype, conflict_with=new_id,
                     mode=mode, low_confidence=result["low_confidence"])
    except sqlite3.Error:
        pass
    return result


# ── 轻量 JSON 工具（避免依赖环境差异）────────────────────────
def json_dumps(obj):
    import json
    return json.dumps(obj, ensure_ascii=False)


def json_loads(s):
    import json
    return json.loads(s)


if __name__ == "__main__":
    # 自检：三分类判定冒烟 + 枚举同源校验
    assert siku_types.validate_conflict_type(INDEPENDENT)
    assert siku_types.validate_conflict_type(EXTENSION)
    assert siku_types.validate_conflict_type(CONFLICT)
    for st in (SUB_SUMMARY_CONCLUSION, SUB_EVENT_FACT, SUB_RULE_SEMANTIC):
        assert siku_types.validate_conflict_subtype(st)
    # 冒烟：三分类各一例
    c1, s1, conf1, m1 = classify("今日上海天气晴朗气温25度", "服务器部署完成后CPU占用率下降")
    c2, s2, conf2, m2 = classify("数据库备份策略：每日全量备份保留30天", "数据库备份策略：每日凌晨2点全量备份到本地磁盘")
    c3, s3, conf3, m3 = classify("规则：开发环境应该允许直接连接生产数据库", "规则：开发环境必须禁止直接连接生产数据库")
    assert c1 == INDEPENDENT, c1
    assert c2 == EXTENSION, c2
    assert c3 == CONFLICT and s3 == SUB_RULE_SEMANTIC, (c3, s3)
    print("siku_conflict 自检 PASS: independent=%s extension=%s conflict=%s(sub=%s) —— 枚举同源 siku_types OK"
          % (c1, c2, c3, s3))
