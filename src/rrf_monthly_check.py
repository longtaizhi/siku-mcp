#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RRF 月度复查 cron 脚本（no_agent 纯脚本 0 token）——检查频率：季度→月度。

背景（B 项，方案 RRF自动微调-方案-20260813.md 候选③）：
  RRF 权重自动微调的第二条防线：每月跑 golden 基线（20260823 重建后=113 可答 g001-g113，train76/val19 固定 split 取 95，
  rrf_golden_split.json），用**当月生产权重**实测 → 与历史基线（r4_golden_results.json
  phase_a 图关口径，NDCG@5/p@5 valid 口径）对比 → 零退化门禁：
    - 不退化 → 静默：写运行日志（JSONL append），stdout 空，0 建卡，EXIT=0
    - 退化   → stdout 输出 JSON 触发证据 + 建卡「RRF月度复查退化」派发看板 assignee（幂等）
  （P3-09 口径标注 2026-08-23：golden-set-20260823.json total=113（g001-g113 全可答，meta.split train76/val19/other18）；
   18 负样本独立文件 golden_neg_20260823.json（neg001-neg018）；旧注释 95 query/g001-g095 为 20260816 版口径，已按现状修正说明）

负样本衔接（R3 卡，2026-08-13）：
  golden 新增 18 条不可答/假前提负样本（neg001-neg018，unanswerable=true，expected_top5=[]）。
  月度复查拆两口径：
    - 可答 query（g001-g113）→ 原有 ndcg/p5 均值门禁（负样本不混入，防 0 分拉低均值）
    - 不可答 query（n001-n018）→ 不可答判定：检索返回非空 top5 = 被作答（幻觉信号），
      空/empty = 正确拒答；幻觉率 = 被作答数/负样本总数，门禁阈值 5%（参考文章
      hallucination_rate≤5%）；幻觉率超阈值 → 退化建卡（幂等同卡）
  兼容 R1 卡（①）：检索结果带 result_quality/weak_match 标记时透传
  入日志证据（当前生产未落地则 None，不影响判定口径——对不可答问题，检索返回条目即被作答）。

口径（与 r4_golden_results.json / r4_golden_test.py 对齐）：
  - ndcg_valid = NDCG@5，relevant = expected_top5 剔 deprecated（当月 DB 查 deprecated）
  - p5_valid   = P@5，同 relevant 集合（基线侧从 phase_a detail 的 top5/expected 重算，
    与当月用同一 dep_ids 集合 → 数字同口径可比）
  - 门禁判定：当月 mean_ndcg_valid >= 基线 mean_ndcg_valid - 1e-9 且
              当月 mean_p5_valid   >= 基线 mean_p5_valid   - 1e-9 且
              负样本幻觉率 <= 5%（负样本存在时）→ 不退化（均值口径，
              逐 query 退化数如实报告）

形态对齐：生产默认图关（SIKU_GRAPH_CHANNEL 默认 "0"）→ 与基线 phase_a（dual_rrf_rerank）
形态一致；当月权重 = l3_retrieval.CHANNEL_WEIGHTS 当前值（生产 (1.5, 0.7)）。

缓存：当月跑测结果缓存到 SIKU_CACHE；缓存键 = golden mtime+size + DB MAX(updated_at)
      + CHANNEL_WEIGHTS 序列化 + 图通道开关 + per_seed（权重变化→键变→必重跑，本脚本核心）。
      --force 忽略缓存强制复跑。

滚动基线（①，2026-08-16）：
  - 零退化门禁（ndcg/p5 均值不降）通过后，自动把当月跑测快照写为新基线文件
    r5_golden_results_YYYYMMDD.json（同日重跑加 _HHMMSS 后缀防覆盖），历史基线保留可追溯；
    负样本幻觉门禁（hall）属 R1 未落地前的既有独立缺陷（18/18 被作答），不阻塞基线滚动，
    但退化卡照常建（degraded 判定不变）——基线滚动只跟检索质量零退化门禁。
  - SIKU_BASELINE 未显式指定时，自动选用基线目录中最新的 r5/r6_golden_results_*.json
    （滚动；r6 重标口径优先于 r5 legacy—— 扩展）。
  - 语料增长提示：基线 meta.corpus_rows vs 当前 COUNT(*) 变化 ≥10%（原 +20% 收紧）
    → stdout 输出重标提示。旧基线无 corpus_rows 元数据时跳过该检查（日志留痕）。

漂移防护机制化（漂移防护机制化，2026-09-05 拍板）：
  - 背景实证：corpus 28183（8/23）→41754（9/5，+48.2%），RECALIBRATE 规则连月 hint
    空报警无强制 → 基线自动滚动不受 golden 重标约束 → 6 条 ndcg 回归=基准过期假象。
  - 双轴（语料行数变化 ≥10% / 权重变更[基线 meta.weights vs 当前 CHANNEL_WEIGHTS]）
    任一触发 → 月度门禁自动冻结转「待重标」态（对照过期基准的退化判定挂起，
    不再 hint 空报警）：
      1) 冻结：主输出 state=recalibrate_frozen + 触发轴证据，不判退化不建退化卡；
      2) 禁滚：golden 未重标前禁基线自动滚动（防新基线建于旧口径）——冻结分支不落快照；
      3) 自动建卡：报警必进看板——幂等建「RRF golden 重标执行」卡
         （owner=维护人 + 时限 3 天，闭环才销——卡 body 含解冻契约）。
  - 解冻：重标完成标志 = 新基线快照 r6_golden_results_YYYYMMDD.json 落盘且
    meta.corpus_rows=当前行数、weights=当前生产、baseline_recalibrated=true
    → discover 自动选 r6 → 双轴不再触发 → 月度门禁自动恢复（对照新口径）。
  - 权重轴可逆（临时变更回滚回基线值 → 轴消除自动解冻）；corpus 轴为永久漂移，
    只能靠 r6 重标解冻。
  - 既有 hint 兼容：growth 字段结构保留（升级不破，旧消费者可继续读 recalibrate_hint）。

零污染纪律：不删 query_cache（缓存键含权重+db_max_updated 保证结果对当前数据有效）；
唯一写入 = 退化/重标建卡的 kanban INSERT + 日志 append + 滚动基线快照文件。

参数：
  --dry-run  只检查不建卡（退化时仍输出 JSON 证据，但不 INSERT 看板卡、不写滚动基线）
  --force    忽略当月结果缓存，强制复跑
环境变量（全部可选）：
  SIKU_GOLDEN    golden 路径（默认 $SIKU_GOLDEN_DIR/golden-set-20260823.json）
  SIKU_SPLIT     split 文件（默认同目录 rrf_golden_split.json）
  SIKU_BASELINE  基线结果（默认自动选用同目录最新 r5_golden_results_*.json，可显式指定）
  SIKU_DB_PATH   memory_store.db 路径（默认 $SIKU_ROOT/memory_store.db）
  SIKU_KANBAN_DB kanban 库路径（默认 $HERMES_HOME/kanban/boards/default/kanban.db）
  SIKU_CACHE     当月结果缓存 JSON（默认 $SIKU_GOLDEN_DIR/.rrf_monthly_cache.json）
  SIKU_LOG       运行日志 JSONL（默认 $SIKU_GOLDEN_DIR/rrf-monthly-log.jsonl）
  SIKU_BASELINE_AUTO 滚动基线自动更新开关（默认 "1"；设 "0" 关闭，一键回退）

用法：
  $SIKU_VENV_PYTHON rrf_monthly_check.py            # 检查+退化建卡
  $SIKU_VENV_PYTHON rrf_monthly_check.py --dry-run  # 只检查不建卡
  $SIKU_VENV_PYTHON rrf_monthly_check.py --force    # 忽略缓存强制复跑
"""
import argparse
import datetime
import json
import os
import re
import sqlite3
import sys

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）
_GOLDEN_DIR = os.environ.get("SIKU_GOLDEN_DIR", os.path.join(_SIKU_ROOT, "golden"))  # 评测 golden 集目录

GOLDEN_DEFAULT = os.path.join(_GOLDEN_DIR, "golden-set-20260823.json")
SPLIT_DEFAULT = os.path.join(_GOLDEN_DIR, "rrf_golden_split.json")
BASELINE_DEFAULT = os.path.join(_GOLDEN_DIR, "r5_golden_results_20260823.json")  # 2026-08-23 基线重建（113 条 golden 全量跑测）
DB_DEFAULT = os.path.join(_SIKU_ROOT, "memory_store.db")
KANBAN_DEFAULT = os.path.join(_HERMES_HOME, "kanban", "boards", "default", "kanban.db")
CACHE_DEFAULT = os.path.join(_GOLDEN_DIR, ".rrf_monthly_cache.json")
LOG_DEFAULT = os.path.join(_GOLDEN_DIR, "rrf-monthly-log.jsonl")
# 负样本独立文件（重建，2026-08-23）：18 条 unanswerable=true（neg001-neg018）。
# 存在时与 golden 合并参与负样本幻觉率统计（n≥1 门禁生效）；不存在时兼容旧 golden（n=0 视为通过）。
NEG_DEFAULT = os.path.join(_GOLDEN_DIR, "golden_neg_20260823.json")

CARD_TITLE = "RRF月度复查退化"
# ── 漂移防护机制（——方案三/五章：双轴冻结+禁滚+自动建卡）──
RECALIB_CARD_TITLE = "RRF golden 重标执行"  # 重标执行卡标题（assignee 经 SIKU_RECALIB_ASSIGNEE；退化卡经 SIKU_KANBAN_ASSIGNEE）
RECALIB_ASSIGNEE = os.environ.get("SIKU_RECALIB_ASSIGNEE", os.environ.get("SIKU_KANBAN_ASSIGNEE", "default"))  # 重标执行卡 assignee（环境变量可覆盖）
RECALIB_DEADLINE_DAYS = 3  # 时限：建卡起 N 天内完成重标执行与闭环（闭环才销）
RECALIB_TASK_PREFIX = "rrf_recalib_"  # 重标卡 task_id 前缀（退化卡保持 rrf_monthly_deg_）
RECALIB_STATE_LABEL = "recalibrate_frozen"  # 冻结态标识（月度门禁转「待重标」）
EPS = 1e-9
HALL_THRESHOLD = 0.05  # 负样本幻觉率门禁阈值（参考文章 hallucination_rate≤5%）
GROWTH_THRESHOLD = 0.10  # 语料行数变化重标提示阈值（①：原 +20% 收紧为 ≥10%）
RECALIBRATE_RULE = "语料 ≥+10% 或权重调整后再重标"
BASELINE_AUTO = os.environ.get("SIKU_BASELINE_AUTO", "1").lower() not in ("0", "off", "false")


def _now_iso():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def ndcg_at5(ranked_ids, relevant):
    """NDCG@5（与 r4_golden_test.py 同款）。"""
    dcg = 0.0
    for i, eid in enumerate(ranked_ids[:5]):
        if eid in relevant:
            dcg += 1.0 / (i + 2)
    k = min(5, len(relevant))
    idcg = sum(1.0 / (i + 2) for i in range(k))
    return dcg / idcg if idcg else 0.0


def p5_at5(ranked_ids, relevant):
    """P@5：top5 中相关命中数 / min(5, |relevant|)（与 ndcg 同 relevant 集合）。"""
    k = min(5, len(relevant))
    if k == 0:
        return 0.0
    hits = sum(1 for eid in ranked_ids[:5] if eid in relevant)
    return hits / k


def load_golden(path):
    with open(path, encoding="utf-8") as f:
        golden = json.load(f)
    return golden["queries"]


def load_split(path):
    """读固定 split：train_ids/val_ids（rrf_golden_split.json）。不存在则全量当 train。"""
    with open(path, encoding="utf-8") as f:
        sp = json.load(f)
    return set(sp.get("train_ids", [])), set(sp.get("val_ids", []))


def load_baseline_detail(path):
    """读基线 r5_golden_results_*.json 的 detail.phase_a 逐 query detail + meta。

    返回 (phase, phase_a_sum, meta)——phase 为 detail.phase_a（95 条），
    phase_a_sum 为顶层 phase_a 汇总（旧格式兼容），meta 含 corpus_rows/weights 等。
    """
    with open(path, encoding="utf-8") as f:
        bl = json.load(f)
    return bl["detail"]["phase_a"], bl.get("phase_a", {}), bl.get("meta", {})


BASELINE_NAME_RE = re.compile(r"(r[56])_golden_results_(\d{8})(?:_(\d{6}))?\.json$")


def discover_latest_baseline():
    """SIKU_BASELINE 未显式指定时，自动选用基线目录中最新的 r5/r6_golden_results_*.json（滚动基线）。

    r6（重标口径）优先于 r5（legacy）；同版本内按文件名内嵌日期（YYYYMMDD[_HHMMSS]）取最大；
    无候选回退 BASELINE_DEFAULT。
    """
    d = os.path.dirname(os.path.abspath(BASELINE_DEFAULT))
    cands = []
    try:
        for fn in os.listdir(d):
            m = BASELINE_NAME_RE.search(fn)
            if m:
                ver, date, t = m.group(1), m.group(2), m.group(3) or ""
                cands.append((1 if ver == "r6" else 0, date, t, fn))
    except OSError:
        pass
    if not cands:
        return BASELINE_DEFAULT
    cands.sort(key=lambda t: (t[0], t[1], t[2]))
    return os.path.join(d, cands[-1][3])


def _db_row_count(db_path):
    # 普通连接 + PRAGMA query_only=ON（实证：mode=ro URI 打开 WAL 库
    # 在 -shm 缺失时瞬态失败 unable to open database file → 冻结检测核心前提，须稳定）
    try:
        c = sqlite3.connect(db_path)
        c.execute("PRAGMA query_only=ON")
        n = c.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
        c.close()
        return n
    except Exception:
        return None


def write_baseline_snapshot(baseline_path, month_phase, neg_phase, stats, weights,
                            queries_total, corpus_rows, prev_baseline, hall_rate):
    """滚动基线：门禁通过后把当月快照写为新基线文件（r5/r6 同格式），历史基线保留可追溯。

    文件名前缀跟随 prev_baseline（r5 基线滚动出 r5 快照；r6 重标口径滚动出 r6 快照——防降级）；
    同日重跑加 _HHMMSS 后缀防覆盖。meta.baseline_recalibrated 继承 prev 基线值
    （r6 重标口径延续 true，滚动快照不降级回旧口径）。
    返回写入路径；失败返回 None（不影响主流程结论）。
    """
    month_sum = summarize(month_phase)
    now = datetime.datetime.now()
    prev_ver, prev_recal = "r5", False
    try:
        m = BASELINE_NAME_RE.search(os.path.basename(prev_baseline))
        prev_ver = m.group(1) if m else "r5"
        with open(prev_baseline, encoding="utf-8") as f:
            prev_recal = bool(json.load(f).get("meta", {}).get("baseline_recalibrated", False))
    except Exception:
        pass
    base_name = f"{prev_ver}_golden_results_{now.strftime('%Y%m%d')}.json"
    out = os.path.join(os.path.dirname(os.path.abspath(baseline_path)), base_name)
    if os.path.exists(out):
        out = os.path.join(os.path.dirname(os.path.abspath(baseline_path)),
                           f"{prev_ver}_golden_results_{now.strftime('%Y%m%d_%H%M%S')}.json")
    snap = {
        "meta": {
            "task": "", "agent": "siku-core", "time": _now_iso(),
            "baseline_recalibrated": prev_recal,
            "reason": (f"滚动基线：月度复查零退化门禁通过（ndcg/p5 不降），基线自动更新为当月快照；"
                       f"历史基线 {os.path.basename(prev_baseline)} 保留可追溯"),
            "queries": queries_total,
            "recalibrate_rule": RECALIBRATE_RULE,
            "weights": weights,
            "same_caliber": "当月生产形态（图关/生产权重/无 rerank），r5 同口径",
            "corpus_rows": corpus_rows,
            "prev_baseline": os.path.basename(prev_baseline),
        },
        "mean_ndcg_valid": month_sum["mean_ndcg_valid"],
        "mean_p5_valid": month_sum["mean_p5_valid"],
        "stats": {
            "mean_ndcg_valid": month_sum["mean_ndcg_valid"],
            "mean_p5_valid": month_sum["mean_p5_valid"],
            "neg_sample_hallucination": (f"{hall_rate:.1%}——无拒答机制既有特性（独立改进项）"
                                         if neg_phase else "无负样本"),
            "n": len(month_phase), "neg_n": len(neg_phase),
            "note": "滚动基线自动快照（r5 同口径，detail.phase_a 可追溯逐 query）",
        },
        "detail": {"phase_a": month_phase},
        "phase_a": [],
        "neg_phase": neg_phase,
        "runtime_s": _now_iso(),
    }
    try:
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False, indent=1)
        return out
    except Exception:
        return None


def _db_max_updated(db_path):
    try:
        c = sqlite3.connect(db_path)
        c.execute("PRAGMA query_only=ON")
        v = c.execute("SELECT MAX(updated_at) FROM memory_store").fetchone()[0]
        c.close()
        return v
    except Exception:
        return None


def _dep_ids(db_path):
    try:
        c = sqlite3.connect(db_path)
        c.execute("PRAGMA query_only=ON")
        ids = {r[0] for r in c.execute("SELECT id FROM memory_store WHERE deprecated=1")}
        c.close()
        return ids
    except Exception:
        return set()


_cur_weights_cache = None


def get_current_weights():
    """当前生产 CHANNEL_WEIGHTS（导入 l3 权威值；缓存避免重复 import 开销）。"""
    global _cur_weights_cache
    if _cur_weights_cache is None:
        os.environ.setdefault("SIKU_ROUTER", "off")
        sys.path.insert(0, os.path.join(_SIKU_ROOT, "scripts"))
        import l3_retrieval as l3
        _cur_weights_cache = dict(l3.CHANNEL_WEIGHTS)
    return _cur_weights_cache


def detect_recalibrate_axes(base_meta, corpus_rows, cur_weights):
    """漂移防护双轴检测（方案三章：语料≥10%/权重变更任一触发 → 冻结待重标）。

    返回触发轴列表（空=不冻结）。corpus 轴沿用既有 abs(≥10%) 口径（兼容既有 hint，
    方向增减均如实报告）；权重轴 = 基线 meta.weights vs 当前生产 CHANNEL_WEIGHTS 不等。
    基线缺 corpus_rows meta → corpus 轴跳过（旧基线兼容，skipped 由 growth 字段留痕）。
    """
    axes = []
    base_corpus = base_meta.get("corpus_rows")
    if base_corpus and corpus_rows:
        growth_pct = (corpus_rows - base_corpus) / float(base_corpus)
        if abs(growth_pct) >= GROWTH_THRESHOLD:
            axes.append({
                "axis": "corpus_growth",
                "baseline_corpus_rows": base_corpus,
                "current_corpus_rows": corpus_rows,
                "growth_pct": round(growth_pct, 4),
                "threshold": GROWTH_THRESHOLD,
                "direction": "增" if growth_pct > 0 else "减",
            })
    base_w = base_meta.get("weights")
    if base_w:
        bw, cw = dict(base_w), dict(cur_weights)
        if bw != cw:
            diff = {k: [bw.get(k), cw.get(k)]
                    for k in sorted(set(bw) | set(cw))
                    if bw.get(k) != cw.get(k)}
            axes.append({
                "axis": "weight_change",
                "baseline_weights": bw,
                "current_weights": cw,
                "diff": diff,
            })
    return axes


def build_growth_hint(base_meta, corpus_rows, axes):
    """语料增长提示（旧 growth_hint 兼容字段——漂移防护升级不破）。

    冻结时含轴信息（recalibrate_hint=True）；非冻结且旧基线无 corpus_rows meta → skipped 留痕；
    非冻结且无触发 → None（原逻辑语义保持一致）。
    """
    base_corpus = base_meta.get("corpus_rows")
    if not base_corpus:
        return {"recalibrate_hint": False, "skipped": True,
                "reason": "旧基线无 corpus_rows 元数据，跳过语料增长检查（首个滚动快照起生效）"}
    cor = next((a for a in axes if a["axis"] == "corpus_growth"), None)
    if cor:
        return {
            "recalibrate_hint": True,
            "baseline_corpus_rows": cor["baseline_corpus_rows"],
            "current_corpus_rows": cor["current_corpus_rows"],
            "growth_pct": cor["growth_pct"],
            "threshold": GROWTH_THRESHOLD,
            "direction": cor["direction"],
            "note": (f"语料行数变化 ≥{GROWTH_THRESHOLD:.0%}（{cor['direction']}）——月度门禁冻结转待重标，"
                     f"已自动建重标执行卡（owner=维护者+时限，闭环才销；规则: {RECALIBRATE_RULE}）"),
        }
    return None


def summarize(phase):
    n = len(phase)
    return {
        "mean_ndcg_valid": sum(p["ndcg_valid"] for p in phase) / n if n else 0.0,
        "mean_p5_valid": sum(p["p5_valid"] for p in phase) / n if n else 0.0,
        "n": n,
    }


def split_summary(phase, train_ids, val_ids):
    train = [p for p in phase if p["id"] in train_ids]
    val = [p for p in phase if p["id"] in val_ids]
    return {"train": summarize(train), "val": summarize(val)}


def run_month(golden_path, db_path, force, cache_path, neg_path=None):
    """当月跑测：当前生产权重 + 生产默认形态（图关）跑 golden query。

    结果按缓存键缓存；键含 golden mtime+size + db_max_updated + 权重 + 图开关 + per_seed
    + neg 文件 mtime/size（负样本变化→键变→必重跑）。
    返回 (phase, neg_phase, stats, cache_hit)——phase 仅可答 query（ndcg/p5 口径），
    neg_phase 仅负样本 query（不可答判定口径，neg001-neg018，unanswerable=true）。
    neg_path 为独立负样本文件；None 时不合并（兼容旧 golden）。
    """
    mtime = os.path.getmtime(golden_path)
    size = os.path.getsize(golden_path)
    neg_mtime = os.path.getmtime(neg_path) if neg_path else None
    neg_size = os.path.getsize(neg_path) if neg_path else None
    db_max_updated = _db_max_updated(db_path)

    # 延迟导入 l3（SIKU_ROUTER=off 直通主检索；DB 路径在 import 前定死）
    os.environ.setdefault("SIKU_ROUTER", "off")
    os.environ["SIKU_DB_PATH"] = db_path
    sys.path.insert(0, os.path.join(_SIKU_ROOT, "scripts"))
    import l3_retrieval as l3

    weights = dict(l3.CHANNEL_WEIGHTS)
    graph_on = l3.GRAPH_CHANNEL_ENABLED
    gate_on = l3.GRAPH_GATE_ENABLED
    cache_key = {
        "golden_path": os.path.abspath(golden_path), "mtime": mtime, "size": size,
        "neg_path": os.path.abspath(neg_path) if neg_path else None,
        "neg_mtime": neg_mtime, "neg_size": neg_size,
        "db_max_updated": db_max_updated,
        "weights": {k: weights.get(k) for k in sorted(weights)},
        "graph": graph_on, "gate": gate_on, "per_seed": l3.GRAPH_PER_SEED,
        "rerank": l3.RERANK_ENABLED, "rrf_k": l3.RRF_K,
    }
    if os.path.exists(cache_path) and not force:
        try:
            with open(cache_path, encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get("key") == cache_key and "phase" in cached:
                return cached["phase"], cached.get("neg_phase", []), cached.get("stats", {}), True
        except Exception:
            pass

    queries = load_golden(golden_path)
    if neg_path:
        queries += load_golden(neg_path)  # 独立负样本文件合并（unanswerable=true，仅入 neg 口径）
    dep_ids = _dep_ids(db_path)
    answer_q = [q for q in queries if not q.get("unanswerable")]
    neg_q = [q for q in queries if q.get("unanswerable")]

    # 预热（加载 jieba/embedding/reranker，不计结果）
    try:
        l3.search_memories("预热 检索 通道", mode="auto", top_k=5, tier="compact")
    except Exception:
        pass

    phase = []
    for q in answer_q:
        qid, query = q["id"], q["query"]
        exp = q.get("expected_top5", [])
        try:
            r = l3.search_memories(query, mode="auto", top_k=5, tier="compact")
            top5 = [x["id"] for x in r.get("results", [])]
            mode = r.get("search_mode", "?")
        except Exception as e:
            top5, mode = [], f"ERR:{e}"
        rel_valid = {e for e in exp if e not in dep_ids}
        phase.append({
            "id": qid,
            "top5": top5,
            "expected": exp,
            "search_mode": mode,
            "ndcg_valid": ndcg_at5(top5, rel_valid),
            "p5_valid": p5_at5(top5, rel_valid),
        })

    # ── 负样本不可答判定（R3 卡）──
    # 口径：检索返回非空 top5 = 被作答（幻觉信号）；空/empty = 正确拒答。
    # 兼容 R1 卡（①）：结果全部带 weak/fallback 显式标记
    # （result_quality==weak 或 weak_match 标记）→ 视为拒答（R1 落地后兜底不再伪装，
    # 负样本幻觉率应随 R1 收敛；R1 未落地时无标记 → 非空 top5 即被作答，如实暴露缺陷）。
    neg_phase = []
    for q in neg_q:
        qid, query = q["id"], q["query"]
        try:
            r = l3.search_memories(query, mode="auto", top_k=5, tier="compact")
            results = r.get("results", [])
            top5 = [x["id"] for x in results]
            mode = r.get("search_mode", "?")
            # RAGI-T 整改③（读取错位修复）：R1 的 result_quality 注入在返回 JSON **顶层**
            # （r["result_quality"]），原实现读 results[0].get("result_quality")（每条结果条目）
            # 恒为 None → 判定实际失效。改读顶层 + 保留条目级弱标记兜底。
            rq = r.get("result_quality")
            top1_score = round(results[0].get("score", 0.0), 4) if results else None
            weak_marked = bool(results) and (
                r.get("result_quality") == "weak"
                or all(x.get("result_quality") == "weak" or x.get("weak_match")
                       for x in results[:5]))
        except Exception as e:
            top5, mode, rq, top1_score, weak_marked = [], f"ERR:{e}", None, None, False
        neg_phase.append({
            "id": qid,
            "top5": top5,
            "search_mode": mode,
            "answered": len(top5) > 0 and not weak_marked,
            "result_quality": rq,       # R1 卡落地后透传（当前未落地为 None）
            "top1_score": top1_score,
            "weak_marked": weak_marked,  # R1 落地后兜底显式标记（weak_match/result_quality==weak）
        })

    stats = {"weights": weights, "graph_on": graph_on, "gate_on": gate_on,
             "per_seed": l3.GRAPH_PER_SEED, "rerank": l3.RERANK_ENABLED, "n": len(phase),
             "neg_n": len(neg_phase)}
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({"key": cache_key, "phase": phase, "neg_phase": neg_phase,
                       "stats": stats, "time": _now_iso()}, f, ensure_ascii=False, indent=1)
    except Exception:
        pass
    return phase, neg_phase, stats, False


def negative_summary(neg_phase):
    """负样本不可答判定汇总：幻觉率 = 被作答数 / 负样本总数。"""
    n = len(neg_phase)
    answered = sum(1 for p in neg_phase if p.get("answered"))
    return {
        "n": n,
        "answered": answered,
        "hallucination_rate": round(answered / n, 4) if n else 0.0,
        "detail": [{"id": p["id"], "answered": p.get("answered"),
                    "mode": p.get("search_mode"), "result_quality": p.get("result_quality"),
                    "top1_score": p.get("top1_score")} for p in neg_phase],
    }


def build_card_body(ev):
    lines = [
        f"【月度复查自动触发】{CARD_TITLE}（RRF 权重月度复查零退化门禁未通过）",
        f"- 触发时间: {ev['time']}",
        f"- 当月权重: {ev['stats']['weights']}（l3_retrieval.CHANNEL_WEIGHTS 生产值）",
        f"- 门禁: 当月 mean_ndcg_valid={ev['month']['mean_ndcg_valid']:.4f} vs 基线 "
        f"{ev['baseline']['mean_ndcg_valid']:.4f}（{'✅' if ev['gates']['ndcg'] else '❌'}）；"
        f"当月 mean_p5_valid={ev['month']['mean_p5_valid']:.4f} vs 基线 "
        f"{ev['baseline']['mean_p5_valid']:.4f}（{'✅' if ev['gates']['p5'] else '❌'}）",
        f"- 逐 query 退化: ndcg_valid 降 {ev['regress_count_ndcg']} 条 / p5_valid 降 "
        f"{ev['regress_count_p5']} 条（均值口径判定，逐条数如实报告）",
    ]
    if ev.get("regress_detail"):
        lines.append(f"- 退化明细(ndcg_valid 降>eps): {ev['regress_detail']}")
    lines += [
        f"- split 分集: train(76) 当月={ev['split_month']['train']['mean_ndcg_valid']:.4f}/"
        f"基线={ev['split_base']['train']['mean_ndcg_valid']:.4f}；val(19) 当月="
        f"{ev['split_month']['val']['mean_ndcg_valid']:.4f}/基线="
        f"{ev['split_base']['val']['mean_ndcg_valid']:.4f}",
        f"- 缓存命中: {ev.get('cache_hit', False)}；当月跑测 n={ev['month']['n']}",
        f"- 来源: rrf_monthly_check.py（no_agent 纯脚本 0 token，cron 每月自动检查）",
        f"- 依据:  B 项月度复查（检查频率季度→月度）+ RRF自动微调-方案-20260813.md 候选③",
        f"- 后续: 排查权重/数据漂移根因，必要时回滚权重（备份 .bak-rrfimpl2-20260812 可还原），"
        f"复查下月 cron；金标过拟合/DB 漂移按既有纪律季度重标",
    ]
    if "negative" in ev:
        neg = ev["negative"]
        lines.insert(4, (
            f"- 负样本不可答判定（R3 卡）: {neg['answered']}/{neg['n']} 被作答，"
            f"幻觉率={neg['hallucination_rate']:.1%}（阈值 ≤{ev.get('hall_threshold', 0.05):.0%}，"
            f"{'✅' if ev['gates']['hall'] else '❌'}）"
        ))
        if neg.get("answered_detail"):
            lines.insert(5, f"- 被作答负样本明细: {neg['answered_detail']}")
    # 库级触发器 trg_flowhard_insert_gate 强制（V15 后新增）：body 必须含非空【三问必答】段
    # （豁免仅看门狗/a2a-task/官方卡；本卡 created_by=维护人 不豁免，缺段则 INSERT 抛 IntegrityError——
    # 2026-09-01 复跑实证：幂等修复后仍中断于此，一并修复）
    lines.append(
        "【三问必答】①相关文件/入口全覆盖：rrf_monthly_check.py 月度复查链路（l3_retrieval 生产权重"
        "+golden 基线+负样本 18 条）②相关 Agent/调用方覆盖：看板 assignee（处置）、看门狗（cron 消费方）"
        "③隐性盲区：权重漂移 vs 基线老化区分、金标过拟合（季度重标纪律）、验收形态≠生产形态"
    )
    return "\n".join(lines)


def build_recalib_card_body(ev):
    """重标执行卡 body（方案五章：hint 升级自动建卡——owner=维护者 + 时限——闭环才销——报警必进看板）。

    ev 为冻结证据（含 axes/weights/corpus/baseline/rule）；body 含解冻契约（r6 基线 meta 要求），
    供执行者照单执行；含【三问必答】段满足库级触发器 trg_flowhard_insert_gate。
    """
    axes_desc = []
    for a in ev["axes"]:
        if a["axis"] == "corpus_growth":
            axes_desc.append(
                f"- 轴1 语料增长: {a['baseline_corpus_rows']} → {a['current_corpus_rows']} 行"
                f"（{a['growth_pct']:+.1%} {a['direction']}，阈值 ≥{a['threshold']:.0%}）")
        else:
            axes_desc.append(
                f"- 轴2 权重变更: 基线 {a['baseline_weights']} vs 当前 {a['current_weights']}"
                f"（diff {a['diff']}）")
    lines = [
        "【自动建卡】RRF golden 重标执行（漂移防护自动触发——月度门禁冻结转待重标）",
        f"- 触发时间: {ev['time']}",
        "- 触发轴:",
    ] + axes_desc + [
        f"- 基线: {ev['baseline']['path']}（meta.corpus_rows={ev['baseline'].get('corpus_rows')}，"
        f"weights={ev['baseline'].get('weights')}，baseline_recalibrated="
        f"{ev['baseline'].get('baseline_recalibrated')}）",
        "- 依据: golden重标执行方案-20260905.md 三/五章（漂移防护机制化——双轴冻结+禁滚+自动建卡）"
        "+ rrf_monthly_check.py 自动触发",
        "- 处置: 按方案一/二/四章执行 golden 重标（全量重判 top-K + 新增行抽样补题 + LLM 预筛人工裁决"
        " + 双人复核抽样 + legacy 锚题永不退役 + 同日同权重建 r6 基线快照）",
        "- 解冻契约（重标完成标志）: 新基线 r6_golden_results_YYYYMMDD.json 落盘且 meta 含 "
        "corpus_rows=当前实际行数/weights=当前生产值/baseline_recalibrated=true"
        " → 月度门禁自动解冻恢复（对照新口径）",
        "- owner: 维护者",
        f"- 时限: 建卡起 {RECALIB_DEADLINE_DAYS} 天内完成重标执行与闭环（{ev.get('deadline', '?')} 前）",
        "- 验收: T 验收 + 标注抽检（方案四章质量门：锚题 A/B kappa≥0.8 零回退才放行）",
        "- 闭环才销: 未重标闭环前禁止归档/关闭；若触发轴已消除（如权重变更回滚回基线值）且经核实"
        "无重标必要 → archived 并注明原因",
        "【三问必答】①文件入口：golden-set（重标对象）+基线 r5/r6（对照/产出）+月测脚本（解冻验证）——覆盖",
        "②调用方：月度门禁（冻结消费——r6 落盘自动解冻）；基线滚动（禁滚消费）；看板（报警闭环）——覆盖",
        "③隐性盲区：轴消除≠重标完成（corpus 永久漂移须 r6 解冻）；解冻依赖新基线 meta 契约"
        "（corpus_rows/weights 必须真实当前值）；质量门独立验收防自评拟合——三盲区覆盖",
    ]
    return "\n".join(lines)


def create_card(kanban_db, title, body, assignee=None, task_id_prefix="rrf_monthly_deg_"):
    assignee = assignee or os.environ.get("SIKU_KANBAN_ASSIGNEE", "default")
    """建卡。幂等：同标题活跃卡（非 done/complete/archived）已存在 → 跳过；
    done/archived 老卡让位——08-16 老卡 done 占位曾致 09-01 退化卡静默丢失（修复）。

    默认=「RRF月度复查退化」派发看板 assignee（task_id rrf_monthly_deg_*）；
    漂移防护重标卡传 assignee=RECALIB_ASSIGNEE + task_id_prefix=RECALIB_TASK_PREFIX
    （rrf_recalib_*，owner=维护者——方案五章）。"""
    conn = sqlite3.connect(kanban_db)
    try:
        row = conn.execute(
            "SELECT id FROM tasks WHERE title=? AND status NOT IN ('done','complete','archived') "
            "ORDER BY created_at DESC LIMIT 1", (title,)).fetchone()
        if row:
            return {"task_id": row[0], "created": False, "reason": "同标题活跃卡已存在（幂等跳过）"}
        task_id = task_id_prefix + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        now = int(datetime.datetime.now().timestamp())
        conn.execute(
            "INSERT INTO tasks (id, title, body, assignee, status, priority, created_by, "
            "created_at, workspace_kind) VALUES (?,?,?,?,?,?,?,?,'scratch')",
            (task_id, title, body, assignee, "ready", 1, os.environ.get("SIKU_KANBAN_CREATED_BY", "siku-core"), now))
        conn.commit()
        return {"task_id": task_id, "created": True, "reason": "新建"}
    finally:
        conn.close()


def append_log(log_path, entry):
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # 日志写失败不影响结论


def main():
    ap = argparse.ArgumentParser(description="RRF 月度复查（权重 vs 基线零退化门禁）")
    ap.add_argument("--dry-run", action="store_true", help="只检查不建卡")
    ap.add_argument("--force", action="store_true", help="忽略当月结果缓存强制复跑")
    args = ap.parse_args()

    golden_path = os.environ.get("SIKU_GOLDEN", GOLDEN_DEFAULT)
    split_path = os.environ.get("SIKU_SPLIT", SPLIT_DEFAULT)
    # 滚动基线：未显式指定 SIKU_BASELINE 时自动选用最新 r5/r6_golden_results_*.json
    baseline_path = os.environ.get("SIKU_BASELINE") or discover_latest_baseline()
    db_path = os.environ.get("SIKU_DB_PATH", DB_DEFAULT)
    kanban_db = os.environ.get("SIKU_KANBAN_DB", KANBAN_DEFAULT)
    cache_path = os.environ.get("SIKU_CACHE", CACHE_DEFAULT)
    log_path = os.environ.get("SIKU_LOG", LOG_DEFAULT)
    # 负样本独立文件：存在即合并（n≥1 幻觉门禁生效）；不存在兼容旧 golden（n=0 视为通过）
    _neg_env = os.environ.get("SIKU_NEG", NEG_DEFAULT)
    neg_path = _neg_env if os.path.exists(_neg_env) else None

    for p, name in [(golden_path, "golden"), (baseline_path, "基线结果")]:
        if not os.path.exists(p):
            sys.stderr.write(f"[rrf_monthly_check] {name} 不存在: {p}\n")
            return 2

    queries = load_golden(golden_path)
    if neg_path:
        queries += load_golden(neg_path)  # 负样本合并入 evidence 统计口径（total/negative 如实）
    train_ids, val_ids = load_split(split_path) if os.path.exists(split_path) else (set(), set())
    dep_ids = _dep_ids(db_path)

    # ── 基线侧：detail.phase_a 重算（与当月同一 dep_ids 集合，同口径可比）──
    base_detail, _base_sum, base_meta = load_baseline_detail(baseline_path)

    # ── 漂移防护双轴检测（方案三章；）：任一触发 → 冻结月度门禁转待重标 ──
    # 双轴 = 语料行数变化 ≥10%（基线 meta.corpus_rows vs 当前 COUNT）+ 权重变更
    #        （基线 meta.weights vs 当前生产 CHANNEL_WEIGHTS）。
    corpus_rows = _db_row_count(db_path)
    cur_weights = get_current_weights()
    axes = detect_recalibrate_axes(base_meta, corpus_rows, cur_weights)
    growth_hint = build_growth_hint(base_meta, corpus_rows, axes)
    if axes:
        # 冻结分支：golden 未重标（旧口径基准）→ 对照过期基准的退化判定无意义——挂起门禁，
        # 不再 hint 空报警：
        #   ① 冻结：主输出 state=recalibrate_frozen + 触发轴证据，不判退化不建退化卡；
        #   ② 禁滚：golden 未重标前禁基线自动滚动（防新基线建于旧口径）——本分支不落快照；
        #   ③ 自动建卡：报警必进看板——幂等建「RRF golden 重标执行」卡
        #      （owner=维护人 + 时限 RECALIB_DEADLINE_DAYS 天——闭环才销，卡 body 含解冻契约）。
        frozen_ev = {
            "time": _now_iso(),
            "state": RECALIB_STATE_LABEL,
            "frozen": True,
            "axes": axes,
            "growth": growth_hint,
            "weights": cur_weights,
            "corpus_rows": corpus_rows,
            "baseline": {
                "path": baseline_path,
                "corpus_rows": base_meta.get("corpus_rows"),
                "weights": base_meta.get("weights"),
                "baseline_recalibrated": base_meta.get("baseline_recalibrated", False),
            },
            "rule": RECALIBRATE_RULE,
            "note": ("月度门禁冻结（对照过期基准无意义）——基线自动滚动已禁；"
                     "解冻契约 = 新基线 r6_golden_results_YYYYMMDD.json 落盘且 "
                     "meta.corpus_rows/weights 与当前一致 → 自动恢复门禁"),
        }
        frozen_ev["deadline"] = (datetime.date.today()
                                 + datetime.timedelta(days=RECALIB_DEADLINE_DAYS)).isoformat()
        if args.dry_run:
            card = {"task_id": None, "created": False, "reason": "dry_run（只检查不建卡）"}
        else:
            card = create_card(kanban_db, RECALIB_CARD_TITLE,
                               build_recalib_card_body(frozen_ev),
                               assignee=RECALIB_ASSIGNEE, task_id_prefix=RECALIB_TASK_PREFIX)
        frozen_ev["card"] = card
        print(json.dumps(frozen_ev, ensure_ascii=False, indent=1))
        append_log(log_path, frozen_ev)
        return 0

    base_phase = []
    for p in base_detail:
        exp = p.get("expected", [])
        rel_valid = {e for e in exp if e not in dep_ids}
        base_phase.append({
            "id": p["id"],
            "ndcg_valid": p.get("ndcg_valid", ndcg_at5(p.get("top5", []), rel_valid)),
            "p5_valid": p5_at5(p.get("top5", []), rel_valid),
        })
    baseline = summarize(base_phase)
    split_base = split_summary(base_phase, train_ids, val_ids) if (train_ids or val_ids) else {}

    # ── 当月侧：生产默认形态跑测 ──
    month_phase, neg_phase, stats, cache_hit = run_month(golden_path, db_path, args.force, cache_path, neg_path)
    month = summarize(month_phase)
    split_month = split_summary(month_phase, train_ids, val_ids) if (train_ids or val_ids) else {}
    neg = negative_summary(neg_phase)

    # ── 零退化门禁（均值口径不降；逐 query 退化数如实报告）──
    mb = {p["id"]: p for p in base_phase}
    regress_ndcg = [(p["id"], round(mb[p["id"]]["ndcg_valid"], 4), round(p["ndcg_valid"], 4))
                    for p in month_phase if p["id"] in mb
                    and p["ndcg_valid"] < mb[p["id"]]["ndcg_valid"] - EPS]
    regress_p5 = [(p["id"], round(mb[p["id"]]["p5_valid"], 4), round(p["p5_valid"], 4))
                  for p in month_phase if p["id"] in mb
                  and p["p5_valid"] < mb[p["id"]]["p5_valid"] - EPS]
    gate_ndcg = month["mean_ndcg_valid"] >= baseline["mean_ndcg_valid"] - EPS
    gate_p5 = month["mean_p5_valid"] >= baseline["mean_p5_valid"] - EPS
    # 负样本幻觉率门禁（R3 卡；恢复生效）：
    # 负样本文件存在（golden_neg_20260823.json，neg001-neg018）→ n≥1 参与统计，
    # 被作答率 >5% 判定退化；负样本文件不存在（旧 golden 无 unanswerable）→ n=0 视为通过（兼容不破坏 9/1 cron）
    gate_hall = neg["hallucination_rate"] <= HALL_THRESHOLD if neg["n"] else True
    degraded = not (gate_ndcg and gate_p5 and gate_hall)

    # ── 滚动基线：零退化门禁（ndcg/p5）通过 → 当月快照自动更新为新基线，历史保留可追溯 ──
    # hall 门禁属 R1 未落地前既有独立缺陷（负样本恢复后若 18/18 被作答），不阻塞基线滚动（退化卡仍照建）；
    # --dry-run / SIKU_BASELINE_AUTO=0 时不写。
    baseline_updated = None
    if (gate_ndcg and gate_p5) and BASELINE_AUTO and not args.dry_run:
        new_base = write_baseline_snapshot(baseline_path, month_phase, neg_phase, stats,
                                           stats["weights"], len(queries), corpus_rows,
                                           baseline_path, neg["hallucination_rate"])
        if new_base:
            baseline_updated = {"new_baseline": new_base, "prev_baseline": baseline_path,
                                "time": _now_iso()}
    elif (gate_ndcg and gate_p5) and not BASELINE_AUTO:
        baseline_updated = {"skipped": True, "reason": "SIKU_BASELINE_AUTO=0 关闭滚动基线"}

    neg_answered_detail = [d for d in neg.get("detail", []) if d.get("answered")]
    evidence = {
        "time": _now_iso(),
        "golden": {"path": golden_path, "total": len(queries), "relational":
                   sum(1 for q in queries if q.get("relational")),
                   "answerable": sum(1 for q in queries if not q.get("unanswerable")),
                   "negative": sum(1 for q in queries if q.get("unanswerable"))},
        "weights": stats["weights"],
        "baseline": {k: round(v, 4) if isinstance(v, float) else v for k, v in baseline.items()},
        "month": {k: round(v, 4) if isinstance(v, float) else v for k, v in month.items()},
        "split_base": {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                           for kk, vv in v.items()} for k, v in split_base.items()},
        "split_month": {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                            for kk, vv in v.items()} for k, v in split_month.items()},
        "negative": {"n": neg["n"], "answered": neg["answered"],
                     "hallucination_rate": neg["hallucination_rate"],
                     "answered_detail": [f"{d['id']}({d.get('mode')},score={d.get('top1_score')})"
                                         for d in neg_answered_detail][:20]},
        "hall_threshold": HALL_THRESHOLD,
        "gates": {"ndcg": gate_ndcg, "p5": gate_p5, "hall": gate_hall, "pass": not degraded},
        "regress_count_ndcg": len(regress_ndcg),
        "regress_count_p5": len(regress_p5),
        "regress_detail": regress_ndcg[:20],
        "stats": stats,
        "cache_hit": cache_hit,
        "degraded": degraded,
        "growth": growth_hint,
        "baseline_updated": baseline_updated,
        "baseline_path": baseline_path,
    }

    card = None
    if degraded:
        if args.dry_run:
            card = {"task_id": None, "created": False, "reason": "dry_run（只检查不建卡）"}
        else:
            card = create_card(kanban_db, CARD_TITLE, build_card_body(evidence))
        evidence["card"] = card
        print(json.dumps(evidence, ensure_ascii=False, indent=1))
    # 日志：退化/不退化都留（不退化静默=stdout 空，日志可查）
    # （旧「不退化但语料≥10% 打印 hint」分支已删除——语料轴触发即冻结分支先行 return，此分支不可达）
    append_log(log_path, evidence)
    return 0


if __name__ == "__main__":
    sys.exit(main())
