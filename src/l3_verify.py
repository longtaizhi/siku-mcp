#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
l3_verify — G2 验证闸门（二期默认关；SIKU_GATE=off 一键开关）。

---
agent: siku-core
type: script
schema_version: "1.0"
updated: 2026-08-13
---

背景（RAG零幻觉 6 项增量之⑤G2 验证闸门）：
  四库是检索工具（无生成环节），G2 闸门 = 证据充分性验证：对检索结果逐条打分，
  输出 grade（strong/medium/weak 提示字段，不拦截），供消费 Agent 决策引用质量。

设计裁定（复核 + 汇聚方案）：
  1. 二期默认关：SIKU_GATE=off（与既有 SIKU_ROUTER/SIKU_MISSMON 体系一致），
     未显式开启时 judge 零加载、主路径零开销。
  2. 本地 judge：bge-reranker-base（CrossEncoder，local_files_only，MPS→CPU 降级），
     全本地红线零外部 API。
  3. AUROC 自证前置（HaluBench 方法参考，文章 verifier AUROC 0.702 为门槛参考）：
     --auroc 子命令在不可答/假前提样本 vs golden 可答样本上实测 AUROC，≥0.7 才启用，
     报告落盘 $SIKU_WORKDIR/AUROC自证-YYYYMMDD.md。
  4. 主路径禁同步等 judge：主路径只允许旁路异步触发（threading daemon 或外部 cron），
     任何同步等待 judge 的接入方式均为违规；judge 打分失败全部吞掉，绝不影响检索返回。

用法：
  # 开闸（显式 SIKU_GATE=on；缺省/off/0/false 一律关）
  SIKU_GATE=on $SIKU_VENV_PYTHON l3_verify.py verify --query "..." --passage "..."
  # 单条验证（gate 关时返回 disabled，零开销）
  $SIKU_VENV_PYTHON l3_verify.py verify --query "..." --passage "..."
  # AUROC 自证（独立子命令，不依赖 gate 开关）
  $SIKU_VENV_PYTHON l3_verify.py auroc --out $SIKU_WORKDIR/AUROC自证-20260813.md
  # 异步旁路验证（库接口：主路径调用后立即返回，judge 在后台线程打分写日志）
  import l3_verify; l3_verify.maybe_verify_async(query, results)
"""
import argparse
import json
import os
import sys
import threading
import time

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_GOLDEN_DIR = os.environ.get("SIKU_GOLDEN_DIR", os.path.join(_SIKU_ROOT, "golden"))  # 评测 golden 集目录

# ── 开关：SIKU_GATE 缺省=off（二期默认关）──
GATE_ENABLED = os.environ.get("SIKU_GATE", "off").lower() not in ("off", "0", "false", "")
AUROC_THRESHOLD = 0.7          # ≥0.7 才启用（文章 0.702 门槛参考）
SELF_CERT_PATH = os.path.join(_SIKU_ROOT, "logs/l3_verify_auroc.json")  # 自证结果缓存
VERIFY_LOG = os.path.join(_SIKU_ROOT, "logs/l3_verify_log.jsonl")       # 异步验证日志
DB_PATH = os.environ.get("SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db"))
GOLDEN_DEFAULT = os.path.join(_GOLDEN_DIR, "golden-set-20260811.json")

_model = None
_model_lock = threading.Lock()
_self_cert = None  # 已通过自证的 judge（进程内缓存）


def gate_enabled():
    """G2 闸门开关（二期默认关）。"""
    return GATE_ENABLED


def load_judge():
    """本地 judge 懒加载：bge-reranker-base（MPS→CPU 降级），全本地零外部 API。"""
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is not None:
            return _model
        from sentence_transformers import CrossEncoder
        for device in ("mps", "cpu"):
            try:
                _model = CrossEncoder("BAAI/bge-reranker-base", device=device, local_files_only=True)
                return _model
            except Exception as e:
                sys.stderr.write(f"[l3_verify] {device} 加载失败({e})\n")
        _model = None
    return None


def judge_score(query, passage, max_chars=400):
    """证据充分性分数：query+passage 相关性（sigmoid 输出 0~1，越高越相关）。
    分数语义（HaluBench 类 verifier）：passage 是否足以支持回答 query。
    """
    m = load_judge()
    if m is None:
        return None
    try:
        return float(m.predict([(query, (passage or "")[:max_chars])])[0])
    except Exception:
        return None


def grade_of(score):
    """分数 → grade 提示（不拦截）：≥0.25 strong / ≥0.05 medium / ≥0.01 weak / 其余 insufficient。"""
    if score is None:
        return "unknown"
    if score >= 0.25:
        return "strong"
    if score >= 0.05:
        return "medium"
    if score >= 0.01:
        return "weak"
    return "insufficient"


def verify(query, passage):
    """单条证据验证。gate 关 → disabled（零开销）；开 → 打分+grade。"""
    if not GATE_ENABLED:
        return {"gate": "off", "enabled": False, "note": "SIKU_GATE=off 二期默认关"}
    if not self_cert_passed():
        return {"gate": "on", "enabled": False,
                "note": f"judge 未通过 AUROC 自证（需 ≥{AUROC_THRESHOLD}），拒绝启用"}
    s = judge_score(query, passage)
    if s is None:
        return {"gate": "on", "enabled": True, "score": None, "grade": "unknown",
                "note": "judge 打分失败（模型不可用）"}
    return {"gate": "on", "enabled": True, "score": round(s, 4), "grade": grade_of(s)}


def verify_results(query, results):
    """对检索结果列表逐条验证（results 元素需含 summary 或 content 文本）。"""
    if not GATE_ENABLED:
        return {"gate": "off", "enabled": False, "note": "SIKU_GATE=off 二期默认关"}
    if not self_cert_passed():
        return {"gate": "on", "enabled": False,
                "note": f"judge 未通过 AUROC 自证（需 ≥{AUROC_THRESHOLD}），拒绝启用"}
    out = []
    for r in results:
        rid = r.get("id", "")
        txt = (r.get("summary") or "") + " " + (r.get("content") or "")
        s = judge_score(query, txt)
        out.append({"id": rid, "score": None if s is None else round(s, 4),
                    "grade": grade_of(s)})
    return {"gate": "on", "enabled": True, "verdicts": out}


# ── 旁路异步：主路径调用后立即返回，judge 后台线程打分写日志（禁同步等 judge）──
def maybe_verify_async(query, results=None):
    """主路径旁路钩子（SIKU_GATE=on 时后台异步验证，默认 off 零开销）。
    任何异常/失败全部吞掉——绝不影响主检索路径与返回。
    """
    if not GATE_ENABLED:
        return
    if not self_cert_passed():
        return
    def _worker():
        try:
            items = results or []
            if not items:
                return
            txts = [(r.get("summary", "") or "") + " " + (r.get("content", "") or "") for r in items[:5]]
            ids = [r.get("id", "") for r in items[:5]]
            pairs = [(query, t[:400]) for t in txts if t.strip()]
            if not pairs:
                return
            m = load_judge()
            if m is None:
                return
            scores = m.predict(pairs)
            os.makedirs(os.path.dirname(VERIFY_LOG), exist_ok=True)
            with open(VERIFY_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "query": query,
                    "verdicts": [{"id": ids[i], "score": round(float(scores[i]), 4),
                                  "grade": grade_of(float(scores[i]))}
                                 for i in range(len(pairs))],
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass  # 旁路异步：任何失败静默
    threading.Thread(target=_worker, daemon=True).start()


# ── AUROC 自证（HaluBench 方法：不可答/假前提 vs 可答，ROC 下面积）──
NEG_QUERIES = [
    "爱因斯坦第二次获得诺贝尔物理学奖的年份",
    "居里夫人完成人类首次登月任务的日期",
    "牛顿发明第一台通用电子计算机的型号",
    "达尔文获得诺贝尔文学奖的获奖感言",
    "秦始皇乘坐航天飞机访问火星的具体时间",
    "达芬奇在月球基地种植土豆的产量报告",
    "伽利略用望远镜发现太阳系第九大行星的日期",
    "特斯拉发明时间机器的实验记录",
    "哥白尼证明地球是宇宙中心的论文标题",
    "爱迪生注册永动机专利的专利号",
    "李白担任唐朝宰相的任期",
    "孔子在联合国发表演讲的全文",
    "苏格拉底获得诺贝尔和平奖的理由",
    "霍金预言第三次世界大战爆发的具体日期",
    "贝多芬在维也纳举办人工智能作曲大赛的成绩",
    "郑和下西洋时使用核动力帆船的技术参数",
    "莎士比亚在硅谷创办科技公司的融资轮次",
    "拿破仑使用无人机侦察滑铁卢战场的部署方案",
    "华盛顿签署美国太空宪法的时间",
    "王阳明在龙场顿悟后发明火箭的图纸",
    "火星殖民地马铃薯种植技术规范",
    "南极洲发现恐龙化石矿藏的储量评估报告",
]


def _load_entries(ids):
    import sqlite3
    if not ids:
        return {}
    q = ",".join("?" * len(ids))
    c = sqlite3.connect(DB_PATH, timeout=8)
    c.execute("PRAGMA query_only=1")  # mode=ro 对中文路径WAL库不可用, 用 query_only 只读（2026-08-31 修复）
    rows = c.execute(f"SELECT id, summary, content FROM memory_store WHERE id IN ({q})", ids).fetchall()
    c.close()
    return {r[0]: (r[1] or "") + " " + (r[2] or "")[:300] for r in rows}


def build_samples():
    """正样本=golden 可答 query+expected_top5 条目（label 1）；负样本=假前提/不可答 query+四库检索返回（label 0）。"""
    with open(GOLDEN_DEFAULT, encoding="utf-8") as f:
        golden = json.load(f)["queries"]
    pos_pairs = []
    for g in golden:
        q = g["query"]
        entries = _load_entries(g.get("expected_top5", [])[:3])
        for eid, txt in entries.items():
            if txt.strip():
                pos_pairs.append((q, txt[:400]))
    import l3_retrieval as l3
    neg_pairs = []
    for q in NEG_QUERIES:
        try:
            r = l3.search_memories(q, mode="auto", top_k=3, tier="expand", track=None)
            for it in r.get("results", []):
                entry = it.get("entry", {})
                txt = (entry.get("summary", "") or "") + " " + (entry.get("content", "") or "")[:200]
                if txt.strip():
                    neg_pairs.append((q, txt[:400]))
        except Exception:
            continue
    return pos_pairs, neg_pairs


def run_auroc():
    """实测 AUROC：本地 judge 对正负样本打分 → roc_auc_score → 判定（≥0.7 才启用）。"""
    import numpy as np
    from sklearn.metrics import roc_auc_score
    pos_pairs, neg_pairs = build_samples()
    m = load_judge()
    if m is None:
        return {"error": "judge 模型不可用（bge-reranker-base 本地加载失败）"}
    y_true, y_score = [], []
    for q, p in pos_pairs:
        y_true.append(1)
        y_score.append(float(m.predict([(q, p[:400])])[0]))
    for q, p in neg_pairs:
        y_true.append(0)
        y_score.append(float(m.predict([(q, p[:400])])[0]))
    y_true = np.array(y_true)
    y_score = np.array(y_score)
    auroc = float(roc_auc_score(y_true, y_score))
    pos_mean = float(y_score[y_true == 1].mean())
    neg_mean = float(y_score[y_true == 0].mean())
    return {
        "auroc": round(auroc, 4),
        "threshold": AUROC_THRESHOLD,
        "passed": auroc >= AUROC_THRESHOLD,
        "n_pos": len(pos_pairs), "n_neg": len(neg_pairs), "n_total": len(y_true),
        "pos_mean_score": round(pos_mean, 4), "neg_mean_score": round(neg_mean, 4),
        "decision": "启用" if auroc >= AUROC_THRESHOLD else "不启用（保持默认关）",
        "method": "HaluBench 方法参考：verifier 对 (query, passage) 证据充分性打分，ROC 下面积；文章 0.702 为门槛参考，≥0.7 才启用",
        "sample_neg_examples": NEG_QUERIES[:8],
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def self_cert_passed():
    """进程内+文件级自证缓存：已通过 AUROC≥0.7 则 judge 可用。"""
    global _self_cert
    if _self_cert is not None:
        return _self_cert
    try:
        if os.path.exists(SELF_CERT_PATH):
            with open(SELF_CERT_PATH, encoding="utf-8") as f:
                data = json.load(f)
            if data.get("passed") and data.get("auroc", 0) >= AUROC_THRESHOLD:
                _self_cert = True
                return True
    except Exception:
        pass
    return False


def main():
    p = argparse.ArgumentParser(description="l3_verify — G2 验证闸门（二期默认关 SIKU_GATE=off）")
    sp = p.add_subparsers(dest="cmd", required=True)

    pv = sp.add_parser("verify", help="单条证据充分性验证")
    pv.add_argument("--query", required=True)
    pv.add_argument("--passage", required=True)

    pa = sp.add_parser("auroc", help="AUROC 自证（≥0.7 才启用 judge）")
    pa.add_argument("--out", default="", help="报告 md 输出路径（默认不写文件）")
    pa.add_argument("--write-cert", action="store_true", help="通过后写自证缓存文件")

    args = p.parse_args()
    if args.cmd == "verify":
        print(json.dumps(verify(args.query, args.passage), ensure_ascii=False))
    elif args.cmd == "auroc":
        res = run_auroc()
        if "error" in res:
            print(json.dumps(res, ensure_ascii=False))
            sys.exit(1)
        print(json.dumps(res, ensure_ascii=False, indent=2))
        if res["passed"] and args.write_cert:
            os.makedirs(os.path.dirname(SELF_CERT_PATH), exist_ok=True)
            with open(SELF_CERT_PATH, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
        if args.out:
            os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(_render_report_md(res))
        sys.exit(0 if res["passed"] else 2)


def _render_report_md(res):
    """AUROC 自证报告 markdown（frontmatter 4 字段 + 数字 + 启用判定）。"""
    pos_ex = "、".join(res.get("sample_neg_examples", [])[:5])
    return f"""---
agent: siku-core
type: report
schema_version: "1.0"
updated: 2026-08-13
---

# G2 验证闸门 AUROC 自证报告（l3_verify）

> 模块：检索验证（G2 验证闸门）· 日期：2026-08-13
> 方法：HaluBench 参考——本地 judge 对 (query, passage) 证据充分性打分，ROC 下面积；文章 verifier AUROC 0.702 为门槛参考，**≥0.7 才启用**

## 一、AUROC 数字

| 指标 | 数值 |
|:--|:--|
| **AUROC** | **{res['auroc']}** |
| 启用门槛（参考文章 0.702） | {res['threshold']} |
| 正样本数（golden 可答 query+相关条目） | {res['n_pos']} |
| 负样本数（假前提/不可答 query+四库检索返回） | {res['n_neg']} |
| 总样本数 | {res['n_total']} |
| 正样本均分（judge 输出） | {res['pos_mean_score']} |
| 负样本均分（judge 输出） | {res['neg_mean_score']} |

## 二、启用判定

**判定：{res['decision']}**（AUROC {res['auroc']} {'≥' if res['passed'] else '<'} {res['threshold']}）

- 本地 judge：bge-reranker-base（CrossEncoder，MPS→CPU 降级，全本地零外部 API）
- 自证通过即写入 `{SELF_CERT_PATH}`，此后 SIKU_GATE=on 时 judge 方可打分
- **二期默认关**：SIKU_GATE 缺省=off，主路径零开销；开闸需显式 SIKU_GATE=on
- **主路径禁同步等 judge**：仅允许旁路异步（maybe_verify_async 后台线程 / 外部 cron），同步等待接入方式违规

## 三、负样本示例（假前提/不可答，label=0）

{pos_ex} 等 {res['n_neg']} 对（每条 query 取四库检索 top3 返回条目配对）

## 四、样本与复现

- 正样本：golden-set-20260811.json 95 query × expected_top5 前 3 条目（剔除空文本）
- 负样本：22 条假前提/不可答 query × 四库检索 top3 返回（time_desc 兜底也会返回条目——正是 judge 要拦的低相关证据）
- 复现：`$SIKU_VENV_PYTHON {os.path.basename(__file__)} auroc`
"""


if __name__ == "__main__":
    main()

