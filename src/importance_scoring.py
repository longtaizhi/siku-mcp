#!/usr/bin/env python3
"""重要性评分 — 每日19:00 cron — 兼容所有YAML格式

A5-P2 峰终双锚升级（2026-08-07）：
- 峰值锚：事件强度信号（耗时>2h / 参与人数>3 / 涉及系统数>=2 / 被 force 记住）→ importance +1
- 终点锚：任务完成时点（completed_at）/ 里程碑（milestone / task_done）→ importance +1
- 基础分仍为 confidence 四级映射 8/6/4/2，峰终加权在基础分上叠加，封顶 10
  （与 grpo_full_pipeline.py "importance 1-10" 及 l4_smart.py min(10,...) 兼容）
- P1-6 幂等保持：importance 未变即跳过，不重写文件
- 兼容：无参调用路径不变（pipeline_runner 19:00 cron）；新增可选 --curation-dir 供验证隔离

A2 双轴加权 + --compare 对照实验（2026-08-10）：
- 复杂度轴 C = 0.4*熵H + 0.3*长度L + 0.3*类型复杂度F（0~1，纯 stdlib）
- 稳定性轴 S = 0.25*验证V + 0.25*G2标注G + 0.25*过期E + 0.25*时效T（0~1）
- 双轴评分 = base + 峰值锚 + 终点锚 + w1*C + w2*S，封顶 10；权重 --w-complexity/--w-stability 可调
- --compare：只读对照实验（不写任何文件），输出「原 top50 vs 双轴加权 top50」差异表
  （新增/退出/保留+排名变化），供门禁观察双轴效果后再决定是否进生产
"""
import os, sys, yaml, glob, logging, re, argparse, time
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

CURATION = os.path.join(_SIKU_ROOT, "smart/curation")
LOG = os.path.join(_SIKU_ROOT, "logs/importance_scoring.log")
logging.basicConfig(filename=LOG, level=logging.INFO, format="%(asctime)s %(message)s")
if os.path.exists(LOG) and os.path.getsize(LOG) > 1_048_576:
    with open(LOG) as f:
        lines = f.readlines()
    with open(LOG, "w") as f:
        f.writelines(lines[-5000:])

# ── A5-P2 峰终双锚配置 ──
PEAK_DURATION_H = 2.0    # 峰值锚：耗时 > 2 小时
PEAK_PARTICIPANTS = 3    # 峰值锚：参与人数 > 3
PEAK_SYSTEMS = 2         # 峰值锚：涉及系统数 >= 2
PEAK_BONUS = 1           # 峰值锚命中 → +1
END_BONUS = 1            # 终点锚命中 → +1
MAX_IMPORTANCE = 10      # 封顶，与全库 1-10 分制兼容


def _num(v):
    """容错数值转换：None/空串/非数值 → None"""
    try:
        if v in (None, ""):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _truthy(v):
    """容错布尔判定：bool / 数值 / 字符串真值"""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y", "on")
    return False


def peak_signal(meta):
    """峰值锚：事件强度信号。命中任一 → True（+1）"""
    dur = _num(meta.get("duration_hours", meta.get("duration")))
    if dur is not None and dur > PEAK_DURATION_H:
        return True
    parts = _num(meta.get("participants", meta.get("participant_count")))
    if parts is not None and parts > PEAK_PARTICIPANTS:
        return True
    syss = _num(meta.get("systems_involved", meta.get("system_count")))
    if syss is not None and syss >= PEAK_SYSTEMS:
        return True
    if _truthy(meta.get("force_remember")):
        return True
    return False


def end_signal(meta):
    """终点锚：任务完成时点 / 里程碑。命中任一 → True（+1）"""
    if meta.get("completed_at"):
        return True
    if _truthy(meta.get("milestone")):
        return True
    if _truthy(meta.get("task_done")):
        return True
    return False


# ── A2 双轴加权配置（2026-08-10）──
W_COMPLEXITY = 1.0    # 复杂度权重 w1（--w-complexity 可调）
W_STABILITY = 1.0     # 稳定性权重 w2（--w-stability 可调）
TOP_N = 50            # --compare 对照实验 top-N 数量（--top 可调）
# source_type 复杂度映射（经验分级：研究/纠错/方法论类 > 事实/信息记录类）
TYPE_COMPLEXITY = {
    "research": 1.0, "correction": 0.95, "principle": 0.9, "method": 0.9,
    "methodology": 0.9, "lesson": 0.85, "decision": 0.8, "instruction": 0.8,
    "digest": 0.75, "insight": 0.7, "estimate": 0.65, "result": 0.6,
    "record": 0.55, "fact": 0.5, "info": 0.45, "context": 0.4,
    "L4b": 0.85, "QA": 0.7,
    # : 8 系统资产类型显式复杂度（评测基准/门禁规则=高复杂度可验证类；
    # 规范/流程/技能=方法论类；训练资产/监控=记录观测类；脚本=工具类——未列出走 0.5 兜底已不漏，
    # 本次显式化提升评分区分度）
    "benchmark": 0.9, "rule": 0.9, "spec": 0.85, "workflow": 0.85,
    "skill": 0.8, "asset": 0.7, "monitor": 0.7, "cron": 0.6,
    # : 5 类新资产复杂度（research 已存在 1.0 键保持；
    # design 方案=方法论 0.85 与 spec/workflow 同级；script 工具 0.6 与 cron 同级；
    # reference 知识参考 0.6；config 配置 0.5 记录类）
    "design": 0.85, "script": 0.6, "config": 0.5, "reference": 0.6,
}


def _shannon_entropy(text):
    """字符级香农熵（bits/char），归一化 0~1。纯 stdlib，无 numpy 依赖。"""
    if not text:
        return 0.0
    text = str(text)[:500]
    n = len(text)
    if n == 0:
        return 0.0
    freq = {}
    for ch in text:
        freq[ch] = freq.get(ch, 0) + 1
    h = -sum((c / n) * __import__("math").log2(c / n) for c in freq.values())
    max_h = __import__("math").log2(min(n, len(freq)))
    return h / max_h if max_h > 0 else 0.0


def _norm_len(text):
    """body 长度归一化 0~1（对数压缩，500 字符封顶）。"""
    n = len(text or "")
    if n <= 0:
        return 0.0
    return min(1.0, __import__("math").log2(1 + n) / __import__("math").log2(501))


def _ts_age(v, now_ts):
    """时间字符串 → 距今秒数；无法解析返回 None。"""
    if not v:
        return None
    v = str(v).strip()
    if v.lower() in ("pending", "none", ""):
        return None
    v = v[:26]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(v, fmt)
            return now_ts - dt.timestamp()
        except ValueError:
            continue
    return None


def complexity_axis(meta, body_text=""):
    """复杂度轴 C = 0.4*熵 + 0.3*长度 + 0.3*类型复杂度，归一化 0~1。"""
    txt = (meta.get("body") or meta.get("summary") or body_text or "")
    H = _shannon_entropy(txt)
    L = _norm_len(txt)
    st = str(meta.get("source_type", "")).strip().lower()
    F = TYPE_COMPLEXITY.get(st, 0.5)
    return 0.4 * H + 0.3 * L + 0.3 * F


def stability_axis(meta, now_ts=None):
    """稳定性轴 S = 0.25*验证 + 0.25*G2标注 + 0.25*过期 + 0.25*时效，归一化 0~1。"""
    import time as _t
    now_ts = now_ts or _t.time()
    # V 验证状态：verified=1.0, pending=0.5, 空/未知=0.3
    v = str(meta.get("verification_status", "")).strip().lower()
    V = 1.0 if v == "verified" else (0.5 if v == "pending" else 0.3)
    # G G2 标注状态：人工标注(l3 补链/非 auto)=1.0, auto=0.5, l3_missing=0.3
    g = str(meta.get("g2_status", "")).strip().lower()
    if g in ("", "none", "missing"):
        G = 0.3
    elif g == "l3_missing":
        G = 0.3
    elif g == "auto":
        G = 0.5
    else:
        G = 1.0
    # E 过期状态：无过期时间=0.7(默认稳定), 未过期=1.0, 已过期=0.1
    ex = meta.get("expires_at")
    if ex:
        age = _ts_age(ex, now_ts)
        E = 1.0 if (age is not None and age > 0) else (0.1 if age is not None else 0.5)
    else:
        E = 0.7
    # T 时效：distilled_at 距今天数越短越稳定（1 天内=1.0, 7 天内=0.8, 30 天内=0.6, 更久=0.4）
    da = _ts_age(meta.get("distilled_at"), now_ts)
    if da is None:
        T = 0.5
    elif da <= 86400:
        T = 1.0
    elif da <= 7 * 86400:
        T = 0.8
    elif da <= 30 * 86400:
        T = 0.6
    else:
        T = 0.4
    return 0.25 * V + 0.25 * G + 0.25 * E + 0.25 * T


def dual_axis_score(meta, w_complexity=W_COMPLEXITY, w_stability=W_STABILITY):
    """双轴加权评分 = base + 峰值锚 + 终点锚 + w1*C + w2*S，封顶 10。
    只读计算，不写文件（供 --compare 对照实验）。"""
    conf = meta.get("confidence", 0.7)
    try:
        conf = float(conf) if conf not in (None, "") else 0.7
    except (TypeError, ValueError):
        conf = 0.7
    if conf >= 0.9:
        base = 8
    elif conf >= 0.7:
        base = 6
    elif conf >= 0.5:
        base = 4
    else:
        base = 2
    peak = PEAK_BONUS if peak_signal(meta) else 0
    end = END_BONUS if end_signal(meta) else 0
    C = complexity_axis(meta)
    S = stability_axis(meta)
    return min(base + peak + end + w_complexity * C + w_stability * S, MAX_IMPORTANCE), base, peak, end, C, S


def get_meta(raw):
    """返回 (metadata_dict, yaml_start, yaml_end) 或 None"""
    # 格式A: --- 包裹的 frontmatter → 提取 --- 之间的纯文本
    m = re.match(r"^---\n(.*?)\n---", raw, re.DOTALL)
    if m:
        try:
            d = yaml.safe_load(m.group(1))
            if isinstance(d, dict):
                return d, 0, m.end()
        except:
            pass

    # 格式B: 多文档 YAML → safe_load_all 取第一个
    try:
        gen = yaml.safe_load_all(raw)
        first = next(gen)
        if isinstance(first, dict):
            # 找到第一个文档的结束位置
            sep = raw.find("\n---\n")
            if sep != -1:
                return first, 0, sep
            else:
                return first, 0, len(raw)
    except:
        pass

    # 格式C: 纯 YAML（concept 格式）
    try:
        d = yaml.safe_load(raw)
        if isinstance(d, dict):
            return d, 0, len(raw)
    except:
        pass

    return None


def process(fpath, dual_axis=False, w1=W_COMPLEXITY, w2=W_STABILITY):
    with open(fpath) as f:
        raw = f.read()
    result = get_meta(raw)
    if result is None:
        return False, "unparseable"
    meta, start, end = result
    if "importance" not in meta and "concept" in meta:
        return False, "concept"
    conf = meta.get("confidence", 0.7)
    # P1-1 容错：confidence 空串/非数值（历史 19 条 '' 崩溃源）→ 按默认 0.7 处理
    try:
        conf = float(conf) if conf not in (None, "") else 0.7
    except (TypeError, ValueError):
        conf = 0.7
    if conf >= 0.9:
        base = 8
    elif conf >= 0.7:
        base = 6
    elif conf >= 0.5:
        base = 4
    else:
        base = 2
    # A5-P2 峰终双锚：基础分 + 峰值锚 + 终点锚，封顶 10
    # 注意：end_signal 返回值不能复用变量名 end——end 是 get_meta 返回的
    # frontmatter 结束位置，被覆盖会导致 body=raw[end:] 取错（2026-08-07 16:40
    # 事故：51 个 curation 文件被写坏，见 /tmp/a5_recover.py 恢复）
    peak_hit = peak_signal(meta)
    end_hit = end_signal(meta)
    # : --dual-axis 生产双轴加权（复用双轴实现——
    # 复杂度轴 C=0.4熵+0.3长度+0.3类型 / 稳定性轴 S=0.25验证+0.25G2+0.25过期+0.25时效，
    # 权重 --w-complexity/--w-stability 默认 1.0/1.0；幂等逻辑不变：importance 未变即跳过）
    if dual_axis:
        _C = complexity_axis(meta)
        _S = stability_axis(meta)
        new_score = min(base + (PEAK_BONUS if peak_hit else 0) + (END_BONUS if end_hit else 0)
                        + w1 * _C + w2 * _S, MAX_IMPORTANCE)
    else:
        new_score = min(base + (PEAK_BONUS if peak_hit else 0) + (END_BONUS if end_hit else 0), MAX_IMPORTANCE)
    # P1-6 幂等：importance 未变即跳过（不再要求 scored_at 存在——
    # 历史无 scored_at 数据（19条）曾导致每次评分全库重写，mtime 大规模扰动）
    if meta.get("importance") == new_score:
        return False, "unchanged"
    meta["importance"] = new_score
    # A5-P2 锚点标注：供审计/验收观测加权来源（仅 importance 变化时写入）
    if peak_hit:
        meta["peak_anchored"] = True
    if end_hit:
        meta["end_anchored"] = True
    # scored_at 仅在 importance 实际变化时更新（幂等核心：不变不重写）
    if "scored_at" not in meta:
        meta["scored_at"] = datetime.now().isoformat()
    new_fm = yaml.dump(meta, allow_unicode=True, sort_keys=False, default_flow_style=False).strip()
    body = raw[end:]
    # 清理 body 中的文档分隔符残留：尾部 `---` 会被 yaml.safe_load
    # 视为第二个文档开始导致解析失败（2026-08-01 19:00 批量事故根因）
    body = body.lstrip("\n")
    while body.startswith("---"):
        idx = body.find("\n")
        body = body[idx + 1:] if idx != -1 else ""
        body = body.lstrip("\n")
    # (2026-08-17): 写重试——文件被占/瞬时错误重试 3 次，不再一次失败即跳过评分
    # : 写回补 `---` 结束符（L4b 三段式标准 `---`+fm+`---`+正文；
    #   原写回缺结束符 → 带 body 的格式A 文件重跑 unparseable（46/31211 存量格式A 文件受害面），
    #   双轴全量首跑会放大——一并修复）
    content = "---\n" + new_fm + "\n---\n"
    if body:
        content += body if body.startswith("\n") else "\n" + body
    content += "\n"
    for _attempt in range(3):
        try:
            with open(fpath, "w") as f:
                f.write(content)
            break
        except OSError as _e:
            if _attempt == 2:
                raise
            time.sleep(1)
    return True, f"ok:{new_score}"


def perm_audit(layer):
    """P0-2 权限写路径接入(2026-08-03): audit模式只记录不拦截, 观察1周后转enforce"""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import check_permission as cp
    cp.check(os.environ.get("SIKU_AGENT", "用户"), "write", layer, mode="audit")


def compare_topn(curation_dir, top_n=TOP_N, w1=W_COMPLEXITY, w2=W_STABILITY):
    """--compare 只读对照实验：原 importance top-N vs 双轴加权 top-N。
    不写任何文件；返回差异统计 dict，并打印对比表。"""
    import time as _t
    yamls = sorted(glob.glob(os.path.join(curation_dir, "*/*.yaml")))
    rows = []  # (fpath, old_score, new_score, base, peak, end, C, S)
    for fpath in yamls:
        try:
            with open(fpath) as f:
                raw = f.read()
            r = get_meta(raw)
            if r is None:
                continue
            meta, _, _ = r
            if "importance" not in meta and "concept" in meta:
                continue
            old = meta.get("importance")
            if old is None:
                continue
            new, base, peak, end, C, S = dual_axis_score(meta, w1, w2)
            rows.append((fpath, float(old), new, base, peak, end, C, S))
        except Exception as e:
            logging.error(f"{fpath}: {e}")
    rows.sort(key=lambda x: (-x[1], x[0]))  # 旧分降序
    old_top = rows[:top_n]
    new_rows = sorted(rows, key=lambda x: (-x[2], x[0]))  # 新分降序
    new_top = new_rows[:top_n]
    old_ids = {os.path.basename(p) for p, *_ in old_top}
    new_ids = {os.path.basename(p) for p, *_ in new_top}
    added = [(p, old, new) for p, old, new, *_ in new_top if os.path.basename(p) not in old_ids]
    removed = [(p, old, new) for p, old, new, *_ in old_top if os.path.basename(p) not in new_ids]
    kept = [(p, old, new) for p, old, new, *_ in new_top if os.path.basename(p) in old_ids]
    # 保留项排名变化：正=新排位比旧排位靠前（上升），负=下降
    old_rank = {os.path.basename(p): i for i, (p, *_ ) in enumerate(old_top)}
    new_rank = {os.path.basename(p): i for i, (p, *_ ) in enumerate(new_top)}
    rank_changes = sorted(
        ((p, new_rank[os.path.basename(p)] - old_rank[os.path.basename(p)]) for p, *_ in kept
         if old_rank[os.path.basename(p)] != new_rank[os.path.basename(p)]),
        key=lambda x: -x[1])

    print("=" * 78)
    print(f"📊 --compare 双轴加权对照实验（{_t.strftime('%Y-%m-%d %H:%M:%S')}）")
    print(f"   样本 {len(rows)} 条 | top {top_n} | w1(复杂度)={w1} w2(稳定性)={w2}")
    print(f"   新评分 = base + 峰值锚 + 终点锚 + {w1}*C + {w2}*S（封顶 10，只读不写库）")
    print("=" * 78)
    print(f"\n【新增进入 top{top_n}】{len(added)} 条（原 top{top_n} 中不存在）：")
    for p, old, new in added:
        print(f"  + {os.path.basename(p)}  原分={old:g} → 新分={new:g}  ({p.replace(os.path.expanduser('~'), '~')})")
    print(f"\n【退出 top{top_n}】{len(removed)} 条（原 top{top_n} 中被挤出）：")
    for p, old, new in removed:
        print(f"  - {os.path.basename(p)}  原分={old:g} → 新分={new:g}  ({p.replace(os.path.expanduser('~'), '~')})")
    print(f"\n【保留且排名变化】{len(rank_changes)} 条（正=上升，负=下降）：")
    for p, d in rank_changes[:20]:
        print(f"  ~ {os.path.basename(p)}  排名{'↑' if d > 0 else '↓'}{abs(d)}  ({p.replace(os.path.expanduser('~'), '~')})")
    if len(rank_changes) > 20:
        print(f"  … 其余 {len(rank_changes) - 20} 条略")
    print(f"\n【汇总】新增 {len(added)} / 退出 {len(removed)} / 保留 {len(kept)} / 排名变化 {len(rank_changes)}")
    return {"n": len(rows), "added": len(added), "removed": len(removed),
            "kept": len(kept), "rank_changed": len(rank_changes),
            "w1": w1, "w2": w2, "top": top_n}


def main():
    perm_audit("L3")
    # A5-P2 验证隔离：--curation-dir 覆盖扫描目录（cron 无参调用不受影响）
    # A2 新增：--compare 只读对照实验 + --top/--w-complexity/--w-stability 权重可调
    ap = argparse.ArgumentParser(description="重要性评分（A5-P2 峰终双锚 + A2 双轴加权）")
    ap.add_argument("--curation-dir", default=CURATION, help="覆盖扫描目录（默认 $SIKU_ROOT/smart/curation）")
    ap.add_argument("--compare", action="store_true", help="只读对照实验：原 top-N vs 双轴加权 top-N（不写库）")
    ap.add_argument("--dual-axis", action="store_true",
                    help="生产评分启用双轴加权（复杂度轴C+稳定性轴S；权重 --w-complexity/--w-stability，默认 1.0/1.0）")
    ap.add_argument("--top", type=int, default=TOP_N, help=f"对照实验 top-N（默认 {TOP_N}）")
    ap.add_argument("--w-complexity", type=float, default=W_COMPLEXITY, help=f"复杂度权重 w1（默认 {W_COMPLEXITY}）")
    ap.add_argument("--w-stability", type=float, default=W_STABILITY, help=f"稳定性权重 w2（默认 {W_STABILITY}）")
    args = ap.parse_args()
    # A2 --compare：只读模式，不写文件、不更新 importance，直接返回
    if args.compare:
        compare_topn(args.curation_dir, top_n=args.top, w1=args.w_complexity, w2=args.w_stability)
        return
    yamls = sorted(glob.glob(os.path.join(args.curation_dir, "*/*.yaml")))
    updated = errors = skipped = peak_hits = end_hits = 0
    for fpath in yamls:
        try:
            ok, msg = process(fpath, dual_axis=args.dual_axis,
                               w1=args.w_complexity, w2=args.w_stability)
            if ok:
                updated += 1
                # 统计锚点命中（读取写回后的 meta 字段统计，避免二次解析开销：
                # process 返回 true 即 importance 变化，锚点标注已写入文件）
            elif msg == "unchanged":
                skipped += 1
            else:
                errors += 1
                logging.warning(f"{fpath}: {msg}")
        except Exception as e:
            errors += 1
            logging.error(f"{fpath}: {e}")
    # 锚点命中统计：只统计本次实际更新的条目（幂等跳过的不重复计）
    for fpath in yamls:
        try:
            with open(fpath) as f:
                raw = f.read()
            r = get_meta(raw)
            if r is None:
                continue
            meta, _, _ = r
            if meta.get("peak_anchored"):
                peak_hits += 1
            if meta.get("end_anchored"):
                end_hits += 1
        except Exception:
            pass
    _mode = "双轴加权" if args.dual_axis else "原公式"
    logging.info(f"完成[{_mode}]: {updated}更新 / {errors}错误 / {skipped}跳过 / {len(yamls)}总计 / 峰值锚{peak_hits} / 终点锚{end_hits}")
    print(f"✅ 评分完成[{_mode}]: {updated}更新 / {errors}错误 / {skipped}跳过 / {len(yamls)}总计 / 峰值锚{peak_hits} / 终点锚{end_hits}")


if __name__ == "__main__":
    main()
