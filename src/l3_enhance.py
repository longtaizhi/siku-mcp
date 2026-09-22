#!/usr/bin/env python3
"""
四库系统 v3.0 · P1 L3加固
============================
功能：
  1. 时间编码（Mem0衰减公式：trust_score × (1-0.05)^天数）
  2. 写入过滤（空摘要/超长内容/low confidence/黑名单模式）
  3. 矛盾检测（同agent同主题新覆盖旧；不同agent→标记待仲裁）
  4. 冲突仲裁降级（检出率不足时→时间戳优先）
  5. 快照合并逻辑（修改前自动快照到 audit_log + merge_history）

用法：
  python3 l3_enhance.py           # 全量加固扫描
  python3 l3_enhance.py --verify  # 验收测试
  python3 l3_enhance.py --dry-run # 预览模式

交付物：本文件
"""

import json, os, sys, time, hashlib, re, sqlite3, yaml
import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）
from datetime import datetime, timezone, timedelta
from pathlib import Path

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

# ── 配置 ──────────────────────────────────────────
BASE = Path(_SIKU_ROOT)
DB_PATH = Path(os.environ.get("SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db")))
TZ = timezone(timedelta(hours=8))
DECAY_RATE = 0.05        # 每日衰减率
CONFIDENCE_FLOOR = 0.1   # 置信度下限
MAX_CONTENT_LEN = 10000  # 内容最大字符数
MIN_SUMMARY_LEN = 4      # 摘要最少字符数
MIN_CONFIDENCE = 0.3     # 写入最低置信度

# ── 黑名单模式（写入过滤） ─────────────────────────
BANNED_PATTERNS = [
    r"^(test|测试|忽略|delete|remove)\s*$",
    r"^你是一个AI(助手|助理|模型)",
    r"system prompt",
    r"^#+\s*$",
]

# ── 相似度判断stopwords ───────────────────────────
SIMILAR_STOPWORDS = {
    "的", "了", "是", "在", "和", "就", "都", "而", "及", "与",
    "着", "或", "一个", "没有", "我们", "你们", "他们", "这个", "那个",
    "不", "也", "很", "到", "说", "要", "有", "会", "可以", "如果",
}

results_log = []


# ═══════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════

def db_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def now_iso():
    return datetime.now(TZ).isoformat()


def log_result(name, ok, detail=""):
    status = "✅" if ok else "❌"
    results_log.append((name, status, detail))
    print(f"  {status} {name} — {detail}")


# ═══════════════════════════════════════════════════
#  1. 时间编码（Mem0衰减公式）
# ═══════════════════════════════════════════════════

def decay_trust_score(original_score: float, days_elapsed: int) -> float:
    """
    Mem0衰减公式：
      trust_score = original_score × (1 - DECAY_RATE) ^ days_elapsed
    下限 CONFIDENCE_FLOOR（0.1），half_life='permanent'不衰减
    """
    if days_elapsed <= 0:
        return round(original_score, 2)
    decayed = original_score * ((1 - DECAY_RATE) ** days_elapsed)
    return round(max(decayed, CONFIDENCE_FLOOR), 2)


def compute_days_elapsed(timestamp_iso: str) -> int:
    """计算从timestamp到今天的天数"""
    try:
        ts = datetime.fromisoformat(timestamp_iso)
        now = datetime.now(TZ)
        delta = now - ts
        return max(0, delta.days)
    except (ValueError, TypeError):
        return 0


def apply_time_decay_all(dry_run: bool = False) -> dict:
    """全量执行时间衰减"""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, trust_score, timestamp, half_life FROM memory_store")
    rows = cur.fetchall()
    
    updated = 0
    skipped = 0
    for row in rows:
        rid = row["id"]
        if row["half_life"] == "permanent":
            skipped += 1
            continue
        days = compute_days_elapsed(row["timestamp"])
        if days <= 0:
            skipped += 1
            continue
        new_score = decay_trust_score(row["trust_score"], days)
        if abs(new_score - row["trust_score"]) >= 0.01:
            if not dry_run:
                cur.execute(
                    "UPDATE memory_store SET trust_score=?, updated_at=? WHERE id=?",
                    (new_score, now_iso(), rid)
                )
            updated += 1
        else:
            skipped += 1
    
    if not dry_run:
        conn.commit()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：trust_score 更新后清缓存
    conn.close()
    return {"total": len(rows), "updated": updated, "skipped": skipped}


# ═══════════════════════════════════════════════════
#  2. 写入过滤（防污染规则）
# ═══════════════════════════════════════════════════

def check_write_filter(summary: str, content: str, confidence: float,
                       source_agent: str) -> dict:
    """
    写入前过滤检查：
    - 空摘要（<4字）→ 拒绝
    - 超长内容（>10000）→ 拒绝
    - 低置信度（<0.3）→ 拒绝
    - 匹配黑名单模式 → 拒绝
    """
    reasons = []
    if not summary or len(summary.strip()) < MIN_SUMMARY_LEN:
        reasons.append(f"摘要过短({len(summary.strip()) if summary else 0}<{MIN_SUMMARY_LEN})")
    if content and len(content) > MAX_CONTENT_LEN:
        reasons.append(f"内容超长({len(content)}>{MAX_CONTENT_LEN})")
    if summary and len(summary) > 2000:
        reasons.append(f"摘要超长({len(summary)}>2000)")
    if confidence < MIN_CONFIDENCE:
        reasons.append(f"置信度过低({confidence}<{MIN_CONFIDENCE})")
    for pattern in BANNED_PATTERNS:
        if re.search(pattern, summary.strip(), re.IGNORECASE):
            reasons.append(f"摘要匹配黑名单: {pattern}")
            break
    return {"pass": len(reasons) == 0, "reasons": reasons}


def batch_write_filter(dry_run: bool = False) -> dict:
    """全量扫描已有记录，不合格→train_eligible=0"""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, summary, content, confidence FROM memory_store")
    rows = cur.fetchall()
    rejected = 0
    for row in rows:
        content_str = row["content"] if row["content"] else ""
        result = check_write_filter(row["summary"], content_str,
                                     row["confidence"], "")
        if not result["pass"]:
            if not dry_run:
                cur.execute(
                    "UPDATE memory_store SET train_eligible=0, updated_at=? WHERE id=?",
                    (now_iso(), row["id"])
                )
            rejected += 1
    if not dry_run:
        conn.commit()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：train_eligible 更新后清缓存
    conn.close()
    return {"total": len(rows), "rejected": rejected}


# ═══════════════════════════════════════════════════
#  3. 矛盾检测（时间戳优先仲裁）
# ═══════════════════════════════════════════════════

def extract_keywords(text: str) -> set:
    if not text:
        return set()
    # 分离中英文：中文单字做独立token，英文按词分割
    # 在中英文/数字之间插入空格，然后分割
    text_lower = text.lower()
    text_lower = re.sub(r'([\u4e00-\u9fff])([a-zA-Z0-9])', r'\1 \2', text_lower)
    text_lower = re.sub(r'([a-zA-Z0-9])([\u4e00-\u9fff])', r'\1 \2', text_lower)
    tokens = re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z0-9]+', text_lower)
    return {t for t in tokens if t not in SIMILAR_STOPWORDS and len(t) > 1}


def topic_similarity(text_a: str, text_b: str) -> float:
    ka = extract_keywords(text_a)
    kb = extract_keywords(text_b)
    if not ka or not kb:
        return 0.0
    overlap = len(ka & kb)
    return overlap / max(len(ka), len(kb))


def detect_conflicts_all(min_similarity: float = 0.5, dry_run: bool = False,
                         max_pairs: int = 50000) -> dict:
    """
    全量矛盾检测（O(n²) 防护版）：
    两两比较→相似≥阈值→检查冲突
    - 同agent→时间戳优先自动仲裁
    - 不同agent→标记待人工仲裁
    - max_pairs 上限：按 updated_at DESC（最近更新优先）取前 N 对比较，
      防全量两两比较 O(n²) 过热；total_pairs=实际处理对数
    """
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, type, summary, content, source_agent, timestamp, confidence "
                "FROM memory_store ORDER BY updated_at DESC")
    rows = cur.fetchall()
    conn.close()
    
    total_pairs_all = len(rows) * (len(rows) - 1) // 2
    processed_pairs = 0
    capped = False
    conflicts = []
    resolved_by_timestamp = 0
    marked_for_arbitration = 0
    
    for i in range(len(rows)):
        if capped:
            break
        for j in range(i + 1, len(rows)):
            if processed_pairs >= max_pairs:
                capped = True
                break
            processed_pairs += 1
            if processed_pairs % 10000 == 0:
                print(f"    [进度] 已处理 {processed_pairs}/{total_pairs_all} 对"
                      f"（覆盖率 {processed_pairs / max(1, total_pairs_all) * 100:.2f}%）")
            a, b = rows[i], rows[j]
            sim_threshold = min_similarity + (0.15 if a["type"] == b["type"] else 0.0)
            sim = topic_similarity(
                f"{a['summary']} {a['content'] if a['content'] else ''}",
                f"{b['summary']} {b['content'] if b['content'] else ''}"
            )
            if sim < sim_threshold:
                continue
            
            if a["source_agent"] == b["source_agent"]:
                newer_id = a["id"] if a["timestamp"] >= b["timestamp"] else b["id"]
                older_id = b["id"] if newer_id == a["id"] else a["id"]
                conflicts.append({
                    "type": "timestamp_resolved",
                    "pair": (a["id"], b["id"]),
                    "similarity": round(sim, 3),
                    "newer_id": newer_id,
                    "resolution": f"同agent({a['source_agent']})→{newer_id}优先"
                })
                resolved_by_timestamp += 1
                if not dry_run:
                    _append_audit(older_id, {
                        "action": "conflict_resolved", "conflict_with": newer_id,
                        "resolution": "timestamp_priority", "newer_wins": newer_id,
                        "reason": f"同agent({a['source_agent']})自动仲裁",
                        "timestamp": now_iso()
                    })
            else:
                conflicts.append({
                    "type": "needs_arbitration",
                    "pair": (a["id"], b["id"]),
                    "similarity": round(sim, 3),
                    "agents": (a["source_agent"], b["source_agent"]),
                    "resolution": "pending_arbitration"
                })
                marked_for_arbitration += 1
                if not dry_run:
                    for rid in [a["id"], b["id"]]:
                        _append_audit(rid, {
                            "action": "conflict_detected",
                            "conflict_with": b["id"] if rid == a["id"] else a["id"],
                            "similarity": round(sim, 3),
                            "resolution": "needs_arbitration_by_laozi",
                            "timestamp": now_iso()
                        })
    
    if capped:
        coverage = processed_pairs / max(1, total_pairs_all) * 100
        print(f"    [警告] 达 max_pairs 上限({max_pairs})，提前终止："
              f"实际处理 {processed_pairs}/{total_pairs_all} 对，"
              f"覆盖率 {coverage:.2f}%")
    
    if not dry_run:
        conn2 = db_conn()
        conn2.commit()
        conn2.close()
        query_cache_invalidate.invalidate_query_cache()  # S2 M3：矛盾检测 audit 后清缓存
    
    return {
        "total_pairs": processed_pairs,
        "total_pairs_all": total_pairs_all,
        "coverage_pct": round(processed_pairs / max(1, total_pairs_all) * 100, 2),
        "conflicts_detected": len(conflicts),
        "resolved_by_timestamp": resolved_by_timestamp,
        "marked_for_arbitration": marked_for_arbitration,
        "conflicts": conflicts[:20]
    }


# ═══════════════════════════════════════════════════
#  4. 冲突仲裁降级
# ═══════════════════════════════════════════════════

ARBITRATION_CONFIG = {
    "mode": "auto",
    "min_similarity": 0.5,
    "auto_downgrade": True,
    "downgrade_mode": "timestamp_first",
    "detection_rate_target": 0.90,
    "false_positive_limit": 0.10,
}


def evaluate_detection_quality() -> dict:
    """评估矛盾检测质量"""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM memory_store")
    total = cur.fetchone()[0]
    conn.close()
    result = detect_conflicts_all(dry_run=True)
    total_pairs = result["total_pairs"]
    detection_count = result["conflicts_detected"]
    estimated_recall = min(1.0, detection_count / max(1, total_pairs * 0.1))
    estimated_fp = min(1.0, detection_count * 0.1 / max(1, total_pairs))
    return {
        "total_entries": total, "total_pairs": total_pairs,
        "detections": detection_count,
        "estimated_recall": round(estimated_recall, 3),
        "estimated_fp_rate": round(estimated_fp, 3),
        "current_mode": ARBITRATION_CONFIG["mode"],
    }


def get_effective_mode() -> str:
    """根据当前指标决定仲裁模式"""
    quality = evaluate_detection_quality()
    if not ARBITRATION_CONFIG["auto_downgrade"]:
        return ARBITRATION_CONFIG["mode"]
    if quality["estimated_recall"] < ARBITRATION_CONFIG["detection_rate_target"]:
        return ARBITRATION_CONFIG["downgrade_mode"]
    if quality["estimated_fp_rate"] > ARBITRATION_CONFIG["false_positive_limit"]:
        return ARBITRATION_CONFIG["downgrade_mode"]
    return "full_detection"


# ═══════════════════════════════════════════════════
#  5. 快照合并逻辑
# ═══════════════════════════════════════════════════

def snapshot_entry(entry_id: str, reason: str = "update") -> dict:
    """修改前快照：当前状态写入audit_log"""
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM memory_store WHERE id=?", (entry_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"error": f"entry {entry_id} not found"}
    snapshot = {k: row[k] for k in row.keys() if k != "rowid"}
    for k, v in snapshot.items():
        if hasattr(v, 'isoformat'):
            snapshot[k] = v.isoformat()
    if "embedding" in snapshot:
        emb = snapshot.get("embedding")
        if emb:
            snapshot["embedding_preview"] = f"<blob:{len(emb)}bytes>"
        snapshot["embedding"] = None
    snapshot_str = json.dumps(snapshot, ensure_ascii=False, default=str)[:500]
    _append_audit(entry_id, {
        "action": "snapshot", "reason": reason,
        "timestamp": now_iso(),
        "snapshot_preview": snapshot_str,
    })
    return {"entry_id": entry_id, "reason": reason, "snapshot_size": len(snapshot_str)}


def _append_audit(entry_id: str, entry: dict):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT audit_log FROM memory_store WHERE id=?", (entry_id,))
    row = cur.fetchone()
    log = json.loads(row[0]) if row and row[0] else []
    log.append(entry)
    cur.execute("UPDATE memory_store SET audit_log=?, updated_at=? WHERE id=?",
                (json.dumps(log, ensure_ascii=False), now_iso(), entry_id))
    conn.commit()
    conn.close()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：audit_log 追加后清缓存


def merge_entries(target_id: str, source_ids: list,
                  reason: str = "manual_merge", merged_by: str = "system") -> dict:
    """合并条目：快照+更新merge_history"""
    conn = db_conn()
    cur = conn.cursor()
    for sid in [target_id] + source_ids:
        snapshot_entry(sid, f"merge: {reason}")
    cur.execute("SELECT merge_history FROM memory_store WHERE id=?", (target_id,))
    row = cur.fetchone()
    existing = json.loads(row[0]) if row and row[0] else []
    for sid in source_ids:
        existing.append({
            "merged_from_id": sid, "merged_into_id": target_id,
            "original_content_snapshot": "见audit_log",
            "reason": reason, "confidence": None,
            "merged_by": merged_by, "merged_at": now_iso(),
        })
    cur.execute("UPDATE memory_store SET merge_history=?, updated_at=? WHERE id=?",
                (json.dumps(existing, ensure_ascii=False), now_iso(), target_id))
    for sid in source_ids:
        _append_audit(sid, {"action": "merged_into", "target_id": target_id,
                            "reason": reason, "timestamp": now_iso()})
    conn.commit()
    conn.close()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：merge_history 追加后清缓存
    return {"target": target_id, "sources": source_ids, "snapshots_taken": 1 + len(source_ids)}


# ═══════════════════════════════════════════════════
#  一键加固
# ═══════════════════════════════════════════════════

def run_all_enhancements(dry_run: bool = False):
    print(f"  [1/5] 时间编码（衰减公式）")
    decay_result = apply_time_decay_all(dry_run=dry_run)
    log_result("时间编码", True,
               f"共{decay_result['total']}条，更新{decay_result['updated']}条，跳过{decay_result['skipped']}条")
    
    print(f"  [2/5] 写入过滤（防污染）")
    filter_result = batch_write_filter(dry_run=dry_run)
    log_result("写入过滤", True,
               f"扫描{filter_result['total']}条，拒绝{filter_result['rejected']}条")
    
    print(f"  [3/5] 矛盾检测+仲裁")
    conflict_result = detect_conflicts_all(dry_run=dry_run)
    skipped_pairs = conflict_result['total_pairs_all'] - conflict_result['total_pairs']
    log_result("矛盾检测", True,
               f"实际处理{conflict_result['total_pairs']}对"
               f"（全量{conflict_result['total_pairs_all']}对，跳过{skipped_pairs}对，"
               f"覆盖率{conflict_result['coverage_pct']}%），"
               f"发现{conflict_result['conflicts_detected']}处矛盾"
               f"（时间戳仲裁{conflict_result['resolved_by_timestamp']}处"
               f" + 待仲裁{conflict_result['marked_for_arbitration']}处）")
    
    print(f"  [4/5] 仲裁降级评估")
    effective = get_effective_mode()
    quality = evaluate_detection_quality()
    log_result("仲裁降级评估", True,
               f"有效模式={effective}, 预估检出率={quality['estimated_recall']}, "
               f"预估误报率={quality['estimated_fp_rate']}")
    
    print(f"  [5/5] 快照合并验证")
    conn = db_conn()
    cur = conn.cursor()
    # NULL id 边缘行防御：id 唯一索引中 NULL 排最前，无 WHERE 的 LIMIT 1 会命中
    # NULL id 行 → snapshot_entry(None) 返回 error 字典 → 取键 KeyError。三段防御：
    # a) SQL 侧过滤 NULL；b) first["id"] 判空；c) 取用前判 snapshot_size 键存在
    cur.execute("SELECT id FROM memory_store WHERE id IS NOT NULL LIMIT 1")
    first = cur.fetchone()
    conn.close()
    if first and first["id"]:
        snap_result = snapshot_entry(first["id"], reason="P1_enhancement")
        if "snapshot_size" in snap_result:
            log_result("快照合并", True,
                       f"条目{first['id']}快照完成: {snap_result['snapshot_size']}bytes")
        else:
            log_result("快照合并", False,
                       f"条目{first['id']}快照失败: {snap_result.get('error', '未知错误')}")
    else:
        log_result("快照合并", False,
                   "memory_store 无有效 id（全部为 NULL），跳过步骤5")


def run_verification():
    """独立验收测试"""
    def check(name, ok, detail=""):
        s = "✅" if ok else "❌"
        print(f"  {s} {name:<35} {detail}")
        return (name, ok, detail)
    
    checks = []
    print("  ── 1. 时间编码 ──")
    d7 = decay_trust_score(1.0, 7)
    checks.append(check("1a 7天衰减",
          abs(d7 - round(0.95**7, 2)) < 0.01,
          f"1.0×0.95^7={round(0.95**7,2)}→{d7}"))
    d365 = decay_trust_score(0.5, 365)
    checks.append(check("1b 衰减不低0.1",
          d365 >= CONFIDENCE_FLOOR, f"0.5×0.95^365→{d365}"))
    checks.append(check("1c 0天不衰减",
          decay_trust_score(0.8, 0) == 0.8, ""))
    days = compute_days_elapsed((datetime.now(TZ) - timedelta(days=3)).isoformat())
    checks.append(check("1d 天数计算", days == 3, f"3天前→{days}天"))
    
    print("  ── 2. 写入过滤 ──")
    checks.append(check("2a 空摘要",
          not check_write_filter("", "内容", 0.9, "")["pass"], ""))
    checks.append(check("2b 低置信度",
          not check_write_filter("摘要", "内容", 0.1, "")["pass"], ""))
    checks.append(check("2c 超长内容",
          not check_write_filter("摘要", "x"*10001, 0.9, "")["pass"], ""))
    checks.append(check("2d 黑名单",
          not check_write_filter("test", "内容", 0.9, "")["pass"], ""))
    checks.append(check("2e 合法通过",
          check_write_filter("飞书Token限流经验", "内容", 0.9, "")["pass"], ""))
    
    print("  ── 3. 矛盾检测 ──")
    kw = extract_keywords("飞书Token每分钟限500次调用")
    checks.append(check("3a 关键词提取",
          "飞书" in kw, f"提取{len(kw)}个"))
    checks.append(check("3b 相似主题",
          topic_similarity("飞书Token限流500次", "飞书Token每分钟500次限制") >= 0.5,
          f"sim={topic_similarity('飞书Token限流500次','飞书Token每分钟500次限制'):.3f}"))
    checks.append(check("3c 不相似",
          topic_similarity("飞书Token限流", "今天天气很好") < 0.3,
          f"sim={topic_similarity('飞书Token限流','今天天气很好'):.3f}"))
    
    print("  ── 4. 仲裁配置 ──")
    checks.append(check("4a 配置完整",
          all(k in ARBITRATION_CONFIG for k in
              ["mode","min_similarity","auto_downgrade","downgrade_mode"]),
          ""))
    checks.append(check("4b 降级模式",
          ARBITRATION_CONFIG["downgrade_mode"] == "timestamp_first", ""))
    
    print("  ── 5. 快照合并 ──")
    test_id = f"test_snap_{int(time.time())}"
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO memory_store (id,type,timestamp,summary,content,confidence,source_agent) VALUES (?,?,?,?,?,?,?)",
                (test_id,"lesson",now_iso(),"快照测试","内容",0.9,"test"))
    conn.commit()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：自检插入后清缓存（全挂姿势）
    snap = snapshot_entry(test_id, "验收测试")
    checks.append(check("5a 快照记录",
          snap.get("error") is None and snap["snapshot_size"] > 0,
          f"size={snap['snapshot_size']}bytes"))
    cur.execute("SELECT audit_log FROM memory_store WHERE id=?", (test_id,))
    alog = cur.fetchone()
    if alog and alog[0]:
        parsed = json.loads(alog[0])
        has_snap = any(e.get("action") == "snapshot" for e in parsed)
        checks.append(check("5b audit_log含快照", has_snap, f"共{len(parsed)}条日志"))
    cur.execute("DELETE FROM memory_store WHERE id=?", (test_id,))
    conn.commit()
    conn.close()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：自检删除后清缓存（全挂姿势）
    
    passed = sum(1 for r in checks if r[1])
    total = len(checks)
    print(f"\n  {'='*40}")
    print(f"  验收: {passed}/{total} 通过 {'✅' if passed==total else '⚠️'}")
    return passed == total


if __name__ == "__main__":
    if "--verify" in sys.argv:
        sys.exit(0 if run_verification() else 1)
    dry_run = "--dry-run" in sys.argv
    print("=" * 55)
    print(f"  四库系统 · P1 L3加固")
    print(f"  模式: {'预览' if dry_run else '正式'}")
    print(f"  时间: {now_iso()}")
    print(f"  环境: {DB_PATH}")
    print("=" * 55)
    run_all_enhancements(dry_run=dry_run)
    print(f"\n  完成。")
    for name, status, detail in results_log:
        print(f"  {status} {name:<25} {detail}")
