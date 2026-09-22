#!/usr/bin/env python3
"""
5-C: 健全性哨兵 — 系统自检
常规: 写入/搜索/权限/审计日志/管道/清理 (每6小时)
完整: + Profile文件frontmatter检查 (每日2次, 06:00/18:00)
"""
import json, os, sqlite3, sys, time, re
import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_OPENCLAW_HOME = os.environ.get("OPENCLAW_HOME", os.path.expanduser("~/.openclaw"))  # OpenClaw 主目录（环境变量可覆盖）

BASE = os.environ.get("SIKU_BASE") or _SIKU_ROOT  # : 沙盒测试可覆盖
WORKSPACE = os.path.join(_OPENCLAW_HOME, "workspace")
DB = os.path.join(BASE, "memory_store.db")
LOG = os.path.join(BASE, "logs", "sentinel.log")
os.makedirs(os.path.join(BASE, "logs"), exist_ok=True)

def log(msg):
    with open(LOG, "a") as f:
        f.write(f"[{datetime.now().isoformat()}] {msg}\n")
    print(msg)

def check_write():
    """T1: 写入测试条目"""
    import uuid
    conn = sqlite3.connect(DB)
    eid = f"sentinel-test-{uuid.uuid4().hex[:8]}"
    ts = datetime.now().isoformat()
    conn.execute("INSERT OR IGNORE INTO memory_store (id, version, timestamp, type, summary, confidence, importance, source_agent, audit_log, created_at, updated_at) VALUES (?, '1.0', ?, 'test', ?, 1.0, 1, 'sentinel', '[]', ?, ?)",
                 (eid, ts, f"哨兵自检: {ts}", ts, ts))
    conn.commit()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：自检插入后清缓存（全挂姿势）
    row = conn.execute("SELECT id FROM memory_store WHERE id=?", (eid,)).fetchone()
    conn.close()
    return row is not None, eid

def check_search(eid):
    """T2: 搜索该条目"""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id FROM memory_store WHERE id=?", (eid,)).fetchall()
    conn.close()
    return len(rows) > 0

def check_permission():
    """T3: 权限配置存在"""
    pf = os.path.join(BASE, "config", "permissions.yaml")
    if not os.path.exists(pf):
        pf = os.path.join(BASE, "permissions.yaml")
    return os.path.exists(pf), pf

def check_audit():
    """T4: 审计日志已生成"""
    ad = os.path.join(BASE, "audit")
    return os.path.isdir(ad), len(os.listdir(ad)) if os.path.isdir(ad) else 0

def check_pipeline():
    """T5: 管道可触发"""
    reta = os.path.join(BASE, "scripts", "reta_pipeline.py")
    graph = os.path.join(BASE, "scripts", "graph_builder.py")
    return os.path.exists(reta), os.path.exists(graph)

def _audit_warn(tag, detail):
    """审计 WARN（复用 gate_check 通道: audit/write/audit-YYYY-MM-DD.yaml, alert: yes）"""
    try:
        wd = os.path.join(BASE, "audit", "write")
        os.makedirs(wd, exist_ok=True)
        af = os.path.join(wd, "audit-%s.yaml" % datetime.now().strftime("%Y-%m-%d"))
        with open(af, "a") as f:
            f.write("---\nts: %s\nop: %s\nop_type: warn\neid: %s\ntype: guard\nsummary_a: %s\nalert: yes\n---\n"
                    % (datetime.now().isoformat(), tag, tag, detail))
    except Exception:
        pass

def check_distill():
    """T9 蒸馏补偿(): 蒸馏日(周一 12:00, pipeline_runner distill-monday)未完成/失败
    → audit WARN + 写补跑标志（次日周二 pipeline_runner 补跑）；补跑完成自动清标志。
    防策展断供：蒸馏缺跑不再静默（tracker 证据: distill-monday / distill_rc）。"""
    flag = os.path.join(BASE, ".locks", "distill_catchup.flag")
    tr = {}
    try:
        with open(os.path.join(BASE, ".locks", "scheduler_tracker.json")) as f:
            tr = json.load(f)
    except Exception:
        pass
    now_dt = datetime.now()
    last = tr.get("distill-monday") or tr.get("distill")
    last_rc = tr.get("distill-monday_rc", tr.get("distill_rc"))
    try:
        last_date = datetime.fromisoformat(last).date() if last else None
    except Exception:
        last_date = None

    if now_dt.weekday() == 0 and now_dt.hour >= 13:
        # 周一 13:00 后：12:00 蒸馏 + 1h 宽限。缺跑或失败 → 告警 + 补跑标志
        if last_date != now_dt.date():
            with open(flag, "w") as f:
                f.write(now_dt.isoformat())
            _audit_warn("distill-missing", "蒸馏日(周一)未完成 → 已写补跑标志，次日(周二)补跑")
            log("  T9 蒸馏补偿: ❌ 周一蒸馏缺失 → audit WARN + 补跑标志已写")
            return False
        if last_rc not in (None, 0):
            with open(flag, "w") as f:
                f.write(now_dt.isoformat())
            _audit_warn("distill-failed", "蒸馏日完成但 rc=%s → 已写补跑标志，次日补跑" % last_rc)
            log("  T9 蒸馏补偿: ❌ 周一蒸馏 rc=%s → audit WARN + 补跑标志已写" % last_rc)
            return False
        return True
    elif now_dt.weekday() == 1 and os.path.exists(flag):
        # 周二：检查补跑是否完成
        if last_date == now_dt.date() and last_rc == 0:
            try:
                os.remove(flag)
            except Exception:
                pass
            log("  T9 蒸馏补偿: ✅ 周二补跑完成，标志已清")
            return True
        _audit_warn("distill-catchup-pending", "补跑日(周二)蒸馏仍未完成 → 持续告警")
        log("  T9 蒸馏补偿: ❌ 周二补跑仍未完成 → 持续告警")
        return False
    return True

def check_cleanup(eid):
    """T6: 清理测试条目"""
    conn = sqlite3.connect(DB)
    conn.execute("DELETE FROM memory_store WHERE id=?", (eid,))
    conn.commit()
    conn.close()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：自检删除后清缓存（全挂姿势）
    return True

def check_frontmatter():
    """T7: 检查Profile文件YAML frontmatter完整性"""
    # 2026-08-03 迁移设计：TOOLS.md 内容已并入 AGENTS.md "Local notes (migrated from TOOLS.md)" 章节，
    # TOOLS.md 文件由迁移机制定期移走（备份至 $OPENCLAW_HOME/backups/tools-md-migration），T7 不再检查该文件。
    profiles = ["SOUL.md", "AGENTS.md", "IDENTITY.md", "MEMORY.md", "USER.md"]
    ok_count = 0
    total = len(profiles)
    details = []
    for pf in profiles:
        path = os.path.join(WORKSPACE, pf)
        if not os.path.exists(path):
            details.append({"file": pf, "ok": False, "reason": "文件不存在"})
            continue
        with open(path) as f:
            content = f.read()
        # 检查YAML frontmatter: 首行 ---, 有agent/type/schema_version
        if not content.startswith("---"):
            details.append({"file": pf, "ok": False, "reason": "首行不是---"})
            continue
        # 找第二个 ---
        second = content.find("---", 3)
        if second == -1:
            details.append({"file": pf, "ok": False, "reason": "缺少结尾---"})
            continue
        yaml_block = content[3:second]
        has_agent = "agent:" in yaml_block
        has_type = "type:" in yaml_block
        if has_agent and has_type:
            ok_count += 1
            details.append({"file": pf, "ok": True})
        else:
            missing = []
            if not has_agent: missing.append("agent")
            if not has_type: missing.append("type")
            details.append({"file": pf, "ok": False, "reason": f"缺少字段: {','.join(missing)}"})
    
    # 验证HEARTBEAT.md不应有YAML frontmatter
    hb = os.path.join(WORKSPACE, "HEARTBEAT.md")
    if os.path.exists(hb):
        with open(hb) as f:
            first = f.read(10)
        if first.startswith("---"):
            details.append({"file": "HEARTBEAT.md", "ok": False, "reason": "不应有YAML frontmatter"})
        else:
            ok_count += 1
            details.append({"file": "HEARTBEAT.md", "ok": True})
            total += 1
    
    all_ok = ok_count == total
    return all_ok, {"passed": ok_count, "total": total, "details": details}

def run_all(full=False):
    mode = "完整" if full else "常规"
    log(f"\n=== 哨兵自检 ({mode}) ===")
    results = []
    all_ok = True

    # T1: 写入
    ok, eid = check_write()
    results.append({"test": "写入", "ok": ok, "detail": eid[:20] if ok else "FAIL"})
    log(f"  T1 写入: {'✅' if ok else '❌'} {eid[:20]}")
    if not ok: all_ok = False

    if ok:
        # T2: 搜索
        ok2 = check_search(eid)
        results.append({"test": "搜索", "ok": ok2, "detail": "found" if ok2 else "missing"})
        log(f"  T2 搜索: {'✅' if ok2 else '❌'}")
        if not ok2: all_ok = False

        # T3: 权限
        ok3, pf = check_permission()
        results.append({"test": "权限", "ok": ok3, "detail": pf})
        log(f"  T3 权限: {'✅' if ok3 else '❌'} {pf}")
        if not ok3: all_ok = False

        # T4: 审计日志
        ok4, cnt = check_audit()
        results.append({"test": "审计日志", "ok": ok4, "detail": f"{cnt}个目录"})
        log(f"  T4 审计: {'✅' if ok4 else '❌'} {cnt}个目录/文件")
        if not ok4: all_ok = False

        # T5: 管道
        ok5a, ok5b = check_pipeline()
        ok5 = ok5a and ok5b
        results.append({"test": "管道", "ok": ok5, "detail": f"reta={'✅' if ok5a else '❌'} graph={'✅' if ok5b else '❌'}"})
        log(f"  T5 管道: {'✅' if ok5 else '❌'} reta={'✅' if ok5a else '❌'} graph={'✅' if ok5b else '❌'}")
        if not ok5: all_ok = False

        # T6: 清理
        check_cleanup(eid)
        results.append({"test": "清理", "ok": True, "detail": eid[:20]})
        log(f"  T6 清理: ✅")

    # T7: Profile文件frontmatter检查（仅完整模式）
    if full:
        ok7, fm_detail = check_frontmatter()
        results.append({"test": "Profile格式", "ok": ok7, "detail": f"{fm_detail['passed']}/{fm_detail['total']} 正常"})
        log(f"  T7 Profile格式: {'✅' if ok7 else '❌'} {fm_detail['passed']}/{fm_detail['total']}")
        if not ok7:
            for d in fm_detail["details"]:
                if not d["ok"]:
                    log(f"      ❌ {d['file']}: {d.get('reason','')}")
            all_ok = False

    # T8: industry/ 目录检查
    ind_dir = os.path.join(BASE, "smart", "curation", "industry")
    if os.path.isdir(ind_dir):
        subdirs = [d for d in os.listdir(ind_dir) if os.path.isdir(os.path.join(ind_dir, d))]
        total_yamls = 0
        for sd in subdirs:
            total_yamls += len([f for f in os.listdir(os.path.join(ind_dir, sd)) if f.endswith((".yaml", ".yml"))])
        ok8 = total_yamls > 0
        results.append({"test": "industry目录", "ok": ok8, "detail": f"{len(subdirs)}个子目录, {total_yamls}个YAML"})
        log(f"  T8 industry目录: {'✅' if ok8 else '❌'} {len(subdirs)}个子目录, {total_yamls}个YAML")
        if not ok8: all_ok = False
    else:
        results.append({"test": "industry目录", "ok": False, "detail": "目录不存在"})
        log("  T8 industry目录: ❌ 目录不存在")
        all_ok = False

    # T9: 蒸馏补偿（周一蒸馏缺跑/失败 → 告警+次日补跑；周二补跑完成清标志）
    ok9 = check_distill()
    results.append({"test": "蒸馏补偿", "ok": ok9, "detail": "蒸馏日检查完成" if ok9 else "需补跑/告警"})
    if not ok9: all_ok = False

    status = "healthy" if all_ok else "degraded"
    report = {"timestamp": datetime.now().isoformat(), "status": status, "mode": mode, "results": results}
    
    rpath = os.path.join(BASE, "logs", f"sentinel-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json")
    with open(rpath, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    n = len(results)
    log(f"\n状态: {status.upper()} ({sum(1 for r in results if r['ok'])}/{n})")
    if all_ok:
        log("✅ 系统健康")
    else:
        log(f"⚠️ {sum(1 for r in results if not r['ok'])}项异常，需人工排查")
        log(f"报告: {rpath}")
    
    return all_ok

if __name__ == "__main__":
    full = "--full" in sys.argv
    ok = run_all(full=full)
    sys.exit(0 if ok else 1)