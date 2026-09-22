#!/usr/bin/env python3
"""
5-E: 多版本回滚 — 快照管理
基于 l3_enhance.py 已有的快照机制扩展

命令: create | list | rollback
"""
import json, os, sqlite3, sys, hashlib, base64
import query_cache_invalidate  # S2 M3 清缓存挂钩（轻量纯 stdlib）
from datetime import datetime

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

BASE = _SIKU_ROOT
DB = os.path.join(BASE, "memory_store.db")
SNAP_DIR = os.path.join(BASE, "smart", "snapshots")
os.makedirs(SNAP_DIR, exist_ok=True)

def get_checksum(conn):
    """计算DB摘要"""
    rows = conn.execute("SELECT id, version, timestamp, summary, confidence FROM memory_store ORDER BY id").fetchall()
    data = json.dumps([dict(r) for r in rows], sort_keys=True)
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def _encode_entry(entry):
    """将dict中的bytes字段转为base64字符串"""
    d = dict(entry)
    for k, v in d.items():
        if isinstance(v, bytes):
            d[k] = base64.b64encode(v).decode('ascii')
    return d

def cmd_create():
    """创建全量快照"""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM memory_store").fetchall()

    snap = {
        "id": f"snap-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        "created_at": datetime.now().isoformat(),
        "total": len(rows),
        "checksum": get_checksum(conn),
        "entries": [_encode_entry(r) for r in rows]
    }

    fpath = os.path.join(SNAP_DIR, f"{snap['id']}.json")
    with open(fpath, "w") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)

    size = os.path.getsize(fpath)
    conn.close()
    print(f"✅ 快照已创建: {snap['id']}")
    print(f"   条目: {snap['total']}, 大小: {size/1024:.1f}KB, 校验: {snap['checksum']}")
    return snap["id"]

def cmd_list():
    """列出所有快照"""
    snaps = sorted([f for f in os.listdir(SNAP_DIR) if f.endswith(".json")])
    if not snaps:
        print("暂无快照")
        return
    
    print(f"{'快照ID':<30} {'条目':>6} {'大小':>8} {'日期':<20}")
    print("-" * 70)
    for fname in snaps:
        fpath = os.path.join(SNAP_DIR, fname)
        size = os.path.getsize(fpath)
        try:
            with open(fpath) as f:
                snap = json.load(f)
            print(f"{snap['id']:<30} {snap['total']:>6} {size/1024:>7.1f}KB {snap['created_at'][:19]:<20}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"⚠️ {fname:<27} {size/1024:>7.1f}KB ❌ 文件损坏: {e}")

def cmd_rollback(snap_id):
    """回滚到指定快照"""
    fpath = os.path.join(SNAP_DIR, f"{snap_id}.json")
    if not os.path.exists(fpath):
        print(f"❌ 快照不存在: {snap_id}")
        return False
    
    try:
        with open(fpath) as f:
            snap = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ 快照文件损坏: {snap_id} ({e})")
        return False
    
    conn = sqlite3.connect(DB)
    conn.isolation_level = None  # 手动事务控制
    conn.execute("BEGIN")
    before = conn.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
    
    # 逐条恢复
    restored = 0
    for entry in snap["entries"]:
        conn.execute("""INSERT OR REPLACE INTO memory_store
            (id, version, timestamp, type, summary, content,
             confidence, trust_score, importance, half_life, source_agent,
             audit_log, merge_history, created_at, updated_at,
             train_eligible, data_type, train_batch_id,
             g1_check_passed, g2_labeled_by, g3_reviewed_by, g3_result,
             correction_count, corrected_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (entry["id"], entry.get("version","1.0"), entry.get("timestamp",""),
             entry.get("type","fact"), entry.get("summary",""), entry.get("content",""),
             entry.get("confidence",0.7), entry.get("trust_score",0.7),
             entry.get("importance",5), entry.get("half_life","permanent"),
             entry.get("source_agent","unknown"),
             entry.get("audit_log","[]"), entry.get("merge_history","[]"),
             entry.get("created_at",""), entry.get("updated_at",""),
             entry.get("train_eligible",0), entry.get("data_type",""),
             entry.get("train_batch_id",""), entry.get("g1_check_passed",0),
             entry.get("g2_labeled_by",""), entry.get("g3_reviewed_by",""),
             entry.get("g3_result",""), entry.get("correction_count",0),
             entry.get("corrected_at","")))
        restored += 1
    
    conn.commit()
    query_cache_invalidate.invalidate_query_cache()  # S2 M3：快照回滚恢复写入后清缓存
    after = conn.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
    conn.close()
    
    print(f"✅ 回滚完成: {snap_id}")
    print(f"   恢复前: {before}条, 恢复后: {after}条")
    print(f"   恢复条目: {restored}条")
    return True

def main():
    if len(sys.argv) < 2:
        print("用法: python3 snapshot.py <create|list|rollback> [snap-id]")
        print("  create   — 创建全量快照")
        print("  list     — 列出所有快照")
        print("  rollback — 回滚到指定快照")
        sys.exit(1)
    
    cmd = sys.argv[1]
    if cmd == "create":
        cmd_create()
    elif cmd == "list":
        cmd_list()
    elif cmd == "rollback":
        if len(sys.argv) < 3:
            print("❌ 请指定快照ID: python3 snapshot.py rollback <snap-id>")
            sys.exit(1)
        cmd_rollback(sys.argv[2])
    else:
        print(f"❌ 未知命令: {cmd}")

if __name__ == "__main__":
    main()
