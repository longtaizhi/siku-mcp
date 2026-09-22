#!/usr/bin/env python3
"""
5-F: 过期数据清理 — 每周运行
标记 expires_at 已过的条目为 deprecated（不删除，保留审计追溯）。
覆盖两层：
  1. L4b: smart/curation/ 下所有 YAML（industry + type 分类）
  2. L3 : memory_store.db 表（A6-P2 新增：L3 过期条目标记 deprecated）

过期判定：expires_at 非空且 < 当前 UTC 时间。时间戳比较前做时区归一
（统一取前19字符 "YYYY-MM-DDTHH:MM:SS"，兼容带 +00:00 后缀的 ISO 格式）。
"""
import json, os, sqlite3, yaml, glob, sys
from datetime import datetime, timezone

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

BASE = _SIKU_ROOT
CURATION_DIR = os.path.join(BASE, "smart", "curation")
L3_DB = os.path.join(BASE, "memory_store.db")

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

def _norm_ts(s):
    """时区归一：只取前19字符（YYYY-MM-DDTHH:MM:SS），兼容带+00:00后缀"""
    return (s or "").strip()[:19]

def _is_expired(expires):
    """expires 非空且已过当前时间 → True"""
    if not expires:
        return False
    e = _norm_ts(expires)
    return bool(e) and e < now_iso()

def cleanup_industry():
    """遍历 industry/ 下所有YAML，标记过期条目"""
    ind_dir = os.path.join(CURATION_DIR, "industry")
    if not os.path.isdir(ind_dir):
        print("industry/ 目录不存在，跳过")
        return 0, 0
    
    total = 0
    expired = 0
    for root, dirs, files in os.walk(ind_dir):
        for f in files:
            if not f.endswith((".yaml", ".yml")):
                continue
            fpath = os.path.join(root, f)
            total += 1
            with open(fpath) as fh:
                content = fh.read()
            
            try:
                parts = content.split("---")
                if len(parts) < 3:
                    continue
                front = yaml.safe_load(parts[1])
            except Exception:
                continue
            
            if front is None: continue
            expires = front.get("expires_at", "") or ""
            if not expires:
                continue
            
            if _is_expired(expires):
                # 标记 deprecated，不删除
                front["deprecated"] = True
                front["deprecated_at"] = now_iso()
                # 重写文件
                new_front = "---\n" + yaml.dump(front, allow_unicode=True, default_flow_style=False, sort_keys=False) + "---\n"
                rest = "---".join(parts[2:])
                with open(fpath, "w") as fw:
                    fw.write(new_front + rest.strip())
                expired += 1
                print(f"  ⚠️ expired: {f}")
    
    return total, expired

def cleanup_l3_table():
    """A6-P2: L3 memory_store 表过期条目标记 deprecated（不删除）。
    幂等：deprecated 列不存在时自动 ALTER TABLE 添加。
    返回: (已检查条数, 新标记数)
    """
    if not os.path.exists(L3_DB):
        print("L3 DB 不存在，跳过")
        return 0, 0

    conn = sqlite3.connect(L3_DB)
    conn.row_factory = sqlite3.Row
    try:
        # 幂等加列
        cols = [r[1] for r in conn.execute("PRAGMA table_info(memory_store)")]
        if "deprecated" not in cols:
            conn.execute("ALTER TABLE memory_store ADD COLUMN deprecated INTEGER DEFAULT 0")
            print("  ➕ 已为 memory_store 添加 deprecated 列")
        if "deprecated_at" not in cols:
            conn.execute("ALTER TABLE memory_store ADD COLUMN deprecated_at TEXT DEFAULT ''")
            print("  ➕ 已为 memory_store 添加 deprecated_at 列")
        conn.commit()

        now = now_iso()
        # 已过期且未标记的条目（一次性全部取出，Python 侧做时区归一比较）
        rows = conn.execute(
            "SELECT id, expires_at, deprecated FROM memory_store "
            "WHERE expires_at IS NOT NULL AND expires_at != ''"
        ).fetchall()
        total = 0
        to_mark = []
        for r in rows:
            total += 1
            if int(r["deprecated"] or 0) == 1:
                continue  # 已标记过
            if _is_expired(r["expires_at"]):
                to_mark.append(r["id"])
        if to_mark:
            marks = ",".join("?" * len(to_mark))
            conn.execute(
                f"UPDATE memory_store SET deprecated=1, deprecated_at=?, updated_at=? "
                f"WHERE id IN ({marks})",
                [now, now] + to_mark,
            )
            conn.commit()
        return total, len(to_mark)
    finally:
        conn.close()

if __name__ == "__main__":
    print(f"[{now_iso()}] 过期数据清理开始")
    total, expired = cleanup_industry()
    print(f"[{now_iso()}] L4b 完成: 检查{total}个YAML, 标记{expired}个过期")
    l3_total, l3_marked = cleanup_l3_table()
    print(f"[{now_iso()}] L3 完成: 检查{l3_total}条, 标记{l3_marked}条过期")
    print("过期数据仅标记 deprecated，未删除。")
