#!/usr/bin/env python3
"""
基因晋升管道 — 将 memory-bank 中高置信 lesson 晋升至四库 L4b（genes 层）
【2026-08-28 三库优化第一批 G0：verdict 信号源改造】
  - per-entry verdict JSONL 落盘（append-only，三源对拍源②）
  - 退出码保留：全部成功 0 / 存在错误 1（与旧语义一致）
  - 参数化 --db/--genes-dir/--verdict-file/--no-drive-card（沙盒/测试可覆盖）

逻辑：
1. 连接 memory-bank.db
2. 查询 type='lesson' 且 confidence >= 0.85 的条目
3. 对每条，检查 L4b genes/ 目录下是否已存在相同 summary 的文件（去重）
4. 不存在的 → 写入 L4b YAML；每条落一行 verdict 到 JSONL

用法:
  python3 gene_promote.py [--db PATH] [--genes-dir DIR] [--verdict-file PATH] [--no-drive-card]
"""

import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

# ── 配置（默认值；命令行可覆盖——沙盒/测试）──────────────────

MEMORY_BANK_DB = os.path.join(_HERMES_HOME, "memory-bank/memory.db")
L4B_GENES_DIR = os.path.join(_SIKU_ROOT, "smart/curation/genes")
VERDICT_FILE = os.path.join(_SIKU_ROOT, "logs/gene_promote_verdict.jsonl")
GENE_DRIVE_CARD = os.path.join(_HERMES_HOME, "scripts/genes/gene_drive_card.py")

PROMOTE_TYPE = "lesson"
CONFIDENCE_THRESHOLD = 0.85


def parse_args(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="基因晋升管道（G0 verdict 信号源——per-entry JSONL+退出码保留）")
    p.add_argument("--db", default=MEMORY_BANK_DB, help="memory-bank 库路径（沙盒可覆盖）")
    p.add_argument("--genes-dir", default=L4B_GENES_DIR, help="L4b genes 目录（沙盒可覆盖）")
    p.add_argument("--verdict-file", default=VERDICT_FILE, help="per-entry verdict JSONL 落盘路径（append-only）")
    p.add_argument("--no-drive-card", action="store_true", help="金牌基因不建草稿卡（沙盒/只读验证用）")
    return p.parse_args(argv)


def _append_verdict(args, entry, verdict, detail=""):
    """per-entry verdict 信号——append-only JSONL（G0 三源对拍源②）。

    verdict 取值：promote（晋升成功）/ skip_dedup（去重跳过，detail 区分 file/summary）/
                  error（写入失败）。非候选（低于阈值）不落记录——对拍时无记录=未晋升。
    """
    import json as _json
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "entry_id": entry.get("id", ""),
        "confidence": entry.get("confidence"),
        "summary": (entry.get("summary", "") or "")[:80],
        "verdict": verdict,
        "exit_code": 0 if verdict in ("promote", "skip_dedup") else 1,
        "detail": detail,
    }
    try:
        os.makedirs(os.path.dirname(args.verdict_file) or ".", exist_ok=True)
        with open(args.verdict_file, "a", encoding="utf-8") as f:
            f.write(_json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"  ⚠️ verdict 落盘失败: {e}")
    return rec


# ── memory-bank 操作 ─────────────────────────────────

def get_memory_conn(db_path):
    if not os.path.exists(db_path):
        print(f"❌ memory-bank 数据库不存在: {db_path}")
        sys.exit(1)
    # (2026-08-17): busy_timeout=5000——并发写卡点不再 locked 崩溃
    conn = sqlite3.connect(db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def query_lesson_candidates(conn):
    rows = conn.execute(
        "SELECT id, type, summary, content, confidence, task_id "
        "FROM memory_fragments "
        "WHERE type = ? AND confidence >= ? "
        "ORDER BY confidence DESC",
        (PROMOTE_TYPE, CONFIDENCE_THRESHOLD),
    ).fetchall()
    return [dict(r) for r in rows]


# ── 去重检查 ─────────────────────────────────────────

def summary_exists_in_genes(summary_text, genes_dir):
    """扫描 L4b genes/ 目录，检查是否有文件包含相同 summary"""
    if not os.path.isdir(genes_dir):
        return False
    for fname in os.listdir(genes_dir):
        if not fname.endswith((".yaml", ".yml")):
            continue
        fpath = os.path.join(genes_dir, fname)
        try:
            with open(fpath, "r") as f:
                content = f.read()
            # 在 YAML 中查找 summary 字段
            for line in content.split("\n"):
                if line.startswith("summary:"):
                    existing = line[len("summary:"):].strip()
                    # 去掉可能的引号包裹
                    if existing.startswith('"') and existing.endswith('"'):
                        existing = existing[1:-1].replace('\\"', '"').replace("\\\\", "\\")
                    if existing == summary_text:
                        return True
                    break  # 一个文件只有一个 summary
        except (OSError, UnicodeDecodeError):
            continue
    return False


# ── L4b YAML 写入 ────────────────────────────────────

def _yaml_quote(s):
    """安全地将字符串写为 YAML 单行标量（必要时加双引号）"""
    if not s:
        return '""'
    # 如果含 YAML 特殊字符，加双引号并转义
    if any(c in s for c in (':', '#', '{', '}', '[', ']', ',', '&', '*', '?', '|', '-', '<', '>', '=', '!', '%', '@', '`', '"', "'", "\\")):
        escaped = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return s


def write_l4b_yaml(entry, genes_dir):
    """将一条 memory-bank 条目写入 L4b YAML 文件"""
    entry_id = entry["id"]
    fpath = os.path.join(genes_dir, f"{entry_id}.yaml")

    if os.path.exists(fpath):
        print(f"  ⏭️  文件已存在: {fpath}")
        return False, "file_exists"

    os.makedirs(os.path.dirname(fpath), exist_ok=True)

    with open(fpath, "w", encoding="utf-8") as f:
        f.write(f"type: {entry['type']}\n")
        f.write(f"source_type: lesson\n")
        f.write(f"confidence: {entry['confidence']}\n")
        f.write(f"importance: 5\n")
        f.write(f"source_agent: memory-bank\n")
        summary = entry.get("summary", "")
        f.write(f"summary: {_yaml_quote(summary)}\n")
        task_id = entry.get("task_id", "")
        if task_id:
            f.write(f"task_ids:\n  - {_yaml_quote(task_id)}\n")
        body = entry.get("content", "")
        f.write(f"body: |\n")
        if body:
            for line in body.split("\n"):
                f.write(f"  {line}\n")
        else:
            f.write(f"  \n")

    print(f"  ✅ 写入: {fpath}")
    return True, ""


# ── 主逻辑 ───────────────────────────────────────────

def main(argv=None):
    args = parse_args(argv)
    print(f"[{datetime.now(timezone.utc).isoformat()}] gene-promote 管道启动 (verdict 信号源已启用)")
    print(f"  DB: {args.db}")
    print(f"  L4b: {args.genes_dir}")
    print(f"  verdict JSONL: {args.verdict_file}")
    print()

    # 连接数据库
    conn = get_memory_conn(args.db)

    # 查询 candidates
    candidates = query_lesson_candidates(conn)
    conn.close()

    print(f"📋 查询到 {len(candidates)} 条待晋升候选 (type=lesson, confidence≥{CONFIDENCE_THRESHOLD})")
    print()

    promoted = 0
    skipped_dedup = 0
    skipped_error = 0

    for entry in candidates:
        summary = entry.get("summary", "")
        print(f"  条目: {entry['id']} | confidence={entry['confidence']} | summary={summary[:50]}{'...' if len(summary) > 50 else ''}")

        # 去重检查
        if summary_exists_in_genes(summary, args.genes_dir):
            print(f"    ⏭️  去重跳过 (summary 已存在于 L4b)")
            skipped_dedup += 1
            _append_verdict(args, entry, "skip_dedup", "summary 已存在于 L4b")
            continue

        # 写入
        try:
            ok, detail = write_l4b_yaml(entry, args.genes_dir)
            if ok:
                promoted += 1
                _append_verdict(args, entry, "promote", "")
                # ── 金牌基因 → 自动建草稿卡 ─────────────────────
                conf = entry.get("confidence", 0)
                if conf >= 0.95 and not args.no_drive_card:  # 金牌基因
                    fpath = os.path.join(args.genes_dir, f"{entry['id']}.yaml")
                    try:
                        r = subprocess.run(
                            [sys.executable, GENE_DRIVE_CARD, fpath],
                            capture_output=True, text=True, timeout=30,
                        )
                        if r.returncode == 0:
                            print(f"    🃏 草稿卡已建: {r.stdout.strip()[:80]}")
                        else:
                            print(f"    ⚠️ 建卡失败: {r.stderr.strip()[:100]}")
                    except Exception as e:
                        print(f"    ⚠️ 建卡异常: {e}")
            else:
                skipped_dedup += 1
                _append_verdict(args, entry, "skip_dedup", detail or "文件已存在")
        except Exception as e:
            print(f"    ❌ 写入失败: {e}")
            skipped_error += 1
            _append_verdict(args, entry, "error", str(e))

    # 汇总
    print()
    total_checked = len(candidates)
    print(f"{'='*50}")
    print(f"  ✅ 晋升:   {promoted}")
    print(f"  ⏭️  去重跳过: {skipped_dedup}")
    print(f"  ❌ 错误:   {skipped_error}")
    print(f"  📊 总计:   {total_checked}")
    print(f"{'='*50}")

    # 退出码保留：全部成功 0 / 存在错误 1（G0 要求——verdict 信号源对拍依据）
    return 0 if skipped_error == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
