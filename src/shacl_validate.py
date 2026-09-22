#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""shacl_validate.py —— SHACL 自检 0-token 校验器（P2a②）

对标 Semantica 本体治理 SHACL 校验——本系统不装 pyshacl（第三方依赖零新增），
以 shapes.yaml 结构化约束 + 纯 stdlib 规则引擎实现同功能自检（0-token：全脚本零 LLM）。

两种模式：
  --full          资产全量自检：只读 SELECT memory_store WHERE type IN 13 资产类型，
                  逐条过 shapes 约束 → 违例清单（id/type/shape/severity/当前值/期望）
                  落盘 JSON+MD（siku_option/shacl_full_YYYYMMDD.json/.md）。
  --entry '<json>' 增量写时自检：单条 entry dict（web_db_ingest/siku_asset_ingest 写前
                  调用 validate_entry()），返回违例列表——调用方按 SIKU_SHACL 门控决定
                  拦截或标记（默认只报不拦，影子模式）。

枚举权威源：siku_types.py（引用不复制——R13.18 防漂移）；shapes.yaml 只放非枚举硬约束。
违例只报告交人工裁决——校验器零写权限，绝不自动修（任何模式不写 memory_store）。
"""
import argparse
import datetime
import hashlib
import json
import os
import re
import sqlite3
import sys
import uuid

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

import siku_types  # noqa: E402  类型/冲突/关系枚举权威源

SHAPES_PATH = os.path.join(SCRIPTS_DIR, "shapes.yaml")
DB_DEFAULT = os.environ.get("SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db"))
OUT_DIR = os.path.join(SCRIPTS_DIR, "siku_option")

# ISO8601 时间戳正则（宽容：秒/小数秒/Z/±时区）
_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$"
)
_HASH12_RE = re.compile(r"^[0-9a-f]{12}$")
_ID_OK_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$|^mem_|^concept_|^g\d{3}$")


def load_shapes(path=SHAPES_PATH):
    """加载 shapes.yaml（YAML 子集解析——纯 stdlib 零依赖）。"""
    shapes = []
    cur = None
    with open(path, encoding="utf-8") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if re.match(r"^\s*-\s*id:", ln):
                cur = {"id": ln.split("id:", 1)[1].strip()}
                shapes.append(cur)
            elif cur is not None:
                m = re.match(r"^\s+([a-z_]+):\s*(.*)$", ln)
                if m:
                    cur[m.group(1)] = m.group(2).strip().strip('"')
    return shapes


def _to_num(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _check_shape(shape, row):
    """单条 shape 校验 → (violated, detail) 或 None（不适用）"""
    sid = shape["id"]
    scope = shape.get("scope", "all")
    t = (row.get("type") or "").strip()
    if scope == "content_types" and not siku_types.is_content_type(t):
        return None
    if scope == "asset_types" and not siku_types.is_asset_type(t):
        return None

    def _violate(detail):
        return (sid, shape.get("severity", "warning"), detail)

    if sid == "type_valid":
        if not siku_types.validate_type(t, strict=False):
            return _violate("type=%r 非法（合法 25 种）" % t)
    elif sid == "summary_required":
        if not (row.get("summary") or "").strip():
            return _violate("summary 为空")
    elif sid == "summary_len":
        s = row.get("summary") or ""
        if len(s) > 200:
            return _violate("summary 长度 %d > 200" % len(s))
    elif sid == "content_required":
        if not (row.get("content") or "").strip():
            return _violate("content 为空")
    elif sid == "source_agent_required":
        if not (row.get("source_agent") or "").strip():
            return _violate("source_agent 为空")
    elif sid == "source_ref_format":
        if not (row.get("source_ref") or "").strip():
            return _violate("source_ref 为空（资产本体单源化要求）")
    elif sid == "confidence_range":
        v = _to_num(row.get("confidence"))
        if v is not None and not (0.0 <= v <= 1.0):
            return _violate("confidence=%r 超出 [0,1]" % row.get("confidence"))
    elif sid == "trust_score_range":
        v = _to_num(row.get("trust_score"))
        if v is not None and not (0.0 <= v <= 1.0):
            return _violate("trust_score=%r 超出 [0,1]" % row.get("trust_score"))
    elif sid == "timestamp_iso":
        ts = row.get("timestamp")
        if not ts or not isinstance(ts, str) or not _ISO_RE.match(ts.strip()):
            return _violate("timestamp=%r 缺失或非 ISO8601" % ts)
    elif sid == "expires_at_iso_or_empty":
        ea = row.get("expires_at")
        if ea and not _ISO_RE.match(str(ea).strip()):
            return _violate("expires_at=%r 非 ISO8601" % ea)
    elif sid == "memory_track_valid":
        mt = (row.get("memory_track") or "").strip()
        if mt not in ("semantic", "episodic"):
            return _violate("memory_track=%r 非法或为空" % row.get("memory_track"))
    elif sid == "data_type_required":
        if not (row.get("data_type") or "").strip():
            return _violate("data_type 为空")
    elif sid == "summary_hash_format":
        h = row.get("summary_hash")
        if h and not (isinstance(h, str) and _HASH12_RE.match(h)):
            return _violate("summary_hash=%r 非 sha256 hex[:12]" % h)
    elif sid == "id_format":
        i = (row.get("id") or "")
        if i and not _ID_OK_RE.match(i):
            return _violate("id=%r 不符合 uuid4/mem_/concept_ 规范" % i)
    return None


def validate_entry(entry, shapes=None):
    """增量写时自检：单条 entry dict → 违例列表 [{"shape","severity","detail"}]。
    调用方（web_db_ingest/siku_asset_ingest 写前）按 SIKU_SHACL 门控使用。
    任何异常 → 返回 []（fail-open：校验故障不阻断写入）。
    """
    try:
        shapes = shapes if shapes is not None else load_shapes()
        out = []
        for sh in shapes:
            r = _check_shape(sh, entry)
            if r:
                out.append({"shape": r[0], "severity": r[1], "detail": r[2]})
        return out
    except Exception:
        return []


def full_scan(db_path=DB_DEFAULT, limit=None):
    """资产全量自检：只读 SELECT 13 资产类型 → 逐条过 shapes → 违例清单。
    返回 {"total", "violations": [...], "by_severity": {...}}。零写库。
    """
    asset_list = ", ".join("?" * len(siku_types.ASSET_TYPES))
    sql = ("SELECT * FROM memory_store WHERE type IN (%s)" % asset_list)
    if limit:
        sql += " LIMIT %d" % int(limit)
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, list(siku_types.ASSET_TYPES)).fetchall()
    conn.close()
    shapes = load_shapes()
    violations = []
    for r in rows:
        row = dict(r)
        for sh in shapes:
            v = _check_shape(sh, row)
            if v:
                violations.append({
                    "id": row.get("id"), "type": row.get("type"),
                    "shape": v[0], "severity": v[1], "detail": v[2],
                })
    by_sev = {"error": 0, "warning": 0}
    for v in violations:
        by_sev[v["severity"]] = by_sev.get(v["severity"], 0) + 1
    return {"total": len(rows), "violations": violations, "by_severity": by_sev}


def _save_report(res, kind="full"):
    os.makedirs(OUT_DIR, exist_ok=True)
    day = datetime.datetime.now().strftime("%Y%m%d")
    jp = os.path.join(OUT_DIR, "shacl_%s_%s.json" % (kind, day))
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    md = os.path.join(OUT_DIR, "shacl_%s_%s.md" % (kind, day))
    lines = ["# SHACL 自检报告（0-token，P2a②）",
             "", "- 运行时间: %s" % datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
             "- 模式: %s（资产全量只读 / 增量写时）" % kind,
             "- 资产条目数: %d" % res.get("total", 0),
             "- 违例数: %d（error=%d, warning=%d）" % (
                 len(res.get("violations", [])),
                 res.get("by_severity", {}).get("error", 0),
                 res.get("by_severity", {}).get("warning", 0)),
             "", "## 违例清单（交人工裁决，不自动修）", ""]
    if res.get("violations"):
        lines.append("| id | type | shape | severity | detail |")
        lines.append("|---|---|---|---|---|")
        for v in res["violations"]:
            lines.append("| %s | %s | %s | %s | %s |" % (
                (v.get("id") or "")[:20], v.get("type"), v["shape"], v["severity"],
                v["detail"][:60]))
    else:
        lines.append("（无违例——error 与 warning 全零）")
    with open(md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return jp, md


def main():
    ap = argparse.ArgumentParser(description="SHACL 自检 0-token 校验器（P2a②）")
    ap.add_argument("--full", action="store_true", help="资产全量自检（只读）")
    ap.add_argument("--entry", metavar="JSON", help="增量写时自检单条 entry")
    ap.add_argument("--db", default=DB_DEFAULT, help="memory_store.db 路径")
    ap.add_argument("--limit", type=int, default=None, help="全量扫描条数上限（测试）")
    args = ap.parse_args()

    if args.entry:
        entry = json.loads(args.entry)
        vs = validate_entry(entry)
        print(json.dumps({"entry_id": entry.get("id"), "violations": vs,
                          "ok": not any(v["severity"] == "error" for v in vs)},
                         ensure_ascii=False, indent=2))
        return 0 if not any(v["severity"] == "error" for v in vs) else 3
    if args.full:
        res = full_scan(args.db, limit=args.limit)
        jp, md = _save_report(res, "full")
        print(json.dumps({
            "total": res["total"], "violation_count": len(res["violations"]),
            "by_severity": res["by_severity"],
            "report_json": jp, "report_md": md,
            "error_zero": res["by_severity"].get("error", 0) == 0,
        }, ensure_ascii=False, indent=2))
        return 0 if res["by_severity"].get("error", 0) == 0 else 3
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
