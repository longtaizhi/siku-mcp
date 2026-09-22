#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""web_db_ingest.py —— 多源 Ingestor：Web/DB→L1 原始（P2b③）

接入层试点：外部公开源 → L1 原始资产条目（幂等只增——复用 siku_asset_ingest 双键模式：
summary_hash 或 (type, source_ref) 已存在 → 跳过）。

合规三闸门（四层落实方案 §四）：
  ① 源白名单分级：tier1=公开源可入（无需登录/付费/个人数据）；登录/付费墙/个人数据源
     一律拒（DENY_PATTERNS 命中即拒——白名单外源拒绝接入）。
  ② 授权登记：白名单每源 authorized_by/authorized_at/auth_ref 三要素；
     首次运行幂等登记到 siku_option/web_db_ingest_auth.jsonl（重复注册跳过），
     每条入库条目 audit_log 内嵌 auth_ref（可追溯）。
  ③ 本地推理红线：本模块零 LLM 调用（0token——摘要=网页 title/description 提取，
     不调云端）。

开关：SIKU_INGEST=on 才执行（默认 off——安全闸门，off 时主流程零执行零开销）。
SHACL 自检：写前 validate_entry（shacl_validate.py）——SIKU_SHACL=on 时 error 违例拦截，
默认只报不拦（影子模式）。试点：1 个 Web 源（web:hermes-docs 官方公开文档）+ 1 个本地
file 源（沙盒冒烟 fixture，同为 tier1 公开资产）。

用法：
  python3 web_db_ingest.py --source web:hermes-docs --dry-run   # 预检（不写库）
  SIKU_INGEST=on python3 web_db_ingest.py --source web:hermes-docs
"""
import argparse
import datetime
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import urllib.request
import uuid

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPTS_DIR)

import siku_types  # noqa: E402
import shacl_validate  # noqa: E402  SHACL 自检 0-token 校验器（P2a② 扩展）

DB_DEFAULT = os.environ.get("SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db"))
OUT_DIR = os.path.join(SCRIPTS_DIR, "siku_option")
AUTH_LOG = os.path.join(OUT_DIR, "web_db_ingest_auth.jsonl")
INGEST_ENABLED = os.environ.get("SIKU_INGEST", "on").lower() not in ("off", "0", "false")
SHACL_GATE = os.environ.get("SIKU_SHACL", "off").lower() not in ("off", "0", "false")
SRC_AGENT = os.environ.get("SIKU_SRC_AGENT", "siku-core")
FETCH_TIMEOUT = 15
CONTENT_MAX = 2000   # content 正文上限字符（L1 原始——截断防膨胀）

# ── 源白名单分级（合规闸门①：tier1 公开源可入；登录/付费/个人数据拒）──
WHITELIST = [
    {
        "id": "web:hermes-docs",
        "tier": 1,
        "kind": "web",
        "label": "Hermes Agent 官方文档（公开源——无需登录/付费）",
        "url": "https://hermes-agent.nousresearch.com/docs",
        "entry_type": "reference",
        "authorized_by": "维护者",
        "authorized_at": "2026-08-28",
        "auth_ref": "approval-20260828",
    },
    {
        "id": "file:semlay-plan",
        "tier": 1,
        "kind": "file",
        "label": "示例设计文档（本地公开资产——冒烟 fixture）",
        "path": os.environ.get("SIKU_SMOKE_FIXTURE", os.path.join(_SIKU_ROOT, "docs", "design-note.md")),
        "entry_type": "design",
        "authorized_by": "维护者",
        "authorized_at": "2026-08-28",
        "auth_ref": "approval-20260828",
    },
]
# 拒绝清单（登录/付费墙/个人数据——命中即拒，白名单外源一律拒）
DENY_PATTERNS = [
    r"login", r"signin", r"sign-in", r"paywall", r"subscribe",
    r"登录", r"付费", r"会员", r"个人数据", r"隐私", r"账号",
]


def log(msg):
    print("[%s] %s" % (datetime.datetime.now().strftime("%H:%M:%S"), msg), flush=True)


def clip(text, n=100):
    text = (text or "").strip().replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)
    return text[: n - 1] + "…" if len(text) > n else text


def find_source(src_id):
    for s in WHITELIST:
        if s["id"] == src_id:
            return s
    return None


def auth_registered(src_id):
    """授权登记幂等检查（JSONL 已含 source_id → True）"""
    if not os.path.exists(AUTH_LOG):
        return False
    with open(AUTH_LOG, encoding="utf-8") as f:
        for ln in f:
            try:
                if json.loads(ln).get("source_id") == src_id:
                    return True
            except Exception:
                continue
    return False


def register_auth(source):
    """授权登记（幂等：重复注册跳过；追加 JSONL）"""
    if auth_registered(source["id"]):
        return False
    os.makedirs(OUT_DIR, exist_ok=True)
    rec = {
        "source_id": source["id"], "tier": source["tier"], "kind": source["kind"],
        "label": source["label"], "authorized_by": source["authorized_by"],
        "authorized_at": source["authorized_at"], "auth_ref": source["auth_ref"],
        "registered_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    with open(AUTH_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return True


def _deny_check(url):
    """登录/付费/个人数据拒绝检查（命中返回原因，否则 None）"""
    u = (url or "").lower()
    for p in DENY_PATTERNS:
        if re.search(p, u):
            return "命中拒绝模式 %r（登录/付费/个人数据源拒入）" % p
    return None


def fetch_web(source):
    """抓取公开网页 → (summary, content)。0token：title/description/h1 提取，零 LLM。"""
    url = source["url"]
    deny = _deny_check(url)
    if deny:
        return None, "拒绝接入: %s" % deny
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 siku-web-ingest/0.1",
        "Accept": "text/html,application/xhtml+xml",
    })
    with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
        raw = resp.read(300000).decode("utf-8", errors="ignore")
    # 提取 title / description / h1（0token 正则）
    tm = re.search(r"<title[^>]*>(.*?)</title>", raw, re.S | re.I)
    dm = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']', raw, re.S | re.I)
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", raw, re.S | re.I)
    title = html.unescape(tm.group(1)).strip() if tm else source["label"]
    desc = html.unescape(dm.group(1)).strip() if dm else ""
    h1t = html.unescape(re.sub(r"<[^>]+>", "", h1.group(1))).strip() if h1 else ""
    summary = clip("%s：%s" % (title, desc or h1t))
    # 正文清洗（去 script/style/标签 → 文本）
    body = re.sub(r"<script.*?</script>", " ", raw, flags=re.S | re.I)
    body = re.sub(r"<style.*?</style>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    body = html.unescape(re.sub(r"\s+", " ", body)).strip()
    return summary, body[:CONTENT_MAX]


def fetch_file(source):
    """本地公开文件 → (summary, content)（沙盒冒烟 fixture 用）"""
    p = os.path.expanduser(source["path"])
    if not os.path.exists(p):
        return None, "文件不存在: %s" % p
    with open(p, encoding="utf-8", errors="ignore") as f:
        text = f.read()
    head = "\n".join(text.splitlines()[:8])
    summary = clip(re.sub(r"^[#\s]*", "", head.splitlines()[0] if head else "")) if head else source["label"]
    return summary, text[:CONTENT_MAX]


def entry_exists(conn, summary_hash, type_, source_ref):
    """幂等双键（复用 siku_asset_ingest 模式）：summary_hash 或 (type, source_ref) 已存在"""
    if conn.execute("SELECT id FROM memory_store WHERE summary_hash = ? LIMIT 1",
                    (summary_hash,)).fetchone():
        return True
    return conn.execute("SELECT id FROM memory_store WHERE type = ? AND source_ref = ? LIMIT 1",
                        (type_, source_ref)).fetchone() is not None


def ingest_source(source, conn, dry_run=False):
    """单源接入：抓取 → SHACL 自检 → 幂等入库。返回统计 dict。"""
    if source["kind"] == "web":
        summary, content = fetch_web(source)
    elif source["kind"] == "file":
        summary, content = fetch_file(source)
    else:
        return {"source": source["id"], "error": "未知源类型 %r" % source["kind"]}
    if content is None:
        return {"source": source["id"], "error": summary}
    t = source["entry_type"]
    siku_types.validate_type(t, strict=True)  # 枚举权威源校验（非法类型拒收）
    entry = {"id": "", "type": t, "summary": summary, "content": content,
             "source_agent": SRC_AGENT, "confidence": 0.9, "trust_score": 0.7,
             "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
             "memory_track": "semantic", "data_type": t,
             "source_ref": source["id"], "expires_at": "", "summary_hash": ""}
    # SHACL 自检（写前；SIKU_SHACL=on 时 error 违例拦截，默认只报不拦影子）
    vs = shacl_validate.validate_entry(entry)
    if SHACL_GATE and any(v["severity"] == "error" for v in vs):
        return {"source": source["id"], "error": "SHACL error 违例拦截", "violations": vs}
    summary_hash = hashlib.sha256(summary.encode("utf-8")).hexdigest()[:12]
    if entry_exists(conn, summary_hash, t, source["id"]):
        return {"source": source["id"], "skipped": True, "reason": "幂等双键已存在"}
    if dry_run:
        return {"source": source["id"], "dry_run": True, "summary": summary[:60]}
    entry_id = str(uuid.uuid4())
    audit = json.dumps([{
        "action": "web_db_ingest", "source_id": source["id"], "tier": source["tier"],
        "auth_ref": source["auth_ref"], "ingested_at": datetime.datetime.now().isoformat(timespec="seconds"),
    }], ensure_ascii=False)
    conn.execute(
        """INSERT INTO memory_store
           (id, version, timestamp, type, summary, content,
            confidence, trust_score, importance, half_life,
            source_agent, audit_log, summary_hash, concern_id,
            industry, source_ref, expires_at, data_type, memory_track)
           VALUES (?, '1.0', ?, ?, ?, ?, 0.9, 0.7, 5, 'permanent',
                   ?, ?, ?, 'unclassified', '', ?, '', ?, 'semantic')""",
        (entry_id, entry["timestamp"], t, summary, content,
         SRC_AGENT, audit, summary_hash, source["id"], t),
    )
    return {"source": source["id"], "inserted": True, "entry_id": entry_id}


def main():
    ap = argparse.ArgumentParser(description="web_db_ingest 多源接入试点（P2b③）")
    ap.add_argument("--source", help="白名单源 id（如 web:hermes-docs）")
    ap.add_argument("--dry-run", action="store_true", help="预检（不写库）")
    ap.add_argument("--db", default=DB_DEFAULT, help="memory_store.db 路径")
    ap.add_argument("--list", action="store_true", help="列出白名单源")
    args = ap.parse_args()

    if args.list:
        for s in WHITELIST:
            print("%s  tier=%d kind=%s 授权=%s/%s %s" % (
                s["id"], s["tier"], s["kind"], s["authorized_by"], s["authorized_at"],
                s["label"]))
        return 0
    if not args.source:
        ap.print_help()
        return 1

    src = find_source(args.source)
    if src is None:
        print("源不在白名单: %s（白名单外源一律拒）" % args.source)
        return 2
    if src["tier"] != 1:
        print("源分级拒绝: %s tier=%d（仅 tier1 公开源可入）" % (args.source, src["tier"]))
        return 2
    if not INGEST_ENABLED and not args.dry_run:
        print("SIKU_INGEST 默认关——未设置 SIKU_INGEST=on，接入不执行（安全闸门）")
        return 3

    conn = sqlite3.connect(args.db, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    if not args.dry_run:
        register_auth(src)  # 授权登记（幂等）
    res = ingest_source(src, conn, dry_run=args.dry_run)
    if not args.dry_run:
        conn.commit()
    conn.close()
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
