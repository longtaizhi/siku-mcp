#!/usr/bin/env python3
"""
entity_archive.py — 实体摘要网络（"关于 X 我已知什么"聚合层）
================================================================
（2026-08-17 实施）——实体表 + 本地模型实体抽取 + 聚合接口
（2026-08-18 实施）——实体层直连边：entity_relations 建表+规则建边+增量+查询

设计：
  1. 独立新模块（分文件隔离）——零修改 l3_retrieval.py 主链路
  2. 实体表：entity_archive（实体名/别名/聚合摘要）+ entity_entries（实体↔条目ID关联）
  3. 抽取：从 memory_store.summary 用本地模型（LLM_ENDPOINT 8081 默认）抽取实体；
     失败降级为规则抽取（书名号/引号/已知实体模式），绝不阻塞
     【端点守卫·红线6/D3 A5】实体抽取端点须为本机或内网本地模型；非本地端点一律
     拒绝调用（validate_llm_endpoint；允许 127.0.0.1/localhost/::1、RFC1918 私网、
     *.local；禁指向公网服务）
  4. 聚合接口："关于 X 我已知什么"——实体档案 + 关联条目聚合
  5. SIKU_* 开关：SIKU_ENTITY_ARCHIVE=off 一键关闭；SIKU_DB_PATH 复用；
     SIKU_ENTITY_LLM_ENDPOINT / SIKU_ENTITY_LLM_TIMEOUT / SIKU_ENTITY_MODEL 可覆盖
  6. 只写实体两张表，不碰 memory_store 任何行（零污染）
  7. U2 直连边（设计 37 号）：
     - entity_relations 表：直连边（entity→entity+类型+来源+证据），与 graph_edges 分层不合并
     - 规则建边 6 类（阶段1 规则增强）：shared_entry/alias_of/same_agent/same_type/
       keyword_overlap/semantic_similar——零 LLM 依赖，全部从既有表/元数据推导
     - 存量迁移：Batch1 确定性别名合并（规范化+括号模式，禁模糊合并）+ Batch2 规则回填
       （source='migration'）+ Batch3 增量（新条目 shared_entry 边）+ Batch4 验证
     - 关系类型体系 8 类（§三）：alias_of(1.0)/shared_entry(0.8)/same_agent(0.6)/
       same_type(0.7)/keyword_overlap(0.7)/semantic_similar(0.8)/part_of(0.9)/depends_on(0.9)
     - SIKU_ENTITY_RELATIONS：默认 on（U2 验证后启用；设计灰度意图，可 =off 一键关闭）
     - max_pairs 上限 + ORDER BY + 覆盖率日志（quadratic-loop-protection 纪律，防 O(n²) 膨胀）

用法：
  python3 entity_archive.py extract --limit 200      # 从 summary 抽取实体（幂等 upsert）
  python3 entity_archive.py query "示例实体"             # "关于 X 我已知什么"
  python3 entity_archive.py list --top 10            # 实体档案列表
  python3 entity_archive.py status                   # 档案统计
  python3 entity_archive.py migrate-relations        # Batch1 别名合并 + Batch2 规则回填
  python3 entity_archive.py relations "四库系统" --depth 2   # 实体关系查询（1跳直连/2跳meta-path）
  python3 entity_archive.py relations-status         # Batch4 验证统计
"""

import sys, os, json, re, sqlite3, hashlib, time, urllib.request, urllib.error
from datetime import datetime, timezone

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）

# ── 配置（SIKU_* 开关）──────────────────────────────────────────────
DB_PATH = os.environ.get("SIKU_DB_PATH", os.path.join(_SIKU_ROOT, "memory_store.db"))
ENTITY_ENABLED = os.environ.get("SIKU_ENTITY_ARCHIVE", "on").lower() not in ("off", "0", "false")
# [红线6/D3 A5] 实体抽取端点须为本机或内网本地模型（禁公网）；
# 非本地端点由 validate_llm_endpoint 守卫拒绝调用（降级规则抽取，绝不出站）
LLM_ENDPOINT = os.environ.get("SIKU_ENTITY_LLM_ENDPOINT", os.environ.get("LLM_ENDPOINT", "http://127.0.0.1:8081"))
# [红线6 数据路径] 本地端点强制直连：禁走系统/环境代理（http_proxy 等可能把内容转发出境）。
# 实测（E4 2026-09-22）：http_proxy 存在时 urllib 默认 opener 会把本地端点调用拐向代理。
_DIRECT_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
LLM_MODEL = os.environ.get("SIKU_ENTITY_MODEL", "")
LLM_TIMEOUT = float(os.environ.get("SIKU_ENTITY_LLM_TIMEOUT", "60"))
EXTRACT_BATCH = int(os.environ.get("SIKU_ENTITY_BATCH", "100"))
SUMMARY_MAX = int(os.environ.get("SIKU_ENTITY_SUMMARY_MAX", "500"))  # 聚合摘要截断

# ── U2 直连边配置───────────────────────────────
RELATIONS_ENABLED = os.environ.get("SIKU_ENTITY_RELATIONS", "on").lower() not in ("off", "0", "false")
REL_MAX_PAIRS = int(os.environ.get("SIKU_ENTITY_MAX_PAIRS", "50000"))  # 每规则两两比较上限（quadratic-loop 纪律）
BGE_ENDPOINT = os.environ.get("SIKU_BGE_ENDPOINT", "http://127.0.0.1:18790")
SEMANTIC_THRESHOLD = float(os.environ.get("SIKU_ENTITY_SEM_THRESHOLD", "0.80"))

# 关系类型体系（设计 37 号 §三，8 类 + 权重 + 白名单）
RELATION_TYPES = ("alias_of", "shared_entry", "same_agent", "same_type",
                  "keyword_overlap", "semantic_similar", "part_of", "depends_on")
RELATION_WEIGHTS = {
    "alias_of": 1.0, "shared_entry": 0.8, "same_agent": 0.6, "same_type": 0.7,
    "keyword_overlap": 0.7, "semantic_similar": 0.8, "part_of": 0.9, "depends_on": 0.9,
}
# 跨层查询白名单对齐（与 graph_query.TRUE_SIGNAL_RELATIONS 同名词同义）
RELATION_WHITELIST = ("alias_of", "shared_entry", "same_agent", "same_type",
                      "keyword_overlap", "semantic_similar", "part_of", "depends_on")

# ── Schema ───────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entity_archive (
    entity_id    TEXT PRIMARY KEY,          -- sha256(规范名) 前16hex
    name         TEXT NOT NULL UNIQUE,      -- 规范名
    aliases      TEXT NOT NULL DEFAULT '[]',-- JSON 别名数组
    summary      TEXT NOT NULL DEFAULT '',  -- 聚合摘要（"我已知什么"）
    entry_count  INTEGER NOT NULL DEFAULT 0,-- 关联条目数
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS entity_entries (
    entity_id   TEXT NOT NULL,
    entry_id    TEXT NOT NULL,
    relation    TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    PRIMARY KEY (entity_id, entry_id)
);
CREATE INDEX IF NOT EXISTS idx_entity_entries_entry ON entity_entries(entry_id);
-- U2 直连边表（设计 37 号 §四）：与 graph_edges 分层不合并
CREATE TABLE IF NOT EXISTS entity_relations (
    entity_id1  TEXT NOT NULL,                 -- entity_archive.entity_id
    entity_id2  TEXT NOT NULL,                 -- entity_archive.entity_id
    relation    TEXT NOT NULL,                 -- 受控枚举（RELATION_TYPES）
    weight      REAL NOT NULL DEFAULT 1.0,     -- 强度/置信度 0-1
    source      TEXT NOT NULL DEFAULT '',      -- rule_shared_entry/rule_alias/rule_agent/rule_type/
                                               -- rule_keyword/rule_semantic/llm_8081/migration/manual
    evidence    TEXT NOT NULL DEFAULT '',      -- JSON: {"entries":[...], "snippet":"..."} 可审计可回滚
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (entity_id1, entity_id2, relation, source)
);
CREATE INDEX IF NOT EXISTS idx_er_1 ON entity_relations(entity_id1, relation);
CREATE INDEX IF NOT EXISTS idx_er_2 ON entity_relations(entity_id2, relation);
CREATE INDEX IF NOT EXISTS idx_er_rel ON entity_relations(relation);
"""

# ── 基础工具 ─────────────────────────────────────────────────────────
def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _conn():
    """独立连接（每次调用新建，防并发/线程问题）。"""
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c

def _init_schema(conn):
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def _entity_id(name):
    return hashlib.sha256(name.strip().encode("utf-8")).hexdigest()[:16]

def _json_parse_tolerant(text):
    """容忍 LLM 输出包裹的 JSON（```json ... ``` / 前后噪音）。"""
    if not text:
        return None
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", t, re.S)
    if m:
        t = m.group(1)
    else:
        # 直接找第一个 { 到最后一个 }
        i, j = t.find("{"), t.rfind("}")
        if i >= 0 and j > i:
            t = t[i:j + 1]
    try:
        return json.loads(t)
    except Exception:
        return None

# ── LLM 实体抽取（失败降级，绝不阻塞）──────────────────────────────
# U2 阶段2：relation 从自由文本 → 受控枚举（类型 + 依据）；LLM 不可用自动降级规则
_LLM_PROMPT = (
    "你是实体抽取器。从用户给的文本中抽取关键实体（人物/系统/项目/产品/组织/技术概念等），"
    "每个实体给出规范名 name、别名 aliases（含简称/英文名，无则空数组）、"
    "relation_type（该文本与实体的受控关系类型，从以下枚举选一个：part_of 组成/隶属、"
    "depends_on 依赖、shared_entry 共享条目、same_agent 同源、same_type 同类型、"
    "keyword_overlap 关键词交叠、semantic_similar 语义相似、related_to 语义相关兜底）、"
    "relation_evidence（一句话依据）。"
    "只输出 JSON，不要任何解释，格式：{\"entities\":[{\"name\":\"...\",\"aliases\":[\"...\"],"
    "\"relation_type\":\"...\",\"relation_evidence\":\"...\"}]}"
)

# ── 端点守卫（红线6·D3 A5）：实体抽取端点须本机或内网本地模型 ────────
# 端点可被环境变量覆盖 → 必须拒绝非本地端点（禁指公网服务，防数据出站）。
# 允许：回环 127.0.0.1/localhost/::1、RFC1918 私网 10/8、172.16/12、192.168/16、
#       *.local（mDNS 内网名）；其余（公网 IP/域名、非 http(s) 协议、空值）一律拒。
_LLM_ENDPOINT_STATUS = "unknown"   # unknown | ok | rejected_nonlocal | request_failed


def validate_llm_endpoint(url):
    """端点守卫：须本机或内网本地模型。

    合规 → 返回规范化（去尾斜杠）URL；非本地/不可解析/非 http(s) → ValueError。
    与 video-to-text skill 的 validate_base_url 同款「入口校验」做法（D3 A5）。
    """
    u = (url or "").strip()
    if not u:
        raise ValueError("[红线6] 实体抽取端点未配置：拒绝调用（须为本机或内网本地模型）")
    m = re.match(r"^https?://([^/:\[\]\s]+|\[[0-9A-Fa-f:]+\])(?::(\d+))?(?:[/?#]|$)", u, re.IGNORECASE)
    if not m:
        raise ValueError(
            "[红线6] 实体抽取端点无法解析：%r —— 须为 http(s):// 形式的本机或内网本地模型" % url)
    host = m.group(1).strip("[]").lower().rstrip(".")
    if host in ("localhost", "127.0.0.1", "::1") or host.endswith(".local"):
        return u.rstrip("/")
    octets = host.split(".")
    if (len(octets) == 4 and all(o.isdigit() and len(o) <= 3 and 0 <= int(o) <= 255 for o in octets)
            and (octets[0] == "10"
                 or (octets[0] == "172" and 16 <= int(octets[1]) <= 31)
                 or (octets[0] == "192" and octets[1] == "168"))):
        return u.rstrip("/")
    raise ValueError(
        "[红线6] 实体抽取端点非本地：%s —— 拒绝调用（须为本机或内网本地模型，禁指向公网服务）；"
        "允许 127.0.0.1/localhost/::1、10.x/172.16-31.x/192.168.x、*.local" % url)


def llm_extract(text):
    """调用本地 LLM 抽取实体；任何失败返回 None（调用方降级），绝不抛异常。

    端点守卫（红线6/D3 A5）：非本机或内网本地端点一律拒——不构造请求、不出站。
    """
    global _LLM_ENDPOINT_STATUS
    try:
        endpoint = validate_llm_endpoint(LLM_ENDPOINT)
    except ValueError as e:
        _LLM_ENDPOINT_STATUS = "rejected_nonlocal"
        print("[siku-entity] %s" % e, file=sys.stderr, flush=True)
        return None
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": _LLM_PROMPT},
            {"role": "user", "content": text[:1200]},
        ],
        "temperature": 0.0,
        "max_tokens": 600,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    req = urllib.request.Request(
        endpoint + "/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _DIRECT_OPENER.open(req, timeout=LLM_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        content = data["choices"][0]["message"].get("content", "")
        parsed = _json_parse_tolerant(content)
        _LLM_ENDPOINT_STATUS = "ok"
        if parsed and isinstance(parsed.get("entities"), list):
            return parsed["entities"]
        return None
    except Exception:
        _LLM_ENDPOINT_STATUS = "request_failed"
        return None

# ── 规则降级抽取（无 LLM 时用：书名号/引号/已知实体模式）────────────
_RULE_KNOWN = re.compile(
    r"[\u4e00-\u9fffA-Za-z0-9_\-]{2,40}（(?:系统|项目|模型|平台|方案|工具|库|模块)）"
)
_RULE_BRACKET = re.compile(r"[《「『\"“]([\u4e00-\u9fffA-Za-z0-9_\-]{2,40})[」』\"”》]")
_RULE_BARE = re.compile(r"[\u4e00-\u9fffA-Za-z0-9_]{2,30}(?:系统|平台|引擎|模型|库|框架|工具|项目)")

def rule_extract(text):
    """规则抽取（零 LLM 依赖兜底）：书名号/引号/已知模式。"""
    out, seen = [], set()
    for m in _RULE_KNOWN.finditer(text):
        name = m.group(0).strip()
        if name not in seen:
            seen.add(name)
            out.append({"name": name, "aliases": [], "relation": "规则抽取（降级模式）"})
    for m in _RULE_BRACKET.finditer(text):
        name = m.group(1).strip()
        if name and name not in seen:
            seen.add(name)
            out.append({"name": name, "aliases": [], "relation": "规则抽取（降级模式）"})
    for m in _RULE_BARE.finditer(text):
        name = m.group(0).strip()
        if name not in seen:
            seen.add(name)
            out.append({"name": name, "aliases": [], "relation": "规则抽取（降级模式）"})
    return out

def extract_entities_from_text(text):
    """LLM 优先，规则兜底；两路都空返回 []。永不抛异常。"""
    entities = llm_extract(text) if ENTITY_ENABLED else None
    if not entities:
        entities = rule_extract(text)
    cleaned = []
    for e in entities or []:
        name = str(e.get("name", "")).strip() if isinstance(e, dict) else ""
        if not name or len(name) < 2 or len(name) > 60:
            continue
        # U2 阶段2：LLM 受控关系类型（relation_type+依据）；无则退回自由文本
        rtype = str(e.get("relation_type", "")).strip() if isinstance(e, dict) else ""
        revid = str(e.get("relation_evidence", "")).strip() if isinstance(e, dict) else ""
        if rtype and rtype in RELATION_TYPES:
            relation = f"{rtype}：{revid}" if revid else rtype
        else:
            relation = str(e.get("relation", "")).strip()[:100]
        cleaned.append({
            "name": name,
            "aliases": [str(a).strip() for a in (e.get("aliases") or []) if str(a).strip() and str(a).strip() != name],
            "relation": relation,
        })
    return cleaned

# ── 入库（幂等 upsert，只写 entity_* 两张表）────────────────────────
def ingest_entries(limit=None, entry_ids=None, conn=None):
    """从 memory_store 读取 summary 抽取实体写入档案。返回统计。"""
    global _LLM_ENDPOINT_STATUS
    if not ENTITY_ENABLED:
        return {"enabled": False, "error": "SIKU_ENTITY_ARCHIVE=off"}
    _LLM_ENDPOINT_STATUS = "unknown"   # 本轮状态（llm_status 汇总取循环后值）
    own_conn = conn is None
    c = conn if conn else _conn()
    try:
        _init_schema(c)
        sql = ("SELECT id, summary, type, source_agent FROM memory_store "
               "WHERE summary IS NOT NULL AND length(summary) >= 4")
        params = []
        if entry_ids:
            ph = ",".join("?" * len(entry_ids))
            sql += f" AND id IN ({ph})"
            params = list(entry_ids)
        elif limit:
            sql += " ORDER BY updated_at DESC LIMIT ?"
            params = [limit]
        rows = c.execute(sql, params).fetchall()

        stats = {"scanned": len(rows), "entries_processed": 0, "entities_created": 0, "links_created": 0, "failed": 0}
        processed_entry_ids = []
        for r in rows:
            text = r["summary"]
            entities = extract_entities_from_text(text)
            if not entities:
                stats["failed"] += 1
                continue
            stats["entries_processed"] += 1
            processed_entry_ids.append(r["id"])
            for e in entities:
                eid = _entity_id(e["name"])
                before = c.total_changes
                c.execute(
                    "INSERT OR IGNORE INTO entity_archive(entity_id,name,aliases,summary,entry_count,created_at,updated_at) "
                    "VALUES(?,?,?,?,0,?,?)",
                    (eid, e["name"], json.dumps(e["aliases"], ensure_ascii=False), e["relation"], _now(), _now()),
                )
                if c.total_changes > before:
                    stats["entities_created"] += 1
                before = c.total_changes
                c.execute(
                    "INSERT OR IGNORE INTO entity_entries(entity_id,entry_id,relation,created_at) VALUES(?,?,?,?)",
                    (eid, r["id"], e["relation"], _now()),
                )
                if c.total_changes > before:
                    stats["links_created"] += 1
        # 汇总：重算 entry_count + 聚合摘要
        _refresh_archive(c)
        stats["llm_status"] = _LLM_ENDPOINT_STATUS   # 端点守卫状态：ok/rejected_nonlocal/request_failed/unknown

        # U2 Batch3：增量建 shared_entry 边（仅本次处理条目关联的实体对）
        if RELATIONS_ENABLED and processed_entry_ids:
            inc = _incremental_shared_entry(c, processed_entry_ids)
            stats["relations_incremental"] = inc
            c.commit()  # 增量边单独提交（_upsert_relation 不自动提交）

        # ── 审计（2026-08-17 P3 补齐：实体写入留痕）──────────────
        try:
            import json as _json, datetime as _dt
            _log = os.path.join(_SIKU_ROOT, "logs/entity_audit.jsonl")
            os.makedirs(os.path.dirname(_log), exist_ok=True)
            with open(_log, "a", encoding="utf-8") as _f:
                _f.write(_json.dumps({
                    "ts": _dt.datetime.now().isoformat(),
                    "op": "entity_ingest",
                    "agent": os.environ.get("SIKU_AGENT_ID", "unknown"),
                    "scanned": stats.get("scanned"),
                    "entities_created": stats.get("entities_created"),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        return stats
    except Exception as ex:
        return {"error": str(ex)}
    finally:
        if own_conn:
            c.close()

def _refresh_archive(conn):
    """重算 entry_count 与聚合摘要（幂等，零污染：只更新 entity_archive 派生列）。"""
    conn.execute(
        "UPDATE entity_archive SET entry_count = ("
        "  SELECT COUNT(*) FROM entity_entries e WHERE e.entity_id = entity_archive.entity_id"
        ")"
    )
    # 聚合摘要 = 最近关联条目的关系串（截断）
    rows = conn.execute(
        "SELECT ea.entity_id, ea.name, "
        "  (SELECT group_concat(e2.relation, '；') FROM ("
        "     SELECT relation FROM entity_entries WHERE entity_id = ea.entity_id "
        "     ORDER BY created_at DESC LIMIT 3) e2) AS rel"
        " FROM entity_archive ea WHERE entry_count > 0"
    ).fetchall()
    for r in rows:
        rel = (r["rel"] or "").strip()
        if rel:
            conn.execute("UPDATE entity_archive SET summary=? WHERE entity_id=?", (rel[:SUMMARY_MAX], r["entity_id"]))
    conn.commit()

# ── 聚合查询："关于 X 我已知什么" ───────────────────────────────────
def query_entity(name, top_k=5):
    """聚合接口：实体档案 + 关联条目。name 可为规范名或别名。"""
    result = {
        "entity": name, "found": False, "archive": None,
        "entries": [], "entry_count": 0, "degraded": False,
    }
    if not ENTITY_ENABLED:
        result["degraded"] = True
        result["error"] = "SIKU_ENTITY_ARCHIVE=off"
        return result
    c = _conn()
    try:
        _init_schema(c)
        norm = name.strip()
        row = c.execute(
            "SELECT * FROM entity_archive WHERE name=? OR aliases LIKE ?",
            (norm, f'%"{norm}"%'),
        ).fetchone()
        if not row:
            return result
        # U2 Batch1 合并标记重定向：被合并行（summary 以【已合并至 开头）→ 重定向到保留实体
        if (row["summary"] or "").startswith("【已合并至"):
            keeper_name = row["summary"].split("【已合并至 ", 1)[1].rstrip("】").strip()
            keeper = c.execute("SELECT * FROM entity_archive WHERE name=?", (keeper_name,)).fetchone()
            if keeper:
                row = keeper
        result["found"] = True
        result["archive"] = {
            "entity_id": row["entity_id"], "name": row["name"],
            "aliases": json.loads(row["aliases"] or "[]"),
            "summary": row["summary"], "entry_count": row["entry_count"],
            "updated_at": row["updated_at"],
        }
        # 关联条目（join memory_store 只读）
        links = c.execute(
            "SELECT ee.entry_id, ee.relation, m.summary, m.type, m.source_agent, m.created_at "
            "FROM entity_entries ee LEFT JOIN memory_store m ON m.id = ee.entry_id "
            "WHERE ee.entity_id=? ORDER BY m.updated_at DESC LIMIT ?",
            (row["entity_id"], top_k),
        ).fetchall()
        result["entry_count"] = len(links)
        result["entries"] = [
            {
                "entry_id": l["entry_id"],
                "relation": l["relation"],
                "summary": (l["summary"] or "")[:200],
                "type": l["type"],
                "source_agent": l["source_agent"],
                "created_at": l["created_at"],
            }
            for l in links
        ]
        return result
    except Exception as ex:
        result["error"] = str(ex)
        return result
    finally:
        c.close()

def list_entities(top=10):
    c = _conn()
    try:
        _init_schema(c)
        rows = c.execute(
            "SELECT name, aliases, entry_count, summary, updated_at FROM entity_archive "
            "ORDER BY entry_count DESC, updated_at DESC LIMIT ?",
            (top,),
        ).fetchall()
        return {"count": len(rows), "entities": [dict(r) for r in rows]}
    finally:
        c.close()

def status():
    c = _conn()
    try:
        _init_schema(c)
        total = c.execute("SELECT COUNT(*) AS n FROM entity_archive").fetchone()["n"]
        linked = c.execute("SELECT COUNT(*) AS n FROM entity_entries").fetchone()["n"]
        rel = c.execute("SELECT COUNT(*) AS n FROM entity_relations").fetchone()["n"] if _table_exists(c, "entity_relations") else 0
        return {"enabled": ENTITY_ENABLED, "db": DB_PATH, "llm_endpoint": LLM_ENDPOINT,
                "entities": total, "links": linked, "relations": rel,
                "relations_enabled": RELATIONS_ENABLED}
    finally:
        c.close()

def _table_exists(conn, name):
    try:
        r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        return r is not None
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════════════
# U2 直连边：名称规范化 / 别名合并（Batch1） / 规则建边（Batch2） / 增量（Batch3）
# ══════════════════════════════════════════════════════════════════════

# ── 名称规范化（Batch1 确定性规则：全半角/空格/尾缀归一 + 括号模式）──
def _full_to_half(text):
    """全角→半角（字母数字+常用标点）。"""
    out = []
    for ch in text:
        code = ord(ch)
        if code == 0x3000:
            code = 0x20
        elif 0xFF01 <= code <= 0xFF5E:
            code -= 0xFEE0
        out.append(chr(code))
    return "".join(out)

_SUFFIX_NORM = (("数据库", "库"),)  # L3数据库 → L3库（尾缀归一，设计 §六 Batch1）

def normalize_name(name):
    """规范化名称：全半角统一 → 去空格 → 尾缀归一。确定性，仅用于别名合并判定。"""
    n = _full_to_half(name)
    n = re.sub(r"[\s　]+", "", n)
    for old, new in _SUFFIX_NORM:
        if n.endswith(old) and len(n) > len(old):
            n = n[: -len(old)] + new
    return n

def _bracket_bare(name):
    """括号模式：《X》/（X）→ 裸名 X（全名被括号包裹时）；否则 None。"""
    m = re.fullmatch(r"[《（(【]\s*(.+?)\s*[》）)】]", name)
    return m.group(1).strip() if m else None

# ── 边写入（幂等 upsert，无向归一 id1<id2，与 graph_builder._upsert_edge 同款）──
def _upsert_relation(conn, eid1, eid2, relation, weight, source, evidence=None, ts=None):
    """插入或更新直连边（id1<id2 无向归一；PK 含 source 支持多通道留痕）。"""
    if eid1 == eid2:
        return False
    a, b = (eid1, eid2) if eid1 < eid2 else (eid2, eid1)
    ts = ts or _now()
    ev = json.dumps(evidence or {}, ensure_ascii=False)
    conn.execute(
        "INSERT OR REPLACE INTO entity_relations(entity_id1,entity_id2,relation,weight,source,evidence,created_at,updated_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (a, b, relation, weight, source, ev, ts, ts),
    )
    return True

# ── Batch1：确定性别名合并（规范化 + 括号模式，禁模糊合并）──────────
def migrate_alias_merge(conn, dry_run=False):
    """
    确定性别名合并（设计 37 号 §六 Batch1，D3：只做能证明同指的）：
      - 名称规范化（全半角/去空格/尾缀归一）后逐字相等 → 合并到保留名（entry_count 大者）
      - 括号模式：《X》/（X）全名被括号包裹 → 裸名进 aliases
      - 被合并行保留在 entity_archive（标记 summary），entity_entries 改指保留实体
      - 每条合并产出 alias_of 边（source='rule_alias'）
    返回统计；幂等可重放（INSERT OR REPLACE + UPDATE 改指）。
    """
    rows = conn.execute("SELECT entity_id, name, entry_count, aliases FROM entity_archive").fetchall()
    by_norm = {}
    for r in rows:
        by_norm.setdefault(normalize_name(r["name"]), []).append(dict(r))

    merged = 0
    alias_edges = 0
    for norm, group in by_norm.items():
        if len(group) < 2:
            continue
        # 保留名 = entry_count 大者；并列取名字典序小者（确定性）
        group.sort(key=lambda e: (-e["entry_count"], e["name"]))
        keeper = group[0]
        for other in group[1:]:
            if other["entity_id"] == keeper["entity_id"]:
                continue
            # 1) entity_entries 改指保留实体
            conn.execute("UPDATE entity_entries SET entity_id=? WHERE entity_id=?",
                         (keeper["entity_id"], other["entity_id"]))
            # 2) 被合并行标记（保留行保证引用完整性 + 可回滚）
            conn.execute(
                "UPDATE entity_archive SET summary=?, updated_at=? WHERE entity_id=?",
                (f"【已合并至 {keeper['name']}】", _now(), other["entity_id"]),
            )
            # 3) keeper aliases 并入被合并名
            try:
                aliases = json.loads(keeper["aliases"] or "[]")
            except Exception:
                aliases = []
            if other["name"] not in aliases and other["name"] != keeper["name"]:
                aliases.append(other["name"])
            conn.execute("UPDATE entity_archive SET aliases=?, updated_at=? WHERE entity_id=?",
                         (json.dumps(aliases, ensure_ascii=False), _now(), keeper["entity_id"]))
            # 4) alias_of 边（source=rule_alias，evidence 留痕）
            _upsert_relation(conn, keeper["entity_id"], other["entity_id"], "alias_of", 1.0,
                             "rule_alias", {"merged_from": other["name"], "merged_to": keeper["name"]})
            merged += 1
            alias_edges += 1

    # 括号模式：全名被《》/包裹 → 裸名进 aliases（不合并，只补别名）
    bracket_added = 0
    for r in rows:
        bare = _bracket_bare(r["name"])
        if not bare or bare == r["name"]:
            continue
        try:
            aliases = json.loads(r["aliases"] or "[]")
        except Exception:
            aliases = []
        if bare not in aliases:
            aliases.append(bare)
            conn.execute("UPDATE entity_archive SET aliases=?, updated_at=? WHERE entity_id=?",
                         (json.dumps(aliases, ensure_ascii=False), _now(), r["entity_id"]))
            bracket_added += 1

    conn.commit()
    return {"merged": merged, "alias_edges": alias_edges, "bracket_aliases": bracket_added}

# ── Batch2：规则直连边回填（阶段1 规则集，source='migration'）────────
def _entity_entry_rows(conn):
    """实体→关联条目元数据（agent/type/timestamp/confidence/summary）。"""
    return conn.execute(
        "SELECT ee.entity_id, m.id AS entry_id, m.source_agent, m.type, m.timestamp, m.confidence, m.summary "
        "FROM entity_entries ee JOIN memory_store m ON m.id=ee.entry_id"
    ).fetchall()

def build_shared_entry_edges(conn, source="migration", max_pairs=None, incremental_entry_ids=None):
    """
    shared_entry 共享条目：同 entry_id 的实体对（SQL 自连接，id1<id2）。
    evidence 记共享条目；仅高质量边（≥1 共享条目为固有约束）。
    incremental 模式：跳过已有 shared_entry 边（任意 source）的实体对，防迁移/增量双写同对。
    """
    if incremental_entry_ids is not None:
        ph = ",".join("?" * len(incremental_entry_ids))
        rows = conn.execute(
            f"SELECT a.entity_id AS e1, b.entity_id AS e2, a.entry_id "
            f"FROM entity_entries a JOIN entity_entries b ON a.entry_id=b.entry_id AND a.entity_id<b.entity_id "
            f"WHERE a.entry_id IN ({ph})", list(incremental_entry_ids)).fetchall()
    else:
        rows = conn.execute(
            "SELECT a.entity_id AS e1, b.entity_id AS e2, a.entry_id "
            "FROM entity_entries a JOIN entity_entries b ON a.entry_id=b.entry_id AND a.entity_id<b.entity_id").fetchall()
    by_pair = {}
    for r in rows:
        by_pair.setdefault((r["e1"], r["e2"]), []).append(r["entry_id"])
    n = 0
    if incremental_entry_ids is not None and by_pair:
        # 跳过已存在 shared_entry 边（任意 source）的实体对——防迁移/增量双写
        # （row-value IN 语法旧 SQLite 不支持，改用 temp 表 JOIN 判定）
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _er_pairs (e1 TEXT, e2 TEXT, PRIMARY KEY(e1,e2))")
        conn.execute("DELETE FROM _er_pairs")
        conn.executemany("INSERT OR IGNORE INTO _er_pairs(e1,e2) VALUES(?,?)", list(by_pair.keys()))
        existing = set(
            (r[0], r[1]) for r in conn.execute(
                "SELECT er.entity_id1, er.entity_id2 FROM entity_relations er "
                "JOIN _er_pairs p ON (er.entity_id1=p.e1 AND er.entity_id2=p.e2) "
                "WHERE er.relation='shared_entry'").fetchall())
        conn.execute("DROP TABLE _er_pairs")
        by_pair = {pair: v for pair, v in by_pair.items() if pair not in existing}
    for (e1, e2), entries in by_pair.items():
        _upsert_relation(conn, e1, e2, "shared_entry", RELATION_WEIGHTS["shared_entry"],
                         source, {"entries": entries[:50], "snippet": "共享条目数 %d" % len(entries)})
        n += 1
    return {"rule": "shared_entry", "edges": n, "total_pairs_all": len(by_pair), "coverage_pct": 100.0}

def _ts_parse(s):
    try:
        return datetime.fromisoformat(str(s)[:19])
    except Exception:
        return None

def build_same_agent_edges(conn, source="migration", max_pairs=None):
    """
    same_agent 同源：实体关联条目集合的 source_agent 相同（复用 graph_builder 规则1：
    同 agent 且时间差 <1h）。全量两两计算（1,734 实体秒级），边数上限 max_pairs + 覆盖率日志。
    """
    max_pairs = max_pairs or REL_MAX_PAIRS
    rows = _entity_entry_rows(conn)
    ent_entries = {}
    for r in rows:
        ent_entries.setdefault(r["entity_id"], []).append(r)
    agents_ent = {}
    for eid, lst in ent_entries.items():
        for r in lst:
            if r["source_agent"]:
                agents_ent.setdefault(r["source_agent"], []).append(eid)
    # 实体按 entry_count 降序（聚焦重要实体）
    ec = {r["entity_id"]: r["entry_count"] for r in conn.execute("SELECT entity_id, entry_count FROM entity_archive")}
    edges = 0
    capped = False
    total_pairs_all = 0
    for agent, ents in agents_ent.items():
        ents = sorted(set(ents), key=lambda e: (-ec.get(e, 0), e))
        total_pairs_all += len(ents) * (len(ents) - 1) // 2
    processed = 0
    for agent, ents in agents_ent.items():
        ents = sorted(set(ents), key=lambda e: (-ec.get(e, 0), e))
        for i in range(len(ents)):
            if capped:
                break
            for j in range(i + 1, len(ents)):
                if edges >= max_pairs:
                    capped = True
                    break
                processed += 1
                e1, e2 = ents[i], ents[j]
                ts1 = sorted(t for t in (_ts_parse(r["timestamp"]) for r in ent_entries[e1] if r["source_agent"] == agent) if t)
                ts2 = sorted(t for t in (_ts_parse(r["timestamp"]) for r in ent_entries[e2] if r["source_agent"] == agent) if t)
                p1 = p2 = 0
                hit = False
                while p1 < len(ts1) and p2 < len(ts2):
                    if abs((ts1[p1] - ts2[p2]).total_seconds()) <= 3600:
                        hit = True
                        break
                    if ts1[p1] < ts2[p2]:
                        p1 += 1
                    else:
                        p2 += 1
                if hit:
                    _upsert_relation(conn, e1, e2, "same_agent", RELATION_WEIGHTS["same_agent"],
                                     source, {"agent": agent, "snippet": "同源 Agent %s" % agent})
                    edges += 1
    coverage = (processed / total_pairs_all * 100) if total_pairs_all else 100.0
    if capped:
        print(f"⚠️ same_agent 达边数上限 {max_pairs}：实际写出 {edges} 条，覆盖率 {coverage:.1f}%")
    return {"rule": "same_agent", "edges": edges, "total_pairs_all": total_pairs_all,
            "processed_pairs": processed, "coverage_pct": round(coverage, 1)}

def build_same_type_edges(conn, source="migration", max_pairs=None):
    """
    same_type 同类型：实体关联条目集合的 type 主类相同（复用 graph_builder 规则2：
    同 type 且 confidence>0.8）。边数上限 max_pairs + 覆盖率日志（S8 伪边教训：主类过滤 + 上限双控）。
    """
    max_pairs = max_pairs or REL_MAX_PAIRS
    rows = _entity_entry_rows(conn)
    ent_types = {}
    for r in rows:
        if r["type"] and r["confidence"] is not None and r["confidence"] > 0.8:
            ent_types.setdefault(r["entity_id"], set()).add(r["type"])
    ec = {r["entity_id"]: r["entry_count"] for r in conn.execute("SELECT entity_id, entry_count FROM entity_archive")}
    types_ent = {}
    for eid, types in ent_types.items():
        for t in types:
            types_ent.setdefault(t, []).append(eid)
    edges = 0
    processed = 0
    capped = False
    total_pairs_all = sum(len(set(v)) * (len(set(v)) - 1) // 2 for v in types_ent.values())
    for typ, ents in types_ent.items():
        ents = sorted(set(ents), key=lambda e: (-ec.get(e, 0), e))
        for i in range(len(ents)):
            if capped:
                break
            for j in range(i + 1, len(ents)):
                if edges >= max_pairs:
                    capped = True
                    break
                processed += 1
                if typ in ent_types.get(ents[i], set()) and typ in ent_types.get(ents[j], set()):
                    _upsert_relation(conn, ents[i], ents[j], "same_type", RELATION_WEIGHTS["same_type"],
                                     source, {"type": typ, "snippet": "同类型 %s" % typ})
                    edges += 1
    coverage = (processed / total_pairs_all * 100) if total_pairs_all else 100.0
    if capped:
        print(f"⚠️ same_type 达边数上限 {max_pairs}：实际写出 {edges} 条，覆盖率 {coverage:.1f}%")
    return {"rule": "same_type", "edges": edges, "total_pairs_all": total_pairs_all,
            "processed_pairs": processed, "coverage_pct": round(coverage, 1)}

# 分词（复用 graph_builder 词袋语义，实体聚合层）
_STOPWORDS = {"的", "了", "是", "在", "有", "和", "与", "就", "也", "都",
              "这", "那", "一个", "不", "很", "要", "会", "可以", "这个", "那个",
              "the", "a", "an", "is", "are", "was", "were", "be", "been",
              "in", "on", "at", "to", "for", "of", "with", "and", "or", "not"}

def _tokenize(text):
    if not text:
        return set()
    words = set()
    for t in re.findall(r"[\w]+", text):
        if len(t) > 1 and t.lower() not in _STOPWORDS:
            words.add(t.lower())
    return words

def build_keyword_overlap_edges(conn, source="migration", max_pairs=None):
    """
    keyword_overlap 关键词交叠：实体关联条目摘要词袋交叠 ≥2（复用 graph_builder 规则3）。
    全量两两计算（词袋集合交，秒级），边数上限 max_pairs + 覆盖率日志；仅高质量边（≥2 词）。
    """
    max_pairs = max_pairs or REL_MAX_PAIRS
    rows = _entity_entry_rows(conn)
    ent_tokens = {}
    for r in rows:
        ent_tokens.setdefault(r["entity_id"], set()).update(_tokenize(r["summary"]))
    ents = sorted(ent_tokens.keys(), key=lambda e: -len(ent_tokens[e]))
    edges = 0
    processed = 0
    capped = False
    total_pairs_all = len(ents) * (len(ents) - 1) // 2
    for i in range(len(ents)):
        if capped:
            break
        ti = ent_tokens[ents[i]]
        if not ti:
            continue
        for j in range(i + 1, len(ents)):
            if edges >= max_pairs:
                capped = True
                break
            tj = ent_tokens[ents[j]]
            if not tj:
                continue
            processed += 1
            overlap = ti & tj
            if len(overlap) >= 2:
                _upsert_relation(conn, ents[i], ents[j], "keyword_overlap",
                                 RELATION_WEIGHTS["keyword_overlap"], source,
                                 {"overlap": sorted(overlap)[:10], "snippet": "交叠词 %d 个" % len(overlap)})
                edges += 1
    coverage = (processed / total_pairs_all * 100) if total_pairs_all else 100.0
    if capped:
        print(f"⚠️ keyword_overlap 达边数上限 {max_pairs}：实际写出 {edges} 条，覆盖率 {coverage:.1f}%")
    return {"rule": "keyword_overlap", "edges": edges, "total_pairs_all": total_pairs_all,
            "processed_pairs": processed, "coverage_pct": round(coverage, 1)}

def _bge_embed(text):
    """BGE daemon（127.0.0.1:18790）embedding；失败返回 None（调用方降级）。"""
    try:
        data = json.dumps({"text": (text or "")[:500]}).encode("utf-8")
        req = urllib.request.Request(
            BGE_ENDPOINT.rstrip("/") + "/embed",
            data=data, headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        vec = result.get("vec")
        if vec:
            return [float(x) for x in vec]
        return None
    except Exception:
        return None

def build_semantic_similar_edges(conn, source="migration", max_pairs=None):
    """
    semantic_similar 语义相似：实体关联条目 embedding 聚合（复用 graph_builder 规则4 +
    BGE daemon，阈值 0.80 以上建边）。复用既有 memory_store.embedding 零新调用；
    无 stored embedding 的实体降级 BGE daemon 聚合摘要。
    numpy 矩阵全量计算（1734² 量级秒级完成），阈值过滤后仅高质量边（sim ≥ SEMANTIC_THRESHOLD）；
    max_pairs 作为边数上限兜底（阈值过滤后自然远低于上限）。
    """
    try:
        import numpy as np
    except Exception:
        return {"rule": "semantic_similar", "edges": 0, "error": "numpy 不可用，跳过"}
    max_pairs = max_pairs or REL_MAX_PAIRS
    # 实体聚合向量：优先 stored embedding 均值，无则 BGE daemon 聚合摘要
    ent_vec = {}
    ent_daemon = 0
    eids = [r["entity_id"] for r in conn.execute("SELECT entity_id FROM entity_archive").fetchall()]
    emb_cache = {}
    for eid in eids:
        if eid in ent_vec:
            continue
        if eid not in emb_cache:
            emb_cache[eid] = conn.execute(
                "SELECT m.embedding FROM entity_entries ee JOIN memory_store m ON m.id=ee.entry_id "
                "WHERE ee.entity_id=? AND m.embedding IS NOT NULL AND length(m.embedding)>0 LIMIT 50", (eid,)).fetchall()
        emb_rows = emb_cache[eid]
        if emb_rows:
            vecs = [np.frombuffer(x["embedding"], dtype=np.float32) for x in emb_rows]
            v = np.mean(np.stack(vecs), axis=0)
            norm = np.linalg.norm(v)
            ent_vec[eid] = (v / norm) if norm > 0 else v
        else:
            sums = [r2["summary"] for r2 in conn.execute(
                "SELECT m.summary FROM entity_entries ee JOIN memory_store m ON m.id=ee.entry_id "
                "WHERE ee.entity_id=? AND m.summary IS NOT NULL LIMIT 20", (eid,)).fetchall()]
            vec = _bge_embed("；".join(sums)[:500])
            if vec:
                arr = np.array(vec, dtype=np.float32)
                nrm = np.linalg.norm(arr)
                ent_vec[eid] = arr / nrm if nrm > 0 else arr
                ent_daemon += 1
    ents = sorted(ent_vec.keys())
    # 过滤非有限向量（NaN/Inf 防 matmul 污染）——先 replace 再 isfinite 双保险
    for e in list(ents):
        v = ent_vec[e]
        if not np.all(np.isfinite(v)):
            ent_vec[e] = np.nan_to_num(v, nan=0.0, posinf=0.0, neginf=0.0)
    n = len(ents)
    total_pairs_all = n * (n - 1) // 2
    edges = 0
    if n >= 2:
        with np.errstate(all="ignore"):  # 归一化向量点积不应产生 warning；防御残留
            M = np.stack([ent_vec[e] for e in ents])          # n x d（已归一化）
            sims = M @ M.T                                     # n x n，点积即余弦
        iu = np.triu_indices(n, k=1)
        pair_sims = [(ents[i], ents[j], float(sims[i, j]))
                     for i, j in zip(iu[0], iu[1]) if float(sims[i, j]) >= SEMANTIC_THRESHOLD]
        pair_sims.sort(key=lambda x: -x[2])                # 高相似优先
        for e1, e2, sim in pair_sims[:max_pairs]:
            w = round(min(sim, 1.0), 2)
            _upsert_relation(conn, e1, e2, "semantic_similar", w, source,
                             {"sim": round(sim, 4), "snippet": "语义相似度 %.3f" % sim})
            edges += 1
    coverage = 100.0  # 矩阵全量计算，无截断（仅边数上限兜底）
    return {"rule": "semantic_similar", "edges": edges, "total_pairs_all": total_pairs_all,
            "processed_pairs": total_pairs_all, "coverage_pct": coverage,
            "entities_embedded": len(ent_vec), "daemon_used": ent_daemon}

# ── Batch3：增量 shared_entry（新条目抽取后只对该条目关联实体对建边）──
def _incremental_shared_entry(conn, entry_ids):
    """增量建 shared_entry 边：仅本次处理条目关联的实体对（source='rule_shared_entry'）。"""
    if not entry_ids:
        return {"edges": 0}
    return build_shared_entry_edges(conn, source="rule_shared_entry",
                                    incremental_entry_ids=list(entry_ids))

# ── 迁移主入口（Batch1 + Batch2）────────────────────────────────────
def migrate_relations(conn=None, max_pairs=None, skip_semantic=False):
    """
    存量直连边迁移（设计 37 号 §六）：Batch1 别名合并 + Batch2 规则回填。
    幂等可重放（INSERT OR REPLACE + UPDATE 改指）；失败可回滚（DROP entity_relations + 恢复备份）。
    """
    own_conn = conn is None
    c = conn if conn else _conn()
    try:
        _init_schema(c)
        t0 = time.time()
        print("── Batch1 确定性别名合并 ──")
        b1 = migrate_alias_merge(c)
        print(f"  合并 {b1['merged']} 对，alias_of 边 {b1['alias_edges']}，括号别名 {b1['bracket_aliases']}")
        _refresh_archive(c)  # 合并后重算 entry_count/聚合摘要
        print("── Batch2 规则直连边回填（source=migration）──")
        results = []
        for fn in (build_shared_entry_edges, build_same_agent_edges, build_same_type_edges,
                   build_keyword_overlap_edges):
            r = fn(c, source="migration", max_pairs=max_pairs)
            results.append(r)
            print(f"  {r['rule']}: {r['edges']} 条边（全量对 {r['total_pairs_all']}，覆盖率 {r['coverage_pct']}%）")
        if not skip_semantic:
            r = build_semantic_similar_edges(c, source="migration", max_pairs=max_pairs)
            results.append(r)
            print(f"  {r['rule']}: {r['edges']} 条边（全量对 {r['total_pairs_all']}，覆盖率 {r['coverage_pct']}%，"
                  f"daemon {r.get('daemon_used', 0)}）")
        c.commit()
        total = sum(r.get("edges", 0) for r in results) + b1["alias_edges"]
        print(f"── 迁移完成：共 {total} 条直连边，耗时 {time.time()-t0:.1f}s ──")
        return {"batch1": b1, "batch2": results, "total_edges": total, "duration_s": round(time.time() - t0, 1)}
    finally:
        if own_conn:
            c.close()

# ── 实体关系查询（验收③：实体关系查询可用）─────────────────────────
def query_relations(name, depth=1, top_k=10):
    """
    实体关系查询（直连边 1 跳 / meta-path 2 跳）。
    - depth=1：entity_relations 直连边（entity→entity+类型+权重+来源+证据）
    - depth=2：meta-path 跨层（entity→entry(entity_entries)→entry(graph_edges 白名单)→entity）
    返回: {"entity":..., "found":..., "relations":[...], "meta_path":[...]}
    """
    result = {"entity": name, "found": False, "relations": [], "meta_path": [], "degraded": False}
    if not RELATIONS_ENABLED:
        result["degraded"] = True
        result["error"] = "SIKU_ENTITY_RELATIONS=off"
        return result
    c = _conn()
    try:
        _init_schema(c)
        row = c.execute(
            "SELECT * FROM entity_archive WHERE name=? OR aliases LIKE ?",
            (name.strip(), f'%"{name.strip()}"%'),
        ).fetchone()
        if not row:
            return result
        result["found"] = True
        eid = row["entity_id"]
        # depth 1：直连边（双向，按权重降序，去重同对同关系）
        rels = c.execute(
            "SELECT er.entity_id1, er.entity_id2, er.relation, er.weight, er.source, er.evidence, "
            "       ea1.name AS n1, ea2.name AS n2 "
            "FROM entity_relations er "
            "JOIN entity_archive ea1 ON ea1.entity_id=er.entity_id1 "
            "JOIN entity_archive ea2 ON ea2.entity_id=er.entity_id2 "
            "WHERE er.entity_id1=? OR er.entity_id2=? "
            "ORDER BY er.weight DESC LIMIT ?", (eid, eid, top_k)).fetchall()
        for r in rels:
            other_id = r["entity_id2"] if r["entity_id1"] == eid else r["entity_id1"]
            other_name = r["n2"] if r["entity_id1"] == eid else r["n1"]
            ev = None
            try:
                ev = json.loads(r["evidence"]) if r["evidence"] else None
            except Exception:
                pass
            result["relations"].append({
                "entity": other_name, "entity_id": other_id,
                "relation": r["relation"], "weight": r["weight"],
                "source": r["source"], "evidence": ev,
            })
        # depth 2：meta-path（entity→entry→entry→entity，复用 graph_edges 真信号白名单）
        if depth >= 2:
            whitelist = ("keyword_overlap", "semantic_similar", "same_agent")  # 与 graph_query 真信号对齐
            marks = ",".join("?" * len(whitelist))
            mp = c.execute(
                "SELECT DISTINCT ea.entity_id AS eid, ea.name AS ename, ge.relation, ge.weight "
                "FROM entity_entries ee1 "
                "JOIN graph_edges ge ON (ge.id1=ee1.entry_id OR ge.id2=ee1.entry_id) AND ge.relation IN (" + marks + ") "
                "JOIN entity_entries ee2 ON ee2.entry_id = CASE WHEN ge.id1=ee1.entry_id THEN ge.id2 ELSE ge.id1 END "
                "JOIN entity_archive ea ON ea.entity_id=ee2.entity_id "
                "WHERE ee1.entity_id=? AND ee2.entity_id<>? "
                "ORDER BY ge.weight DESC LIMIT ?",
                whitelist + (eid, eid, top_k)).fetchall()
            seen = {r["entity_id"] for r in result["relations"]}
            for r in mp:
                if r["eid"] in seen:
                    continue
                result["meta_path"].append({
                    "entity": r["ename"], "entity_id": r["eid"],
                    "relation": r["relation"], "weight": r["weight"], "hop": 2,
                })
        return result
    except Exception as ex:
        result["error"] = str(ex)
        return result
    finally:
        c.close()

# ── Batch4：验证（引用完整性/类型分布/双层对账/占位串零残留）────────
def relations_status():
    """
    迁移后验证（设计 37 号 §六 Batch4）：
      1. 边数/类型分布（relation ∈ 8 类枚举、占位串零残留）
      2. 引用完整性：两端点全在 entity_archive；evidence.entries 全在 entity_entries
      3. 双层对账：entity_entries.entry_id 在 graph_edges 节点集占比（≥55.7% 不降级）+ 白名单对齐
    """
    c = _conn()
    try:
        _init_schema(c)
        out = {"relations_enabled": RELATIONS_ENABLED}
        total = c.execute("SELECT COUNT(*) n FROM entity_relations").fetchone()["n"]
        out["total_edges"] = total
        by_rel = c.execute(
            "SELECT relation, COUNT(*) n, COUNT(DISTINCT source) srcs FROM entity_relations GROUP BY relation ORDER BY n DESC").fetchall()
        out["by_relation"] = [dict(r) for r in by_rel]
        bad_rel = c.execute(
            "SELECT COUNT(*) n FROM entity_relations WHERE relation NOT IN (%s)"
            % ",".join("?" * len(RELATION_TYPES)), RELATION_TYPES).fetchone()["n"]
        out["relation_not_in_enum"] = bad_rel
        placeholder = c.execute("SELECT COUNT(*) n FROM entity_relations WHERE relation LIKE '%降级模式%' OR relation LIKE '%占位%'").fetchone()["n"]
        out["placeholder_residue"] = placeholder
        # 引用完整性
        dangling = c.execute(
            "SELECT COUNT(*) n FROM entity_relations er "
            "WHERE NOT EXISTS (SELECT 1 FROM entity_archive ea WHERE ea.entity_id=er.entity_id1) "
            "OR NOT EXISTS (SELECT 1 FROM entity_archive ea WHERE ea.entity_id=er.entity_id2)").fetchone()["n"]
        out["dangling_endpoints"] = dangling
        bad_evidence = 0
        rows = c.execute("SELECT evidence FROM entity_relations WHERE evidence != ''").fetchall()
        for r in rows:
            try:
                ev = json.loads(r["evidence"])
                for eid in ev.get("entries") or []:
                    hit = c.execute("SELECT 1 FROM entity_entries WHERE entry_id=?", (eid,)).fetchone()
                    if not hit:
                        bad_evidence += 1
            except Exception:
                pass
        out["evidence_entries_not_in_bridge"] = bad_evidence
        # 双层对账：桥覆盖（entity_entries.entry_id 在 graph_edges 节点集）
        bridge_total = c.execute("SELECT COUNT(DISTINCT entry_id) n FROM entity_entries").fetchone()["n"]
        bridge_in_graph = c.execute(
            "SELECT COUNT(DISTINCT ee.entry_id) n FROM entity_entries ee "
            "WHERE ee.entry_id IN (SELECT id1 FROM graph_edges UNION SELECT id2 FROM graph_edges)").fetchone()["n"]
        out["bridge_coverage"] = {"total": bridge_total, "in_graph": bridge_in_graph,
                                  "pct": round(bridge_in_graph / bridge_total * 100, 1) if bridge_total else 0}
        # 白名单对齐：两层 same_agent/keyword_overlap/semantic_similar 类型串一致
        er_set = set(r["relation"] for r in c.execute(
            "SELECT DISTINCT relation FROM entity_relations WHERE relation IN ('same_agent','keyword_overlap','semantic_similar')"))
        ge_set = set(r["relation"] for r in c.execute(
            "SELECT DISTINCT relation FROM graph_edges WHERE relation IN ('same_agent','keyword_overlap','semantic_similar')"))
        out["whitelist_alignment"] = {"entity_layer": sorted(er_set), "entry_layer": sorted(ge_set),
                                      "aligned": er_set == ge_set}
        return out
    finally:
        c.close()

# ── CLI ──────────────────────────────────────────────────────────────
def main():
    import argparse
    p = argparse.ArgumentParser(description="实体摘要网络（关于 X 我已知什么）")
    sp = p.add_subparsers(dest="cmd")
    pe = sp.add_parser("extract", help="从 memory_store.summary 抽取实体入档案")
    pe.add_argument("--limit", type=int, default=EXTRACT_BATCH)
    pe.add_argument("--ids", nargs="*", help="指定条目ID（测试用）")
    pq = sp.add_parser("query", help="聚合查询：关于 X 我已知什么")
    pq.add_argument("name")
    pq.add_argument("--top-k", type=int, default=5)
    pl = sp.add_parser("list", help="实体档案列表")
    pl.add_argument("--top", type=int, default=10)
    ps = sp.add_parser("status", help="档案统计")
    pm = sp.add_parser("migrate-relations", help="Batch1 别名合并 + Batch2 规则回填（直连边迁移）")
    pm.add_argument("--max-pairs", type=int, default=None, help="每规则两两比较上限（默认 SIKU_ENTITY_MAX_PAIRS）")
    pm.add_argument("--skip-semantic", action="store_true", help="跳过 semantic_similar（BGE 不可用时）")
    pr = sp.add_parser("relations", help="实体关系查询（depth=1 直连边 / depth=2 meta-path）")
    pr.add_argument("name")
    pr.add_argument("--depth", type=int, default=1)
    pr.add_argument("--top-k", type=int, default=10)
    pv = sp.add_parser("relations-status", help="Batch4 验证统计（引用完整性/类型分布/双层对账）")
    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        return
    if args.cmd == "extract":
        print(json.dumps(ingest_entries(limit=args.limit, entry_ids=args.ids), ensure_ascii=False))
    elif args.cmd == "query":
        print(json.dumps(query_entity(args.name, top_k=args.top_k), ensure_ascii=False))
    elif args.cmd == "list":
        print(json.dumps(list_entities(args.top), ensure_ascii=False))
    elif args.cmd == "status":
        print(json.dumps(status(), ensure_ascii=False))
    elif args.cmd == "migrate-relations":
        print(json.dumps(migrate_relations(max_pairs=args.max_pairs, skip_semantic=args.skip_semantic),
                         ensure_ascii=False))
    elif args.cmd == "relations":
        print(json.dumps(query_relations(args.name, depth=args.depth, top_k=args.top_k),
                         ensure_ascii=False, indent=1))
    elif args.cmd == "relations-status":
        print(json.dumps(relations_status(), ensure_ascii=False, indent=1))

if __name__ == "__main__":
    main()
