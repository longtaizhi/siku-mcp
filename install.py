#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
a1-siku-core 安装向导 v1.0.0
作者: 维护者团队｜功能: 一键安装（引导安装/自检/升级维护，跨平台纯标准库）
引导式一键安装：交互向导 + 样例库初始化 + 自检依赖 + 配置保护
用法:
  python3 install.py                 # 交互向导
  python3 install.py --yes           # 全默认非交互
  python3 install.py --target <目录> # 指定安装目录
  python3 install.py --dry-run       # 演练（零写入）
跨平台：macOS / Linux / Windows（Python 3.8+，纯标准库）
"""
import argparse
import os
import re
import shutil
import socket
import sqlite3
import sys
import time
from pathlib import Path
import subprocess

MODULE_ID = "a1-siku-core"
MODULE_NAME = "知识库核心（检索 L1-L3 + 蒸馏 L4 + 权限 + 评分）"
def _pkg_version():
    """版本单源化（P1-20260816）：从同目录 manifest.yaml 读包版本，禁止硬编码（硬编码→升级幂等误判）。"""
    _mf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manifest.yaml")
    try:
        with open(_mf, encoding="utf-8") as _f:
            for _line in _f:
                if _line.strip().startswith("version:"):
                    return _line.split(":", 1)[1].strip()
    except OSError:
        pass
    raise SystemExit("❌ 版本单源化失败：无法读取同目录 manifest.yaml 的 version 字段，中止安装（避免幂等误判）")


VERSION = _pkg_version()
DEFAULT_TARGET = os.path.expanduser("~/siku-core")

# 自检依赖定义（A1）: id/名称/硬性/类型
CHECKS = [
    {"id": "db",      "name": "记忆库 DB 存在可读",       "hard": True,  "kind": "db"},
    {"id": "chroma",  "name": "向量库目录存在",           "hard": True,  "kind": "dir", "cfg_key": "chroma_dir"},
    {"id": "port",    "name": "嵌入服务端口存活",          "hard": False, "kind": "port"},
    {"id": "model",   "name": "嵌入模型目录存在",          "hard": False, "kind": "dir", "cfg_key": "model_dir"},
    {"id": "pyver",   "name": "python3 >= 3.11",          "hard": True,  "kind": "pyver"},
    {"id": "deps",    "name": "jieba / numpy 可导入",      "hard": False, "kind": "import"},
    {"id": "mcp",    "name": "MCP 握手+工具调用",          "hard": True,  "kind": "mcp"}
]

TOKENS = {
    "<DATA_DIR>": None,
    "<MEMORY_DB_FILE>": "sample_memory.db",
    "<CHROMA_DIR>": "chroma",
    "<MODEL_DIR>": "<MODEL_DIR>",
    "<EMBEDDING_PORT>": "18790",
    "<INSTALL_DIR>": None,
}

# ── 样例库 schema（E3 修正 2026-09-21）─────────────────────────────────────────
# 与生产 memory_store 逐列对齐（全量 41 列 + 3 索引 + FTS5 mem_fts + 3 同步触发器）。
# 生产基线：生产库 memory_store 实读（PRAGMA table_info / sqlite_master，2026-09-21 冻结）。
# 背景：旧版 10 列简表缺 source_agent/confidence/industry/source_ref/expires_at 等列——
# 生产脚本（expand_entry/entity_extract 等）在样例环境会 no such column。
SAMPLE_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS memory_store (
    id TEXT PRIMARY KEY,
    version TEXT DEFAULT '1.0',
    timestamp TEXT NOT NULL,
    type TEXT NOT NULL,
    summary TEXT NOT NULL,
    content TEXT DEFAULT '',
    confidence REAL DEFAULT 1.0,
    trust_score REAL DEFAULT 0.7,
    importance INTEGER DEFAULT 5,
    half_life TEXT DEFAULT 'permanent',
    source_agent TEXT DEFAULT '',
    embedding BLOB,
    audit_log TEXT DEFAULT '[]',
    merge_history TEXT DEFAULT '[]',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    train_eligible INTEGER DEFAULT 0,
    data_type TEXT DEFAULT '',
    train_batch_id TEXT DEFAULT '',
    g1_check_passed INTEGER DEFAULT 0,
    g2_labeled_by TEXT DEFAULT '',
    g3_reviewed_by TEXT DEFAULT '',
    g3_result TEXT DEFAULT '',
    correction_count INTEGER DEFAULT 0,
    corrected_at TEXT DEFAULT '',
    summary_hash TEXT,
    concern_id TEXT DEFAULT 'unclassified',
    industry TEXT DEFAULT '',
    source_ref TEXT DEFAULT '',
    expires_at TEXT DEFAULT '',
    deprecated INTEGER DEFAULT 0,
    deprecated_at TEXT DEFAULT '',
    quadrant TEXT,
    quadrant_labeled_at TEXT,
    merged_to TEXT DEFAULT '',
    memory_track TEXT DEFAULT 'semantic' CHECK (memory_track IN ('episodic','semantic')),
    "references" TEXT,
    "entities" TEXT,
    "relations" TEXT,
    event_date TEXT,
    conflict_type TEXT DEFAULT ''
);
"""

# 生产同构索引（仅在列结构对齐后创建——旧版表缺列时 CREATE INDEX 会 no such column）
SAMPLE_DB_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_memory_store_updated_at ON memory_store(updated_at);
CREATE INDEX IF NOT EXISTS idx_memory_store_track ON memory_store(memory_track);
CREATE INDEX IF NOT EXISTS idx_memory_store_timestamp ON memory_store(timestamp);
"""

# 生产表列名（顺序=生产 cid 序）——旧版库差列检测与日志用
SAMPLE_DB_COLUMNS = [
    "id", "version", "timestamp", "type", "summary", "content", "confidence",
    "trust_score", "importance", "half_life", "source_agent", "embedding",
    "audit_log", "merge_history", "created_at", "updated_at", "train_eligible",
    "data_type", "train_batch_id", "g1_check_passed", "g2_labeled_by",
    "g3_reviewed_by", "g3_result", "correction_count", "corrected_at",
    "summary_hash", "concern_id", "industry", "source_ref", "expires_at",
    "deprecated", "deprecated_at", "quadrant", "quadrant_labeled_at",
    "merged_to", "memory_track", "references", "entities", "relations",
    "event_date", "conflict_type",
]

# FTS5 检索通道 + 同步触发器（与生产同构；构建不支持 FTS5 时降级为 WARN，不阻断安装）
SAMPLE_DB_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS mem_fts USING fts5(summary, content, tokenize='unicode61');
CREATE TRIGGER IF NOT EXISTS trg_mem_fts_ai AFTER INSERT ON memory_store
BEGIN
  INSERT INTO mem_fts(rowid, summary, content) VALUES (NEW.rowid, NEW.summary, NEW.content);
END;
CREATE TRIGGER IF NOT EXISTS trg_mem_fts_ad AFTER DELETE ON memory_store
BEGIN
  DELETE FROM mem_fts WHERE rowid = OLD.rowid;
END;
CREATE TRIGGER IF NOT EXISTS trg_mem_fts_au AFTER UPDATE OF summary, content ON memory_store
BEGIN
  DELETE FROM mem_fts WHERE rowid = OLD.rowid;
  INSERT INTO mem_fts(rowid, summary, content) VALUES (NEW.rowid, NEW.summary, NEW.content);
END;
"""

SAMPLE_SOURCE_AGENT = "siku-core-sample"

SAMPLE_ROWS = [
    ("smp_00001", "2026-08-01T10:00:00Z", "insight", "示例：检索冒烟条目一", "用于验证 FTS5 检索通道的样例内容"),
    ("smp_00002", "2026-08-02T10:00:00Z", "insight", "示例：检索冒烟条目二", "用于验证向量通道的样例内容"),
    ("smp_00003", "2026-08-03T10:00:00Z", "rule",   "示例：规则条目",         "规则型样例内容"),
    ("smp_00004", "2026-08-04T10:00:00Z", "fact",   "示例：事实条目",         "事实型样例内容"),
    ("smp_00005", "2026-08-05T10:00:00Z", "insight", "示例：检索冒烟条目五",  "用于验证 RRF 融合的样例内容"),
]


def log(msg, tag="INFO"):
    print("[%s] %s" % (tag, msg), flush=True)


def log_ok(msg):
    log(msg, "OK")


def log_warn(msg):
    log(msg, "WARN")


def log_err(msg):
    log(msg, "ERROR")


def ask(prompt, default, yes):
    """交互提问；--yes 或 --dry-run 时取默认值"""
    if yes:
        return default
    if default is None:
        sys.stdout.write("%s: " % prompt)
        sys.stdout.flush()
        return sys.stdin.readline().strip()
    sys.stdout.write("%s [默认 %s]: " % (prompt, default))
    sys.stdout.flush()
    line = sys.stdin.readline().strip()
    return line if line else default


def load_template(tpl_path):
    with open(tpl_path, "r", encoding="utf-8") as f:
        return f.read()


def fill_template(text, target):
    data_dir = str(target)
    TOKENS["<DATA_DIR>"] = data_dir
    TOKENS["<INSTALL_DIR>"] = data_dir
    for k, v in TOKENS.items():
        text = text.replace(k, str(v))
    return text


def config_has_real_values(cfg_path):
    """配置保护：内容实质判断——含非占位符赋值即视为真实配置"""
    if not os.path.exists(cfg_path):
        return False
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith(";"):
                    continue
                val = line.split("=", 1)[1].strip() if "=" in line else ""
                if val and "<" not in val and "PLACEHOLDER" not in val:
                    return True
    except OSError:
        return False
    return False


def init_sample_db(target, dry_run):
    """生成样例库：完整 schema（与生产逐列对齐）+ 5 行合成模板数据（零真实数据）。
    幂等：重复安装不覆盖既有库；旧版（≤10 列）库仅告警并给出补丁指引（不静默改库）。"""
    db_path = os.path.join(target, TOKENS["<MEMORY_DB_FILE>"])
    if dry_run:
        log("DRY-RUN 将生成样例库: %s（完整 schema：41 列+3 索引+FTS5+3 触发器；5 行合成模板数据）" % db_path)
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript(SAMPLE_DB_SCHEMA)
    have = [r[1] for r in cur.execute("PRAGMA table_info(memory_store)").fetchall()]
    missing = [c for c in SAMPLE_DB_COLUMNS if c not in have]
    if missing:
        log_warn("样例库 schema 为旧版（缺 %d 列：%s%s）——请按 INSTALL.md「样例库 schema 补丁」升级，"
                 "或删除 %s 后重跑安装（样例库为合成模板数据，删除安全）"
                 % (len(missing), ", ".join(missing[:5]), " 等" if len(missing) > 5 else "", db_path))
    else:
        cur.executescript(SAMPLE_DB_INDEXES)
        try:
            cur.executescript(SAMPLE_DB_FTS)
            fts_ok = True
        except sqlite3.Error as e:  # FTS5 不可用的构建 → 检索降级，不阻断安装
            fts_ok = False
            log_warn("FTS5 检索通道未建立（%s）——检索降级为 LIKE 通道（不阻断安装）" % e)
        if fts_ok:
            n_store = cur.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
            n_fts = cur.execute("SELECT COUNT(*) FROM mem_fts").fetchone()[0]
            if n_fts == 0 and n_store > 0:
                cur.execute("INSERT INTO mem_fts(rowid, summary, content) "
                            "SELECT rowid, summary, content FROM memory_store")
        if cur.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0] == 0:
            cur.executemany(
                "INSERT INTO memory_store (id,timestamp,type,summary,content,source_agent) "
                "VALUES (?,?,?,?,?,?)",
                [tuple(r) + (SAMPLE_SOURCE_AGENT,) for r in SAMPLE_ROWS],
            )
    conn.commit()
    n_cols = len(cur.execute("PRAGMA table_info(memory_store)").fetchall())
    n_rows = cur.execute("SELECT COUNT(*) FROM memory_store").fetchone()[0]
    conn.close()
    if missing:
        log_ok("样例库就绪: %s（schema %d 列·旧版结构未改动·存量 %d 行；升级指引见上述 WARN）"
               % (db_path, n_cols, n_rows))
    else:
        log_ok("样例库生成: %s（schema %d 列·%d 行合成模板数据·零真实数据）" % (db_path, n_cols, n_rows))


def sync_module_files(target, dry_run):
    """镜像模块 7 件套到安装目录，使已安装副本可独立升级/回滚"""
    here = Path(__file__).resolve().parent
    for name in ("install.py", "update.py", "README.md", "INSTALL.md", "manifest.yaml", "config.example", "mcp_server.py", "samples", "src"):
        src = os.path.join(str(here), name)
        dst = os.path.join(target, name)
        if not os.path.exists(src):
            continue
        if os.path.exists(dst) and os.path.samefile(src, dst):
            continue
        if dry_run:
            log("DRY-RUN 镜像模块文件: %s" % name)
            continue
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)
    if not dry_run:
        log_ok("模块文件镜像完成（已安装副本可独立升级/回滚）")


def check_port(port, host="127.0.0.1"):
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


def check_mcp_handshake(target, tool_name):
    """MCP 自检（验收③）：握手 initialize + 工具调用 1 例，走真实 stdio JSON-RPC"""
    import json as _json
    import subprocess as _sp
    srv = os.path.join(target, "mcp_server.py")
    if not os.path.exists(srv):
        return False, "mcp_server.py 缺失"
    init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    call = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": tool_name, "arguments": {"target": target}}}
    framed = b""
    for m in (init, call):
        body = _json.dumps(m).encode("utf-8")
        framed += b"Content-Length: %d\r\n\r\n" % len(body) + body
    try:
        r = _sp.run([sys.executable, srv, "--target", target], input=framed,
                    capture_output=True, timeout=30)
        out = r.stdout
        ok = r.returncode == 0 and out.count(b"Content-Length:") >= 2 and b'"id": 2' in out
        detail = "握手+工具调用 %s 通过" % tool_name if ok else "MCP 调用失败（rc=%d）" % r.returncode
        return ok, detail
    except Exception as e:  # noqa: BLE001
        return False, "MCP 异常: %s" % e


def run_checks(target, cfg, dry_run, skip_mcp=False):
    results = []
    for c in CHECKS:
        ok = False
        detail = ""
        if c["kind"] == "db":
            dbp = os.path.join(target, cfg.get("db_file", TOKENS["<MEMORY_DB_FILE>"]))
            ok = os.path.exists(dbp)
            try:
                if ok:
                    conn = sqlite3.connect(dbp)
                    conn.execute("SELECT COUNT(*) FROM memory_store")
                    conn.close()
                detail = dbp
            except sqlite3.Error as e:
                ok = False
                detail = "DB 读取失败: %s" % e
        elif c["kind"] == "dir":
            key = c.get("cfg_key", c["id"])
            p = cfg.get(key, "")
            if not p:
                p = TOKENS["<CHROMA_DIR>"] if c["id"] == "chroma" else TOKENS["<MODEL_DIR>"]
            if p and os.path.isabs(p):
                ok = os.path.isdir(p)
                detail = p
            else:
                p2 = os.path.join(target, p)
                ok = os.path.isdir(p2)
                detail = p2
        elif c["kind"] == "port":
            port = cfg.get("embedding_port", TOKENS["<EMBEDDING_PORT>"])
            ok = check_port(port)
            detail = "127.0.0.1:%s" % port
        elif c["kind"] == "pyver":
            ok = sys.version_info >= (3, 11)
            detail = "%d.%d.%d" % sys.version_info[:3]
        elif c["kind"] == "import":
            miss = []
            for mod in ("jieba", "numpy"):
                try:
                    __import__(mod)
                except ImportError:
                    miss.append(mod)
            ok = not miss
            detail = "缺失: %s" % ",".join(miss) if miss else "全部可导入"
        elif c["kind"] == "mcp":
            if skip_mcp:
                ok, detail = True, "跳过（MCP 工具内不自检自身，防递归）"
            else:
                ok, detail = check_mcp_handshake(target, "%s.check" % MODULE_ID.split("-")[0])
        results.append({"id": c["id"], "name": c["name"], "hard": c["hard"], "ok": ok, "detail": detail})
    return results


def write_manifest(target):
    """写入安装清单 <target>/config/.manifest.json（与 uninstall.py 卸载契约一致）"""
    import datetime
    import hashlib
    import json
    cfg_dir = os.path.join(target, "config")
    os.makedirs(cfg_dir, exist_ok=True)
    files = {}
    skip_dirs = {"config", "backups", "__pycache__"}
    for root, dirs, fnames in os.walk(target):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for fn in fnames:
            fp = os.path.join(root, fn)
            rel = os.path.relpath(fp, target)
            if fn.endswith(".pyc") or fn == ".manifest.json":
                continue
            try:
                with open(fp, "rb") as f:
                    files[rel] = hashlib.md5(f.read()).hexdigest()
            except OSError:
                files[rel] = ""
    man = {"module": MODULE_ID, "version": VERSION,
           "installed_at": datetime.datetime.now().isoformat(timespec="seconds"),
           "files": files}
    mpath = os.path.join(cfg_dir, ".manifest.json")
    with open(mpath, "w", encoding="utf-8") as f:
        json.dump(man, f, ensure_ascii=False, indent=1)
    log_ok("安装清单写入: %s（%d 个文件指纹，卸载/升级据此精确移除）" % (mpath, len(files)))



# ============================================================
# 环境检测门禁（模块升级 2026-08-16 升级）—— 对不齐拒装（exit 2）
# 三层次: a) 基础环境(OS/Python≥3.11)  b) 软件依赖(库/命令/宿主)
#         c) 多 Agent 检测(目标系统 Agent 配置/角色/技能)
# 不通过 → 打印缺失清单+补齐指引并返回 2（拒装）；通过 → 对齐引导后继续
# ============================================================
GATE_SPEC = {'module': 'a1-siku-core', 'py_min': (3, 11), 'os_allow': ['darwin', 'linux', 'windows'], 'libs': [{'name': 'jieba', 'hard': True, 'fix': 'python3.11 -m pip install jieba'}, {'name': 'numpy', 'hard': True, 'fix': 'python3.11 -m pip install numpy'}], 'cmds': [], 'hosts': [{'name': 'Hermes', 'path': '<hermes>', 'hard': False, 'fix': '安装 Hermes Agent 或设置 HERMES_HOME'}], 'agents': [{'name': 'Hermes 配置', 'path': '<hermes>/config.yaml', 'kind': 'file', 'hard': False, 'fix': '初始化 Hermes 配置'}, {'name': '看板 boards 目录', 'path': '<hermes>/kanban/boards', 'kind': 'dir', 'hard': False, 'fix': '初始化看板'}], 'guide': '安装后配置位于 <TARGET>/config.ini；目标系统 MCP 注册位置：Hermes <HERMES_HOME>/config.yaml → mcp_servers.a1-siku-core（顶层段·详见安装生成 MCP_CONFIG.md）'}

def env_gate(extra=None):
    """环境检测门禁。通过返回 0；不通过打印缺失清单并返回 2（拒装）。"""
    import importlib as _ilib
    import platform as _plat
    import socket as _sock
    extra = extra or {}
    spec = GATE_SPEC
    home = extra.get("home") or os.path.expanduser("~/.hermes")
    def _p(p):
        hermes = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
        openclaw = os.environ.get("OPENCLAW_HOME") or os.path.expanduser("~/.openclaw")
        p = str(p).replace("<hermes>", hermes).replace("<openclaw>", openclaw)
        p = p.replace("<home>", home).replace("<TARGET>", "<TARGET>")
        p = p.replace("<INSTALL_ROOT>", "<INSTALL_ROOT>").replace("<KANBAN_HOME>", extra.get("kanban_home", "<KANBAN_HOME>"))
        p = p.replace("<target_home>", extra.get("target_home", home)).replace("<target_skills>", extra.get("target_skills", ""))
        return os.path.expanduser(p)
    problems, warns, oks = [], [], []
    # ---------- L1 基础环境 ----------
    py = sys.version_info[:2]
    if py < tuple(spec["py_min"]):
        problems.append("[L1] Python %d.%d < 必需 %d.%d —— 升级: 安装 Python %d.%d+（macOS: brew install python@%d.%d）"
                        % (py[0], py[1], spec["py_min"][0], spec["py_min"][1], spec["py_min"][0], spec["py_min"][1], spec["py_min"][0], spec["py_min"][1]))
    else:
        oks.append("[L1] Python %d.%d.%d >= %d.%d" % (sys.version_info[0], sys.version_info[1], sys.version_info[2], spec["py_min"][0], spec["py_min"][1]))
    osname = _plat.system().lower()
    if spec.get("os_allow") and not any(osname.startswith(a) for a in spec["os_allow"]):
        problems.append("[L1] OS %s 不在支持列表 %s" % (osname, spec["os_allow"]))
    else:
        oks.append("[L1] OS %s 受支持" % osname)
    # ---------- L2 软件依赖 ----------
    for lib in spec.get("libs", []):
        try:
            _ilib.import_module(lib["name"]); oks.append("[L2] 依赖库 %s 可用" % lib["name"])
        except Exception:
            (problems if lib.get("hard", True) else warns).append("[L2] 依赖库 %s 缺失 —— 补齐: %s%s" % (lib["name"], lib["fix"], "" if lib.get("hard", True) else "（软性，可继续）"))
    for cmd in spec.get("cmds", []):
        if cmd.get("platform") and not osname.startswith(cmd["platform"]):
            continue
        if shutil.which(cmd["name"]):
            oks.append("[L2] 命令 %s 可用" % cmd["name"])
        elif cmd.get("hard", True):
            problems.append("[L2] 命令 %s 缺失 —— 补齐: %s" % (cmd["name"], cmd["fix"]))
        else:
            warns.append("[L2] 命令 %s 缺失（软性，可继续）" % cmd["name"])
    for hst in spec.get("hosts", []):
        hp = _p(hst["path"])
        if os.path.isdir(hp):
            oks.append("[L3] 宿主系统 %s 就位: %s" % (hst["name"], hp))
        elif hst.get("hard", True):
            problems.append("[L3] 宿主系统 %s 缺失: %s —— 补齐: %s" % (hst["name"], hp, hst["fix"]))
        else:
            warns.append("[L3] 宿主系统 %s 缺失: %s（软性）" % (hst["name"], hp))
    # ---------- L3b 宿主版本检测（Hermes ≥0.20.1——硬性） ----------
    try:
        _hv = subprocess.run(["hermes", "--version"], capture_output=True, text=True, timeout=10)
        _vline = (_hv.stdout or _hv.stderr).strip()
        _m = re.search(r"v?(\d+)\.(\d+)\.(\d+)", _vline)
        if _m:
            _ver = tuple(int(x) for x in _m.groups())
            if _ver < (0, 20, 1):
                problems.append("[L3b] Hermes 版本 %s < 必需 0.20.1 —— 升级: 运行 hermes update 或按官方指引升级" % _vline)
            else:
                oks.append("[L3b] Hermes %s >= 0.20.1" % _vline)
        else:
            warns.append("[L3b] 无法解析 Hermes 版本（%s）—— 跳过版本门禁" % (_vline[:60] or "hermes 命令无输出"))
    except Exception as _e:
        warns.append("[L3b] Hermes 版本检测失败（%s）—— 跳过版本门禁" % str(_e)[:60])
    # ---------- L3 多 Agent 检测 ----------
    for ag in spec.get("agents", []):
        ap = _p(ag["path"])
        if ag.get("kind") == "port":
            port = int(ag["path"].split(":")[1])
            try:
                with _sock.create_connection(("127.0.0.1", port), timeout=1):
                    oks.append("[L3] %s 存活" % ag["name"]); continue
            except OSError:
                pass
            (problems if ag.get("hard", True) else warns).append("[L3] %s 未连通 —— 补齐: %s%s" % (ag["name"], ag["fix"], "" if ag.get("hard", True) else "（软性）"))
            continue
        ok = os.path.isfile(ap) if ag.get("kind") == "file" else os.path.isdir(ap)
        if ok:
            oks.append("[L3] %s 就位: %s" % (ag["name"], ap))
        elif ag.get("hard", True):
            problems.append("[L3] %s 缺失: %s —— 补齐: %s" % (ag["name"], ap, ag["fix"]))
        else:
            warns.append("[L3] %s 缺失: %s（软性）" % (ag["name"], ap))
    # ---------- 输出 ----------
    print("")
    print("── 环境检测门禁（%s）──" % spec["module"])
    for o in oks: print("  ✅ " + o)
    for w in warns: print("  ⚠️  " + w)
    for p in problems: print("  ❌ " + p)
    if problems:
        print("")
        print("❌ 环境检测不通过（%d 项硬性缺失），拒绝安装（exit 2）。" % len(problems))
        print("   对齐引导：请按上述缺失项逐一补齐后重跑 install.py；不要硬装。")
        return 2
    print("")
    print("✅ 环境检测通过，对齐引导：")
    print("   配置写入指引：" + spec["guide"])
    return 0

def main():
    ap = argparse.ArgumentParser(description="%s 安装向导 v%s" % (MODULE_NAME, VERSION))
    ap.add_argument("--yes", action="store_true", help="非交互，全默认")
    ap.add_argument("--target", default=None, help="安装目录（默认 %s）" % DEFAULT_TARGET)
    ap.add_argument("--dry-run", action="store_true", help="演练，零写入")
    args = ap.parse_args()
    rc = env_gate()
    if rc:
        return rc

    here = Path(__file__).resolve().parent
    tpl = here / "config.example"
    if not tpl.exists():
        log_err("缺少 config.example，请使用完整模块包")
        return 1

    log("=" * 60)
    log("欢迎安装 %s v%s" % (MODULE_NAME, VERSION))
    log("=" * 60)

    if args.dry_run:
        log_warn("演练模式（--dry-run）：只打印动作，零写入")
    target = args.target or DEFAULT_TARGET
    if not args.yes and not args.dry_run:
        target = ask("安装目录", target, False)
    target = os.path.abspath(os.path.expanduser(target))
    log("安装目录: %s" % target)

    # ============================================================
    # 冲突检测与处理（模块升级 2026-08-16）—— 同名/旧版本/残留 + 智能体命名
    # ============================================================
    _conflict_parent = os.path.dirname(os.path.abspath(__file__))
    while _conflict_parent and not os.path.isfile(os.path.join(_conflict_parent, "_conflict_lib.py")):
        _up = os.path.dirname(_conflict_parent)
        if _up == _conflict_parent:
            _conflict_parent = None
            break
        _conflict_parent = _up
    if not _conflict_parent:
        _conflict_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _conflict_parent not in sys.path:
        sys.path.insert(0, _conflict_parent)
    try:
        import _conflict_lib
    except ImportError:
        _conflict_lib = None
    if _conflict_lib is None:
        log("[conflict] ⚠️ 未找到 _conflict_lib.py（需在完整模块包中运行）——跳过冲突保护")
    else:
        _rc = _conflict_lib.conflict_guard(
            module='a1-siku-core', version=VERSION,
            targets=[target], configs=[os.path.join(target, "config.ini")],
            template=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.example"),
            registry_path=os.path.join(_conflict_parent, ".install-registry.json"),
            legacy=[("~/m3-siku", "旧包 m3-siku（四库 55+ 脚本副本）")],
            yes=args.yes, dry_run=args.dry_run, log=log)
        if _rc == 2:
            log("[conflict] 已装同版/新版，跳过安装（幂等）")
            return(0)
        if _rc:
            return(_rc)
        _conflict_lib.agent_naming(target, yes=args.yes, dry_run=args.dry_run, log=log)

    # 1. 建目录
    for d in ("", TOKENS["<CHROMA_DIR>"], "backups"):
        p = os.path.join(target, d)
        if args.dry_run:
            log("DRY-RUN 创建目录: %s" % p)
        else:
            os.makedirs(p, exist_ok=True)
    log_ok("目录结构就绪")

    # 2. 写配置（配置保护）—— P1 修复 2026-08-16：合并产生的占位符（新键）也必须填充为实际值，
    #    否则 config_has_real_values 因旧值误判"已填充"→跳过模板填充→自检 int('<EMBEDDING_PORT>') 崩溃
    cfg_path = os.path.join(target, "config.ini")
    if os.path.exists(cfg_path):
        cur = load_template(str(cfg_path))
        filled = fill_template(cur, target)
        if filled != cur:
            if args.dry_run:
                log("DRY-RUN 填充配置占位符: %s" % cfg_path)
            else:
                with open(cfg_path, "w", encoding="utf-8") as f:
                    f.write(filled)
                log_ok("配置占位符填充: %s（旧值保留，<...> 已替换为实际值）" % cfg_path)
        else:
            log_warn("检测到已填充真实配置，跳过覆盖（配置保护）")
    else:
        text = fill_template(load_template(str(tpl)), target)
        if args.dry_run:
            log("DRY-RUN 写入配置: %s" % cfg_path)
        else:
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(text)
            log_ok("配置写入: %s（占位符已填充安装目录）" % cfg_path)

    # 3. 样例库初始化
    init_sample_db(target, args.dry_run)

    # 5. 镜像模块文件（已安装副本可独立升级/回滚）
    sync_module_files(target, args.dry_run)

    # 4. 自检


    # 6. MCP 注册（维护方 2026-08-16 硬性要求：MCP 接口为每模块必交付物，安装必注册）
    mcp_src = here / "mcp_server.py"
    if not mcp_src.exists():
        log_err("缺少 mcp_server.py——MCP 为必交付物，请使用完整模块包")
        return 1
    if args.dry_run:
        log("DRY-RUN 注册 MCP 服务（mcp_server.py + MCP_CONFIG.md）")
    else:
        shutil.copy2(str(mcp_src), os.path.join(target, "mcp_server.py"))
        with open(os.path.join(target, "MCP_CONFIG.md"), "w", encoding="utf-8") as f:
            f.write('# MCP 服务注册说明（stdio JSON-RPC 2.0）——安装必做步骤产物\n\n- 服务文件：`mcp_server.py`（与 CLI 共享同一核心逻辑：复用 install.py 的 CHECKS/run_checks，薄壳）\n- 启动命令：`python3 <TARGET>/mcp_server.py [--target <TARGET>]`\n- 暴露工具：a1.check / a1.smoke_search / a1.config_get（带输入输出 schema）\n- **目标系统 MCP 配置文件位置（按宿主选择其一）**：\n  | 宿主 | 配置文件位置 | 配置段 |\n  |:-----|:-------------|:-------|\n  | Hermes Agent | `<HERMES_HOME>/config.yaml` | 顶层 `mcp_servers.<name>` |\n  | Claude Desktop | `<USER_HOME>/Library/Application Support/Claude/claude_desktop_config.json` | `mcpServers.<name>` |\n  | 通用 MCP 客户端（stdio） | 客户端自身 servers 配置 | `command` + `args` |\n- 配置示例（Hermes·顶层 `mcp_servers`）：\n\n```yaml\nmcp_servers:\n  a1-siku-core:\n    command: python3\n    args: ["<TARGET>/mcp_server.py", "--target", "<TARGET>"]\n```\n\n> OpenClaw 侧为 `mcp.servers.<name>`（见包根 MCP_CONFIG.md）。\n')
        log_ok("MCP 服务注册完成：mcp_server.py + MCP_CONFIG.md（工具: %s）" % 'a1.check / a1.smoke_search / a1.config_get')

    cfg = {}
    if not args.dry_run and os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith(("#", ";")):
                    k, _, v = line.partition("=")
                    cfg[k.strip()] = v.strip()
    log("自检依赖（%d 项）:" % len(CHECKS))
    if args.dry_run:
        for c in CHECKS:
            log("  演练项 %s %s（安装后实际检查）" % ("[硬]" if c["hard"] else "[软]", c["name"]))
        log_ok("演练完成：动作预览如上，零写入；真实安装将执行自检")
        return 0
    hard_fail = False
    for r in run_checks(target, cfg, args.dry_run):
        status = "✅" if r["ok"] else ("❌" if r["hard"] else "⚠️")
        if not r["ok"] and r["hard"]:
            hard_fail = True
        log("%s %-28s %s %s" % (status, r["name"], "通过" if r["ok"] else "未通过", r["detail"]))

    if hard_fail:
        log_err("硬性自检未通过，请检查上述 ❌ 项后重跑")
        return 1
    write_manifest(target)
    log_ok("安装完成：%s（样例模式）" % target)
    log("生产接入：将生产脚本/库/模型路径填入 %s 后重跑自检" % cfg_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
