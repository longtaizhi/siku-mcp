#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""a1-siku-core MCP 验证脚本（零依赖·stdlib）

对包内 src/siku_mcp_server.py 做 stdio JSON-RPC 全链断言：
  A 协议握手 / B 工具面（7 工具+schema）/ C 工具真实调用（只读 5 + 写类拒答臂）
  D 端点守卫（D3 A5：非本地端点必拒）/ E 单一权威源（enums ⟷ siku_types.py 全等）/ F 未知工具拒绝面

用法:
  python3 test_mcp.py                     # 自定位包内 server 与样例库（样例库缺失时数据断言=S KIP）
  python3 test_mcp.py --db /path/db       # 指定记忆库（默认 <包根>/sample_memory.db）
  python3 test_mcp.py --require-db        # CI 模式：数据断言不可 SKIP
  python3 test_mcp.py --allow-write-arms  # 额外跑 entity_extract 空集臂（会建 entity_* 空表，仅对样例库默认开）
退出码: 0=全过（SKIP 不失败；--require-db 时 SKIP 计失败）；1=存在 FAIL。
"""
import argparse
import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

sys.dont_write_bytecode = True   # 父进程侧不生成 __pycache__（包卫生）

HERE = os.path.dirname(os.path.abspath(__file__))
EXPECTED_TOOLS = [
    "search_memories", "concept_query", "ontology_explore", "expand_entry",
    "entity_knowledge", "entity_extract", "hard_delete_memory",
]

RESULTS = []

def record(status, tid, desc, detail=""):
    RESULTS.append((status, tid, desc))
    line = "%-4s %-4s %s" % (status, tid, desc)
    if detail:
        line += "  | " + str(detail)[:200]
    print(line)

class Session:
    """stdio JSON-RPC 子进程会话（线程读响应）。"""

    def __init__(self, cmd, env, cwd=None):
        if len(cmd) >= 2 and not any(a.startswith("-") for a in cmd[1:2]):
            cmd = [cmd[0], "-B"] + list(cmd[1:])   # -B：子进程零 pyc（包卫生）
        self.p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True, bufsize=1,
                                  env=env, cwd=cwd)
        self.resp = {}
        self.err_lines = []
        self._id = 0
        self._lock = threading.Lock()
        threading.Thread(target=self._reader, daemon=True).start()
        threading.Thread(target=self._err_reader, daemon=True).start()

    def _reader(self):
        for ln in self.p.stdout:
            ln = ln.strip()
            if not ln.startswith("{"):
                continue
            try:
                msg = json.loads(ln)
            except Exception:
                continue
            if "id" in msg:
                with self._lock:
                    self.resp[msg["id"]] = msg

    def _err_reader(self):
        for ln in self.p.stderr:
            self.err_lines.append(ln.rstrip())
            if len(self.err_lines) > 200:
                del self.err_lines[:100]

    def call(self, method, params=None, timeout=60):
        self._id += 1
        req = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params is not None:
            req["params"] = params
        self.p.stdin.write(json.dumps(req, ensure_ascii=False) + "\n")
        self.p.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self._id in self.resp:
                    return self.resp.pop(self._id)
            if self.p.poll() is not None:
                raise RuntimeError("server 进程提前退出 rc=%s stderr尾=%s"
                                   % (self.p.returncode, self.err_lines[-3:]))
            time.sleep(0.05)
        raise RuntimeError("等待响应超时 %ss: %s" % (timeout, method))

    def call_tool(self, name, args, timeout=60):
        msg = self.call("tools/call", {"name": name, "arguments": args}, timeout=timeout)
        if "error" in msg:
            return None, msg["error"], msg
        res = msg.get("result", {})
        text = ""
        for item in res.get("content", []):
            if item.get("type") == "text":
                text += item.get("text", "")
        try:
            body = json.loads(text)
        except Exception:
            body = None
        return body, None, res

    def close(self):
        try:
            self.p.stdin.close()
        except Exception:
            pass
        try:
            self.p.wait(timeout=5)
        except Exception:
            self.p.terminate()


def detect_python():
    env_py = os.environ.get("SIKU_VENV_PYTHON")
    if env_py and os.path.exists(env_py):
        return env_py
    for cand in (os.path.join(os.path.expanduser("~"), ".hermes/hermes-agent/venv/bin/python"),
                 sys.executable):
        if cand and os.path.exists(cand):
            return cand
    return "python3"


def sqlite_has_table(db, table):
    """探测记忆库存在且含 memory_store 表。ro→immutable→普通连接 回退链（WAL 库 ro 无 -shm 会拒开）。"""
    if not db or not os.path.exists(db):
        return False
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    for uri, kw in (("file:%s?mode=ro" % db, {"uri": True}),
                    ("file:%s?immutable=1" % db, {"uri": True}),
                    (db, {})):
        try:
            c = sqlite3.connect(uri, **kw)
            r = c.execute(q, (table,)).fetchone()
            c.close()
            return bool(r)
        except Exception:
            continue
    return False


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=os.path.join(HERE, "src", "siku_mcp_server.py"))
    ap.add_argument("--db", default=None)
    ap.add_argument("--concepts", default=None, help="概念集目录（含 种子概念集.json/domain-graph.jsonld）")
    ap.add_argument("--python", default=None)
    ap.add_argument("--timeout", type=float, default=90.0)
    ap.add_argument("--require-db", action="store_true")
    ap.add_argument("--allow-write-arms", action="store_true")
    args = ap.parse_args()

    py = args.python or detect_python()
    db = args.db or os.environ.get("SIKU_DB_PATH") or os.path.join(HERE, "sample_memory.db")
    db_ok = sqlite_has_table(db, "memory_store")
    concepts_dir = args.concepts or os.environ.get("SIKU_CONCEPTS_DIR")
    tmp_root = tempfile.mkdtemp(prefix="siku_test_mcp_")

    env = dict(os.environ)
    env["SIKU_DB_PATH"] = db
    env["SIKU_ROOT"] = tmp_root            # 审计落临时目录，零外写
    env["PYTHONDONTWRITEBYTECODE"] = "1"   # 不污染包目录
    env.setdefault("SIKU_ENTITY_ARCHIVE", "on")
    if concepts_dir:
        env["SIKU_CONCEPTS_DIR"] = concepts_dir

    print("== a1-siku-core MCP 验证 ==")
    print("server : %s" % args.server)
    print("python : %s" % py)
    print("db     : %s (%s)" % (db, "ok" if db_ok else "缺失/无表→数据断言 SKIP"))
    print("concepts: %s" % (concepts_dir or "(未指定)"))
    print("")

    if not os.path.exists(args.server):
        record("FAIL", "A0", "server 文件存在", args.server)
        return 1

    s = Session([py, args.server], env=env, cwd=HERE)
    try:
        # A 协议
        init = s.call("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                                     "clientInfo": {"name": "test_mcp", "version": "1.0"}})
        r = init.get("result", {})
        record("PASS" if r.get("protocolVersion") else "FAIL", "A1",
               "initialize 握手", "protocolVersion=%s server=%s" % (
                   r.get("protocolVersion"), (r.get("serverInfo") or {}).get("name")))

        # B 工具面
        tl = s.call("tools/list", {})
        tools = (tl.get("result") or {}).get("tools", [])
        names = [t.get("name") for t in tools]
        record("PASS" if sorted(names) == sorted(EXPECTED_TOOLS) else "FAIL", "B1",
               "tools/list = 7 工具（精确集合）", "n=%d %s" % (len(names), ",".join(names)))
        schema_bad = [t.get("name") for t in tools
                      if not isinstance(t.get("inputSchema"), dict)
                      or t["inputSchema"].get("type") != "object"
                      or not t["inputSchema"].get("properties")]
        record("PASS" if not schema_bad else "FAIL", "B2",
               "每工具 inputSchema 形态（object+properties 非空）", "bad=%s" % schema_bad)

        # C1 search_memories
        search_body = None
        if db_ok:
            body, err, _ = s.call_tool("search_memories",
                                       {"query": "检索", "mode": "fts5", "top_k": 5},
                                       timeout=args.timeout)
            if body is None:
                record("FAIL", "C1", "search_memories 调用", err or "不可解析响应")
            else:
                hits = body.get("results") or body.get("hits") or []
                ok = isinstance(hits, list) and len(hits) >= 1 and "error" not in body
                record("PASS" if ok else "FAIL", "C1", "search_memories(fts5) 命中≥1",
                       "hits=%s" % (len(hits) if isinstance(hits, list) else hits))
                search_body = body
        else:
            record("SKIP", "C1", "search_memories（缺库）", db)

        # C2 expand_entry
        if db_ok and search_body:
            hits = search_body.get("results") or []
            eid = None
            for h in hits:
                if isinstance(h, dict):
                    eid = h.get("id") or h.get("entry_id")
                    if eid:
                        break
            if eid:
                body, err, _ = s.call_tool("expand_entry", {"entry_id": eid}, timeout=args.timeout)
                ent = (body or {}).get("entry") or {}
                ok = isinstance(body, dict) and ent.get("id") and ent.get("summary") is not None
                record("PASS" if ok else "FAIL", "C2", "expand_entry 全字段",
                       "entry_id=%s tier=%s" % (eid, (body or {}).get("tier")))
            else:
                record("SKIP", "C2", "expand_entry（检索无 id 可展开）")
        else:
            record("SKIP", "C2", "expand_entry（缺库）")

        # C3 concept_query
        if concepts_dir and os.path.isdir(concepts_dir):
            body, err, _ = s.call_tool("concept_query", {"concept": "记忆", "top_k": 3},
                                       timeout=args.timeout)
            ok = isinstance(body, dict) and ("hits" in body or "ok" in body)
            record("PASS" if ok else "FAIL", "C3", "concept_query 受控返回",
                   "hits=%s" % (body or {}).get("hits"))
        else:
            record("SKIP", "C3", "concept_query（未指定概念目录）")

        # C4 ontology_explore(enums)
        body, err, _ = s.call_tool("ontology_explore", {"scope": "enums"}, timeout=args.timeout)
        enums = (body or {}).get("enums") or {}
        auth = (enums.get("meta") or {}).get("authority", "")
        record("PASS" if enums and "siku_types" in auth else "FAIL", "C4",
               "ontology_explore(enums) 权威源口径", auth)

        # C5 entity_knowledge（受控：found 布尔在场，不依赖数据量）
        body, err, _ = s.call_tool("entity_knowledge", {"entity": "基因", "top_k": 3},
                                   timeout=args.timeout)
        ok = isinstance(body, dict) and "found" in body
        record("PASS" if ok else "FAIL", "C5", "entity_knowledge 受控结构", "found=%s" % (body or {}).get("found"))

        # C7 hard_delete_memory 拒答（裁定 c：业务级拒答·零删除·非异常）
        body, err, res = s.call_tool("hard_delete_memory",
                                     {"entry_id": "test_mcp_probe", "agent": "test_mcp",
                                      "why": "验证脚本拒答臂（不真删）", "confirm": True},
                                     timeout=args.timeout)
        is_err = bool(res.get("isError"))
        ok = (isinstance(body, dict) and body.get("refused") is True
              and body.get("deletion_performed") is False
              and body.get("reason") == "reserved_not_implemented" and not is_err)
        record("PASS" if ok else "FAIL", "C7", "hard_delete_memory 业务级拒答·零删除·无 isError",
               "refused=%s" % (body or {}).get("refused"))

        # F 未知工具拒绝面
        body, err, res = s.call_tool("__no_such_tool__", {}, timeout=args.timeout)
        record("PASS" if res.get("isError") else "FAIL", "F1",
               "未知工具 → isError=true（拒绝面）")

        # C6 写类拒答臂（第二会话：SIKU_ENTITY_ARCHIVE=off）
        env2 = dict(env)
        env2["SIKU_ENTITY_ARCHIVE"] = "off"
        s2 = Session([py, args.server], env=env2, cwd=HERE)
        try:
            s2.call("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                                   "clientInfo": {"name": "test_mcp", "version": "1.0"}})
            body, err, _ = s2.call_tool("entity_extract", {"limit": 1}, timeout=args.timeout)
            ok = isinstance(body, dict) and body.get("enabled") is False
            record("PASS" if ok else "FAIL", "C6", "entity_extract 开关拒答臂（off）",
                   str(body)[:100])
        finally:
            s2.close()

        # C6b 空集臂（可选；仅包内样例库默认开，真实库须显式 --allow-write-arms）
        sample_db = (os.path.basename(db) == "sample_memory.db") and db_ok
        if args.allow_write_arms or sample_db:
            body, err, _ = s.call_tool("entity_extract", {"ids": ["__test_mcp_nonexistent__"]},
                                       timeout=args.timeout)
            ok = isinstance(body, dict) and body.get("scanned") == 0 and body.get("entities_created") == 0
            record("PASS" if ok else "FAIL", "C6b", "entity_extract 空集臂（scanned=0 零写入）",
                   "scanned=%s" % (body or {}).get("scanned"))

        # E 单一权威源
        try:
            st = load_module(os.path.join(HERE, "src", "siku_types.py"), "siku_types_test")
            pairs = [("content_types", st.CONTENT_TYPES), ("asset_types", st.ASSET_TYPES),
                     ("valid_types", st.VALID_TYPES), ("relation_types", st.RELATION_TYPES),
                     ("pseudo_edge_relations", st.PSEUDO_EDGE_RELATIONS),
                     ("conflict_types", st.CONFLICT_TYPES), ("conflict_subtypes", st.CONFLICT_SUBTYPES)]
            bad = [k for k, ref in pairs if enums.get(k) != ref]
            record("PASS" if not bad else "FAIL", "E1",
                   "单一权威源：enums ⟷ siku_types.py 全等（7 清单）", "bad=%s" % bad)
        except Exception as e:
            record("FAIL", "E1", "单一权威源对拍", str(e)[:120])

        # D 端点守卫（D3 A5）
        try:
            ea = load_module(os.path.join(HERE, "src", "entity_archive.py"), "entity_archive_test")
            pos = ["http://127.0.0.1:8081", "http://localhost:8081", "http://[::1]:8081",
                   "http://192.168.1.10:8081", "http://10.0.0.5:8081", "http://box.local:8081"]
            neg = ["http://8.8.8.8:8081", "https://api.openai.com/v1", "http://192.0.2.1",
                   "http://172.32.0.1", "ftp://127.0.0.1:8081", ""]
            p_bad = [u for u in pos if not _guard_ok(ea, u)]
            n_bad = [u for u in neg if _guard_ok(ea, u)]
            record("PASS" if not p_bad and not n_bad else "FAIL", "D1",
                   "端点守卫正样例放行 %d/%d" % (len(pos) - len(p_bad), len(pos)), "bad=%s" % p_bad)
            record("PASS" if not n_bad else "FAIL", "D2",
                   "端点守卫负样例全拒 %d/%d（D3 A5）" % (len(neg) - len(n_bad), len(neg)), "bad=%s" % n_bad)
        except Exception as e:
            record("FAIL", "D1/D2", "端点守卫对拍", str(e)[:120])

    except Exception as e:
        record("FAIL", "X", "会话异常", str(e)[:200])
    finally:
        s.close()
        shutil.rmtree(tmp_root, ignore_errors=True)

    n_pass = sum(1 for st_, _, _ in RESULTS if st_ == "PASS")
    n_fail = sum(1 for st_, _, _ in RESULTS if st_ == "FAIL")
    n_skip = sum(1 for st_, _, _ in RESULTS if st_ == "SKIP")
    if args.require_db:
        n_fail += n_skip
    print("\n== 汇总: PASS=%d FAIL=%d SKIP=%d%s ==" % (
        n_pass, n_fail, n_skip, "（--require-db：SKIP 计失败）" if args.require_db else ""))
    return 1 if n_fail else 0

def _guard_ok(ea, url):
    try:
        ea.validate_llm_endpoint(url)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    sys.exit(main())
