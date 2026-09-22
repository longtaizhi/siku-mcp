#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
a1-siku-core MCP 服务 v1.0.0（stdio JSON-RPC 2.0）
作者: 维护者团队｜功能: 将模块核心命令（check/smoke_search/config_get）暴露为 MCP 工具；
与 CLI 共享同一核心逻辑（薄壳：复用 install.py 的 CHECKS/run_checks）。
用法: python3 mcp_server.py [--target <安装目录>]
协议: 标准输入 Content-Length 帧（MCP stdio 传输），输出 JSON-RPC 2.0
工具: a1.check / a1.smoke_search / a1.config_get
"""
import json
import os
import re
import sqlite3
import sys

MODULE_ID = "a1-siku-core"
MODULE_NAME = "知识库核心"
VERSION = "1.0.0"
DEFAULT_TARGET = os.path.expanduser("~/siku-core")
PROTOCOL_VERSION = "2024-11-05"

TOOLS = [
    {
        "name": "a1.check",
        "description": "知识库核心自检：DB 存在可读/向量库目录/嵌入端口/模型目录/python 版本/依赖（复用安装自检核心）",
        "inputSchema": {
            "type": "object",
            "properties": {"target": {"type": "string", "description": "安装目录，默认 ~/siku-core"}},
        },
    },
    {
        "name": "a1.smoke_search",
        "description": "样例库检索冒烟：对 memory_store 表做 FTS 关键字查询",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "检索关键字"},
                "limit": {"type": "integer", "description": "返回条数上限，默认 5"},
                "target": {"type": "string", "description": "安装目录"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "a1.config_get",
        "description": "读取已安装配置（data_dir/db_file/chroma_dir/model_dir/embedding_port）",
        "inputSchema": {
            "type": "object",
            "properties": {"target": {"type": "string", "description": "安装目录"}},
        },
    },
]


def send(msg):
    body = json.dumps(msg, ensure_ascii=False).encode("utf-8")
    sys.stdout.buffer.write(b"Content-Length: %d\r\n\r\n" % len(body) + body)
    sys.stdout.buffer.flush()


def read_frame():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        k, _, v = line.decode("utf-8", "replace").partition(":")
        headers[k.strip().lower()] = v.strip()
    n = int(headers.get("content-length", 0))
    if n <= 0:
        return None
    body = sys.stdin.buffer.read(n)
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError:
        return None


def _read_cfg(target):
    cfg = {}
    p = os.path.join(target, "config.ini")
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith(("#", ";")):
                    k, _, v = line.partition("=")
                    cfg[k.strip()] = v.strip()
    return cfg


def _install_module(target):
    sys.path.insert(0, target)
    import install
    return install


def tool_check(target):
    mod = _install_module(target)
    cfg = _read_cfg(target)
    results = [{"id": r["id"], "name": r["name"], "hard": r["hard"], "ok": r["ok"], "detail": r["detail"]}
               for r in mod.run_checks(target, cfg, False, skip_mcp=True)]
    return {"ok": True, "module": MODULE_ID, "version": VERSION, "checks": results}


def tool_smoke_search(args):
    target = os.path.abspath(os.path.expanduser(args.get("target") or DEFAULT_TARGET))
    cfg = _read_cfg(target)
    dbp = os.path.join(target, cfg.get("db_file", "sample_memory.db"))
    if not os.path.exists(dbp):
        return {"ok": False, "reason": "db_not_found", "db": dbp}
    q = args.get("query", "")
    limit = int(args.get("limit", 5))
    conn = sqlite3.connect(dbp)
    rows = conn.execute(
        "SELECT id, summary FROM memory_store WHERE summary LIKE ? OR content LIKE ? LIMIT ?",
        ("%" + q + "%", "%" + q + "%", limit),
    ).fetchall()
    conn.close()
    return {"ok": True, "query": q, "hits": [{"id": r[0], "summary": r[1]} for r in rows]}


def tool_config_get(args):
    target = os.path.abspath(os.path.expanduser(args.get("target") or DEFAULT_TARGET))
    return {"ok": True, "target": target, "config": _read_cfg(target)}


def handle_call(name, args):
    if name == "a1.check":
        return tool_check(os.path.abspath(os.path.expanduser(args.get("target") or DEFAULT_TARGET)))
    if name == "a1.smoke_search":
        return tool_smoke_search(args)
    if name == "a1.config_get":
        return tool_config_get(args)
    raise ValueError("unknown tool: %s" % name)


def main():
    target = DEFAULT_TARGET
    argv = sys.argv[1:]
    if "--target" in argv:
        target = os.path.abspath(os.path.expanduser(argv[argv.index("--target") + 1]))
    while True:
        req = read_frame()
        if req is None:
            break
        mid = req.get("id")
        method = req.get("method")
        params = req.get("params") or {}
        if method == "initialize":
            send({"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": MODULE_ID, "version": VERSION},
            }})
        elif method == "notifications/initialized":
            pass
        elif method == "ping":
            send({"jsonrpc": "2.0", "id": mid, "result": {}})
        elif method == "tools/list":
            send({"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            try:
                result = handle_call(params.get("name", ""), params.get("arguments") or {})
                send({"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                    "structuredContent": result,
                }})
            except Exception as e:  # noqa: BLE001
                send({"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": "tool error: %s" % e}],
                    "isError": True,
                }})
        else:
            send({"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "method not found: %s" % method}})


if __name__ == "__main__":
    main()
