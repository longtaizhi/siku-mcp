#!/usr/bin/env python3
"""
siku_mcp_server.py — 四库全书 MCP Server (stdlib only)

Wraps l3_retrieval.py as a standard MCP stdio server.
Implements MCP JSON-RPC 2.0 protocol with zero external dependencies.

Protocol:
  - Client sends JSON-RPC requests via stdin (one JSON object per line)
  - Server writes JSON-RPC responses via stdout (one JSON object per line)
  - stderr is used only for logging/debug output
"""

import json
import sys
import os
import time

_SIKU_ROOT = os.environ.get("SIKU_ROOT", os.path.expanduser("~/siku-core"))  # 数据根目录（环境变量可覆盖；默认=安装目录）
_HERMES_HOME = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))  # Hermes 主目录（环境变量可覆盖）

# Ensure l3_retrieval.py is importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

SERVER_NAME = "siku-4k"
SERVER_VERSION = "1.0.0"

# ── M4 前缀版本化（S2 检索优化）：检索命令模板版本常量 ────────────────
# 常驻前缀 = MCP 客户端(LLM) 消费的 search_memories 指令固定段（命令模板）。
# 版本常量 PREFIX_V<n>：前缀文本/语义任何变更必须 bump（PREFIX_V1→V2...），
#   客户端可据 [prefix-version:...] 标记失效旧工具描述缓存。
# 动态内容禁入常驻前缀：mode 枚举、参数描述、top_k 等动态字段一律留在
#   inputSchema（tools/list 时按请求组装），禁止以任何方式写入
#   SEARCH_MEMORIES_PREFIX —— 由 _prefix_static_guard() 结构守卫强制。
SEARCH_MEMORIES_PREFIX_VERSION = "PREFIX_V1"
SEARCH_MEMORIES_PREFIX = (
    "搜索四库全书记忆库。支持全文检索(FTS5)、向量语义检索(Embedding)和双通道RRF融合检索。"
    "返回渐进式compact结果（索引视图），包含50字摘要、置信度、token预估。"
    "需要完整详情时使用expand_entry展开。"
    "[prefix-version:" + SEARCH_MEMORIES_PREFIX_VERSION + "]"
)

def _prefix_static_guard():
    """动态内容禁入常驻前缀（M4 结构守卫，双保险）：
    常驻前缀只允许纯字面量文本 + 固定版本标记；
    任何动态内容（变量插值、枚举、运行时值）必须放在 inputSchema 或调用侧拼接。
    前缀含插值符号即 AssertionError——宁可崩在守卫也不要静默把动态内容带进前缀。
    模块加载与 tools/list 各执行一次。"""
    assert "{" not in SEARCH_MEMORIES_PREFIX, \
        "常驻前缀含插值符号：动态内容禁入前缀（应放 inputSchema）"
    return True

def log(msg):
    """Write debug message to stderr (never stdout)."""
    print(f"[siku_mcp] {msg}", file=sys.stderr, flush=True)

def send_response(request_id, result):
    """Send a JSON-RPC 2.0 success response to stdout."""
    resp = {"jsonrpc": "2.0", "id": request_id, "result": result}
    sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
    sys.stdout.flush()

def send_error(request_id, code, message):
    """Send a JSON-RPC 2.0 error response to stdout."""
    resp = {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
    sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
    sys.stdout.flush()

# ── MCP Method Handlers ─────────────────────────────────────────────

def handle_initialize(params):
    """Handle MCP initialize request."""
    return {
        "protocolVersion": "2024-11-05",
        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        "capabilities": {"tools": {}}
    }

def handle_tools_list(params):
    """Handle MCP tools/list request."""
    _prefix_static_guard()  # M4 结构守卫：常驻前缀动态内容检查（tools/list 时执行）
    return {
        "tools": [
            {
                "name": "search_memories",
                "description": SEARCH_MEMORIES_PREFIX,
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "搜索关键词。支持中文、英文、混合查询。"
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["auto", "fts5", "embed", "dual"],
                            "description": "检索模式：auto(自动选择最佳通道)、fts5(仅全文检索)、embed(仅向量语义检索)、dual(双通道RRF融合)",
                            "default": "auto"
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "返回结果数量",
                            "default": 5
                        },
                        "track": {
                            "type": "string",
                            "enum": ["episodic", "semantic"],
                            "description": "记忆分轨过滤：episodic=情景轨 / semantic=语义轨（缺省不限轨，全量检索，向后兼容）"
                        },
                        "time_from": {
                            "type": "string",
                            "description": "时间过滤下界（ISO 或 epoch，如 2026-08-01 / 1786242300；缺省不过滤）"
                        },
                        "time_to": {
                            "type": "string",
                            "description": "时间过滤上界（ISO 或 epoch，如 2026-08-10 / 1786328700；缺省不过滤）"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "concept_query",
                "description": (
                    "概念查询（P3 MCP 扩展）：按概念名（prefLabel/altLabel 命中）"
                    "返回种子概念集（C5 权威源）与语义层 jsonld 的概念档案"
                    "（prefLabel/altLabels/context/定义）+ 一层本体关系"
                    "（broader 父类/narrower 子类——子类/同义映射）。"
                    "纯只读，供 concept 通道与推理引擎 R2/R3 规则接线。"
                    "[prefix-version:" + SEARCH_MEMORIES_PREFIX_VERSION + "]"
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "concept": {
                            "type": "string",
                            "description": "概念名或同义词（如 门禁审核 / A2A通信）"
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "返回命中数",
                            "default": 5
                        }
                    },
                    "required": ["concept"]
                }
            },
            {
                "name": "ontology_explore",
                "description": (
                    "本体探索（P3 MCP 扩展）：只读查看四库本体资产——"
                    "enums（siku_types.py 唯一权威源：25 种类型/关系/冲突分类）、"
                    "shapes（shapes.yaml 非枚举硬约束）、"
                    "concepts（种子概念集 + jsonld 概念统计）。"
                    "scope 可选 enums/shapes/concepts/all（默认 all）。"
                    "[prefix-version:" + SEARCH_MEMORIES_PREFIX_VERSION + "]"
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "scope": {
                            "type": "string",
                            "enum": ["enums", "shapes", "concepts", "all"],
                            "description": "探索范围",
                            "default": "all"
                        }
                    }
                }
            },
            {
                "name": "expand_entry",
                "description": (
                    "展开单条四库记忆条目的完整详情（渐进式第二层）。"
                    "包含完整摘要、内容正文、来源Agent、时间戳等。"
                    "需要先用search_memories获取entry_id。"
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "entry_id": {
                            "type": "string",
                            "description": "记忆条目ID（由search_memories返回的id字段）"
                        }
                    },
                    "required": ["entry_id"]
                }
            },
            {
                "name": "entity_knowledge",
                "description": (
                    "实体摘要聚合查询（\"关于 X 我已知什么\"）。"
                    "按实体名（或别名）返回实体档案（aliases/聚合摘要/关联条目数）"
                    "及关联记忆条目（relation/摘要/来源Agent）。"
                    "实体档案由 entity_extract 从记忆摘要抽取构建。"
                    "[prefix-version:" + SEARCH_MEMORIES_PREFIX_VERSION + "]"
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "entity": {
                            "type": "string",
                            "description": "实体名或别名（如 四库系统/OpenClaw）"
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "返回关联条目数",
                            "default": 5
                        }
                    },
                    "required": ["entity"]
                }
            },
            {
                "name": "entity_extract",
                "description": (
                    "实体抽取入库：从四库记忆条目 summary 用本地模型抽取实体，"
                    "构建/更新实体档案（幂等 upsert）。LLM 端点不可用时自动降级规则抽取，不阻塞。"
                    "SIKU_ENTITY_ARCHIVE=off 时返回关闭提示。"
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "抽取条目数上限（默认 100）",
                            "default": 100
                        }
                    }
                }
            },
            {
                "name": "hard_delete_memory",
                "description": (
                    "【预留声明·本版本未实现删除执行】合规删除记忆条目（物理删除——不可恢复）"
                    "的声明占位：当前版本零删除能力，调用返回业务级拒答、不执行任何删除。"
                    "预留门禁（未来实现接线口径）：WRITE_GATE=1 + agent + why + confirm=true。"
                    "如需物理删除请走受控人工 CLI 通道。"
                    "[prefix-version:PREFIX_V1]"
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["entry_id", "agent", "why", "confirm"],
                    "properties": {
                        "entry_id": {
                            "type": "string",
                            "description": "要删除的记忆条目 ID（如 mem_20260817_xxxxxx 或四库 UUID）"
                        },
                        "agent": {
                            "type": "string",
                            "description": "操作 Agent 标识（如 agent/default）"
                        },
                        "why": {
                            "type": "string",
                            "description": "删除原因（必填——审计用）"
                        },
                        "confirm": {
                            "type": "boolean",
                            "description": "二次确认标志（预留门禁；本版本不执行删除）"
                        }
                    }
                }
            },
        ]
    }

_SENSITIVE_AUDIT_KEYS = frozenset({
    'why', 'reason', 'password', 'passwd', 'token', 'secret',
    'api_key', 'apikey', 'authorization', 'auth', 'credential',
})


def _summarize_params(arguments, max_len=50):
    """调用参数摘要（审计脱敏）：字符串截断前 50 字，跳过敏感键，列表只记长度。"""
    if not isinstance(arguments, dict):
        return {'raw': str(arguments)[:max_len]}
    out = {}
    for k, v in arguments.items():
        if k.lower() in _SENSITIVE_AUDIT_KEYS:
            continue
        if isinstance(v, str):
            out[k] = v[:max_len]
        elif isinstance(v, (int, float, bool)) or v is None:
            out[k] = v
        elif isinstance(v, (list, tuple)):
            out[k] = '[%d items]' % len(v)
        elif isinstance(v, dict):
            out[k] = {kk: (vv[:max_len] if isinstance(vv, str) else vv)
                      for kk, vv in list(v.items())[:10]}
        else:
            out[k] = str(v)[:max_len]
    return out


def _write_mcp_audit(tool_name, arguments, start_time, result, agent):
    """MCP 调用审计落盘（6 字段 ts/tool/agent/params/result/duration_ms，
    对齐 mcp-audit.py 审计记录格式）。旧记录不删，追加式写入。"""
    try:
        import json as _j, datetime as _dt
        _log = os.path.join(_SIKU_ROOT, 'logs/mcp_access.jsonl')
        os.makedirs(os.path.dirname(_log), exist_ok=True)
        with open(_log, 'a', encoding='utf-8') as _f:
            _f.write(_j.dumps({
                'ts': _dt.datetime.now().isoformat(),
                'tool': tool_name,
                'agent': agent,
                'params': _summarize_params(arguments),
                'result': result,
                'duration_ms': int((time.perf_counter() - start_time) * 1000),
            }, ensure_ascii=False) + '\n')
    except Exception:
        pass


def handle_tools_call(params):
    """Handle MCP tools/call request. Delegates to l3_retrieval.py."""
    tool_name = params.get("name", "")
    arguments = params.get("arguments", {}) or {}
    # ── 调用审计（2026-08-17  补齐 6 字段：
    #    ts/tool/agent/params/result/duration_ms，对齐 mcp-audit.py 审计记录；
    #    params 为脱敏摘要，调用结束后落盘）──────────────────────────────
    _t0 = time.perf_counter()
    _agent = os.environ.get('AGENT_ID', os.environ.get('SIKU_AGENT_ID', 'unknown'))

    try:
        if tool_name in ("search_memories", "expand_entry"):
            # Lazy import — l3_retrieval has heavy deps (chromadb≥0.5, numpy, etc.);
            # chromadb pulls in hnswlib, onnxruntime, pydantic, etc. — lazy import avoids
            # loading these at startup unless search_memories is actually called.
            # (: entity_* 工具不 import l3_retrieval——entity_archive 纯 stdlib，
            #  任意 python 可跑；同时规避系统 python 下 l3_retrieval venv 重执行守卫打断 stdio)
            import l3_retrieval

        if tool_name == "search_memories":
            query = arguments.get("query", "")
            mode = arguments.get("mode", "auto")
            top_k = int(arguments.get("top_k", 5))
            track = arguments.get("track") or None
            time_from = arguments.get("time_from") or None
            time_to = arguments.get("time_to") or None
            result = l3_retrieval.mcp_search_tool(query, mode=mode, top_k=top_k, track=track,
                                                  time_from=time_from, time_to=time_to)

        elif tool_name == "expand_entry":
            entry_id = arguments.get("entry_id", "")
            result = l3_retrieval.mcp_expand_tool(entry_id)

        elif tool_name in ("entity_knowledge", "entity_extract"):
            # 实体摘要网络——独立模块懒加载，零影响主链路
            import entity_archive
            if tool_name == "entity_knowledge":
                entity = arguments.get("entity", "")
                top_k = int(arguments.get("top_k", 5))
                result = entity_archive.query_entity(entity, top_k=top_k)
            else:
                limit = int(arguments.get("limit", 100))
                ids = arguments.get("ids") or None
                result = entity_archive.ingest_entries(limit=limit, entry_ids=ids)

        elif tool_name in ("concept_query", "ontology_explore"):
            # P3 MCP 扩展——概念/本体只读查询，
            # 独立 stdlib 模块懒加载（同 entity_archive 模式，零影响主链路）
            import siku_concept_meta
            if tool_name == "concept_query":
                concept = arguments.get("concept", "")
                top_k = int(arguments.get("top_k", 5))
                result = siku_concept_meta.query_concept(concept, top_k=top_k)
            else:
                scope = arguments.get("scope", "all")
                result = siku_concept_meta.explore(scope)
                result["_tool"] = "ontology_explore"

        elif tool_name == "hard_delete_memory":
            # 【预留声明·拒答路径】（编排裁定 c·2026-09-22；维护方 2026-08-19「MCP 暴露搁置」）：
            # 本版本零删除能力——不实现物理删除执行；调用一律返回业务级拒答（受控返回，
            # 非异常抛出）。声明门禁（WRITE_GATE=1 + agent/why/confirm）保留为未来实现接线口径。
            _entry_id = arguments.get("entry_id", "")
            _gate_on = (os.environ.get("HERMES_WRITE_GATE") == "1"
                        or os.environ.get("SIKU_WRITE_GATE") == "1")
            _missing = [k for k in ("entry_id", "agent", "why") if not arguments.get(k)]
            if arguments.get("confirm") is not True:
                _missing.append("confirm=true")
            _refusal = {
                "ok": False,
                "refused": True,
                "reason": "reserved_not_implemented",
                "tool": "hard_delete_memory",
                "message": ("hard_delete_memory 为预留声明（本版本未实现删除执行）：零删除能力，"
                            "MCP 通道不执行任何物理删除；声明门禁 WRITE_GATE=1 + agent/why/"
                            "confirm=true（未接线）。如需物理删除请走受控人工 CLI 通道。"),
                "deletion_performed": False,
                "entry_id": _entry_id,
                "gates": {"write_gate": _gate_on, "missing_params": _missing},
            }
            _write_mcp_audit(tool_name, arguments, _t0, 'refused', _agent)
            return {
                "content": [{"type": "text", "text": json.dumps(_refusal, ensure_ascii=False)}]
            }

        else:
            _write_mcp_audit(tool_name, arguments, _t0, 'error', _agent)
            return {
                "content": [{"type": "text", "text": json.dumps(
                    {"error": f"Unknown tool: {tool_name}"}, ensure_ascii=False
                )}],
                "isError": True
            }

        _write_mcp_audit(tool_name, arguments, _t0, 'success', _agent)
        return {
            "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}]
        }

    except Exception as e:
        log(f"Tool call error [{tool_name}]: {e}")
        _write_mcp_audit(tool_name, arguments, _t0, 'error', _agent)
        return {
            "content": [{"type": "text", "text": json.dumps(
                {"error": str(e)}, ensure_ascii=False
            )}],
            "isError": True
        }

# ── Main Loop ───────────────────────────────────────────────────────

def _execv_venv_if_needed():
    """3.9 启动场景提前切换 venv（2026-08-25）。

    必须在读取任何 stdin 请求行之前 execv——l3_retrieval.py 顶层守卫在
    handle_tools_call 首次 lazy import 时才触发切换，彼时请求行已读入
    进程缓冲；execv 只保留 fd、清空进程内存缓冲 → 该请求丢失 → 客户端超时。
    此处启动即切：stdin 尚无数据被读入，管道中未消费的数据在 exec 后
    仍由新进程从同一 fd 读取，零丢失。
    """
    if sys.version_info < (3, 11):
        _vp = os.environ.get("SIKU_VENV_PYTHON") or os.path.join(_HERMES_HOME, "hermes-agent/venv/bin/python")
        if os.path.exists(_vp):
            log(f"Python {sys.version.split()[0]} < 3.11 — execv to venv python: {_vp}")
            os.execv(_vp, [_vp] + sys.argv)
        else:
            log(f"WARN venv-guard: {_vp} 不存在 → 继续用系统解释器 {sys.executable}（embed 通道可能降级；可用 SIKU_VENV_PYTHON 覆盖）")

def main():
    _execv_venv_if_needed()
    log(f"Starting {SERVER_NAME} v{SERVER_VERSION} on Python {sys.version.split()[0]}")
    log(f"Script dir: {SCRIPT_DIR}")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except json.JSONDecodeError as e:
            log(f"JSON parse error: {e}")
            continue

        if request.get("jsonrpc") != "2.0":
            continue

        method = request.get("method", "")
        request_id = request.get("id")
        params = request.get("params", {})

        # Notification (no id) — no response needed
        if request_id is None:
            if method == "notifications/initialized":
                log("Client initialized")
            else:
                log(f"Notification: {method}")
            continue

        log(f"→ {method} (id={request_id})")

        try:
            if method == "initialize":
                result = handle_initialize(params)
            elif method == "tools/list":
                result = handle_tools_list(params)
            elif method == "tools/call":
                result = handle_tools_call(params)
            else:
                send_error(request_id, -32601, f"Method not found: {method}")
                continue

            send_response(request_id, result)
        except Exception as e:
            log(f"Handler error [{method}]: {e}")
            send_error(request_id, -32603, f"Internal error: {e}")

if __name__ == "__main__":
    main()

