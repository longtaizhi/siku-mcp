# 四库 MCP 接入配置（三端模板 · 7 工具主入口）

- **服务文件**：`src/siku_mcp_server.py`（stdio JSON-RPC 2.0 · 零第三方依赖）
- **暴露工具（7）**：`search_memories` / `expand_entry` / `entity_knowledge` / `entity_extract` /
  `concept_query` / `ontology_explore` / `hard_delete_memory`
  - `hard_delete_memory` = **预留声明（本版本未实现删除执行）**：调用返回业务级拒答、零删除能力
    （非异常抛出；编排裁定 2026-09-22，与 2026-08-19「MCP 暴露搁置」一致）。如需物理删除请走受控人工 CLI 通道。
- **前置**：安装完成（`python3 install.py --target <TARGET>`）后 `<TARGET>/src/siku_mcp_server.py` 与样例库就位；
  生产使用经 `SIKU_DB_PATH` 指向真实记忆库。

## 0. 通用要点

- 启动命令一律 `python3 <TARGET>/src/siku_mcp_server.py`（由宿主按 stdio 管理进程，勿手动常驻）。
- 可选环境变量：

  | 变量 | 用途 | 默认 |
  |:--|:--|:--|
  | `SIKU_DB_PATH` | 记忆库路径 | `<HERMES_HOME>/memory_store.db` 等按包默认 |
  | `SIKU_CONCEPTS_DIR` | 概念集目录（种子概念集.json / domain-graph.jsonld） | `<HERMES_HOME>/memory-bank/concepts` |
  | `SIKU_ROOT` | 日志/审计根（mcp_access.jsonl） | 按包默认 |
  | `SIKU_ENTITY_MODEL` | 实体抽取本地模型 id（不设则仅规则抽取） | 空 |
  | `SIKU_VENV_PYTHON` | ≥3.11 的解释器路径（系统 python3 <3.11 时自动转此解释器） | 空 |

- **安全基线（勿改）**：实体抽取端点须为**本机或内网本地模型**（`127.0.0.1/localhost/::1`、`10.x/172.16-31.x/192.168.x`、`*.local`）；
  非本地端点一律拒绝调用（D3 A5 判据）。示例配置中**勿填公网地址**。

## 1. Hermes Agent

配置文件：`<HERMES_HOME>/config.yaml`（顶层 `mcp_servers:` 段，与生产同构）：

```yaml
mcp_servers:
  siku-4k:                      # 建议名；亦可用「四库检索」
    command: python3
    args:
      - <TARGET>/src/siku_mcp_server.py
    env:
      PYTHONPATH: <TARGET>/src
    timeout: 120
```

生效：实例重载后，工具面可见 7 工具（`siku-4k` 前缀）。

## 2. OpenClaw

配置文件：`~/.openclaw/openclaw.json`（`mcp.servers` 段，与生产同构）：

```json
{
  "mcp": {
    "servers": {
      "siku-4k": {
        "command": "python3",
        "args": ["<TARGET>/src/siku_mcp_server.py"],
        "env": {"PYTHONPATH": "<TARGET>/src"}
      }
    }
  }
}
```

## 3. Claude Code

项目级 `.mcp.json`（或用户级配置）：

```json
{
  "mcpServers": {
    "siku-4k": {
      "command": "python3",
      "args": ["<TARGET>/src/siku_mcp_server.py"],
      "env": {"PYTHONPATH": "<TARGET>/src"}
    }
  }
}
```

或 CLI 一条命令：

```bash
claude mcp add siku-4k -- python3 <TARGET>/src/siku_mcp_server.py
```

## 4. 验证（装完必做）

```bash
# 包内验证脚本（零依赖；样例库缺失时数据断言自动 SKIP，--require-db 可将 SKIP 计失败）
python3 test_mcp.py
python3 test_mcp.py --db <库路径> --concepts <概念集目录> --require-db

# 官方 Inspector（交互式；或 --cli 模式单发 tools/list）
npx @modelcontextprotocol/inspector python3 <TARGET>/src/siku_mcp_server.py
npx @modelcontextprotocol/inspector --cli python3 <TARGET>/src/siku_mcp_server.py --method tools/list
```

## 附：与安装生成的 `MCP_CONFIG.md` 的关系

`install.py` 安装时会在 `<TARGET>/MCP_CONFIG.md` 生成「**模块自检薄壳**」（`mcp_server.py`：
`a1.check / a1.smoke_search / a1.config_get` 3 工具）的注册说明。两者用途不同、**可并存**：

| 注册名建议 | 服务 | 用途 |
|:--|:--|:--|
| `siku-4k` / 四库检索 | `<TARGET>/src/siku_mcp_server.py`（本文件） | 四库 7 工具主入口（检索/实体/概念/本体） |
| `a1-siku-core` | `<TARGET>/mcp_server.py`（安装生成） | 模块自检与轻量查询（3 工具） |

> 本模板点位于 `src/siku_mcp_server.py`（与 E1 后置接入项 §三「主入口」一致）；安装生成的薄壳模板见 `<TARGET>/MCP_CONFIG.md`。
