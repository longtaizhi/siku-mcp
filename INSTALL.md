---
agent: maintainers
type: module-doc
schema_version: "1.0"
updated: 2026-09-22
---

# a1-siku-core · 安装手册（INSTALL.md）

> 版本 **1.2.0**（单源：`manifest.yaml` 的 `version` 字段；变更记录见该文件 `changelog` 段）｜结构：七节（①环境要求 ②安装步骤 ③安装后验证 ④接入配置 ⑤注意事项 ⑥问题清单 ⑦卸载）＋ 附录 A/B/C
> 配套文档：`README.md`（模块概述 / 资产清单 / 后置接入项）｜`MCP_CONFIG.md`（三端 MCP 接入模板·7 工具主入口）｜`skills/README.md`（技能包安装与走通）｜`manifest.yaml`（版本 + changelog 单源）
> 命名约定：`<PKG>` = 包目录（解压后含 `install.py` 的目录）｜`<TARGET>` = 安装目录（默认 `~/siku-core`）｜`<PY311>` = Python ≥3.11 解释器（例：`~/.hermes/hermes-agent/venv/bin/python3.11`）

---

## ① 环境要求

### 1.1 硬性要求（环境门禁：不满足 → 拒装，exit 2）

| 项 | 要求 | 检查方式 | 不满足时 |
|:---|:-----|:---------|:---------|
| 操作系统 | macOS / Linux / Windows | 安装前自动检测（平台判定） | 不在支持列表 → 拒装（exit 2） |
| Python | **≥ 3.11** | 安装前自动检测（版本判定） | 拒装（exit 2）+ 升级指引 |
| 依赖库 | `jieba`、`numpy` 可导入（实测口径 jieba ≥0.42.1 / numpy ≥2.4.3） | 安装前自动 `import` | 拒装（exit 2）+ 安装命令指引 |
| Hermes Agent | **≥ 0.20.1** | 安装前 `hermes --version` 解析 | 低于 → 拒装；`hermes` 命令不可用 → 仅告警（跳过该门禁） |

> 铁律：**对不齐不要硬装**。版本不满足 → 拒装并给出补齐指引 → 对齐后再装。

### 1.2 运行期按需件（缺失自动降级，不阻断安装）

| 项 | 使用面 | 缺失时行为 |
|:---|:-------|:-----------|
| `PyYAML`（yaml） | 权限/配置类脚本模块级引入 | 对应脚本不可用（安装与检索主链不受影响） |
| `chromadb` | chroma 同步 / L3 chroma 通道 | 对应通道不可用（检索自动降级词面/余弦通道）；注意 opentelemetry 版本冲突（见 ⑥ Q10） |
| `sentence_transformers` / `sklearn` | L3 验证增强 | 对应增强功能自动跳过 |
| `mlx` / `mlx_lm` | 训练链（**不在本包**，GRPO 已移出） | 与本包运行无关 |
| 本地嵌入模型（`model_dir`） | 向量通道 | 样例模式仅告警；生产部署须就位 |
| 嵌入服务端口（`embedding_port`，默认 18790） | 自检 TCP 连接 | 样例模式仅告警（WARN）；生产部署须存活 |

### 1.3 资源与网络

- **磁盘**：解压后包体 ≈ 1.4 MB（其中 `src/` ≈ 1.1 MB）；安装新增 ≈ 1.2 MB（镜像 9 项含 `src/`；样例库 ≈ 45 KB 另计）；生产数据（真实记忆库/向量库/模型）另计。
- **网络**：安装与运行**零网络依赖**（纯标准库 ＋ 本地模型/端点）；仅官方 Inspector 首次使用需联网拉包（可选验证手段，见 ③ 3.3）。
- **端口**：不新开端口；仅对 `embedding_port`（默认 18790）做 TCP 可达性检查。

### 1.4 装前自查（一条龙，30 秒）

```bash
# ① 解释器版本（需 ≥3.11；macOS 自带 python3 常为 3.9.x，不满足）
<PY311> --version                 # 期望: Python 3.11.x（或更高）

# ② 依赖库可导入（无输出 = 可用）
<PY311> -c "import jieba, numpy"  # 期望: 静默；报 ModuleNotFoundError 见 ⑥ Q6

# ③ 宿主版本（安装器 L3b 门禁）
hermes --version                  # 期望: ≥ 0.20.1
```

---

## ② 安装步骤

每一步都给出：**命令 → 预期输出 → 成功的样子 / 失败的样子**。

### 2.1 步骤 1｜取得包并确认目录

包目录（`<PKG>`）解压后应含以下顶层件（缺件即包不完整，见 2.3 失败表）：

```
<PKG>/
├── install.py           # 安装向导（本手册主角）
├── update.py            # 升级件（check / apply / rollback）
├── manifest.yaml        # 版本单源 + changelog
├── INSTALL.md / README.md / MCP_CONFIG.md
├── config.example       # 配置模板（全占位符）
├── mcp_server.py        # 模块自检薄壳 MCP（3 工具）
├── test_mcp.py          # MCP 验证脚本（7 工具真实调用·16 项检查）
├── sample_memory.db     # 验证脚本运行产物（可缺省；正式安装会另生成 <TARGET>/sample_memory.db）
├── samples/             # 样例库说明
├── skills/              # 技能三件 + 一键安装器（见 2.4）
└── src/                 # 核心脚本（50 件 + 随件数据）
```

### 2.2 步骤 2｜演练（`--dry-run`，零写入）

```bash
cd <PKG>
<PY311> install.py --dry-run --target <TARGET>
```

**预期输出（节选·路径已泛化）**：

```
── 环境检测门禁（a1-siku-core）──
  ✅ [L1] Python 3.11.15 >= 3.11
  ✅ [L1] OS darwin 受支持
  ✅ [L2] 依赖库 jieba 可用
  ✅ [L2] 依赖库 numpy 可用
  ✅ [L3] 宿主系统 Hermes 就位: <HERMES_HOME>
  ✅ [L3b] Hermes ... >= 0.20.1
  ✅ [L3] Hermes 配置 就位: <HERMES_HOME>/config.yaml
  ⚠️  [L3] 看板 boards 目录 缺失: <HERMES_HOME>/kanban/boards（软性）
✅ 环境检测通过，对齐引导：
   配置写入指引：安装后配置位于 <TARGET>/config.ini；目标系统 MCP 注册位置：Hermes <HERMES_HOME>/config.yaml → mcp_servers.a1-siku-core（顶层段·详见安装生成 MCP_CONFIG.md）
[INFO] 欢迎安装 知识库核心（检索 L1-L3 + 蒸馏 L4 + 权限 + 评分） v1.2.0
[WARN] 演练模式（--dry-run）：只打印动作，零写入
[INFO] 安装目录: <TARGET>
[INFO] [conflict] ⚠️ 未找到 _conflict_lib.py（需在完整模块包中运行）——跳过冲突保护
[INFO] DRY-RUN 创建目录: <TARGET>/
[INFO] DRY-RUN 创建目录: <TARGET>/chroma
[INFO] DRY-RUN 创建目录: <TARGET>/backups
[OK] 目录结构就绪
[INFO] DRY-RUN 写入配置: <TARGET>/config.ini
[INFO] DRY-RUN 将生成样例库: <TARGET>/sample_memory.db（完整 schema：41 列+3 索引+FTS5+3 触发器；5 行合成模板数据）
[INFO] DRY-RUN 镜像模块文件: install.py
[INFO] DRY-RUN 镜像模块文件: update.py
…（README.md / INSTALL.md / manifest.yaml / config.example / mcp_server.py / samples / src 各一行，共 9 项）
[INFO] DRY-RUN 注册 MCP 服务（mcp_server.py + MCP_CONFIG.md）
[INFO] 自检依赖（7 项）:
[INFO]   演练项 [硬] 记忆库 DB 存在可读（安装后实际检查）
…（7 项逐行：硬 4 项 / 软 3 项）
[OK] 演练完成：动作预览如上，零写入；真实安装将执行自检
```

- **成功的样子**：末行 `演练完成`，且**目录里没有任何新文件**（`--dry-run` 零写入）。
- **失败的样子**：见 2.3 失败表（演练与正式安装的门禁判定相同）。

### 2.3 步骤 3｜正式安装

**交互式（首次安装推荐）**：

```bash
cd <PKG>
<PY311> install.py                 # 向导：欢迎 → 安装目录 → 配置确认 → 样例库 → 自检 → 汇总
```

**非交互式（自动化/复用）**：

```bash
<PY311> install.py --yes --target <TARGET>          # 全默认非交互
<PY311> install.py --yes --target <TARGET>          # 幂等：重复执行不覆盖真实配置、不重置数据
```

**预期输出（节选·非交互；路径已泛化）**：

```
[OK] 目录结构就绪
[OK] 配置写入: <TARGET>/config.ini（占位符已填充安装目录）
[OK] 样例库生成: <TARGET>/sample_memory.db（schema 41 列·5 行合成模板数据·零真实数据）
[OK] 模块文件镜像完成（已安装副本可独立升级/回滚）
[OK] MCP 服务注册完成：mcp_server.py + MCP_CONFIG.md（工具: a1.check / a1.smoke_search / a1.config_get）
[INFO] 自检依赖（7 项）:
[INFO] ✅ 记忆库 DB 存在可读                  通过 <TARGET>/sample_memory.db
[INFO] ✅ 向量库目录存在                      通过 <TARGET>/chroma
[INFO] ✅ 嵌入服务端口存活                     通过 127.0.0.1:18790        ← 无嵌入服务时为 ⚠️（软性）
[INFO] ⚠️ 嵌入模型目录存在                     未通过 <TARGET>/<MODEL_DIR>   ← 样例模式允许（软性）
[INFO] ✅ python3 >= 3.11              通过 3.11.15
[INFO] ✅ jieba / numpy 可导入            通过 全部可导入
[INFO] ✅ MCP 握手+工具调用                  通过 握手+工具调用 a1.check 通过
[OK] 安装清单写入: <TARGET>/config/.manifest.json（逐个文件指纹，卸载/升级据此精确移除）
[OK] 安装完成：<TARGET>（样例模式）
```

**成功的样子**：末行 `安装完成`，退出码 **0**；自检 7 项中 **4 个硬性项**（DB 可读 / 向量库目录 / python ≥3.11 / MCP 握手）全 ✅（**3 个软性项**按环境可为 ✅/⚠️，不阻断）。

**失败的样子（对照处理）**：

| 现象 | 原因 | 处理 |
|:-----|:-----|:-----|
| `❌ 环境检测不通过（N 项硬性缺失），拒绝安装（exit 2）` | Python <3.11 / jieba·numpy 缺失 / 系统不在支持列表 | 按提示逐项补齐（用 ≥3.11 解释器运行，或 `pip install jieba numpy`）后重跑 |
| `❌ 硬性自检未通过，请检查上述 ❌ 项后重跑`（exit 1） | 硬性自检项（DB 可读 / 向量库目录 / python 版本 / MCP 握手）失败 | 按日志中 ❌ 项逐条处理（DB 写权限、目录存在性、MCP 见 ⑥ Q2） |
| `缺少 config.example，请使用完整模块包` | 未在包目录运行，或包不完整 | 进入 `<PKG>` 后重跑；核对 2.1 顶层件 |
| `缺少 mcp_server.py——MCP 为必交付物` | 包不完整 | 重新获取完整包 |
| `[INFO] [conflict] ⚠️ 未找到 _conflict_lib.py（需在完整模块包中运行）——跳过冲突保护` | 单包运行（无完整模块包父目录） | **属正常**：跳过冲突保护，不影响安装（见 ⑤ 10） |

### 2.4 步骤 4｜技能包安装

**技能包安装（`skills/`·推荐——目标 Agent"装完即会用"）**：

- 随包 3 个技能：`siku-query`（检索）/ `siku-write`（写入）/ `siku-ops`（运维·管理员向）
- 一键安装：`python3 skills/install_skills.py --target <目标 skills 目录>`——Hermes 默认 `~/.hermes/skills`，OpenClaw 默认 `~/.openclaw/skills`（自动探测；`--dry-run` 演练、`--list` 状态、`--uninstall` 卸载）
- 验证：三件"已安装"；会话遇触发词（查四库/写入四库/四库巡检）自动加载
- 写入姿势与 L2 受控写纪律见技能 `siku-write`；走通示例/FAQ 见 `skills/README.md`（§三）

> 命令在 `<PKG>` 目录执行（技能装载到**目标 Agent 的 skills 目录**，与安装目录 `<TARGET>` 无关）。安装器幂等：内容一致跳过、同名旧件先备份（`.bak-时间戳`）再覆盖。

**自检依赖**（安装时自动执行，硬性失败 → 退出非 0）：

| 项 | 检查 | 样例安装预期 |
|:---|:-----|:------------|
| DB 存在可读 | 打开 `db_file` 读表 | ✅ 样例库生成 |
| 向量库目录 | `chroma_dir` 存在 | ✅ 样例创建 |
| 嵌入端口 | TCP 连接 `127.0.0.1:<port>` | ⚠️ WARN（生产依赖） |
| 嵌入模型 | `model_dir` 存在 | ⚠️ WARN（生产依赖） |
| python ≥ 3.11 | 版本判定 | ✅ |
| jieba/numpy | 可导入 | ⚠️ WARN（生产依赖） |
| MCP 握手+工具调用 | 薄壳 1 例调用 | ✅ |

### 2.5 安装产物（`<TARGET>` 结构）

| 路径 | 说明 |
|:-----|:-----|
| `config.ini` | 安装时由 `config.example` 填充（占位符 → 实际值）；**配置保护**：已填真实值的配置不被升级/重装覆盖 |
| `config/.manifest.json` | 安装清单（逐文件指纹 + 版本 + 安装时点·数量随发行内容变化）；卸载/升级据此精确处理 |
| `sample_memory.db` | 样例库：41 列同构 schema（3 索引 + FTS5 + 3 触发器）+ 5 行**合成模板数据**（零真实数据） |
| `src/` | 核心脚本（50 脚本 + 2 随件数据；安装器整目录镜像） |
| `samples/README.md` | 样例库说明（含旧版升级指引） |
| `chroma/`、`backups/` | 向量库目录（空）、升级备份目录 |
| `install.py` `update.py` `README.md` `INSTALL.md` `manifest.yaml` `config.example` `mcp_server.py` `MCP_CONFIG.md` | 模块文件镜像（9 项）＋ 安装生成的薄壳 MCP 说明 |

**镜像口径**：安装器镜像 **9 项**（`install.py` / `update.py` / `README.md` / `INSTALL.md` / `manifest.yaml` / `config.example` / `mcp_server.py` / `samples` / `src`）到 `<TARGET>`，使已安装副本可独立升级/回滚；**不镜像**：`skills/`（装到目标 Agent 的 skills 目录，见 2.4）、`test_mcp.py` 与包根 `MCP_CONFIG.md`（包内验证/接入模板，留在 `<PKG>` 使用）。

---

## ③ 安装后验证（一条龙）

### 3.1 ① 安装器自检

`install.py` 退出码 **0** 且自检表硬性项全 ✅（见 2.3 预期输出）。

### 3.2 ② 样例库可查（两路任选）

```bash
# 路 1｜schema 与行数直查（期望输出 41 / 5）
sqlite3 <TARGET>/sample_memory.db "PRAGMA table_info(memory_store);" | wc -l
sqlite3 <TARGET>/sample_memory.db "SELECT count(*) FROM memory_store;"

# 路 2｜检索接口冒烟（期望：total=5·search_mode=fts5_only）
<PY311> <TARGET>/src/l3_retrieval.py search --query "示例" --limit 5
```

> 样例库是 5 行合成数据（`示例：…`），检索命中 5 条即正常；`score` 在样例库为 0.0 属预期（无向量通道）。

### 3.3 ③ MCP 验证（7 工具真实调用）

```bash
# 包内验证脚本（零依赖；对 7 工具逐项断言，含端点守卫与拒答臂）
cd <PKG>
<PY311> test_mcp.py --db <TARGET>/sample_memory.db              # 样例模式：期望 PASS=14 FAIL=0 SKIP=1·rc=0
<PY311> test_mcp.py --db <TARGET>/sample_memory.db --concepts <概念集目录> --require-db   # 生产模式：SKIP 计失败

# 官方 Inspector（可选·首次需联网拉包；或 --cli 单发 tools/list）
npx @modelcontextprotocol/inspector --cli <PY311> <TARGET>/src/siku_mcp_server.py --method tools/list
# 期望：返回 7 工具（search_memories / concept_query / ontology_explore / expand_entry /
#                    entity_knowledge / entity_extract / hard_delete_memory）
```

**成功的样子**：`test_mcp.py` 汇总 `FAIL=0`；Inspector 输出含 7 个工具名。
**说明**：不给 `--db` 时脚本默认取 `<PKG>/sample_memory.db`——建议**始终显式传 `--db <TARGET>/sample_memory.db`**，避免在包目录产生新的样例库文件。

---

## ④ 接入配置（三端）

完整模板与说明同时随包提供：包根 **`MCP_CONFIG.md`**（7 工具主入口，推荐）。

### 4.1 Hermes Agent

配置文件：`<HERMES_HOME>/config.yaml`（顶层 `mcp_servers:` 段）：

```yaml
mcp_servers:
  siku-4k:                       # 建议名；亦可用「四库检索」等
    command: python3
    args:
      - <TARGET>/src/siku_mcp_server.py
    env:
      PYTHONPATH: <TARGET>/src
    timeout: 120
```

生效：实例重载后工具面可见 7 工具（`siku-4k` 前缀）。

### 4.2 Claude Code

```bash
# CLI 一条命令
claude mcp add siku-4k -- python3 <TARGET>/src/siku_mcp_server.py
```

或项目级 `.mcp.json`：

```json
{ "mcpServers": { "siku-4k": {
    "command": "python3",
    "args": ["<TARGET>/src/siku_mcp_server.py"],
    "env": {"PYTHONPATH": "<TARGET>/src"} } } }
```

### 4.3 OpenClaw

配置文件：`~/.openclaw/openclaw.json`（`mcp.servers` 段）：

```json
{ "mcp": { "servers": { "siku-4k": {
    "command": "python3",
    "args": ["<TARGET>/src/siku_mcp_server.py"],
    "env": {"PYTHONPATH": "<TARGET>/src"} } } } }
```

### 4.4 两套服务的关系（可并存）

| 注册名建议 | 服务文件 | 工具 | 用途 |
|:-----------|:---------|:-----|:-----|
| `siku-4k` / 四库检索 | `<TARGET>/src/siku_mcp_server.py` | 7 工具 | 检索/实体/概念/本体（**主入口**） |
| `a1-siku-core` | `<TARGET>/mcp_server.py`（安装生成的薄壳） | 3 工具 | 模块自检与轻量查询（`a1.check` / `a1.smoke_search` / `a1.config_get`） |

> 安装生成的 `<TARGET>/MCP_CONFIG.md` 描述的是**薄壳**（3 工具）；7 工具主入口的口径以包根 `MCP_CONFIG.md` 与本手册为准。

### 4.5 关键环境变量（接入常用）

| 变量 | 用途 | 默认 |
|:-----|:-----|:-----|
| `SIKU_ROOT` | 数据根目录（日志/审计/规则文件根） | `~/siku-core`（= 安装目录） |
| `HERMES_HOME` | 宿主家目录（概念集/venv 等默认根） | `~/.hermes` |
| `SIKU_DB_PATH` | 记忆库路径 | `<SIKU_ROOT>/memory_store.db` |
| `SIKU_CONCEPTS_DIR` | 概念集目录（种子概念集/domain-graph） | `<HERMES_HOME>/memory-bank/concepts` |
| `SIKU_VENV_PYTHON` | ≥3.11 解释器路径（系统 python3 <3.11 时自动转用） | 空 |
| `SIKU_WRITE_GATE` / `HERMES_WRITE_GATE` | 写闸（受控写必须开启，见 ⑤ 2） | 关 |

> 全量环境变量覆盖口共 100+ 个（检索/维护/组织各链），自查：`grep -rn "os.environ.get" <PKG>/src/`。

---

## ⑤ 注意事项（逐条列全）

1. **首次安装生成空库（零真实数据）**：样例库为 5 行合成模板数据（`smp_00001`~`smp_00005`），真实数据自行积累或按共享流程获取（见 `README.md` 后置接入项）。
2. **写操作需写闸（WRITE_GATE）**：受控写需 `HERMES_WRITE_GATE=1` + 令牌 + 白名单；**被拒=正常现象**。L3 直改禁止——唯一通道 = L2 → 管道自动晋升；高危删除为三重门槛 + 审计。
3. **端口/路径冲突处理**：安装目录已存在旧版本 → 运行 `update.py --check` 查看；同名目录被占用 → 换 `--target`；`embedding_port` 被占用 → 改 `config.ini` 的 `embedding_port` 后重跑自检（样例模式该告警不阻断）。
4. **与已有 Hermes 环境共存**：MCP 注册是**新增段**（`mcp_servers.siku-4k`），不改动既有条目；`<HERMES_HOME>/config.yaml` 若已有同名注册，改注册名即可并存。
5. **升级/回滚姿势**（详见 附录 A·R12）：
   - 升级：`<PY311> <TARGET>/update.py --apply`（自动备份 → 增量镜像 → 版本写回）；升级前 `--check` 对比版本。
   - 回滚：`<PY311> <TARGET>/update.py --rollback`（恢复最近一次 `backups/upd-*` 快照）；`--list-backups` 列快照。
   - 备份位置：**`<TARGET>/backups/`**（R12：保留 3 份 + 归位，禁散落根目录）。
6. **配置保护**：`config.ini` 中已填真实值（非占位符）→ 重装/升级**不覆盖**（复跑时该步显示 `[WARN] 检测到已填充真实配置，跳过覆盖（配置保护）`）；仅全占位符模板才更新。
7. **版本纪律**：包版本单源 = `manifest.yaml` 的 `version`（严格递增）＋ `changelog` 段；`README.md`/`INSTALL.md` 不另立版本号。升级幂等判定依赖本字段，勿手改。
8. **样例库 schema 版本**：安装生成 **41 列**同构库；历史版本（≤2026-09-21）生成的 10 列旧库只会**告警**并给补丁指引（不静默改库）——按 ⑥「样例库 schema 补丁」处理。
9. **FTS5 可用性**：宿主 SQLite 缺 FTS5 时安装不阻断（告警），检索自动降级 LIKE 通道。
10. **`_conflict_lib.py` 可选件**：单包运行会出现 `[INFO] [conflict] ⚠️ 未找到 _conflict_lib.py（需在完整模块包中运行）——跳过冲突保护` —— 属正常（该件为完整模块安装包的冲突保护设施）；放入完整模块包父目录时自动启用。
11. **包内构建期备份件**：`*.bak-*` 与 `__pycache__/`（若存在）为构建/验证过程产物，**非运行依赖**；注意安装器对 `src/` 是**整目录镜像**（含 `.bak-*`），故清单件数会随残迹变化——正式分发前由打包环节剔除（剔除后清单数稳定）。
12. **生产接入**：`config.ini` 填真实库/模型路径 + 把 `<TARGET>/src/` 脚本落位到生产 `scripts/`（**无需替换占位符**——脚本已去个性化，路径走环境变量，见 附录 B）→ 重跑自检。

---

## ⑥ 问题清单（FAQ）

> 每项格式：**现象 → 原因 → 解决命令**。

**Q1 环境门禁拒装（exit 2）：`[L1] Python 3.9.x < 必需 3.11`**
→ 用系统自带 `python3` 运行了安装器（macOS 自带为 3.9）。
→ 换 ≥3.11 解释器运行：
```bash
<PY311> install.py --yes --target <TARGET>
```

**Q2 MCP 连不上（宿主工具面看不到 7 工具）——排查树**
→ 逐层排查（由浅入深）：
```bash
# ① 服务文件在不在、能否直跑（直接问它要工具清单）
<PY311> <TARGET>/src/siku_mcp_server.py --help || echo "服务文件缺失/不可执行"
# ② 单发 tools/list（stdio 手工握手，看是否返回 7 工具）
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' | <PY311> <TARGET>/src/siku_mcp_server.py
# ③ 宿主注册段（Hermes：顶层 mcp_servers；键名/路径/解释器三查）
# ④ 用包内脚本定位（含协议/工具面/端点守卫全项）
<PY311> <PKG>/test_mcp.py --db <TARGET>/sample_memory.db
```
→ 常见根因：路径写错（指向 `<PKG>` 而非 `<TARGET>`）、解释器 <3.11、宿主未重载、注册段键名写错（Hermes 是顶层 `mcp_servers:`）。

**Q3 查询为空 / `total=0`**
→ 两种正常情形：①**空库**（首装样例库只有 5 行合成数据，且不含你的业务词）②过滤条件（`--type`/`--track`/`--industry`）过窄。
→ 先验证通路再谈数据：
```bash
<PY311> <TARGET>/src/l3_retrieval.py search --query "示例" --limit 5   # 期望 total=5
```

**Q4 `no such column: confidence` / `no such column: source_agent`**
→ 样例库是**旧版 10 列结构**（历史安装器生成），生产脚本按 41 列读写时报缺列。
→ 见下方「**样例库 schema 补丁**」小节：先判定版本，再走处理 A（推荐）或处理 B。

**Q5 写库被拒 / `permission denied`（权限拒绝）**
→ 写路径受 **WRITE_GATE** 管控（读全开、写受控），未带令牌或不在白名单即为拒绝——**属预期行为**。
→ 受控写法：设 `HERMES_WRITE_GATE=1` 并走 L2 暂存 → 管道晋升；`permissions.yaml` 未配置时写路径 fail-closed（只读检索不受影响）；接入步骤见 `README.md` 后置接入项 #1。

**Q6 依赖装不上（`ModuleNotFoundError: jieba / numpy`）**
→ 解释器选错（装到了系统 python3，却用 venv 解释器跑）或离线环境。
→ 补齐：
```bash
<PY311> -m pip install "jieba>=0.42.1" "numpy>=2.4.3"
```
→ 离线环境：在可联网机器 `pip download` 同名包后 `pip install --no-index --find-links <目录>`。

**Q7 安装日志出现 `[INFO] [conflict] ⚠️ 未找到 _conflict_lib.py（需在完整模块包中运行）——跳过冲突保护`**
→ 单包运行（父目录无完整模块安装包设施）。
→ 属正常，无需处理（见 ⑤ 10）。

**Q8 检索 stderr 出现 `[query_cache_put] 写失败 ... no such table: query_cache`**
→ 样例库未含 `query_cache` 表（缓存写失败仅提示，不影响检索结果，rc 仍为 0）。
→ 忽略或用生产库（生产库含该表）。

**Q9 `permissions.yaml` 读取警告（`搜索配置读取失败…使用默认值`）**
→ 权限文件属**后置接入项 #1**（各机自配），未配置时用默认值。
→ 需要写路径管控时按 `README.md` 后置接入项 #1 建 `permissions.yaml`。

**Q10 `chromadb` 导入失败（opentelemetry 版本冲突）**
→ 宿主已装 chromadb 但依赖版本冲突 → chroma 通道不可用。
→ 检索主链不受影响（自动降级）；需要 chroma 通道时修宿主依赖（`pip install -U "opentelemetry-api" "opentelemetry-sdk"`）或跳过（见 ① 1.2）。

**Q11 Inspector / npx 拉包失败（离线或企业网络）**
→ 官方 Inspector 首次需联网拉 npm 包。
→ 改用包内脚本（零依赖，等价覆盖协议/工具面/调用面）：`<PY311> test_mcp.py --db <TARGET>/sample_memory.db`。

**Q12 自检 `⚠️ 嵌入服务端口未通过 / 嵌入模型目录未通过`**
→ 生产依赖件未就位（样例模式允许）。
→ 生产部署：启动本地嵌入服务（默认端口 18790）并让 `model_dir` 指向真实模型；重跑自检。

**Q13 `[L3b] Hermes 版本 < 0.20.1 —— 升级`**
→ 宿主版本低于门禁。
→ 升级宿主后重跑；若 `hermes` 命令不可用，仅告警不阻断（可先装后升）。

**Q14 重复安装会不会破坏配置/数据？**
→ 不会：配置保护（真实值不覆盖）＋ 幂等（重复安装不覆盖既有样例库、不重置数据）。升级同理，且升级前自动备份（见 ⑤ 5）。

**Q15 R1 校验/拒答"误拦"了正常查询**
→ 校验规则由 `rules.json` 的 `obqc_mode` 决定；默认 `shadow`（只记录不拦截），`enforce` 才拒答。
→ 排查与调整见 附录 A·R1（改 `rules.json` 一行实时生效；改后复跑 golden 评测）。

### 样例库 schema 补丁

**背景（一句话）**：历史版本（≤ 2026-09-21）安装器生成的 `sample_memory.db` 只有 10 列简表；生产脚本（`expand_entry` / `entity_archive` 等）按 41 列生产 schema 读写样例库时，会报 `no such column: confidence` / `no such column: source_agent` 之类错误。新版安装器已按生产 schema 生成（41 列 + 3 索引 + FTS5 `mem_fts` + 3 个同步触发器）。

**第一步：判定自己的样例库是否旧版**

```bash
# 输出 41 = 新版（无需处理）；输出 10 = 旧版（走处理 A 或 B）
python3 -c "import sqlite3;print(len(sqlite3.connect('<安装目录>/sample_memory.db').execute('PRAGMA table_info(memory_store)').fetchall()))"
```

**处理 A（推荐）：重生成**

样例库是**合成模板数据**（`smp_00001`~`smp_00005`，零真实数据），删除安全：

```bash
rm <安装目录>/sample_memory.db && python3 install.py --yes --target <安装目录>
```

**处理 B：保留存量行，原库补丁**

> 适用：你往样例库里加过自测数据、不想重置。补丁 = 35 个 `ALTER TABLE ADD COLUMN` + 3 索引 + FTS5 + 3 触发器 + FTS 回填；**已在沙箱对「10 列历史库 + 存量行」实测**（11 项断言全过，存量保留）。
> 同一补丁也产出为可单独执行的 `legacy_patch.sql`（`sqlite3 <安装目录>/sample_memory.db < legacy_patch.sql`）。

```sql
-- 旧版样例库 schema 补丁（E3-2026-09-21 生成；35 列 + 索引 + FTS5 + 触发器）
-- 已知差异：旧版独有 4 列保留；created_at/updated_at 为常量默认 ''；严格同构请重生成。

-- ① 补 35 列
ALTER TABLE memory_store ADD COLUMN version TEXT DEFAULT '1.0';
ALTER TABLE memory_store ADD COLUMN confidence REAL DEFAULT 1.0;
ALTER TABLE memory_store ADD COLUMN trust_score REAL DEFAULT 0.7;
ALTER TABLE memory_store ADD COLUMN importance INTEGER DEFAULT 5;
ALTER TABLE memory_store ADD COLUMN half_life TEXT DEFAULT 'permanent';
ALTER TABLE memory_store ADD COLUMN source_agent TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN embedding BLOB;
ALTER TABLE memory_store ADD COLUMN audit_log TEXT DEFAULT '[]';
ALTER TABLE memory_store ADD COLUMN merge_history TEXT DEFAULT '[]';
ALTER TABLE memory_store ADD COLUMN created_at TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN updated_at TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN train_eligible INTEGER DEFAULT 0;
ALTER TABLE memory_store ADD COLUMN data_type TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN train_batch_id TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN g2_labeled_by TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN g3_reviewed_by TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN g3_result TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN correction_count INTEGER DEFAULT 0;
ALTER TABLE memory_store ADD COLUMN corrected_at TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN summary_hash TEXT;
ALTER TABLE memory_store ADD COLUMN concern_id TEXT DEFAULT 'unclassified';
ALTER TABLE memory_store ADD COLUMN industry TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN source_ref TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN expires_at TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN deprecated INTEGER DEFAULT 0;
ALTER TABLE memory_store ADD COLUMN deprecated_at TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN quadrant TEXT;
ALTER TABLE memory_store ADD COLUMN quadrant_labeled_at TEXT;
ALTER TABLE memory_store ADD COLUMN merged_to TEXT DEFAULT '';
ALTER TABLE memory_store ADD COLUMN memory_track TEXT DEFAULT 'semantic' CHECK (memory_track IN ('episodic','semantic'));
ALTER TABLE memory_store ADD COLUMN "references" TEXT;
ALTER TABLE memory_store ADD COLUMN "entities" TEXT;
ALTER TABLE memory_store ADD COLUMN "relations" TEXT;
ALTER TABLE memory_store ADD COLUMN event_date TEXT;
ALTER TABLE memory_store ADD COLUMN conflict_type TEXT DEFAULT '';

-- ② 索引
CREATE INDEX IF NOT EXISTS idx_memory_store_updated_at ON memory_store(updated_at);
CREATE INDEX IF NOT EXISTS idx_memory_store_track ON memory_store(memory_track);
CREATE INDEX IF NOT EXISTS idx_memory_store_timestamp ON memory_store(timestamp);

-- ③ FTS5 + 同步触发器
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

-- ④ FTS 回填（全量重建）
DELETE FROM mem_fts;
INSERT INTO mem_fts(rowid, summary, content) SELECT rowid, summary, content FROM memory_store;
```

**已知差异（处理 B 相对全新生成）**

1. 旧版独有 4 列（`project` / `agent` / `keywords` / `embedding_dim`）无法用 `ALTER` 删除，保留不影响读写；
2. `created_at` / `updated_at` 为常量默认 `''`（全新生成为 `datetime('now')`）；
3. 列序不同（旧 10 列在前）。——**要求逐列严格同构请走处理 A**。

**验证（补丁后执行，应全部无报错）**

```bash
sqlite3 <安装目录>/sample_memory.db "SELECT id, version, confidence, source_agent, memory_track FROM memory_store LIMIT 1;"
sqlite3 <安装目录>/sample_memory.db "SELECT count(*) FROM mem_fts;"   # = memory_store 行数
```

---

## ⑦ 卸载

> 本包不随附独立卸载器；按下列步骤手工卸载（安装清单 `<TARGET>/config/.manifest.json` 记录逐件指纹，可据此核对）。

**口径 A｜保留数据（推荐·只卸程序）**

```bash
# ① 卸载技能（仅移除本包三件，自动保留 .uninstalled-bak-* 备份）
cd <PKG> && python3 skills/install_skills.py --uninstall

# ② 移除宿主 MCP 注册段（删除 siku-4k 与 a1-siku-core 两个条目后再重载）
#    Hermes: <HERMES_HOME>/config.yaml → mcp_servers；Claude Code: .mcp.json；OpenClaw: ~/.openclaw/openclaw.json

# ③ 保留数据，仅删程序件（数据文件与自建目录按需保留）
#    程序件 = <TARGET>/src/ 、install.py、update.py、mcp_server.py、MCP_CONFIG.md、
#            README.md、INSTALL.md、manifest.yaml、config.example、samples/
#    保留件 = 你的记忆库（*.db）、chroma/、backups/、config.ini
```

**口径 B｜全清（连数据一起删）**

```bash
# ① 先备份（务必：库文件先复制到安全位置）
# ② 卸载技能 + 移除 MCP 注册（同口径 A ①②）
# ③ 删除安装目录
rm -rf <TARGET>
```

> 说明：删除前建议 `python3 <TARGET>/update.py --list-backups` 确认备份位置；生产库（若 `SIKU_DB_PATH` 指向别处）不在 `<TARGET>` 内，需单独处置。

---

## 附录 A｜核心机制运维（R1 校验/拒答 · GRPO 管道（件已移出）· R12 备份轮转）

### R1 校验与拒答（检索层·OBQC 确定性规则）

- **机制**：返回前用确定性规则检测「低相关 / 低证据 / 来源不明 / 低置信度」，把"可能错"变成"可证明错 / 可拒绝回答"。模式由 `rules.json` 的 `obqc_mode` 字段决定：
  - `shadow`（**默认**）：全量校验只写 `obqc_shadow.log`（查询 / 判定 / 若拦截会怎样），**返回零改动**；
  - `enforce`：相关性不达标（best < 0.01）或证据分不足（best evidence < 0.10）→ **拒答**（`results=[]` + note + obqc 字段）；来源不明 / 低置信度 → 标记。
- **规则文件**：`<TARGET>/scripts/siku_option/rules.json`（可用 `SIKU_OBQC_RULES` 覆盖路径、`SIKU_OBQC_LOG` 覆盖日志）；**改一行实时生效**（mtime 自动重载）。
- **切换纪律**：切 `enforce` 前跑统计报告（误杀率 <5% 且命中率 >80% 方可切），切后双跑 1 周。
- **置信度显性化（另一开关）**：`SIKU_GATE=on` 时返回 JSON 增加 `result_quality`（high/medium/low/weak）、条目级 `weak_match`、`fallback` 标注；默认 `off` = 纯透传（行为与现状逐位一致）。
- **消费端语义**：`weak_match=true` / `fallback` 条目**不得作为回答依据**；`result_quality=weak` 时优先拒答（"低置信绝不硬造"）。
- **历史口径**：v1.0.2 文档记载的"阈值 0.040 + `SIKU_REFUSAL` 开关"为旧版单阈值实现，现由 OBQC 规则化阈值（0.01 / 0.10）与 `SIKU_GATE` 标注取代。
- 调参后务必复跑 golden 评测：`<TARGET>/src/eval_retrieval.py`。

### GRPO 管道（grpo_full_pipeline.py —— **本包未含该件**）

> 状态说明：GRPO/MLX 属训练链、非四库运行时功能（封装时已移出 `src/`）；本节保留为机制说明。如需启用，请联系维护方（建议以独立模块另行交付）。

- **链路**：`L4b 数据 → Rollout(apply_chat_template + generate) → Reward（基于生成文本）→ Advantage → LoRA`，多轮迭代。
- **用法（历史口径）**：`python3 src/grpo_full_pipeline.py --rounds 3 --batch-size 8 --min-importance 3 --lora-steps 50 --max-tokens 128 --seed 42`
- **10 样本冒烟实践**：`--batch-size 10 --rounds 1`（Rollout→Reward→Advantage→LoRA 四步全走一遍），确认管道与环境就绪后再上正式批次。
- 日志 `grpo_full.log`（1MB×5 轮转）；产出 `generated/train.jsonl` + `adapters/`；模型 `<BASE_MODEL>`（本地 MLX 模型，训练侧另配）。

### R12 备份轮转（保留 3 份 + db 归位）

- **保留 3 份**：备份产物只留最近 3 份——`ls -1t 重要程序备份-*.tar.gz | tail -n +4 | rm -f`（实证：三份轮转）。
- **db 归位**：备份产物（db 快照 / tar.gz）一律归位到 `<SIKU_ROOT>/backup/`（或 `weekly/`），**禁止散落根目录**——根目录残留 `memory_store_pre_*_*.db-shm/wal` 即反例。
- **生命周期**：`.bak-*` 只留最近 N 份；`backup/` 旧库 tar 归档压缩；中间产物随轮转清理（111G→51G 瘦身实践）。

## 附录 B｜生产接入（环境变量覆盖口 + 脚本落位）

**要点：包内脚本已去个性化（零真实绝对路径），生产接入无需替换占位符**——路径一律走环境变量覆盖口（默认值 = 安装目录/宿主家目录）。

1. **填配置**：`<TARGET>/config.ini` 填真实库文件、向量库目录、嵌入模型目录与端口（已有真实值的配置受"配置保护"，重装/升级不覆盖）。
2. **脚本落位**：把 `<TARGET>/src/` 下需要的脚本**直接复制**到生产 `scripts/`（无占位符需替换）：

```bash
cd <TARGET>/src
cp l3_retrieval.py entity_archive.py siku_mcp_server.py "$HOME/四库全书/scripts/"
# 其余按需：维护/组织链脚本（check_permission / weekly_backup / rebuild_fts_jieba / reta_pipeline …）
```

3. **重跑自检**：`<PY311> <PKG>/install.py --yes --target <TARGET>`
4. **关键覆盖口**（详见 ④ 4.5）：`SIKU_ROOT`（数据根）/ `SIKU_DB_PATH`（库）/ `SIKU_CONCEPTS_DIR`（概念集）/ `SIKU_VENV_PYTHON`（≥3.11 解释器）/ `SIKU_EMBED_DIR`（嵌入模块目录）/ `SIKU_OBQC_RULES`（校验规则）/ `LLM_ENDPOINT`（本地 LLM 端点）。

> 历史口径说明：v1.0.2 及以前包内含 `{{SIKU_ROOT}}` 等占位符、需 `sed` 替换后落位；当前版本已改为环境变量覆盖（默认值见上），脚本可直接复制使用。

## 附录 C｜包内容清单与版本

**顶层件（发行内容 · 见 `manifest.yaml` 的 `files`）**：`INSTALL.md` / `MCP_CONFIG.md` / `README.md` / `config.example` / `install.py` / `manifest.yaml` / `mcp_server.py` / `samples/` / `skills/` / `src/` / `test_mcp.py` / `update.py`

**规模与版本**：

| 项 | 值 |
|:---|:---|
| 包版本 | 1.2.0（`manifest.yaml` → `version`） |
| 规模 | 顶层 12 项；`src/` 52 件（50 脚本 + 2 随件数据）；`skills/` 5 件；`samples/` 1 件；解压后 ≈ 1.4 MB |
| 样例库 schema | 41 列 + 3 索引 + FTS5 `mem_fts` + 3 同步触发器（与生产逐列对齐） |
| 版本变更 | 见 `manifest.yaml` → `changelog` |

> 计数口径：上表为**发行件**计数（构建期备份 `*.bak-*` 不计入，见 ⑤ 11）；`src/` 镜像到 `<TARGET>` 时整目录复制。

**环境版本要求（速查）**：

| 软件 | 最低要求 | 检测 |
|:-----|:---------|:-----|
| Python | ≥ 3.11 | 安装前自动——不满足拒装（exit 2） |
| Hermes Agent | ≥ 0.20.1 | 安装前自动——低于拒装+升级指引 |
| 依赖库 | jieba ≥0.42.1 / numpy ≥2.4.3 | 安装前自动——缺装拒装+安装指引 |

> 对不齐不要硬装（铁律）：版本不满足 → 拒装（exit 2）+ 补齐指引 → 对齐后再装。
