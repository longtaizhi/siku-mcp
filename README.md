---
agent: validator
type: module-doc
schema_version: "1.0"
updated: 2026-09-22
---

# a1-siku-core · 知识库核心

> ⚠️ **平台支持：macOS（本版本为 Mac 版）** —— Windows 适配待后续版本（启动器/路径需改造+实测）。
> 版本：1.2.0（单源：`manifest.yaml` 的 `version` 字段；变更记录见该文件 `changelog` 段）｜README 四件套：①模块概述 ②安装与配置 ③使用与验证 ④升级与维护
> v2 更新（2026-08-23）：四库核心当前版入包——l3_retrieval R1 拒答版 + GRPO 管道 + R12 备份轮转规范（详见「核心机制」节）
> v2.0 批次更新（2026-09-21~22·E1-E6）：src 全量入包（50 件 + 2 随件数据·去个性化走环境变量）｜样例库 schema 修正（41 列同构 + FTS5）｜MCP 验证链（`test_mcp.py` + `MCP_CONFIG.md` 三端模板）｜技能三件随包（`skills/`）｜INSTALL.md 七节全文重写（环境/步骤/验证/接入/注意事项/FAQ/卸载）

## ① 模块概述

**是什么**：知识域数据底座核心——知识检索与加工管道。数据服务类模块（DB + 向量库 + 本地嵌入模型）。

**资产清单**（来源：内部资产清单，只读收集；包内路径为本模块；生产路径用环境变量覆盖口（`<SIKU_ROOT>` 等，默认值 = 安装目录/宿主家目录，见「环境变量覆盖口」节））：

| 组件 | 包内文件 | 职责 |
|:-----|:---------|:-----|
| L3 检索（R1 拒答版） | `src/l3_retrieval.py` | 双通道（FTS5 + 向量）+ RRF 融合 + 渐进式披露 + **校验/拒答出口**（OBQC 规则：`shadow` 默认 / `enforce` 拒答） |
| GRPO 后训练管道 | **（已移出本包·训练链不入包）** | L4b → Rollout → Reward → Advantage → LoRA 完整 RL 后训练链（本包不含该件；如需启用以独立模块另行交付） |
| MCP 服务 | `src/siku_mcp_server.py` | 四库 MCP 工具面（搜索/展开/实体·7 工具主入口） |
| MCP 验证链 | `test_mcp.py`、`MCP_CONFIG.md` | 7 工具协议/调用断言（16 项检查：样例模式 PASS=14/SKIP=1·含端点守卫与拒答臂）＋ 三端接入模板（Hermes/Claude Code/OpenClaw） |
| 技能包 | `skills/`（3 技能 + 安装器 + README） | 面向使用者 Agent："查四库 / 写四库 / 运维四库"标准姿势（装完即会用） |
| 实体档案 | `src/entity_archive.py` | 实体抽取/档案构建 |
| 蒸馏管道 | `src/l4_smart.py`、`src/l4c_sync.py` | L4 系列蒸馏/增强（**已封装入包**；l4b→SFT 属训练链，不入包） |
| 权限 | **后置接入项 #1**：`<SIKU_ROOT>/permissions.yaml`（各机自配） | S/A/D 三级 + 外部 Agent 管控（含接入步骤） |
| 评分 | `src/importance_scoring.py` | 重要性评分（**已封装入包**） |
| 记忆库 DB 模板 | 安装生成 `sample_memory.db` | 存储层主库（**安装时生成小样例，绝不复制真实大库**） |
| 配套 | `src/graph_query.py`、`src/pipeline_runner.py`（**已封装入包**）；`memory_store.chromadb`=**后置接入项 #2**（数据自建）、`graph_builder.py`=**后置接入项 #3** | 向量库目录、图谱构建/查询、内部调度管道 |

**为什么独立**：数据服务类，需 DB/模型/端口三件就位才可用——安装自检全覆盖。

## 后置接入项（装完补齐清单 · 含接入步骤）

> 原则：**能自包含的组件全部封装入包（src）；确不能自包含的逐件明示为「后置接入项」+ 接入步骤**（接入后功能完整）——零静默缺漏。

### 一、包内明示项（2 件）

| # | 项 | 为何后置 | 接入步骤 |
|:-:|:---|:---------|:---------|
| #1 | 权限 `<SIKU_ROOT>/permissions.yaml` | 权限模型按各机自配（S/A/D 三级 + `external_agents` + `rules`；包内 6 个脚本读取：`check_permission` / `l3_retrieval` / `router` / `sentinel` / `siku_asset_ingest` / `weekly_backup`） | ① 在 `<SIKU_ROOT>/` 建 `permissions.yaml`：配 `levels`（S=读写+审批 / A=全量只读 / D=默认只读）、`external_agents`、`rules`；② 验证：`python3 src/check_permission.py --list` 列出名册、`--agent <名> --action write` 返回判定；③ 未配置前：走权限检查的**写路径 fail-closed（拒绝）**，只读检索不受影响 |
| #2 | 向量库数据 `<SIKU_ROOT>/memory_store.chromadb/` | **数据类不入包**（真实库不得复制；零真实数据纪律——包内只给空库模板/样例） | ① 需要 CHROMA 向量通道时：宿主环境装配 `chromadb`（注意 opentelemetry 版本冲突）；② `python3 src/chroma_sync.py` 由主库同步生成；③ 或跳过——无 chromadb 时检索自动降级（词面/余弦通道），主链不受影响 |

### 二、生产侧后置脚本（4 件 · 需启用对应功能时接入）

> 本包不含这 4 件（依赖外部资源）；需启用时从维护方取得对应脚本，按步骤接入。

| # | 件 | 为何后置 | 接入步骤 |
|:-:|:---|:---------|:---------|
| #3 | `graph_builder.py` | 图通道建边；生产默认关（`SIKU_GRAPH_CHANNEL=0`；历史图边 95.27% 伪边） | 启用图通道时手动建一次：设 `SIKU_GRAPH_CHANNEL=1` 后执行 `python3 graph_builder.py`（按生产参数）；验证=图查询回包非空 |
| #4 | `l1_harvest_all.py` | 依赖 OpenClaw 环境（`l1_extractor.py` + 18 Agent 名册） | ① 装 OpenClaw 侧 `l1_extractor`；② 回填脚本内 L1 路径常量（或设 `OPENCLAW_HOME`）；③ 试跑一次核对名册条目数 |
| #5 | `siku_content_backfill.py` | 依赖本地 MLX 端点（8083）批量补 content | ① 部署本地 LLM 端点（8083 或自定）；② 设端点开关（`LLM_ENDPOINT`）；③ 小批量（limit 小值）试跑后全量 |
| #6 | `siku_graph_option_check.py` | 图通道期权定时检查（默认不装） | 启用图通道后再挂 hermes cron（周一期权检查）；未启用图通道前保持不装 |

### 三、宿主侧可选件（缺失自动降级 · 不影响主链）

| 模块 | 用途 | 缺失时行为 | 接入 |
|:-----|:-----|:-----------|:-----|
| `embed` | L3 向量检索通道 | fail-soft 降级（SQLite BLOB 余弦） | 放置于 `SIKU_EMBED_DIR`（默认 `<HERMES_HOME>/scripts/embedding`） |
| `verifier` | 评测连续分增强 | fail-soft 跳过（生产已退役） | 如需要，置于 `<HERMES_HOME>/scripts/` |
| `mlx_bge` | backfill_embeddings 批量补向量 | 该脚本不可用（主链不影响） | 宿主按需安装 |
| `chromadb` | chroma_sync / L3 chroma 通道 | 对应脚本/通道不可用 | 宿主环境装配（见 #2） |

## ② 安装与配置

详见 INSTALL.md（安装 / 配置 / 自检 / 升级 / 维护 / 回滚）。

## ③ 使用与验证

```bash
# 冒烟：样例库检索（安装目录内）
python3 src/l3_retrieval.py search --query "示例"
# 自检可重复执行
python3 install.py --yes --target <INSTALL_DIR>   # 幂等，重跑自检
# MCP 验证（7 工具协议/调用断言；显式传 --db 指向安装目录样例库）
python3 test_mcp.py --db <INSTALL_DIR>/sample_memory.db
```

**验证清单**：① `install.py` exit 0 ② 自检表硬性项全 ✅ ③ `config.ini` 占位符已填充为安装目录 ④ 样例库 5 行可查 ⑤ `test_mcp.py` 全过（FAIL=0）。
**技能包（Agent 自动加载）**：`python3 skills/install_skills.py` 一键安装三个技能（查/写/运维），走通示例与 FAQ 见 `skills/README.md`。

## 核心机制（R1 校验/拒答 / GRPO 管道（件已移出） / R12 备份轮转）

### R1 校验与拒答（l3_retrieval 检索层·OBQC 确定性规则）

- **机制**：返回前用确定性规则检测「低相关 / 低证据 / 来源不明 / 低置信度」，把"可能错"变成"可证明错 / 可拒绝回答"；模式由 `rules.json` 的 `obqc_mode` 字段决定：
  - `shadow`（**默认**）：全量校验只写 `obqc_shadow.log`（查询 / 判定 / 若拦截会怎样），**返回零改动**；
  - `enforce`：相关性不达标（best < 0.01）或证据分不足（best evidence < 0.10）→ **拒答**（`results=[]` + note + obqc 字段）；来源不明 / 低置信度 → 标记。
- **规则文件**：`<SIKU_ROOT>/scripts/siku_option/rules.json`（`SIKU_OBQC_RULES` / `SIKU_OBQC_LOG` 可覆盖路径与日志）；**改一行实时生效**（mtime 自动重载）。
- **置信度显性化（另一开关）**：`SIKU_GATE=on` 时返回 JSON 增加 `result_quality`（high/medium/low/weak）、条目级 `weak_match`、`fallback` 标注；默认 `off` = 纯透传（行为与现状逐位一致）。
- **历史口径**：v1.0.2 文档记载的"阈值 0.040 + `SIKU_REFUSAL` 开关"为旧版单阈值实现，现由 OBQC 规则化阈值（0.01 / 0.10）与 `SIKU_GATE` 标注取代；调参后复跑 golden 评测（`src/eval_retrieval.py`）。
- 消费端语义：`weak_match=true` / `fallback` 条目不得作为回答依据；`result_quality=weak` 时优先拒答。

### GRPO 管道（grpo_full_pipeline.py）

> **状态说明：本包未含该件**——GRPO/MLX 属训练链、非四库运行时功能（封装时已移出 src）；本节保留为机制说明。如需启用，请联系维护方（建议以独立模块另行交付）。

- **链路**：`L4b 数据 → Rollout(apply_chat_template + generate) → Reward(基于生成文本) → Advantage → LoRA`，多轮迭代。
- **用法**：`python3 src/grpo_full_pipeline.py --rounds 3 --batch-size 8 --min-importance 3 --lora-steps 50 --max-tokens 128 --seed 42`
- **10 样本冒烟实践**：小样本跑通全链验证——`--batch-size 10 --rounds 1`（Rollout→Reward→Advantage→LoRA 四步全走一遍，确认管道与环境就绪后再上正式批次）。
- 日志：`grpo_full.log`（1MB×5 轮转）；产出：`<GRPO_RUNS>/generated/train.jsonl` + `<GRPO_RUNS>/adapters/`（训练侧目录，另配）。
- 模型：`<BASE_MODEL>`（本地 MLX 模型，训练侧另配）。

### R12 备份轮转规范（111G→51G 瘦身实践）

- **保留 3 份**：备份产物只留最近 3 份——`ls -1t 重要程序备份-*.tar.gz | tail -n +4 | rm -f`（实证：07-31 / 07-23 / 06-25 三份轮转）。
- **db 归位**：备份产物（db 快照 / tar.gz）一律归位到 `<SIKU_ROOT>/backup/`（或 `weekly/`），**禁止散落根目录**——根目录残留 `memory_store_pre_*_*.db-shm/wal` 即反例。
- **生命周期**：`.bak-*` 只留最近 N 份；`backup/` 旧库 tar 归档压缩；中间产物（pre_audit/pre_graph 等）随轮转清理——从 111G 瘦身至 51G 的关键。

## ④ 升级与维护

详见 INSTALL.md（升级 / 维护 / 回滚）。

## 环境变量覆盖口（v2.0 零真实数据纪律）

包内脚本与文档**零真实绝对路径**；路径一律走环境变量覆盖口（默认值 = 安装目录 / 宿主家目录），生产接入**无需替换脚本**：

| 变量 | 含义 | 默认 |
|:-----|:-----|:-----|
| `SIKU_ROOT` | 四库数据根目录 | `~/siku-core`（= 安装目录） |
| `HERMES_HOME` | 宿主家目录 | `~/.hermes` |
| `SIKU_DB_PATH` | 记忆库路径 | `<SIKU_ROOT>/memory_store.db` |
| `SIKU_CONCEPTS_DIR` | 概念集目录 | `<HERMES_HOME>/memory-bank/concepts` |
| `SIKU_VENV_PYTHON` | ≥3.11 解释器路径 | 空（缺省走宿主 venv） |
| `SIKU_OBQC_RULES` / `SIKU_OBQC_LOG` | 校验规则文件 / 校验日志 | `<SIKU_ROOT>/scripts/siku_option/…` |
| `SIKU_EMBED_DIR` | 嵌入模块目录（向量通道） | `<HERMES_HOME>/scripts/embedding` |

> 全量覆盖口 100+ 个（自查：`grep -rn "os.environ.get" src/`）；历史口径（v1.0.2）：包内 `{{SIKU_ROOT}}` 等占位符需 `sed` 替换后落位——现版本已改为环境变量，脚本可直接复制到生产 `scripts/`（见 `INSTALL.md` 附录 B）。

## 环境版本要求

| 软件 | 最低要求 | 检测 |
|---|---|---|
| Python | ≥ 3.11 | L1 自动——不满足拒装（exit 2） |
| Hermes Agent | ≥ 0.20.1 | L3 自动——低于拒装+升级指引 |
| 依赖库 | jieba ≥0.42.1 / numpy ≥2.4.3 | L2 自动——缺装拒装+安装指引 |

> 对不齐不要硬装（铁律）：版本不满足 → 拒装（exit 2）+补齐指引 → 对齐后再装。

## 许可（双许可模式）

本项目采用**双许可（Dual License）**：

- **开源用途** —— 免费，以 **GNU AGPL-3.0** 授权（全文见同目录 `LICENSE`）；个人、学习、研究、非商业开源项目均可使用。
- **商业用途** —— **直接或间接商用（含公司闭源使用）须取得商业授权**，详见 `COMMERCIAL.md`。

> AGPL 概要：修改版通过网络向他人提供服务时，须公开对应的完整源码（第 13 条）。商用请先联系获取授权。
