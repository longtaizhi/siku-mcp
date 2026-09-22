---
name: siku-query
description: Use when 查询四库/知识库记忆——检索经验/教训/决策, 查找"之前是否记过", 调用 siku-query/siku MCP 搜索, 追问四库条目详情. 双通道检索+渐进式消费+R1 低置信拒答语义.
author: a1-siku-core
version: "1.0.0"
schema_version: "1.0"
updated: 2026-09-21
keywords: [四库, 检索, 记忆, 知识库, FTS5, 向量, RRF, 渐进式, R1拒答, MCP, search_memories]
tags: [siku, query, retrieval, mcp]
agent: a1-siku-core
type: workflow
---

# siku-query — 四库检索（怎么查·怎么消费结果）

> 触发词：查四库 / 搜记忆 / 知识库检索 / "之前记过什么" / 经验教训查找 / 让 Agent 检索四库
> 定位：面向所有使用者（只读）。写入走 `siku-write`，运维走 `siku-ops`。
> 前置：四库已安装。脚本在 `$SIKU_ROOT/src/`（默认 `~/siku-core`）；或已注册 MCP 工具（见安装产物 `MCP_CONFIG.md`）。

## 一、触发场景

- 回答前需要历史经验/教训/决策支撑（"我们之前怎么处理的"）
- 写代码/做决策前查同类先例、查规范文档（spec/rule/design）
- 用户明确要求"查一下四库/知识库/记忆"

## 二、工作流（先 MCP，后 CLI——同一实现，二选一）

**首选：MCP 工具**（目标系统已注册四库 MCP 时）：

1. `search_memories`：参数 `query`（必填）、`mode`（auto/fts5/embed/dual）、`top_k`（默认 5）、
   `track`（episodic/semantic）、`time_from`/`time_to`（ISO 或 epoch）
2. 拿到 `id` 后二跳 `expand_entry`（`entry_id`）取全文——**不要**只凭 compact 摘要下结论
3. 相关工具：`concept_query`（概念档案）、`ontology_explore`（枚举/本体）、`entity_knowledge`（"关于 X 已知什么"）

**备选：CLI**（未注册 MCP 或脚本化场景）：

```bash
# 检索（默认 compact 渐进式第一层）
python3 $SIKU_ROOT/src/l3_retrieval.py search --query "关键词" --limit 5
# 二跳展开（拿到 id 后）
python3 $SIKU_ROOT/src/l3_retrieval.py expand --id <entry_id> --tier expand
# 时间轴回溯
python3 $SIKU_ROOT/src/l3_retrieval.py timeline --start 2026-09-01 --end 2026-09-21 --limit 20
# 复核循环（R1：关键数据回传校验；需 SIKU_GATE=on）
python3 $SIKU_ROOT/src/l3_retrieval.py verify --query "含关键数据的问句" --limit 3
```

## 三、三条消费纪律（结果怎么用）

1. **渐进式消费**：compact（摘要+元数据）→ expand（正文）→ full（全字段）。
   摘要不足以支撑结论时，必须 expand 二跳。
2. **分数不是绝对阈值**：score 随通道标定不同（fts5 原分约 5-16；dual RRF 融合分约 0.01-0.05）。
   只做**同次结果内相对比较**，不设固定及格线。
3. **R1 拒答语义（低置信绝不硬造）**：返回 `weak_match=true` / `result_quality=weak` /
   `fallback`（如 time_desc 兜底）的条目**不得作为回答依据**；低置信时宁可说"四库没有相关记录"，
   也不要拿弱匹配结果硬凑。启用 `SIKU_GATE=on` 时结果带质量档字段（详见 INSTALL）。

**模式选择法**：默认 `auto`；确定要找精确词/ID → `fts5`；语义近义/换述 → `embed`；
两者都要 → `dual`（RRF 融合，生产默认强档）。

## 四、检查点（回答前自检）

- [ ] 结论引用的每条依据都有 `id`（可被 expand 复核），未展开全文的条目已标注"仅摘要"
- [ ] 弱匹配/兜底条目未被当作依据；命中为空时如实说明（空库 ≠ 无此知识，见 INSTALL FAQ）
- [ ] 未把检索结果当实时数据源（四库=历史记忆，时效性条目注意 `expires_at`/deprecated 标记）

## 五、失败模式与处置

| 现象 | 原因 | 处置 |
|:-----|:-----|:-----|
| 结果为 0 条 | 库为空 / 真未命中 / 被拒答 | 先分辨：空库→按 `siku-write` 入库；拒答→换 query 或如实说未命中；看 `note`/`obqc` 字段 |
| `search_memories` 报错/超时 | 向量通道依赖未就绪（嵌入服务/模型） | 降级 `--mode fts5` 照常可用；或检查嵌入服务（端口见 config.ini） |
| `no such column` / `no such table` | 库 schema 与脚本版本不齐 | 库文件与脚本须同版本配套；按 INSTALL FAQ 的 schema 指引处理 |
| 结果里 `📋关联教训` 之类标记 | 条目携带关联/历史标记 | 正常，按需 expand 看正文 |
| CLI 报解释器/依赖错误 | python3 < 3.11 或依赖缺失 | 用 python3.11+ 解释器跑；依赖见 README 环境要求 |

## 六、版本历史

| 版本 | 日期 | 说明 |
|:-----|:-----|:-----|
| 1.0.0 | 2026-09-21 | 初始版本：随 a1-siku-core 模块交付（E5·技能三件之一） |
