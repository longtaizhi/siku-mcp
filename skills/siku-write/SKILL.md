---
name: siku-write
description: Use when 往四库/知识库写入记忆——记录经验/教训/决策/资产, 保存要点, 入库新知识. L2 私有暂存区受控写法+类型枚举校验+WRITE_GATE 写闸纪律（与主系统一致）.
author: a1-siku-core
version: "1.0.0"
schema_version: "1.0"
updated: 2026-09-21
keywords: [四库, 写入, 入库, L2, 类型枚举, WRITE_GATE, 受控写, 晋升, 记忆]
tags: [siku, write, l2, gate]
agent: a1-siku-core
type: workflow
---

# siku-write — 四库写入（L2 受控写法）

> 触发词：写入四库 / 记录经验 / 保存教训 / 入库 / 记住这个 / 帮我把这条存下来
> 定位：需要写入四库的使用者（查询走 `siku-query`）。**L2=传送带，L3=仓库**：写入 L2 后由管道自动晋升，不直改 L3。

## 一、触发场景

- 任务收尾沉淀经验/教训/决策；用户说"记住/存到四库"
- 产出资产（spec/script/design…）登记入库

## 二、前置检查（一次）

- [ ] `$SIKU_ROOT/private/` 可写；`$SIKU_ROOT/permissions.yaml` 就位（权限分级）；`$SIKU_ROOT/audit/write/` 目录存在（写审计）
- [ ] 本机身份在 permissions.yaml 中有 write_access（S 级可写；A/D 级与外部 Agent 只读——被拒属正常，见 §五）

## 三、标准写入（唯一常规通道）

**路径**：`$SIKU_ROOT/private/{agent_id}/<uuid>.yaml`（不确定归属写 `common`）

```yaml
id: <uuid4 字符串>
type: <类型枚举·见下>          # 必填·非法拒收
summary: <一句话摘要·必填·≥10 字符>
confidence: 0.75               # 0-1；≥0.8 直接晋升 L3
timestamp: <ISO8601 UTC>
source: <来源标识·如 agent 名/脚本名>
content: |                     # 正文（可选·多行）
  <正文内容>
```

**类型枚举（25 种·权威源 `$SIKU_ROOT/src/siku_types.py`·非法拒收）**：

| 类别 | 枚举值 |
|:-----|:-------|
| 内容类（13） | lesson / fact / decision / result / insight / principle / info / instruction / correction / record / idea / estimate / rule |
| 资产类（13） | spec / skill / cron / workflow / rule / benchmark / asset / monitor / design / research / script / config / reference |

（`rule` 双重语义：内容=行为规则，资产=门禁规则）

**晋升条件（L2→L3，任一满足）**：① `confidence ≥ 0.8` ② 同 summary 重复 ≥2 次 ③ summary/content 含"记住"。
不满足则滞留 L2 等下一轮。晋升后 **L2 源文件被自动清理**（这是设计，不是丢失）。

**验证（可选·不等管道轮询时）**：

```bash
python3 $SIKU_ROOT/src/reta_pipeline.py --stage l2-l3     # 手动触发晋升
python3 $SIKU_ROOT/src/l3_retrieval.py search --query "<summary 关键词>" --limit 3   # 按 summary 核查
```

> 注意：晋升会为新条目分配**新的 entry_id**（L2 文件的 uuid 仅作暂存句柄）——核查请按 summary 关键词检索，
> 不要拿 L2 文件 id 去 L3 里找。晋升成功还会打印 `{"promoted": N, "skipped": M}`。

## 四、受控写（WRITE_GATE·与主系统一致）

1. **L3 直改＝禁止**。唯一通道＝L2 → `reta_pipeline` 自动晋升；任何"直接 INSERT memory_store"都是违规姿势。
2. **高危写需要令牌**：库物理操作/上位切换等由管理员执行，且带写闸令牌环境变量（`SIKU_WRITE_GATE=1`）——与主系统一致。
3. **跨库基因写入**（如装配基因库）走双因子写门：`HERMES_WRITE_GATE=1` + token（token 文件由本机 `write_gate_token` 管理）——未开双因子即被拒。
4. **合规删除 `hard_delete_memory`**：三重门槛（`--agent --why --yes` + WRITE_GATE），走审计。**当前版本为预留声明**——调用返回拒绝（不含真实删除）；合规删除需求请联系管理员。
5. **审计**：所有写操作留痕（`audit/write/`）；写前记录基线、写后可复核。

## 五、检查点（写完自检）

- [ ] 五要素齐全：id / type / summary(≥10字) / confidence / timestamp（source 建议填）
- [ ] type 语义对（内容 vs 资产；拿不准→ `fact`）
- [ ] summary 首字符若为 `@`/`-`/`?`/`:` 等特殊符号 → **加引号**（否则 YAML 解析失败）
- [ ] 不直改 L3；不为"省事"跳管道

## 六、失败模式与处置

| 现象 | 原因 | 处置 |
|:-----|:-----|:-----|
| 管道日志 `类型校验拒晋升` | type 不在 25 枚举内 | 对照 §三 枚举表改正后重写 |
| 文件写入成功但 `private/` 里没了 | 已晋升 L3（5 分钟内） | 查 L3 确认，不要重写；"记住"关键词语义=强制晋升 |
| 按 L2 文件 id 在 L3 查不到 | 晋升分配的是**新 entry_id** | 改为按 summary 关键词检索核查 |
| 解析失败/条目不识别 | YAML 标量以 `@` 等特殊字符开头 | summary 加引号重写 |
| 条目长期滞留 L2（未晋升） | confidence<0.8 且无重复 | 补证/提高置信度，或含"记住"强制晋升 |
| 权限拒绝（BLOCKED/写入受限） | 不在可写级别 / 缺写闸令牌 | 按 §四 走受控通道；被拒=机制正常，不绕行 |
| 管道报 permissions/audit 目录相关错误 | 前置件缺失 | 按 INSTALL 补齐 `permissions.yaml`/`audit/write/` 后重试 |

## 七、版本历史

| 版本 | 日期 | 说明 |
|:-----|:-----|:-----|
| 1.0.0 | 2026-09-21 | 初始版本：随 a1-siku-core 模块交付（E5·技能三件之一） |
