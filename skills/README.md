---
agent: maintainers
type: module-doc
schema_version: "1.0"
updated: 2026-09-21
---

# 技能包（skills/）— 四库 MCP 模块配套技能三件

> 随 `a1-siku-core` 模块交付（2026-09-21·v1.0.0）。目标：**面向用户的 Agent 装完即会用**——安装本包三个技能，
> Agent 即掌握"查四库 / 写四库 / 运维四库"的标准姿势；与模块 INSTALL/README 人工指引互为双保险。

## 一、包内清单

| 技能 | 使用者 | 触发（何时自动加载） | 一句话 |
|:-----|:-------|:---------------------|:-------|
| `siku-query` | 所有使用者（只读） | 查四库 / 搜记忆 / "之前记过什么" | 双通道检索 + 渐进式消费 + 低置信拒答语义 |
| `siku-write` | 需要写入者 | 写入四库 / 记录经验 / 保存教训 | L2 私有暂存受控写法 + 类型枚举 + WRITE_GATE 纪律 |
| `siku-ops` | 管理员（可选装） | 四库巡检 / 备份 / 快照 / 管道排查 | 巡检·备份·维护（先只读后动写） |

```
skills/
├── README.md            # 本文件（安装与走通指引）
├── install_skills.py    # 一键安装器（Hermes / OpenClaw 双支持）
├── siku-query/SKILL.md  # 技能 1（触发词+工作流+失败模式+检查点）
├── siku-write/SKILL.md  # 技能 2
└── siku-ops/SKILL.md    # 技能 3
```

## 二、安装（装完即会用）

**方式 A·一键脚本（推荐）**：

```bash
# Hermes 目标（默认自动探测 ~/.hermes/skills 或 $HERMES_HOME/skills）
python3 skills/install_skills.py
# OpenClaw 目标
python3 skills/install_skills.py --target ~/.openclaw/skills
# 演练/查看状态
python3 skills/install_skills.py --dry-run
python3 skills/install_skills.py --list
```

安装器行为：幂等（内容一致跳过）→ 同名旧技能先备份（`.bak-时间戳`）再覆盖 → 校验各 SKILL.md 的
`frontmatter name` 与目录名一致（名字不对齐会被技能装载排除）→ 打印汇总。

**方式 B·手动复制**（等价于方式 A）：

```bash
cp -R skills/siku-query skills/siku-write skills/siku-ops <目标 skills 目录>/
```

**验证安装**（3 步）：

1. `python3 skills/install_skills.py --list` → 三件均"已安装"
2. 目标 skills 目录存在 `siku-query/SKILL.md`、`siku-write/SKILL.md`、`siku-ops/SKILL.md`
3. 每个 SKILL.md 首部 frontmatter 的 `name` 与目录名一致（query/write/ops）

**卸载**：`python3 skills/install_skills.py --uninstall`（仅移除本包三件，自动保留 `.uninstalled-bak-*` 备份）

## 三、装后走通（按本 README 全流程·录输出）

> 前置：模块已安装（`install.py` exit 0），下文 `$SIKU_ROOT` 默认=安装目录（如 `~/siku-core`）。

```bash
# ① 装——技能三件进目标 skills 目录
python3 skills/install_skills.py --target ~/.hermes/skills

# ② 查（siku-query）——检索冒烟
python3 $SIKU_ROOT/src/l3_retrieval.py search --query "示例" --limit 3
#（注册了 MCP 的系统亦可：search_memories(query="示例") → expand_entry(entry_id=...)）

# ③ 写（siku-write）——L2 受控写入 → 管道晋升
mkdir -p $SIKU_ROOT/private/my-agent
cat > $SIKU_ROOT/private/my-agent/$(uuidgen).yaml <<'YAML'
id: demo-entry-0001
type: lesson
summary: 演示写入：第一条四库条目
confidence: 0.85
timestamp: 2026-09-21T00:00:00+00:00
source: my-agent
content: |
  按 siku-write 规范写入 L2，等待自动晋升 L3。
YAML
python3 $SIKU_ROOT/src/reta_pipeline.py --stage l2-l3     # 手动触发晋升（或等定时管道）

# ④ 运维一角（siku-ops）——巡检 + 快照
python3 $SIKU_ROOT/src/l3_retrieval.py search --query "演示写入" --limit 1   # 晋升后应能检索到
python3 $SIKU_ROOT/src/check_permission.py --list
python3 $SIKU_ROOT/src/snapshot.py create && python3 $SIKU_ROOT/src/snapshot.py list
```

每步的"成功的样子"：②③ 有 JSON/条目返回；③ 的 L2 文件随后被管道清理并在 L3 可检索；
④ 快照列表出现新快照（条目数/大小/校验）。

## 四、与 INSTALL/README 的双保险

- **自动侧（本包）**：技能装入 skills 目录后，Agent 在遇到触发词时自动加载对应 SKILL.md 执行标准姿势。
- **人工侧（模块文档）**：模块 `INSTALL.md` / `README.md` 给出同样的安装/使用/排障指引（人工照做即可）。
- 两侧引用关系：本 README 的安装命令 = INSTALL 的"技能包安装"章节；本 README 的 §三 走通 = INSTALL 的"安装后验证"延伸。
- 一致性维护：技能内容随模块版本走（见各 SKILL.md 版本历史），模块 changelog 同步。

## 五、FAQ

1. **装到哪个目录？** Agent 系统的 skills 目录：Hermes=`~/.hermes/skills`；OpenClaw=`~/.openclaw/skills`。安装器会自动探测，也可 `--target` 指定。
2. **和模块是什么关系？** 模块（`a1-siku-core`）=引擎（脚本/DB/MCP）；本包=面向 Agent 的"怎么用"。两者同包分发。
3. **Agent 没自动用技能？** 检查：① SKILL.md 是否在 skills 目录 ② `frontmatter name` 与目录名是否一致 ③ 技能是否被目标系统禁用列表排除。
4. **只想用不需要写？** 可以只装 `siku-query`（`siku-write`/`siku-ops` 按需）。
5. **技能里的命令跑不通？** 先确认模块安装完整、`$SIKU_ROOT` 指向安装目录、python3 ≥3.11；逐条命令的失败处置见各 SKILL.md「失败模式」表。
6. **更新技能？** 模块升级后重跑安装器（同名旧件自动备份 `.bak-*`）。

## 六、版本历史

| 版本 | 日期 | 说明 |
|:-----|:-----|:-----|
| 1.0.0 | 2026-09-21 | 初始版本：技能三件（query/write/ops）+ 安装器 + 本 README（随 a1-siku-core 模块交付） |
