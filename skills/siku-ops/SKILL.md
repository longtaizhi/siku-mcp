---
name: siku-ops
description: Use when 四库系统运维——巡检/健康检查, 备份与快照恢复, 管道(晋升)排查, 库维护(衰减分级/过期标记/物理修复). 管理员向技能(可选装).
author: a1-siku-core
version: "1.0.0"
schema_version: "1.0"
updated: 2026-09-21
keywords: [四库, 运维, 巡检, 健康检查, 备份, 快照, 回滚, 管道, 衰减, 维护, 管理员]
tags: [siku, ops, maintenance, backup]
agent: a1-siku-core
type: workflow
---

# siku-ops — 四库运维（巡检·备份·维护）

> 触发词：四库巡检 / 四库健康检查 / 四库备份 / 快照回滚 / 管道排查 / 库维护
> 定位：管理员向（可选装）。日常使用只需 `siku-query` / `siku-write`。
> 纪律：**先只读巡检、后动写**；破坏性操作先备份/先在副本上预演。

## 一、例行巡检（约 5 分钟·只读为主）

```bash
python3 $SIKU_ROOT/src/l3_retrieval.py search --query "示例" --limit 1   # ① 检索冒烟：有返回即通
python3 $SIKU_ROOT/src/check_permission.py --list                        # ② 权限表：级别/读写口径
cat $SIKU_ROOT/logs/reta_status.json                                     # ③ 管道状态：last_success / consecutive_fails
python3 $SIKU_ROOT/src/missmon_check.py                                  # ④ 未命中率：静默=正常（≤20%）
ls -lt $SIKU_ROOT/logs/                                                  # ⑤ 日志总览（见 §五 日志表）
```

解读：

- ③ `consecutive_fails ≥ 2` → 查 `logs/reta_alerts.log` + `logs/reta_pipeline.log` 定位再处置
- ④ 未命中率 >20% 会输出提示（提示入库方向，不自动写库）
- 可选全量自检：`sentinel.py`（写/搜/权限/审计/管道/清理 6 项）——**注意**：其写入项会向 L3 插入 1 条 `sentinel-test-*` 测试行，仅在独立/沙盒库上跑，勿在生产库例行跑

## 二、备份与恢复

```bash
python3 $SIKU_ROOT/src/weekly_backup.py                 # 全量备份（周日执行；非周日 SKIP 属正常）→ weekly/siku-full-YYYY-MM-DD.tar.gz（保留最近 4 周）
python3 $SIKU_ROOT/src/snapshot.py create               # 库快照（smart/snapshots/）
python3 $SIKU_ROOT/src/snapshot.py list                 # 快照列表（条目数/大小/校验）
python3 $SIKU_ROOT/src/snapshot.py rollback <snap-id>   # 回滚（先备份现状再回滚）
```

- 备份产物核验：文件存在 + 大小 >0 + `tar -tzf` 可列出内容
- 回滚后必须复检：检索冒烟（§一①）+ 条目数对照

## 三、库维护（先预演后实跑）

| 动作 | 命令 | 说明 |
|:-----|:-----|:-----|
| 衰减分级预演 | `python3 $SIKU_ROOT/src/decay_stale.py --grade --dry-run` | 按类型回填 half_life（幂等）；预演确认后去掉 `--dry-run` |
| 半衰期衰减 | `python3 $SIKU_ROOT/src/decay_stale.py` | 只降 confidence、只标记，不删除 |
| 过期标记 | `python3 $SIKU_ROOT/src/cleanup_expired.py` | expires_at 过期 → 标记 deprecated（保留审计追溯，不删除） |
| 检索质量监控 | `rrf_golden_monitor.py` / `rrf_monthly_check.py` | golden 集命中与月度调参检查 |
| 物理修复/瘦身 | `vacuum_into.py --src <源库> --dst <新库>` | 只写新文件不碰源库；**切换上位须 `SIKU_WRITE_GATE=1` 令牌 + 备份 + 完整性复检** |
| 审计仪表盘 | `python3 $SIKU_ROOT/src/report_dashboard.py` | 生成 `reports/latest.html`（写入/注入事件统计） |
| 统一调度（可选） | `python3 $SIKU_ROOT/src/pipeline_runner.py scheduler` | 一条调度管全部定时任务（配 cron 时用） |

## 四、检查点（每轮运维收尾）

- [ ] 动写前记录基线（mtime / md5 / 条目数），动写后对照复核
- [ ] 备份/快照产物已核验（存在 + 大小 + 可读）
- [ ] 破坏性动作（回滚 / vacuum 上位）前有备份、后有冒烟复检
- [ ] 全程操作有日志（`logs/`）可追溯；被机制拒绝的操作不绕行（写闸=正常机制）

## 五、日志与状态文件速查

| 文件 | 看什么 |
|:-----|:-------|
| `logs/reta_status.json` | 管道最近一次成功时间 / 连续失败数 |
| `logs/reta_pipeline.log` | 晋升逐条明细（含类型拒收等 WARN） |
| `logs/reta_alerts.log` | 管道连续失败告警 |
| `audit/write/` | 写入审计（谁/何时/写了什么） |
| `reports/latest.html` | 审计仪表盘（事件统计） |

## 六、失败模式与处置

| 现象 | 原因 | 处置 |
|:-----|:-----|:-----|
| `weekly_backup` 打印 SKIP 即退出 | 非周日（设计如此） | 需要立即备份 → 用 `snapshot.py create` |
| `missmon_check` 无输出 rc=0 | 未命中率正常（静默设计） | 无需处置；有输出时按提示跟进 |
| 检索报错、向量通道不可用 | 嵌入服务/模型未就绪 | 先降级 `--mode fts5` 保可用；恢复服务后复检 |
| 管道连续失败 | 前置件缺失/schema 不齐/permissions 目录 | 按 INSTALL FAQ 逐项排查；修复后重跑 `--stage l2-l3` |
| 巡检命令报 permissions.yaml 缺失 | 前置件未就位 | 按 INSTALL 补齐后重试 |
| 快照回滚后检索异常 | DB 完整性/版本不匹配 | `PRAGMA integrity_check` + 版本对照，必要时回退到备份 |

## 七、边界声明

- 运维动作**不改变数据语义**：只降权/标记/备份，不做物理删除；删除类属管理员审批通道
- 高敏写带令牌（`SIKU_WRITE_GATE=1`），与主系统写闸一致；被拦不绕行
- 生产库上不实验；实验一律在库副本（`cp` 复制后指向 `SIKU_ROOT`/`SIKU_DB_PATH` 环境变量）进行

## 八、版本历史

| 版本 | 日期 | 说明 |
|:-----|:-----|:-----|
| 1.0.0 | 2026-09-21 | 初始版本：随 a1-siku-core 模块交付（E5·技能三件之一） |
