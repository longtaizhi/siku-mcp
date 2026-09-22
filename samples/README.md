---
agent: validator
type: module-doc
schema_version: "1.0"
updated: 2026-09-21
---
# samples · a1-siku-core 样例说明

- **样例库**：安装时由 `install.py` 生成 `sample_memory.db`（主表 `memory_store`），
  用于检索冒烟与自检——**小库模板，非真实库，绝不复制生产大库**。
- **表结构（2026-09-21 起与生产逐列对齐）**：`memory_store` 全量 41 列（含 `source_agent`/`confidence`/`industry`/`source_ref`/`expires_at`/`memory_track` 等；列定义见 `install.py` 的 `SAMPLE_DB_SCHEMA`），
  并含与生产同构的 3 个索引、FTS5 检索表 `mem_fts`（tokenize=unicode61）与 3 个同步触发器。
- **样例数据**：5 行**合成**模板行（`smp_00001`~`smp_00005`，`source_agent=siku-core-sample`）——零真实数据。
- **旧版升级**：历史 10 列简表按 INSTALL.md「样例库 schema 补丁」升级，或删除 `sample_memory.db` 后重跑安装。
- **样例用途**：验证安装链路 + 检索通道可运行；生产接入后以真实库为准。
- **重新生成**：删除目标目录内 `sample_memory.db` 后重跑 `python3 install.py --yes --target <安装目录>`。
