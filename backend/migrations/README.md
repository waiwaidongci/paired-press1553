# Migrations

- **v1** 初始 schema 由 SQLAlchemy 模型定义，启动时经 `Base.metadata.create_all` 创建。本地小应用不执行破坏性迁移；修改 schema 前备份命名卷 `db-data`。
- **v2** `002_reliable_execution.sql`：运行可靠执行（幂等受理键、认领围栏 `claim_seq`、租约 `lease_until`、完成时间）。全部为 `ADD COLUMN IF NOT EXISTS` / `CREATE UNIQUE INDEX IF NOT EXISTS`，可重复执行；backend 与 worker 启动时经 `src.config.database.migrate()` 自动应用（仅 PostgreSQL）。历史行语义不变：`request_key` 为 NULL 表示无幂等键的旧记录；`claim_seq=0` 且租约为 NULL 的 queued/running 行可被重新认领接续，已完成行不受影响。
