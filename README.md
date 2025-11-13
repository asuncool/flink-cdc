# SQL Server -> PostgreSQL 增量同步（Spring Boot + Flink CDC 3.5.0）

本工程使用 Spring Boot 启动嵌入式 Flink CDC 作业，从 SQL Server 指定表/字段，自动建表并增量同步到 PostgreSQL，支持 insert/update/delete。

## 功能
- 多表同步：在 `application.yml` 的 `cdc.tables` 中配置
- 指定字段同步：Debezium `column.include.list` + Sink 端字段投影双保险
- 目标端自动建表：启动前读取源端元数据，生成 PostgreSQL DDL（含主键）
- 初始快照 + 增量：首次全量，随后持续增量
- DML 同步：INSERT/UPDATE -> Upsert，DELETE -> 删除（依赖主键）

## 快速开始
1. 准备 SQL Server 与 PostgreSQL，并在 `src/main/resources/application.yml` 中填写连接信息与表配置。
2. 构建与启动：
   ```bash
   mvn clean package -DskipTests
   java -jar target/sqlserver-to-postgres-cdc-1.0.0.jar
   ```
3. 首次运行会在目标库自动创建缺失的表；之后源端变更会同步至目标端。

## 重要说明
- 删除/Upsert 依赖主键：若源端无主键，请在 `cdc.tables[].primaryKeys` 显式配置，且该主键字段需包含在 `columns` 白名单中。
- 类型映射可在 `PgDdlCreator` 中扩展，或在 `columns[].typeOverride` 指定目标类型。
- 本示例为嵌入式运行，生产建议独立打包为 Flink 作业部署到集群，并配置外部化 checkpoint。
