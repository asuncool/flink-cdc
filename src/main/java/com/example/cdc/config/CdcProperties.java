package com.example.cdc.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "cdc")
public class CdcProperties {

  // 多源配置（推荐）
  private List<Source> sources;

  // 兼容单源旧配置：如只配置了 source，会在运行时包装成一个 sources[0]
  private Source source;

  // 目标端
  private Sink sink;

  // 表级同步配置（每条绑定一个 sourceId）
  private List<TableSyncConfig> tables;

  private Checkpoint checkpoint = new Checkpoint();

  @Data
  public static class Source {
    private String id;          // 必填：唯一标识该源，同时将作为 Debezium database.server.name
    private String hostname;
    private int port = 1433;
    private String database;    // SQL Server database name
    private String username;
    private String password;
    private String schema = "dbo";
    private Map<String, String> debezium; // 额外 Debezium 配置（可选）
  }

  @Data
  public static class Sink {
    private String hostname;
    private int port = 5432;
    private String database;
    private String username;
    private String password;
    private String schema = "public";
  }

  @Data
  public static class TableSyncConfig {
    // 绑定到哪个源
    private String sourceId;      // 必填：对应上面 Source.id

    // 源端（如不填，默认取该源的 database/schema）
    private String sourceDatabase;
    private String sourceSchema;
    private String sourceTable;   // 必填

    // 目标端（如不填，默认 sink.schema + 同名表）
    private String targetSchema;
    private String targetTable;

    // 指定同步字段（白名单）；留空=全量字段
    // name: 源字段名, alias: 目标字段名（可选）, typeOverride: 目标类型覆盖（可选）
    private List<ColumnItem> columns;

    // 主键（Upsert/Delete 需要）
    private List<String> primaryKeys;
  }

  @Data
  public static class ColumnItem {
    private String name;         // source column
    private String alias;        // target column (optional)
    private String typeOverride; // 覆盖目标字段类型（可选）
  }

  @Data
  public static class Checkpoint {
    private long intervalMs = 60000;
    private String storage = "file:///tmp/flink-checkpoints";
    private int retention = 2;
  }
}