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

  private Source source;
  private Sink sink;
  private List<TableSyncConfig> tables;
  private Checkpoint checkpoint = new Checkpoint();

  @Data
  public static class Source {
    private String hostname;
    private int port = 1433;
    private String database; // SQL Server database name
    private String username;
    private String password;
    private String schema = "dbo";
    private Map<String, String> debezium; // extra debezium props
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
    // 源端
    private String sourceDatabase; // 默认 source.database
    private String sourceSchema;   // 默认 source.schema
    private String sourceTable;    // 必填
    // 目标端
    private String targetSchema;   // 默认 sink.schema
    private String targetTable;    // 默认同 sourceTable
    // 指定同步字段（白名单）；留空=全量字段
    // name: 源字段名, alias: 目标字段名（可选）
    private List<ColumnItem> columns;
    // 主键（强烈建议填写）
    private List<String> primaryKeys;
  }

  @Data
  public static class ColumnItem {
    private String name;   // source column
    private String alias;  // target column (optional)
    private String typeOverride; // 覆盖目标字段类型（可选）
  }

  @Data
  public static class Checkpoint {
    private long intervalMs = 60000;
    private String storage = "file:///tmp/flink-checkpoints";
    private int retention = 2;
  }
}
