package com.example.cdc.sink;

import com.example.cdc.config.CdcProperties;
import lombok.Data;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class CdcJobConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  // sink (PostgreSQL)
  private String pgHost;
  private int pgPort;
  private String pgDatabase;
  private String pgUsername;
  private String pgPassword;
  private String pgSchema;

  // 每张表的同步配置（按源分组后展开）
  private List<TableConfig> tables = new ArrayList<>();

  @Data
  public static class TableConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String sourceId;       // Debezium database.server.name
    private String sourceSchema;
    private String sourceTable;
    private String targetSchema;
    private String targetTable;
    private List<ColumnMapping> columns;   // 按插入顺序
    private List<String> primaryKeys;
  }

  @Data
  public static class ColumnMapping implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;   // source column
    private String alias;  // target column
  }

  public static CdcJobConfig from(CdcProperties props) {
    CdcJobConfig cfg = new CdcJobConfig();
    cfg.pgHost = props.getSink().getHostname();
    cfg.pgPort = props.getSink().getPort();
    cfg.pgDatabase = props.getSink().getDatabase();
    cfg.pgUsername = props.getSink().getUsername();
    cfg.pgPassword = props.getSink().getPassword();
    cfg.pgSchema = props.getSink().getSchema();

    // 归一化 sources（兼容单源）
    List<CdcProperties.Source> sources;
    if (props.getSources() != null && !props.getSources().isEmpty()) {
      sources = props.getSources();
    } else if (props.getSource() != null) {
      CdcProperties.Source s = props.getSource();
      if (s.getId() == null || s.getId().isBlank()) s.setId("default");
      sources = List.of(s);
    } else {
      sources = List.of();
    }

    Map<String, CdcProperties.Source> sourceById =
        sources.stream().collect(Collectors.toMap(CdcProperties.Source::getId, s -> s));

    if (props.getTables() != null) {
      for (var t : props.getTables()) {
        CdcProperties.Source src = sourceById.get(t.getSourceId());
        if (src == null) {
          throw new IllegalArgumentException("tables[].sourceId not found in cdc.sources: " + t.getSourceId());
        }
        TableConfig tc = new TableConfig();
        tc.sourceId = src.getId();
        tc.sourceSchema = Optional.ofNullable(t.getSourceSchema()).orElse(src.getSchema());
        tc.sourceTable = Objects.requireNonNull(t.getSourceTable(), "tables[].sourceTable required");
        tc.targetSchema = Optional.ofNullable(t.getTargetSchema()).orElse(cfg.pgSchema);
        tc.targetTable = Optional.ofNullable(t.getTargetTable()).orElse(tc.sourceTable);
        tc.primaryKeys = Optional.ofNullable(t.getPrimaryKeys()).orElse(Collections.emptyList());

        List<ColumnMapping> cms = new ArrayList<>();
        if (t.getColumns() != null) {
          for (var c : t.getColumns()) {
            ColumnMapping m = new ColumnMapping();
            m.setName(c.getName());
            m.setAlias(Optional.ofNullable(c.getAlias()).orElse(c.getName()));
            cms.add(m);
          }
        }
        tc.columns = cms;
        cfg.tables.add(tc);
      }
    }
    return cfg;
  }
}