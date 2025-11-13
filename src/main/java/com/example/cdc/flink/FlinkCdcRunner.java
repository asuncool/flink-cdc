package com.example.cdc.flink;

import com.example.cdc.config.CdcProperties;
import com.example.cdc.util.JdbcUtils;
import com.example.cdc.util.PgDdlCreator;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class FlinkCdcRunner implements CommandLineRunner {

  private final CdcProperties props;

  @Override
  public void run(String... args) throws Exception {
    var tables = props.getTables();
    if (tables == null || tables.isEmpty()) {
      log.warn("No tables configured under cdc.tables, skip starting CDC job.");
      return;
    }

    // 构建源端表清单（schema.table）
    List<String> tableList = new ArrayList<>();
    for (var t : tables) {
      String schema = Optional.ofNullable(t.getSourceSchema()).orElse(props.getSource().getSchema());
      String table = Objects.requireNonNull(t.getSourceTable(), "sourceTable is required");
      tableList.add(String.format("%s.%s", schema, table));
    }

    // Debezium column.include.list
    String columnIncludeList = buildColumnIncludeList();

    // 启动前在目标端自动建表
    try (Connection srcConn = JdbcUtils.openMssql(
             props.getSource().getHostname(),
             props.getSource().getPort(),
             props.getSource().getDatabase(),
             props.getSource().getUsername(),
             props.getSource().getPassword());
         Connection pgConn = JdbcUtils.openPostgres(
             props.getSink().getHostname(),
             props.getSink().getPort(),
             props.getSink().getDatabase(),
             props.getSink().getUsername(),
             props.getSink().getPassword())) {

      for (var t : tables) {
        String srcDb = Optional.ofNullable(t.getSourceDatabase()).orElse(props.getSource().getDatabase());
        String srcSchema = Optional.ofNullable(t.getSourceSchema()).orElse(props.getSource().getSchema());
        String srcTable = t.getSourceTable();

        String tgtSchema = Optional.ofNullable(t.getTargetSchema()).orElse(props.getSink().getSchema());
        String tgtTable = Optional.ofNullable(t.getTargetTable()).orElse(srcTable);

        PgDdlCreator.createTargetTableIfAbsent(
            srcConn, pgConn,
            srcDb, srcSchema, srcTable,
            tgtSchema, tgtTable,
            t.getColumns(), t.getPrimaryKeys()
        );
      }
      log.info("Target tables ensured.");
    }

    // Flink 环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(props.getCheckpoint().getIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointStorage(props.getCheckpoint().getStorage());
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(props.getCheckpoint().getRetention());

    Properties debeziumProps = new Properties();
    if (props.getSource().getDebezium() != null) {
      debeziumProps.putAll(props.getSource().getDebezium());
    }
    if (!columnIncludeList.isBlank()) {
      debeziumProps.setProperty("column.include.list", columnIncludeList);
    }

    SqlServerSource<String> source = SqlServerSource.<String>builder()
        .hostname(props.getSource().getHostname())
        .port(props.getSource().getPort())
        .database(props.getSource().getDatabase())
        .tableList(tableList.toArray(new String[0])) // "schema.table"
        .username(props.getSource().getUsername())
        .password(props.getSource().getPassword())
        .deserializer(new JsonDebeziumDeserializationSchema())
        .debeziumProperties(debeziumProps)
        .includeSchemaChanges(false)
        .startupOptions(com.ververica.cdc.connectors.base.options.StartupOptions.initial())
        .build();

    DataStreamSource<String> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "sqlserver-cdc");

    // 写入 PostgreSQL（Upsert/Delete）
    stream.addSink(new CdcJsonToPgSink(props));

    env.execute("SQLServer -> PostgreSQL CDC Sync");
  }

  private String buildColumnIncludeList() {
    if (props.getTables() == null) return "";
    List<String> cols = new ArrayList<>();
    for (var t : props.getTables()) {
      if (t.getColumns() == null || t.getColumns().isEmpty()) continue;
      String schema = Optional.ofNullable(t.getSourceSchema()).orElse(props.getSource().getSchema());
      String table = t.getSourceTable();
      cols.addAll(
          t.getColumns().stream()
              .map(c -> String.format("%s.%s.%s", schema, table, c.getName()))
              .collect(Collectors.toList())
      );
    }
    return String.join(",", cols);
  }
}
