package com.example.cdc.util;

import com.example.cdc.config.CdcProperties.ColumnItem;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 从 SQL Server 元数据读取表结构（按字段白名单），在 Postgres 自动建表（含主键）。
 */
@Slf4j
public class PgDdlCreator {

  public static void createTargetTableIfAbsent(
      Connection mssql,
      Connection pg,
      String srcDb,
      String srcSchema,
      String srcTable,
      String tgtSchema,
      String tgtTable,
      List<ColumnItem> columns,
      List<String> primaryKeysOverride
  ) throws SQLException {

    ensureSchema(pg, tgtSchema);

    if (existsTable(pg, tgtSchema, tgtTable)) {
      log.info("Target table {}.{} already exists", tgtSchema, tgtTable);
      return;
    }

    DatabaseMetaData meta = mssql.getMetaData();

    // 源表所有列的元数据
    Map<String, ColumnDef> allCols = new LinkedHashMap<>();
    try (ResultSet rs = meta.getColumns(srcDb, srcSchema, srcTable, "%")) {
      while (rs.next()) {
        String col = rs.getString("COLUMN_NAME");
        int dataType = rs.getInt("DATA_TYPE");
        String typeName = rs.getString("TYPE_NAME");
        int columnSize = rs.getInt("COLUMN_SIZE");
        int decimalDigits = rs.getInt("DECIMAL_DIGITS");
        int nullable = rs.getInt("NULLABLE");
        allCols.put(col, new ColumnDef(col, dataType, typeName, columnSize, decimalDigits, nullable == DatabaseMetaData.columnNullable));
      }
    }

    // 主键列（保持顺序）
    LinkedHashSet<String> pkCols = new LinkedHashSet<>();
    try (ResultSet rs = meta.getPrimaryKeys(srcDb, srcSchema, srcTable)) {
      Map<Short, String> seqToCol = new TreeMap<>();
      while (rs.next()) {
        String pkCol = rs.getString("COLUMN_NAME");
        short keySeq = rs.getShort("KEY_SEQ");
        seqToCol.put(keySeq, pkCol);
      }
      pkCols.addAll(seqToCol.values());
    }
    if (primaryKeysOverride != null && !primaryKeysOverride.isEmpty()) {
      pkCols.clear();
      pkCols.addAll(primaryKeysOverride);
    }

    // 选用列：优先字段白名单；否则全量列
    List<ColumnItem> selected = (columns == null || columns.isEmpty())
        ? allCols.keySet().stream().map(c -> {
            ColumnItem item = new ColumnItem();
            item.setName(c);
            item.setAlias(c);
            return item;
          }).toList()
        : columns;

    // 生成列定义
    List<String> columnDefs = new ArrayList<>();
    for (ColumnItem c : selected) {
      String srcName = c.getName();
      String tgtName = Optional.ofNullable(c.getAlias()).orElse(srcName);

      ColumnDef def = allCols.get(srcName);
      if (def == null) {
        throw new SQLException("Column " + srcName + " not found in source table " + srcSchema + "." + srcTable);
      }
      String pgType = (c.getTypeOverride() != null && !c.getTypeOverride().isBlank())
          ? c.getTypeOverride()
          : TypeMapping.mssqlToPg(def);
      String nullable = def.nullable ? "" : " NOT NULL";
      columnDefs.add(quoteIdent(tgtName) + " " + pgType + nullable);
    }

    // 主键子句
    String pkClause = "";
    if (!pkCols.isEmpty()) {
      String pk = pkCols.stream().map(PgDdlCreator::quoteIdent).collect(Collectors.joining(", "));
      pkClause = ", PRIMARY KEY (" + pk + ")";
    } else {
      log.warn("No primary key detected/specified for {}.{}, upsert/delete may not work.", tgtSchema, tgtTable);
    }

    // 整体 DDL
    String ddl = String.format(
        "CREATE TABLE %s.%s (\n  %s%s\n)",
        quoteIdent(tgtSchema), quoteIdent(tgtTable),
        String.join(",\n  ", columnDefs),
        pkClause
    );

    try (Statement st = pg.createStatement()) {
      st.execute(ddl);
      log.info("Created target table {}.{} with DDL:\n{}", tgtSchema, tgtTable, ddl);
    }
  }

  private static boolean existsTable(Connection pg, String schema, String table) throws SQLException {
    String sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        "