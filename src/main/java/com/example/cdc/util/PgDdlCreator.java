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
        """;
    try (PreparedStatement ps = pg.prepareStatement(sql)) {
      ps.setString(1, schema);
      ps.setString(2, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static void ensureSchema(Connection pg, String schema) throws SQLException {
    String sql = "CREATE SCHEMA IF NOT EXISTS " + quoteIdent(schema);
    try (Statement st = pg.createStatement()) {
      st.execute(sql);
      log.debug("Ensured schema {}", schema);
    }
  }

  private static String quoteIdent(String ident) {
    return "\"" + ident.replace("\"", "\"\"") + "\"";
  }

  static class ColumnDef {
    String name;
    int dataType;
    String typeName;
    int columnSize;
    int decimalDigits;
    boolean nullable;

    ColumnDef(String name, int dataType, String typeName, int columnSize, int decimalDigits, boolean nullable) {
      this.name = name;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.decimalDigits = decimalDigits;
      this.nullable = nullable;
    }
  }

  static class TypeMapping {
    public static String mssqlToPg(ColumnDef col) {
      int jdbcType = col.dataType;
      String typeName = col.typeName.toLowerCase();
      return switch (jdbcType) {
        case Types.BIT, Types.BOOLEAN -> "boolean";
        case Types.TINYINT, Types.SMALLINT -> "smallint";
        case Types.INTEGER -> "integer";
        case Types.BIGINT -> "bigint";
        case Types.REAL -> "real";
        case Types.FLOAT, Types.DOUBLE -> "double precision";
        case Types.DECIMAL, Types.NUMERIC -> "numeric(" + col.columnSize + "," + col.decimalDigits + ")";
        case Types.CHAR -> "char(" + col.columnSize + ")";
        case Types.VARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.LONGVARCHAR ->
            col.columnSize > 10485760 ? "text" : "varchar(" + col.columnSize + ")";
        case Types.DATE -> "date";
        case Types.TIME, Types.TIME_WITH_TIMEZONE -> "time";
        case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> "timestamp";
        case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> "bytea";
        case Types.CLOB, Types.NCLOB -> "text";
        default -> {
          if (typeName.contains("datetime")) yield "timestamp";
          if (typeName.contains("money")) yield "numeric(19,4)";
          if (typeName.contains("uniqueidentifier")) yield "uuid";
          yield "text";
        }
      };
    }
  }
}
