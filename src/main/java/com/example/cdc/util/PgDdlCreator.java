package com.example.cdc.util;

import com.example.cdc.config.CdcProperties.ColumnItem;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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

    List<ColumnItem> selected = (columns == null || columns.isEmpty())
        ? allCols.keySet().stream().map(c -> {
          ColumnItem item = new ColumnItem();
          item.setName(c);
          item.setAlias(c);
          return item;
        }).toList()
        : columns;

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

    String pkClause = "";
    if (!pkCols.isEmpty()) {
      String pk = pkCols.stream().map(PgDdlCreator::quoteIdent).collect(Collectors.joining(", "));
      pkClause = 