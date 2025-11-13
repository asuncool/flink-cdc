package com.example.cdc.sink;

import com.example.cdc.config.CdcProperties;
import com.example.cdc.config.CdcProperties.ColumnItem;
import com.example.cdc.config.CdcProperties.TableSyncConfig;
import com.example.cdc.util.JdbcUtils;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class PostgresUpsertSink {

  private final CdcProperties props;

  private volatile Connection pgConn;

  private final Map<String, PreparedSqlBundle> prepared = new ConcurrentHashMap<>();
  private final Map<String, TableSyncConfig> tableConfigBySrc = new HashMap<>();

  public PostgresUpsertSink(CdcProperties props) {
    this.props = props;
    for (TableSyncConfig t : props.getTables()) {
      String schema = Optional.ofNullable(t.getSourceSchema()).orElse(props.getSource().getSchema());
      String table = t.getSourceTable();
      tableConfigBySrc.put((schema + "." + table).toLowerCase(), t);
    }
  }

  private void ensureConn() throws SQLException {
    if (pgConn == null || pgConn.isClosed()) {
      pgConn = JdbcUtils.openPostgres(
          props.getSink().getHostname(),
          props.getSink().getPort(),
          props.getSink().getDatabase(),
          props.getSink().getUsername(),
          props.getSink().getPassword()
      );
      pgConn.setAutoCommit(true);
    }
  }

  public void invoke(JsonNode root) throws Exception {
    ensureConn();

    String op = root.path("op").asText(); // c,u,d,r
    JsonNode src = root.path("source");
    String schema = src.path("schema").asText();
    String table = src.path("table").asText();

    String srcKey = (schema + "." + table).toLowerCase();
    TableSyncConfig cfg = tableConfigBySrc.get(srcKey);
    if (cfg == null) {
      log.debug("Skip table not configured: {}", srcKey);
      return;
    }

    Map<String, Object> after = nodeToMap(root.path("after"));
    Map<String, Object> before = nodeToMap(root.path("before"));

    switch (op) {
      case "c":
      case "r":
        upsert(cfg, after);
        break;
      case "u":
        upsert(cfg, after);
        break;
      case "d":
        delete(cfg, before);
        break;
      default:
        log.warn("Unknown op: {}", op);
    }
  }

  private Map<String, Object> nodeToMap(JsonNode node) {
    Map<String, Object> map = new HashMap<>();
    if (node == null || node.isMissingNode() || node.isNull()) return map;
    Iterator<Map.Entry<String, JsonNode>> it = node.fields();
    while (it.hasNext()) {
      var e = it.next();
      JsonNode v = e.getValue();
      if (v.isNull()) {
        map.put(e.getKey(), null);
      } else if (v.isNumber()) {
        map.put(e.getKey(), v.numberValue());
      } else if (v.isBoolean()) {
        map.put(e.getKey(), v.booleanValue());
      } else {
        map.put(e.getKey(), v.asText());
      }
    }
    return map;
  }

  private PreparedSqlBundle ensurePrepared(TableSyncConfig cfg) throws SQLException {
    String tgtSchema = Optional.ofNullable(cfg.getTargetSchema()).orElse(props.getSink().getSchema());
    String tgtTable = Optional.ofNullable(cfg.getTargetTable()).orElse(cfg.getSourceTable());
    String key = (tgtSchema + "." + tgtTable).toLowerCase();
    PreparedSqlBundle bundle = prepared.get(key);
    if (bundle != null) return bundle;

    List<String> targetCols = new ArrayList<>();
    if (cfg.getColumns() != null && !cfg.getColumns().isEmpty()) {
      for (ColumnItem c : cfg.getColumns()) {
        targetCols.add(Optional.ofNullable(c.getAlias()).orElse(c.getName()));
      }
    } else {
      throw new SQLException("columns not configured for table " + key + " (请在配置中指定 columns 以明确字段顺序)");
    }

    List<String> pks = Optional.ofNullable(cfg.getPrimaryKeys()).orElse(Collections.emptyList());
    if (pks.isEmpty()) {
      throw new SQLException("Primary keys must be provided for table " + key + " to support upsert/delete.");
    }

    String colList = String.join(", ", quoteAll(targetCols));
    String placeholders = String.join(", ", Collections.nCopies(targetCols.size(), "?"));
    String conflict = String.join(", ", quoteAll(pks));
    String setClause = String.join(", ", buildUpdateSet(targetCols, pks));

    String insertSql = String.format(
        "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
        quoteIdent(tgtSchema), quoteIdent(tgtTable), colList, placeholders, conflict, setClause
    );

    String where = String.join(" AND ", pks.stream().map(pk -> quoteIdent(pk) + " = ?").toList());
    String deleteSql = String.format(
        "DELETE FROM %s.%s WHERE %s",
        quoteIdent(tgtSchema), quoteIdent(tgtTable), where
    );

    PreparedSqlBundle newBundle = new PreparedSqlBundle(
        targetCols, pks,
        pgConn.prepareStatement(insertSql),
        pgConn.prepareStatement(deleteSql)
    );
    prepared.put(key, newBundle);
    log.info("Prepared SQL for {}: upsert=[{}], delete=[{}]", key, insertSql, deleteSql);
    return newBundle;
  }

  private void upsert(TableSyncConfig cfg, Map<String, Object> data) throws SQLException {
    if (data == null || data.isEmpty()) return;
    PreparedSqlBundle b = ensurePrepared(cfg);

    for (int i = 0; i < b.targetCols.size(); i++) {
      Object val = data.getOrDefault(b.targetCols.get(i), data.get(b.targetCols.get(i).toLowerCase()));
      b.psUpsert.setObject(i + 1, val);
    }
    b.psUpsert.executeUpdate();
  }

  private void delete(TableSyncConfig cfg, Map<String, Object> before) throws SQLException {
    if (before == null || before.isEmpty()) return;
    PreparedSqlBundle b = ensurePrepared(cfg);

    for (int i = 0; i < b.primaryKeys.size(); i++) {
      String pk = b.primaryKeys.get(i);
      Object val = before.getOrDefault(pk, before.get(pk.toLowerCase()));
      b.psDelete.setObject(i + 1, val);
    }
    b.psDelete.executeUpdate();
  }

  private List<String> quoteAll(List<String> cols) {
    return cols.stream().map(this::quoteIdent).toList();
  }
  private String quoteIdent(String ident) {
    return "\"" + ident.replace("\"", "\\\"\\"") + "\"";
  }
  private List<String> buildUpdateSet(List<String> targetCols, List<String> pks) {
    Set<String> pkSet = new HashSet<>(pks.stream().map(String::toLowerCase).toList());
    List<String> sets = new ArrayList<>();
    for (String c : targetCols) {
      if (!pkSet.contains(c.toLowerCase())) {
        sets.add(quoteIdent(c) + " = EXCLUDED." + quoteIdent(c));
      }
    }
    return sets;
  }

  private record PreparedSqlBundle(
      List<String> targetCols,
      List<String> primaryKeys,
      PreparedStatement psUpsert,
      PreparedStatement psDelete
  ) {}
}
