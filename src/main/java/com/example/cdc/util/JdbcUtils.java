package com.example.cdc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtils {
  public static Connection openMssql(String host, int port, String db, String user, String pass) throws SQLException {
    String url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=false;trustServerCertificate=true",
        host, port, db);
    return DriverManager.getConnection(url, user, pass);
  }

  public static Connection openPostgres(String host, int port, String db, String user, String pass) throws SQLException {
    String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);
    return DriverManager.getConnection(url, user, pass);
  }
}
