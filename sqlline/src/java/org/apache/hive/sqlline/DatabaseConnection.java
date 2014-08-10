/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.sqlline;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;

/**
 * Database connection.
 */
class DatabaseConnection {
  private final SqlLine sqlLine;
  Connection connection;
  DatabaseMetaData meta;
  Quoting quoting;
  private final String driver;
  private final String url;
  private final String username;
  private final String password;
  private final LinkedHashMap<String, String> info;
  private Schema schema = null;
  private Completer sqlCompleter = null;

  public DatabaseConnection(SqlLine sqlLine, String driver, String url,
      Map<String, String> info, String username, String password)
      throws SQLException {
    this.sqlLine = sqlLine;
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
    this.info = new LinkedHashMap<String, String>(info);
  }

  @Override
  public String toString() {
    return getUrl() + "";
  }

  void setCompletions(boolean skipmeta) throws SQLException, IOException {
    // Deduce the string used to quote identifiers. For example, Oracle
    // uses double-quotes:
    //   SELECT * FROM "My Schema"."My Table"
    String startQuote = meta.getIdentifierQuoteString();
    final boolean upper = meta.storesUpperCaseIdentifiers();
    if (startQuote == null
        || startQuote.equals("")
        || startQuote.equals(" ")) {
      if (meta.getDatabaseProductName().startsWith("MySQL")) {
        // Some version of the MySQL JDBC driver lie.
        quoting = new Quoting('`', '`', upper);
      } else {
        quoting = new Quoting((char) 0, (char) 0, false);
      }
    } else if (startQuote.equals("[")) {
      quoting = new Quoting('[', ']', upper);
    } else if (startQuote.length() > 1) {
      sqlLine.error(
          "Identifier quote string is '" + startQuote
              + "'; quote strings longer than 1 char are not supported");
      quoting = Quoting.DEFAULT;
    } else {
      quoting =
          new Quoting(startQuote.charAt(0), startQuote.charAt(0), upper);
    }

    final String extraNameCharacters =
        meta == null || meta.getExtraNameCharacters() == null
            ? ""
            : meta.getExtraNameCharacters();

    // setup the completer for the database
    sqlCompleter = new ArgumentCompleter(
        new ArgumentCompleter.WhitespaceArgumentDelimiter() {
            // delimiters for SQL statements are any
            // non-letter-or-number characters, except
            // underscore and characters that are specified
            // by the database to be valid name identifiers.
          @Override
          public boolean isDelimiterChar(
              final CharSequence buffer, int pos) {
            char c = buffer.charAt(pos);
            if (Character.isWhitespace(c)) {
              return true;
            }

            return !(Character.isLetterOrDigit(c))
                && (c != '_')
                && (extraNameCharacters.indexOf(c) == -1);
          }
        },
        new SqlCompleter(sqlLine, skipmeta));

    // not all argument elements need to hold true
    ((ArgumentCompleter) sqlCompleter).setStrict(false);
  }

  /**
   * Connection to the specified data source.
   */
  boolean connect() throws SQLException {
    try {
      if (driver != null && driver.length() != 0) {
        Class.forName(driver);
      }
    } catch (ClassNotFoundException cnfe) {
      return sqlLine.error(cnfe);
    }

    boolean foundDriver = false;
    Driver theDriver = null;
    try {
      theDriver = DriverManager.getDriver(url);
      foundDriver = theDriver != null;
    } catch (Exception e) {
      // ignore
    }

    if (!foundDriver) {
      sqlLine.output(sqlLine.loc("autoloading-known-drivers", url));
      sqlLine.registerKnownDrivers();
    }

    try {
      close();
    } catch (Exception e) {
      return sqlLine.error(e);
    }

    // Avoid using DriverManager.getConnection(). It is a synchronized
    // method and thus holds the lock while making the connection.
    // Deadlock can occur if the driver's connection processing uses any
    // synchronized DriverManager methods.  One such example is the
    // RMI-JDBC driver, whose RJDriverServer.connect() method uses
    // DriverManager.getDriver(). Because RJDriverServer.connect runs in
    // a different thread (RMI) than the getConnection() caller (here),
    // this sequence will hang every time.
/*
          connection = DriverManager.getConnection (url, username, password);
*/
    // Instead, we use the driver instance to make the connection

    final Properties info = new Properties();
    info.put("user", username);
    info.put("password", password);
    for (Map.Entry<String, String> entry : this.info.entrySet()) {
      info.put(entry.getKey(), entry.getValue());
    }
    connection = theDriver.connect(url, info);
    meta = connection.getMetaData();

    try {
      sqlLine.debug(
          sqlLine.loc("connected", meta.getDatabaseProductName(),
              meta.getDatabaseProductVersion()));
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      sqlLine.debug(
          sqlLine.loc("driver", meta.getDriverName(),
              meta.getDriverVersion()));
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      connection.setAutoCommit(sqlLine.getOpts().getAutoCommit());
      if (!sqlLine.getOpts().isCautious()) {
        // TODO: Setting autocommit should not generate an exception as long as
        // it is set to false
        sqlLine.autocommitStatus(connection);
      }
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      // nothing is done off of this command beyond the handle so no
      // need to use the callback.
      sqlLine.getCommands().isolation("isolation: " + sqlLine.getOpts()
          .getIsolation(),
          new DispatchCallback());
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    sqlLine.showWarnings();

    return true;
  }

  public Connection getConnection() throws SQLException {
    if (connection != null) {
      return connection;
    }
    if (!connect()) {
      throw new RuntimeException("Connection failed");
    }
    return connection;
  }

  public void reconnect() throws Exception {
    close();
    getConnection();
  }

  public void close() {
    try {
      try {
        if (connection != null && !connection.isClosed()) {
          sqlLine.output(sqlLine.loc("closing", connection));
          connection.close();
        }
      } catch (Exception e) {
        sqlLine.handleException(e);
      }
    } finally {
      setConnection(null);
      setDatabaseMetaData(null);
    }
  }

  public String[] getTableNames(boolean force) {
    Schema.Table[] t = getSchema().getTables();
    Set<String> names = new TreeSet<String>();
    for (int i = 0; t != null && i < t.length; i++) {
      names.add(t[i].getName());
    }
    return names.toArray(new String[names.size()]);
  }

  Schema getSchema() {
    if (schema == null) {
      schema = new Schema();
    }
    return schema;
  }

  void setConnection(Connection connection) {
    this.connection = connection;
  }

  DatabaseMetaData getDatabaseMetaData() {
    return meta;
  }

  void setDatabaseMetaData(DatabaseMetaData meta) {
    this.meta = meta;
  }

  String getUrl() {
    return url;
  }

  public Completer getSqlCompleter() {
    return sqlCompleter;
  }

  /** Schema definition. */
  class Schema {
    private Schema.Table[] tables = null;

    Schema.Table[] getTables() {
      if (tables != null) {
        return tables;
      }

      final List<Table> tnames = new LinkedList<Table>();
      try {
        ResultSet rs =
            meta.getTables(
                connection.getCatalog(),
                null,
                "%",
                new String[]{"TABLE"});
        try {
          while (rs.next()) {
            tnames.add(new Schema.Table(rs.getString("TABLE_NAME")));
          }
        } finally {
          try {
            rs.close();
          } catch (Exception e) {
            // ignore
          }
        }
      } catch (Throwable t) {
        // ignore
      }
      return tables = tnames.toArray(new Table[tnames.size()]);
    }

    Schema.Table getTable(String name) {
      Schema.Table[] t = getTables();
      for (int i = 0; (t != null) && (i < t.length); i++) {
        if (name.equalsIgnoreCase(t[i].getName())) {
          return t[i];
        }
      }
      return null;
    }

    /** Table definition. */
    class Table {
      final String name;
      Schema.Table.Column[] columns;

      public Table(String name) {
        this.name = name;
      }

      public String getName() {
        return name;
      }

      /** Column definition. */
      class Column {
        final String name;
        boolean isPrimaryKey;

        public Column(String name) {
          this.name = name;
        }
      }
    }
  }
}
