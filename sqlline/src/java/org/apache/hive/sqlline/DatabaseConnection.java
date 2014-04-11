package org.apache.hive.sqlline;

import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;

import java.io.IOException;
import java.sql.*;
import java.util.*;

class DatabaseConnection {
  private final SqlLine sqlLine;
  Connection connection;
  DatabaseMetaData meta;
  Quoting quoting;
  private final String driver;
  private final String url;
  private final String username;
  private final String password;
  private Schema schema = null;
  private Completer sqlCompletor = null;

  public DatabaseConnection(SqlLine sqlLine,
      String driver,
      String url,
      String username,
      String password)
      throws SQLException {
    this.sqlLine = sqlLine;
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

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
        ((meta == null) || (meta.getExtraNameCharacters() == null)) ? ""
            : meta.getExtraNameCharacters();

    // setup the completor for the database
    sqlCompletor =
        new ArgumentCompleter(
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
    ((ArgumentCompleter) sqlCompletor).setStrict(false);
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
    connection = theDriver.connect(url, info);
    meta = connection.getMetaData();

    try {
      sqlLine.debug(sqlLine.loc("connected", new Object[]{
          meta.getDatabaseProductName(),
          meta.getDatabaseProductVersion()}));
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      sqlLine.debug(sqlLine.loc("driver", new Object[]{
          meta.getDriverName(),
          meta.getDriverVersion()}));
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      connection.setAutoCommit(sqlLine.getOpts().getAutoCommit());
      sqlLine.autocommitStatus(connection);
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

  public Connection getConnection()
      throws SQLException {
    if (connection != null) {
      return connection;
    }

    connect();

    return connection;
  }

  public void reconnect()
      throws Exception {
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
      connection = null;
      meta = null;
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

  String getUrl() {
    return url;
  }

  public DatabaseMetaData getDatabaseMetaData() {
    return meta;
  }

  public Completer getSQLCompletor() {
    return sqlCompletor;
  }

  class Schema {
    private Schema.Table[] tables = null;

    Schema.Table[] getTables() {
      if (tables != null) {
        return tables;
      }

      List tnames = new LinkedList();

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
          }
        }
      } catch (Throwable t) {
      }

      return tables = (Schema.Table[]) tnames.toArray(new Schema.Table[0]);
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

    class Table {
      final String name;
      Schema.Table.Column[] columns;

      public Table(String name) {
        this.name = name;
      }

      public String getName() {
        return name;
      }

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
