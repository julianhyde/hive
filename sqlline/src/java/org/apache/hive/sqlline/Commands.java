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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.History;

/** Contains implementations of SqlLine's built-in commands. */
public class Commands {
  private static final String[] METHOD_NAMES = {
    "allProceduresAreCallable",
    "allTablesAreSelectable",
    "dataDefinitionCausesTransactionCommit",
    "dataDefinitionIgnoredInTransactions",
    "doesMaxRowSizeIncludeBlobs",
    "getCatalogSeparator",
    "getCatalogTerm",
    "getDatabaseProductName",
    "getDatabaseProductVersion",
    "getDefaultTransactionIsolation",
    "getDriverMajorVersion",
    "getDriverMinorVersion",
    "getDriverName",
    "getDriverVersion",
    "getExtraNameCharacters",
    "getIdentifierQuoteString",
    "getMaxBinaryLiteralLength",
    "getMaxCatalogNameLength",
    "getMaxCharLiteralLength",
    "getMaxColumnNameLength",
    "getMaxColumnsInGroupBy",
    "getMaxColumnsInIndex",
    "getMaxColumnsInOrderBy",
    "getMaxColumnsInSelect",
    "getMaxColumnsInTable",
    "getMaxConnections",
    "getMaxCursorNameLength",
    "getMaxIndexLength",
    "getMaxProcedureNameLength",
    "getMaxRowSize",
    "getMaxSchemaNameLength",
    "getMaxStatementLength",
    "getMaxStatements",
    "getMaxTableNameLength",
    "getMaxTablesInSelect",
    "getMaxUserNameLength",
    "getNumericFunctions",
    "getProcedureTerm",
    "getSchemaTerm",
    "getSearchStringEscape",
    "getSQLKeywords",
    "getStringFunctions",
    "getSystemFunctions",
    "getTimeDateFunctions",
    "getURL",
    "getUserName",
    "isCatalogAtStart",
    "isReadOnly",
    "nullPlusNonNullIsNull",
    "nullsAreSortedAtEnd",
    "nullsAreSortedAtStart",
    "nullsAreSortedHigh",
    "nullsAreSortedLow",
    "storesLowerCaseIdentifiers",
    "storesLowerCaseQuotedIdentifiers",
    "storesMixedCaseIdentifiers",
    "storesMixedCaseQuotedIdentifiers",
    "storesUpperCaseIdentifiers",
    "storesUpperCaseQuotedIdentifiers",
    "supportsAlterTableWithAddColumn",
    "supportsAlterTableWithDropColumn",
    "supportsANSI92EntryLevelSQL",
    "supportsANSI92FullSQL",
    "supportsANSI92IntermediateSQL",
    "supportsBatchUpdates",
    "supportsCatalogsInDataManipulation",
    "supportsCatalogsInIndexDefinitions",
    "supportsCatalogsInPrivilegeDefinitions",
    "supportsCatalogsInProcedureCalls",
    "supportsCatalogsInTableDefinitions",
    "supportsColumnAliasing",
    "supportsConvert",
    "supportsCoreSQLGrammar",
    "supportsCorrelatedSubqueries",
    "supportsDataDefinitionAndDataManipulationTransactions",
    "supportsDataManipulationTransactionsOnly",
    "supportsDifferentTableCorrelationNames",
    "supportsExpressionsInOrderBy",
    "supportsExtendedSQLGrammar",
    "supportsFullOuterJoins",
    "supportsGroupBy",
    "supportsGroupByBeyondSelect",
    "supportsGroupByUnrelated",
    "supportsIntegrityEnhancementFacility",
    "supportsLikeEscapeClause",
    "supportsLimitedOuterJoins",
    "supportsMinimumSQLGrammar",
    "supportsMixedCaseIdentifiers",
    "supportsMixedCaseQuotedIdentifiers",
    "supportsMultipleResultSets",
    "supportsMultipleTransactions",
    "supportsNonNullableColumns",
    "supportsOpenCursorsAcrossCommit",
    "supportsOpenCursorsAcrossRollback",
    "supportsOpenStatementsAcrossCommit",
    "supportsOpenStatementsAcrossRollback",
    "supportsOrderByUnrelated",
    "supportsOuterJoins",
    "supportsPositionedDelete",
    "supportsPositionedUpdate",
    "supportsSchemasInDataManipulation",
    "supportsSchemasInIndexDefinitions",
    "supportsSchemasInPrivilegeDefinitions",
    "supportsSchemasInProcedureCalls",
    "supportsSchemasInTableDefinitions",
    "supportsSelectForUpdate",
    "supportsStoredProcedures",
    "supportsSubqueriesInComparisons",
    "supportsSubqueriesInExists",
    "supportsSubqueriesInIns",
    "supportsSubqueriesInQuantifieds",
    "supportsTableCorrelationNames",
    "supportsTransactions",
    "supportsUnion",
    "supportsUnionAll",
    "usesLocalFilePerTable",
    "usesLocalFiles",
  };

  private static final String[] URL_PROPERTIES = {
    "url",
    "javax.jdo.option.ConnectionURL",
    "ConnectionURL",
  };
  private static final String[] DRIVER_PROPERTIES = {
    "driver",
    "javax.jdo.option.ConnectionDriverName",
    "ConnectionDriverName",
  };
  private static final String[] USER_PROPERTIES = {
    "user",
    "javax.jdo.option.ConnectionUserName",
    "ConnectionUserName",
  };
  private static final String[] PASSWORD_PROPERTIES = {
    "password",
    "javax.jdo.option.ConnectionPassword",
    "ConnectionPassword",
  };

  private final SqlLine sqlLine;

  public Commands(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public void metadata(String line, DispatchCallback callback) {
    sqlLine.debug(line);

    final List<String> parts = sqlLine.split(line);
    if (parts.isEmpty()) {
      dbinfo("", callback);
      return;
    }

    final List<String> params = new LinkedList<String>(parts);
    params.remove(0);
    params.remove(0);
    sqlLine.debug(params.toString());
    metadata(parts.get(1), params, callback);
  }

  public void metadata(String cmd, List argList, DispatchCallback callback) {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }
    try {
      final DatabaseMetaData metaData = sqlLine.getDatabaseMetaData();
      final Set<String> methodNames = new TreeSet<String>();
      final Set<String> methodNamesUpper = new TreeSet<String>();
      for (Method method : metaData.getClass().getMethods()) {
        methodNames.add(method.getName());
        methodNamesUpper.add(method.getName().toUpperCase());
      }

      if (!methodNamesUpper.contains(cmd.toUpperCase())) {
        sqlLine.error(sqlLine.loc("no-such-method", cmd));
        sqlLine.error(sqlLine.loc("possible-methods"));
        for (String methodName : methodNames) {
          sqlLine.error("   " + methodName);
        }
        callback.setToFailure();
        return;
      }

      Object res = sqlLine.getReflector().invoke(metaData,
          DatabaseMetaData.class, cmd, argList);

      if (res instanceof ResultSet) {
        ResultSet rs = (ResultSet) res;
        try {
          sqlLine.print(rs, callback);
        } finally {
          rs.close();
        }
      } else if (res != null) {
        sqlLine.output(res.toString());
      }
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
    callback.setToSuccess();
  }

  public void history(String line, DispatchCallback callback) {
    int index = 1;
    for (History.Entry entry : sqlLine.getConsoleReader().getHistory()) {
      index++;
      sqlLine.output(
          sqlLine.getColorBuffer()
              .pad(index + ".", 6)
              .append(entry.toString()));
    }
    callback.setToSuccess();
  }

  String arg1(String line, String paramname) {
    return arg1(line, paramname, null);
  }

  String arg1(String line, String paramname, String def) {
    List<String> ret = sqlLine.split(line);

    if (ret.size() != 2) {
      if (def != null) {
        return def;
      }
      throw new IllegalArgumentException(
          sqlLine.loc("arg-usage", ret.get(0), paramname));
    }
    return ret.get(1);
  }

  /**
   * Constructs a list of string parameters for a metadata call.
   * <p/>
   * <p>The number of items is equal to the number of items in the
   * <tt>strings</tt> parameter, typically three (catalog, schema, table
   * name).
   * <p/>
   * <p>Parses the command line, and assumes that the the first word is
   * a compound identifier. If the compound identifier has fewer parts
   * than required, fills from the right.
   * <p/>
   * <p>The result is a mutable list of strings.
   *
   *
   * @param line          Command line
   * @param paramName     Name of parameter being read from command line
   * @param defaultValues Default values for each component of parameter
   * @return Mutable list of strings
   */
  private List<Object> buildMetadataArgs(
      String line,
      String paramName,
      String[] defaultValues) {
    final List<Object> list = new ArrayList<Object>();
    final String[][] ret = sqlLine.splitCompound(line);
    String[] compound;
    if (ret == null || ret.length != 2) {
      if (defaultValues[defaultValues.length - 1] == null) {
        throw new IllegalArgumentException(
            sqlLine.loc("arg-usage", ret.length == 0 ? "" : ret[0][0],
                paramName));
      }
      compound = new String[0];
    } else {
      compound = ret[1];
    }
    if (compound.length <= defaultValues.length) {
      list.addAll(
          Arrays.asList(defaultValues).subList(
              0, defaultValues.length - compound.length));
      list.addAll(Arrays.asList(compound));
    } else {
      list.addAll(
          Arrays.asList(compound).subList(0, defaultValues.length));
    }
    return list;
  }

  public void indexes(String line, DispatchCallback callback) throws Exception {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args = buildMetadataArgs(line, "table name", strings);
    args.add(Boolean.FALSE);
    args.add(Boolean.TRUE);
    metadata("getIndexInfo", args, callback);
  }

  public void primarykeys(String line, DispatchCallback callback)
      throws Exception {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args = buildMetadataArgs(line, "table name", strings);
    metadata("getPrimaryKeys", args, callback);
  }

  public void exportedkeys(String line, DispatchCallback callback)
      throws Exception {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args = buildMetadataArgs(line, "table name", strings);
    metadata("getExportedKeys", args, callback);
  }

  public void importedkeys(String line, DispatchCallback callback)
      throws Exception {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args = buildMetadataArgs(line, "table name", strings);
    metadata("getImportedKeys", args, callback);
  }

  public void procedures(String line, DispatchCallback callback)
      throws Exception {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args =
        buildMetadataArgs(line, "procedure name pattern", strings);
    metadata("getProcedures", args, callback);
  }

  public void tables(String line, DispatchCallback callback)
      throws SQLException {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args = buildMetadataArgs(line, "table name", strings);
    args.add(null);
    metadata("getTables", args, callback);
  }

  public void typeinfo(String line, DispatchCallback callback)
      throws Exception {
    metadata("getTypeInfo", Collections.EMPTY_LIST, callback);
  }

  public void nativesql(String sql, DispatchCallback callback)
      throws Exception {
    if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
      sql = sql.substring(1);
    }
    if (sql.startsWith("native")) {
      sql = sql.substring("native".length() + 1);
    }
    String nat = sqlLine.getConnection().nativeSQL(sql);
    sqlLine.output(nat);
    callback.setToSuccess();
  }

  public void columns(String line, DispatchCallback callback)
      throws SQLException {
    String[] strings = {sqlLine.getConnection().getCatalog(), null, "%"};
    List<Object> args = buildMetadataArgs(line, "table name", strings);
    args.add("%");
    metadata("getColumns", args, callback);
  }

  public void dropall(String line, DispatchCallback callback) {
    final DatabaseConnection databaseConnection =
        sqlLine.getDatabaseConnection();
    if (databaseConnection == null || databaseConnection.getUrl() == null) {
      sqlLine.error(sqlLine.loc("no-current-connection"));
      callback.setToFailure();
      return;
    }
    try {
      final String prompt = sqlLine.loc("really-drop-all");
      if (!(sqlLine.getConsoleReader().readLine(prompt).equals("y"))) {
        sqlLine.error("abort-drop-all");
        callback.setToFailure();
        return;
      }

      List<String> cmds = new LinkedList<String>();
      ResultSet rs = sqlLine.getTables();
      try {
        while (rs.next()) {
          cmds.add("DROP TABLE "
              + rs.getString("TABLE_NAME") + ";");
        }
      } finally {
        try {
          rs.close();
        } catch (Exception e) {
          // ignore
        }
      }
      // run as a batch
      if (sqlLine.runCommands(cmds, callback) == cmds.size()) {
        callback.setToSuccess();
      } else {
        callback.setToFailure();
      }
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }

  public void reconnect(String line, DispatchCallback callback) {
    final DatabaseConnection databaseConnection =
        sqlLine.getDatabaseConnection();
    if (databaseConnection == null || databaseConnection.getUrl() == null) {
      sqlLine.error(sqlLine.loc("no-current-connection"));
      callback.setToFailure();
      return;
    }

    sqlLine.info(sqlLine.loc("reconnecting", databaseConnection.getUrl()));
    try {
      databaseConnection.reconnect();
    } catch (Exception e) {
      sqlLine.error(e);
      callback.setToFailure();
      return;
    }

    callback.setToSuccess();
  }

  public void scan(String line, DispatchCallback callback) throws IOException {
    TreeSet<String> names = new TreeSet<String>();

    if (sqlLine.getDrivers() == null) {
      sqlLine.setDrivers(sqlLine.scanDrivers(line));
    }

    sqlLine.info(
        sqlLine.locChoice("drivers-found-count", sqlLine.getDrivers().size()));

    // unique the list
    for (Driver driver : sqlLine.getDrivers()) {
      names.add(driver.getClass().getName());
    }

    sqlLine.output(sqlLine.getColorBuffer()
        .bold(SqlLine.pad(sqlLine.loc("compliant"), 10))
        .bold(SqlLine.pad(sqlLine.loc("jdbc-version"), 8))
        .bold(sqlLine.loc("driver-class")));

    for (String name : names) {
      try {
        Driver driver = (Driver) Class.forName(name).newInstance();
        String msg =
            SqlLine.pad(driver.jdbcCompliant() ? "yes" : "no", 10)
            + SqlLine.pad(
                driver.getMajorVersion() + "." + driver.getMinorVersion(), 8)
            + name;
        if (driver.jdbcCompliant()) {
          sqlLine.output(msg);
        } else {
          sqlLine.output(sqlLine.getColorBuffer().red(msg));
        }
      } catch (Throwable t) {
        sqlLine.output(sqlLine.getColorBuffer().red(name)); // error with driver
      }
    }
    callback.setToSuccess();
  }

  public void save(String line, DispatchCallback callback)
      throws IOException {
    sqlLine.info(
        sqlLine.loc("saving-options", sqlLine.getOpts().getPropertiesFile()));
    sqlLine.getOpts().save();
    callback.setToSuccess();
  }

  public void load(String line, DispatchCallback callback) throws IOException {
    sqlLine.getOpts().load();
    sqlLine.info(
       sqlLine.loc("loaded-options", sqlLine.getOpts().getPropertiesFile()));
    callback.setToSuccess();
  }

  public void config(String line, DispatchCallback callback) {
    try {
      for (Map.Entry<String, String> entry
          : sqlLine.getOpts().toMap().entrySet()) {
        sqlLine.output(
            sqlLine.getColorBuffer()
                .green(
                    SqlLine.pad(
                        entry.getKey().substring(
                            SqlLineOpts.PROPERTY_PREFIX.length()),
                        20))
                .append(entry.getValue()));
      }
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
      return;
    }

    callback.setToSuccess();
  }

  public void set(String line, DispatchCallback callback) {
    if (line == null || line.trim().equals("set")
        || line.length() == 0) {
      config(null, callback);
      return;
    }

    List<String> parts = sqlLine.split(line, 3, "Usage: set <key> <value>");
    if (parts == null) {
      callback.setToFailure();
      return;
    }

    String key = parts.get(1);
    String value = parts.get(2);
    boolean success = sqlLine.getOpts().set(key, value, false);
    // if we autosave, then save
    if (success && sqlLine.getOpts().getAutosave()) {
      try {
        sqlLine.getOpts().save();
      } catch (Exception saveException) {
        // ignore
      }
    }
    callback.setToSuccess();
  }

  private void reportResult(String action, long start, long end) {
    if (sqlLine.getOpts().getShowElapsedTime()) {
      sqlLine.info(action + " " + sqlLine.locElapsedTime(end - start));
    } else {
      sqlLine.info(action);
    }
  }

  public void commit(String line, DispatchCallback callback)
      throws SQLException {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }
    if (!sqlLine.assertAutoCommit()) {
      callback.setToFailure();
      return;
    }
    final Connection connection =
        sqlLine.getDatabaseConnection().getConnection();
    try {
      long start = System.currentTimeMillis();
      connection.commit();
      long end = System.currentTimeMillis();
      sqlLine.showWarnings();
      reportResult(sqlLine.loc("commit-complete"), start, end);
      callback.setToSuccess();
      return;
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
      return;
    }
  }

  public void rollback(String line, DispatchCallback callback)
      throws SQLException {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }
    if (!sqlLine.assertAutoCommit()) {
      callback.setToFailure();
      return;
    }
    final Connection connection =
        sqlLine.getDatabaseConnection().getConnection();
    try {
      long start = System.currentTimeMillis();
      connection.rollback();
      long end = System.currentTimeMillis();
      sqlLine.showWarnings();
      reportResult(sqlLine.loc("rollback-complete"), start, end);
      callback.setToSuccess();
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }

  public void autocommit(String line, DispatchCallback callback)
      throws SQLException {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }

    final Connection connection =
        sqlLine.getDatabaseConnection().getConnection();
    if (line.endsWith("on")) {
      connection.setAutoCommit(true);
    } else if (line.endsWith("off")) {
      connection.setAutoCommit(false);
    }

    sqlLine.showWarnings();
    sqlLine.autocommitStatus(connection);
    callback.setToSuccess();
  }

  public void dbinfo(String line, DispatchCallback callback) {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }

    sqlLine.showWarnings();
    final int padlen = 50;

    final DatabaseMetaData metaData = sqlLine.getDatabaseMetaData();
    for (String methodName : METHOD_NAMES) {
      try {
        final Object value =
            sqlLine.getReflector().invoke(metaData, methodName,
                Collections.emptyList());
        sqlLine.output(
            sqlLine.getColorBuffer().pad(methodName, padlen)
                .append("" + value));
      } catch (Exception e) {
        sqlLine.handleException(e);
      }
    }
    callback.setToSuccess();
  }

  public void verbose(String line, DispatchCallback callback) {
    sqlLine.info("verbose: on");
    set("set verbose true", callback);
  }

  public void outputformat(String line, DispatchCallback callback) {
    set("set " + line, callback);
  }

  public void brief(String line, DispatchCallback callback) {
    sqlLine.info("verbose: off");
    set("set verbose false", callback);
  }

  public void isolation(String line, DispatchCallback callback)
      throws SQLException {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }

    int i;

    if (line.endsWith("TRANSACTION_NONE")) {
      i = Connection.TRANSACTION_NONE;
    } else if (line.endsWith("TRANSACTION_READ_COMMITTED")) {
      i = Connection.TRANSACTION_READ_COMMITTED;
    } else if (line.endsWith("TRANSACTION_READ_UNCOMMITTED")) {
      i = Connection.TRANSACTION_READ_UNCOMMITTED;
    } else if (line.endsWith("TRANSACTION_REPEATABLE_READ")) {
      i = Connection.TRANSACTION_REPEATABLE_READ;
    } else if (line.endsWith("TRANSACTION_SERIALIZABLE")) {
      i = Connection.TRANSACTION_SERIALIZABLE;
    } else {
      callback.setToFailure();
      sqlLine.error(
          "Usage: isolation <TRANSACTION_NONE "
              + "| TRANSACTION_READ_COMMITTED "
              + "| TRANSACTION_READ_UNCOMMITTED "
              + "| TRANSACTION_REPEATABLE_READ "
              + "| TRANSACTION_SERIALIZABLE>");
      return;
    }

    final Connection connection =
        sqlLine.getDatabaseConnection().getConnection();
    connection.setTransactionIsolation(i);

    int isol = connection.getTransactionIsolation();
    final String isoldesc;
    switch (i) {
    case Connection.TRANSACTION_NONE:
      isoldesc = "TRANSACTION_NONE";
      break;
    case Connection.TRANSACTION_READ_COMMITTED:
      isoldesc = "TRANSACTION_READ_COMMITTED";
      break;
    case Connection.TRANSACTION_READ_UNCOMMITTED:
      isoldesc = "TRANSACTION_READ_UNCOMMITTED";
      break;
    case Connection.TRANSACTION_REPEATABLE_READ:
      isoldesc = "TRANSACTION_REPEATABLE_READ";
      break;
    case Connection.TRANSACTION_SERIALIZABLE:
      isoldesc = "TRANSACTION_SERIALIZABLE";
      break;
    default:
      isoldesc = "UNKNOWN";
    }

    sqlLine.debug(sqlLine.loc("isolation-status", isoldesc));
    callback.setToSuccess();
  }

  public void batch(String line, DispatchCallback callback) {
    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }
    if (sqlLine.getBatch() == null) {
      sqlLine.setBatch(new LinkedList<String>());
      sqlLine.info(sqlLine.loc("batch-start"));
      callback.setToSuccess();
    } else {
      sqlLine.info(sqlLine.loc("running-batch"));
      try {
        sqlLine.runBatch(sqlLine.getBatch());
        callback.setToSuccess();
      } catch (Exception e) {
        callback.setToFailure();
        sqlLine.error(e);
      } finally {
        sqlLine.setBatch(null);
      }
    }
  }

  public void sql(String line, DispatchCallback callback) {
    execute(line, false, callback);
  }

  public void sh(String line, DispatchCallback callback) {
    if (!line.startsWith("sh")) {
      callback.setToFailure();
      return;
    }

    line = line.substring("sh".length()).trim();

    if (line.length() == 0) {
      callback.setToFailure();
      return;
    }
    try {
      ShellCmdExecutor executor =
          new ShellCmdExecutor(line, sqlLine.getOutputStream(),
              sqlLine.getErrorStream());
      int ret = executor.execute();
      if (ret != 0) {
        sqlLine.output("Command failed with exit code = " + ret);
        callback.setToFailure();
      } else {
        callback.setToSuccess();
      }
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error("Exception raised from Shell command " + e);
      sqlLine.error(e);
    }
  }

  public void call(String line, DispatchCallback callback) {
    execute(line, true, callback);
  }

  private void execute(String line, boolean call, DispatchCallback callback) {
    if (line == null || line.length() == 0) {
      callback.setStatus(DispatchCallback.Status.FAILURE);
      return;
    }

    // ### FIXME: doing the multi-line handling down here means
    // higher-level logic never sees the extra lines. So,
    // for example, if a script is being saved, it won't include
    // the continuation lines! This is logged as sf.net
    // bug 879518.

    // use multiple lines for statements not terminated by ";"
    try {
      while (!line.trim().endsWith(";")
          && sqlLine.getOpts().isAllowMultiLineCommand()) {
        StringBuilder prompt = new StringBuilder(sqlLine.getPrompt());
        for (int i = 0; i < prompt.length() - 1; i++) {
          if (prompt.charAt(i) != '>') {
            prompt.setCharAt(i, i % 2 == 0 ? '.' : ' ');
          }
        }

        String extra = sqlLine.getConsoleReader().readLine(prompt.toString());
        if (null == extra) {
          break; // reader is at the end of data
        }
        if (!sqlLine.isComment(extra)) {
          line += SqlLine.getSeparator() + extra;
        }
      }
    } catch (UserInterruptException uie) {
      // CTRL-C'd out of the command. Note it, but don't call it an
      // error.
      callback.setStatus(DispatchCallback.Status.CANCELED);
      sqlLine.output(sqlLine.loc("command-canceled"));
      return;
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    if (line.trim().endsWith(";")) {
      line = line.trim();
      line = line.substring(0, line.length() - 1);
    }

    if (!sqlLine.assertConnection()) {
      callback.setToFailure();
      return;
    }

    String sql = line;

    if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
      sql = sql.substring(1);
    }

    String prefix = call ? "call" : "sql";

    if (sql.startsWith(prefix)) {
      sql = sql.substring(prefix.length());
    }

    // batch statements?
    if (sqlLine.getBatch() != null) {
      sqlLine.getBatch().add(sql);
      callback.setToSuccess();
      return;
    }

    try {
      Statement stmnt = null;
      boolean hasResults;

      try {
        long start = System.currentTimeMillis();

        if (call) {
          PreparedStatement p = sqlLine.prepare(sql);
          callback.trackSqlQuery(p);
          hasResults = p.execute();
          stmnt = p;
        } else {
          stmnt = sqlLine.createStatement();
          callback.trackSqlQuery(stmnt);
          hasResults = stmnt.execute(sql);
          callback.setToSuccess();
        }

        sqlLine.showWarnings();
        sqlLine.showWarnings(stmnt.getWarnings());

        if (hasResults) {
          do {
            ResultSet rs = stmnt.getResultSet();
            try {
              int count = sqlLine.print(rs, callback);
              long end = System.currentTimeMillis();

              reportResult(sqlLine.locChoice("rows-selected", count), start,
                  end);
            } finally {
              rs.close();
            }
          } while (SqlLine.getMoreResults(stmnt));
        } else {
          int count = stmnt.getUpdateCount();
          long end = System.currentTimeMillis();
          reportResult(sqlLine.locChoice("rows-affected", count), start, end);
        }
      } finally {
        if (stmnt != null) {
          sqlLine.showWarnings(stmnt.getWarnings());
          stmnt.close();
        }
      }
    } catch (UserInterruptException uie) {
      // CTRL-C'd out of the command. Note it, but don't call it an
      // error.
      callback.setStatus(DispatchCallback.Status.CANCELED);
      sqlLine.output(sqlLine.loc("command-canceled"));
      return;
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
      return;
    }
    sqlLine.showWarnings();
    callback.setToSuccess();
  }

  public void quit(String line, DispatchCallback callback) {
    sqlLine.setExit(true);
    close(null, callback);
  }

  /**
   * Close all connections.
   */
  public void closeall(String line, DispatchCallback callback) {
    close(null, callback);
    if (!callback.isSuccess()) {
      return;
    }
    while (callback.isSuccess()) {
      close(null, callback);
    }
    // the last "close" will set it to fail so reset it to success.
    callback.setToSuccess();
  }

  /**
   * Close the current connection.
   */
  public void close(String line, DispatchCallback callback) {
    final DatabaseConnection databaseConnection =
        sqlLine.getDatabaseConnection();
    if (databaseConnection == null) {
      callback.setToFailure();
      return;
    }

    try {
      final Connection connection = databaseConnection.getConnection();
      if (connection != null && !connection.isClosed()) {
        int index = sqlLine.getDatabaseConnections().getIndex();
        sqlLine.info(sqlLine.loc("closing", index, databaseConnection));
        connection.close();
      } else {
        sqlLine.info(sqlLine.loc("already-closed"));
      }
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
      return;
    }

    sqlLine.getDatabaseConnections().remove();
    callback.setToSuccess();
  }

  /**
   * Connect to the database defined in the specified properties file.
   */
  public void properties(String line, DispatchCallback callback)
      throws Exception {
    String example = "";
    example += "Usage: properties <properties file>" + SqlLine.getSeparator();

    List<String> parts = sqlLine.split(line);
    if (parts.size() < 2) {
      callback.setToFailure();
      sqlLine.error(example);
      return;
    }

    int successes = 0;

    for (int i = 1; i < parts.size(); i++) {
      Properties props = new Properties();
      InputStream stream = new FileInputStream(parts.get(i));
      try {
        props.load(stream);
      } finally {
        closeStream(stream);
      }

      connect(props, callback);
      if (callback.isSuccess()) {
        successes++;
      }
    }

    if (successes != (parts.size() - 1)) {
      callback.setToFailure();
    } else {
      callback.setToSuccess();
    }
  }

  static void closeStream(Closeable stream) {
    try {
      if (stream != null) {
        stream.close();
      }
    } catch (IOException e) {
      // ignore
    }
  }

  public void connect(String line, DispatchCallback callback) throws Exception {
    String example = "Usage: connect <url> <username> <password> [driver]"
        + SqlLine.getSeparator();

    List<String> parts = sqlLine.split(line);
    if (parts == null) {
      callback.setToFailure();
      return;
    }

    if (parts.size() < 2) {
      callback.setToFailure();
      sqlLine.error(example);
      return;
    }

    String url = parts.size() < 2 ? null : parts.get(1);
    String user = parts.size() < 3 ? null : parts.get(2);
    String pass = parts.size() < 4 ? null : parts.get(3);
    String driver = parts.size() < 5 ? null : parts.get(4);

    Properties props = new Properties();
    if (url != null) {
      props.setProperty("url", url);
    }
    if (driver != null) {
      props.setProperty("driver", driver);
    }
    if (user != null) {
      props.setProperty("user", user);
    }
    if (pass != null) {
      props.setProperty("password", pass);
    }

    connect(props, callback);
  }

  private static String getProperty(Properties props, String[] keys) {
    for (String key : keys) {
      String val = props.getProperty(key);
      if (val != null) {
        return val;
      }
    }

    //noinspection unchecked
    for (String key : (Set<String>) (Set) props.keySet()) {
      for (String key1 : keys) {
        if (key.endsWith(key1)) {
          return props.getProperty(key);
        }
      }
    }

    return null;
  }

  public void connect(Properties props, DispatchCallback callback)
      throws IOException {
    String url = getProperty(props, URL_PROPERTIES);
    String driver = getProperty(props, DRIVER_PROPERTIES);
    String username = getProperty(props, USER_PROPERTIES);
    String password = getProperty(props, PASSWORD_PROPERTIES);

    if (url == null || url.length() == 0) {
      callback.setToFailure();
      sqlLine.error("Property \"url\" is required");
      return;
    }
    if (driver == null || driver.length() == 0) {
      if (!sqlLine.scanForDriver(url)) {
        callback.setToFailure();
        sqlLine.error(sqlLine.loc("no-driver", url));
        return;
      }
    }

    sqlLine.debug("Connecting to " + url);

    if (username == null) {
      username = sqlLine.getConsoleReader()
          .readLine("Enter username for " + url + ": ");
    }
    if (password == null) {
      password = sqlLine.getConsoleReader()
          .readLine("Enter password for " + url + ": ", '*');
    }

    try {
      final Map<String, String> info = new LinkedHashMap<String, String>();
      final String url2 = sqlLine.fixUpUrl(url, info);
      sqlLine.getDatabaseConnections().setConnection(
          new DatabaseConnection(sqlLine, driver, url2, info,
              username, password));
      sqlLine.getDatabaseConnection().getConnection();
      sqlLine.runInit(callback);
      sqlLine.setCompletions();
      callback.setToSuccess();
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }

  public void rehash(String line, DispatchCallback callback) {
    try {
      if (!sqlLine.assertConnection()) {
        callback.setToFailure();
      }

      final DatabaseConnection databaseConnection =
          sqlLine.getDatabaseConnection();
      if (databaseConnection != null) {
        databaseConnection.setCompletions(false);
      }

      callback.setToSuccess();
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }

  /**
   * List the current connections
   */
  public void list(String line, DispatchCallback callback) {
    int index = 0;
    final DatabaseConnections databaseConnections =
        sqlLine.getDatabaseConnections();
    sqlLine.info(sqlLine.locChoice("active-connections",
        databaseConnections.size()));

    for (DatabaseConnection c : databaseConnections) {
      boolean closed;
      try {
        closed = c.connection.isClosed();
      } catch (Exception e) {
        closed = true;
      }

      sqlLine.output(
          sqlLine.getColorBuffer()
              .pad(" #" + index++ + "", 5)
              .pad(closed ? sqlLine.loc("closed") : sqlLine.loc("open"), 9)
              .append(c.getUrl()));
    }

    callback.setToSuccess();
  }

  public void all(String line, DispatchCallback callback) {
    final DatabaseConnections databaseConnections =
        sqlLine.getDatabaseConnections();
    final int index = databaseConnections.getIndex();
    boolean success = true;

    for (int i = 0; i < databaseConnections.size(); i++) {
      databaseConnections.setIndex(i);
      sqlLine.output(
          sqlLine.loc("executing-con", sqlLine.getDatabaseConnection()));

      // ### FIXME:  this is broken for multi-line SQL
      sql(line.substring("all ".length()), callback);
      success = callback.isSuccess() && success;
    }

    // restore index
    databaseConnections.setIndex(index);
    if (success) {
      callback.setToSuccess();
    } else {
      callback.setToFailure();
    }
  }

  public void go(String line, DispatchCallback callback) {
    List<String> parts = sqlLine.split(line, 2, "Usage: go <connection index>");
    if (parts == null) {
      callback.setToFailure();
      return;
    }

    int index = Integer.parseInt(parts.get(1));
    if (!(sqlLine.getDatabaseConnections().setIndex(index))) {
      sqlLine.error(sqlLine.loc("invalid-connection", "" + index));
      list("", callback); // list the current connections
      callback.setToFailure();
      return;
    }

    callback.setToSuccess();
  }

  /**
   * Save or stop saving a script to a file
   */
  public void script(String line, DispatchCallback callback) {
    if (sqlLine.getScriptOutputFile() == null) {
      startScript(line, callback);
    } else {
      stopScript(line, callback);
    }
  }

  /**
   * Stop writing to the script file and close the script.
   */
  private void stopScript(String line, DispatchCallback callback) {
    try {
      sqlLine.getScriptOutputFile().close();
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    sqlLine.output(sqlLine.loc("script-closed", sqlLine.getScriptOutputFile()));
    sqlLine.setScriptOutputFile(null);
    callback.setToSuccess();
  }

  /**
   * Start writing to the specified script file.
   */
  private void startScript(String line, DispatchCallback callback) {
    if (sqlLine.getScriptOutputFile() != null) {
      callback.setToFailure();
      sqlLine.error(
          sqlLine.loc("script-already-running", sqlLine.getScriptOutputFile()));
      return;
    }

    List<String> parts = sqlLine.split(line, 2, "Usage: script <filename>");
    if (parts == null) {
      callback.setToFailure();
      return;
    }

    try {
      sqlLine.setScriptOutputFile(new OutputFile(parts.get(1)));
      sqlLine.output(
          sqlLine.loc("script-started", sqlLine.getScriptOutputFile()));
      callback.setToSuccess();
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }

  /**
   * Run a script from the specified file.
   */
  public void run(String line, DispatchCallback callback) {
    List<String> parts = sqlLine.split(line, 2, "Usage: run <scriptfile>");
    if (parts == null) {
      callback.setToFailure();
      return;
    }

    List<String> cmds = new LinkedList<String>();

    try {
      BufferedReader reader =
          new BufferedReader(new FileReader(parts.get(1)));
      try {
        // ### NOTE: fix for sf.net bug 879427
        StringBuilder cmd = null;
        for (;;) {
          String scriptLine = reader.readLine();

          if (scriptLine == null) {
            break;
          }

          String trimmedLine = scriptLine.trim();
          if (sqlLine.getOpts().getTrimScripts()) {
            scriptLine = trimmedLine;
          }

          if (cmd != null) {
            // we're continuing an existing command
            cmd.append(" \n");
            cmd.append(scriptLine);
            if (trimmedLine.endsWith(";")) {
              // this command has terminated
              cmds.add(cmd.toString());
              cmd = null;
            }
          } else {
            // we're starting a new command
            if (sqlLine.needsContinuation(scriptLine)) {
              // multi-line
              cmd = new StringBuilder(scriptLine);
            } else {
              // single-line
              cmds.add(scriptLine);
            }
          }
        }

        if (cmd != null) {
          // ### REVIEW: oops, somebody left the last command
          // unterminated; should we fix it for them or complain?
          // For now be nice and fix it.
          cmd.append(";");
          cmds.add(cmd.toString());
        }
      } finally {
        reader.close();
      }

      // success only if all the commands were successful
      if (sqlLine.runCommands(cmds, callback) == cmds.size()) {
        callback.setToSuccess();
      } else {
        callback.setToFailure();
      }
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
      return;
    }
  }

  /**
   * Save or stop saving all output to a file.
   */
  public void record(String line, DispatchCallback callback) {
    if (sqlLine.getRecordOutputFile() == null) {
      startRecording(line, callback);
    } else {
      stopRecording(line, callback);
    }
  }

  /**
   * Stop writing output to the record file.
   */
  private void stopRecording(String line, DispatchCallback callback) {
    try {
      sqlLine.getRecordOutputFile().close();
    } catch (Exception e) {
      sqlLine.handleException(e);
    }
    sqlLine.setRecordOutputFile(null);
    sqlLine.output(sqlLine.loc("record-closed", sqlLine.getRecordOutputFile()));
    callback.setToSuccess();
  }

  /**
   * Start writing to the specified record file.
   */
  private void startRecording(String line, DispatchCallback callback) {
    if (sqlLine.getRecordOutputFile() != null) {
      callback.setToFailure();
      sqlLine.error(
          sqlLine.loc("record-already-running", sqlLine.getRecordOutputFile()));
      return;
    }

    List<String> parts = sqlLine.split(line, 2, "Usage: record <filename>");
    if (parts == null) {
      callback.setToFailure();
      return;
    }

    try {
      final OutputFile recordOutput = new OutputFile(parts.get(1));
      sqlLine.output(sqlLine.loc("record-started", recordOutput));
      sqlLine.setRecordOutputFile(recordOutput);
      callback.setToSuccess();
    } catch (Exception e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }

  public void describe(String line, DispatchCallback callback)
      throws SQLException {
    String[][] cmd = sqlLine.splitCompound(line);
    if (cmd.length != 2) {
      sqlLine.error("Usage: describe <table name>");
      callback.setToFailure();
      return;
    }

    if (cmd[1].length == 1
        && cmd[1][0] != null
        && cmd[1][0].equalsIgnoreCase("tables")) {
      tables("tables", callback);
    } else {
      columns(line, callback);
    }
  }

  public void help(String line, DispatchCallback callback) {
    List<String> parts = sqlLine.split(line);
    String cmd = (parts.size() > 1) ? parts.get(1) : "";
    TreeSet<ColorBuffer> colorBuffers = new TreeSet<ColorBuffer>();

    for (CommandHandler commandHandler : sqlLine.commandHandlers) {
      if (cmd.length() == 0
          || Arrays.asList(commandHandler.getNames()).contains(cmd)) {
        colorBuffers.add(sqlLine.getColorBuffer()
            .pad("!" + commandHandler.getName(), 20)
            .append(SqlLine.wrap(commandHandler.getHelpText(), 60, 20)));
      }
    }

    for (ColorBuffer colorBuffer : colorBuffers) {
      sqlLine.output(colorBuffer);
    }

    if (cmd.length() == 0) {
      sqlLine.output("");
      sqlLine.output(
          sqlLine.loc("comments", sqlLine.getApplicationContactInformation()));
    }

    callback.setToSuccess();
  }

  public void manual(String line, DispatchCallback callback)
      throws IOException {
    InputStream in = SqlLine.class.getResourceAsStream("manual.txt");
    if (in == null) {
      callback.setToFailure();
      sqlLine.error(sqlLine.loc("no-manual"));
      return;
    }

    final BufferedReader breader =
        new BufferedReader(new InputStreamReader(in));
    final ConsoleReader consoleReader = sqlLine.getConsoleReader();
    final int page = sqlLine.getOpts().getMaxHeight() - 1;
    String man;
    int index = 0;
    while ((man = breader.readLine()) != null) {
      index++;
      sqlLine.output(man);

      // silly little pager
      if (consoleReader != null && index % page == 0) {
        String ret = consoleReader.readLine(sqlLine.loc("enter-for-more"));
        if (ret != null && ret.startsWith("q")) {
          break;
        }
      }
    }

    breader.close();

    callback.setToSuccess();
  }

  public void nullemptystring(String line, DispatchCallback callback) {
    // "nullemptystring foo" becomes "set nullemptystring foo"
    set("set " + line, callback);
  }

  /** Executes an operating system command, and deals with stdout and stderr.
   *
   * <p>Copied from org.apache.hadoop.hive.common.cli. */
  private static class ShellCmdExecutor {
    private String cmd;
    private PrintStream out;
    private PrintStream err;

    public ShellCmdExecutor(String cmd, PrintStream out, PrintStream err) {
      this.cmd = cmd;
      this.out = out;
      this.err = err;
    }

    public int execute() throws Exception {
      try {
        Process executor = Runtime.getRuntime().exec(cmd);
        StreamPrinter outPrinter =
            new StreamPrinter(executor.getInputStream(), null, out);
        StreamPrinter errPrinter =
            new StreamPrinter(executor.getErrorStream(), null, err);

        outPrinter.start();
        errPrinter.start();

        int ret = executor.waitFor();
        outPrinter.join();
        errPrinter.join();
        return ret;
      } catch (IOException ex) {
        throw new Exception("Failed to execute " + cmd, ex);
      }
    }
  }

  /** Copied from org.apache.hive.common.util. */
  private static class StreamPrinter extends Thread {
    final InputStream is;
    final String type;
    final PrintStream os;

    public StreamPrinter(InputStream is, String type, PrintStream os) {
      this.is = is;
      this.type = type;
      this.os = os;
    }

    @Override
    public void run() {
      BufferedReader br = null;
      try {
        InputStreamReader isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        String line;
        if (type != null) {
          while ((line = br.readLine()) != null) {
            os.println(type + ">" + line);
          }
        } else {
          while ((line = br.readLine()) != null) {
            os.println(line);
          }
        }
        br.close();
        br = null;
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } finally {
        closeStream(br);
      }
    }
  }
}
