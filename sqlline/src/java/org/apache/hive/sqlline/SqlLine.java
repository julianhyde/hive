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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.ChoiceFormat;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import jline.Terminal;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;

/**
 * A console SQL shell with command completion.
 * <p>
 * TODO:
 * <ul>
 * <li>User-friendly connection prompts</li>
 * <li>Page results</li>
 * <li>Handle binary data (blob fields)</li>
 * <li>Implement command aliases</li>
 * <li>Stored procedure execution</li>
 * <li>Binding parameters to prepared statements</li>
 * <li>Scripting language</li>
 * <li>XA transactions</li>
 * </ul>
 */
public class SqlLine {
  private static final ResourceBundle RESOURCE_BUNDLE =
      ResourceBundle.getBundle(SqlLine.class.getName());

  private static final String SEPARATOR = System.getProperty("line.separator");
  public static final String COMMAND_PREFIX = "!";
  private static final String[] SPACES = {
    "",
    " ",
    "  ",
    "   ",
    "    ",
    "     ",
    "      ",
    "       ",
    "        ",
    "         ",
    "          ",
    "           ",
    "            ",
    "             ",
    "              ",
    "               ",
    "                ",
    "                 ",
    "                  ",
    "                   ",
    "                    ",
    "                     ",
    "                      ",
    "                       ",
    "                        ",
  };

  private static boolean initComplete = false;

  private final String defaultJdbcDriver;
  private final String defaultJdbcUrl;
  private boolean exit = false;
  private final DatabaseConnections connections = new DatabaseConnections();
  private Collection<Driver> drivers = null;
  private final SqlLineOpts opts;
  private String lastProgress = null;
  private final Map<SQLWarning, Date> seenWarnings =
      new HashMap<SQLWarning, Date>();
  private final Commands commands = new Commands(this);
  private OutputFile scriptOutputFile = null;
  private OutputFile recordOutputFile = null;
  private PrintStream outputStream = new PrintStream(System.out, true);
  private PrintStream errorStream = new PrintStream(System.err, true);
  private ConsoleReader consoleReader;
  private List<String> batch = null;
  private final Reflector reflector;

  // saveDir() is used in various opts that assume it's set. But that means
  // properties starting with "sqlline" are read into props in unspecific
  // order using reflection to find setter methods. Avoid
  // confusion/NullPointer due about order of config by prefixing it.
  public static final String SQLLINE_BASE_DIR = "x.sqlline.basedir";

  static final Object[] EMPTY_OBJ_ARRAY = new Object[0];

  private final SqlLineSignalHandler signalHandler;
  private final Completer sqlLineCommandCompleter;

  private final Map<String, Object> formats = map(
      "vertical", new VerticalOutputFormat(this),
      "table", new TableOutputFormat(this),
      "csv", new SeparatedValuesOutputFormat(this, ','),
      "tsv", new SeparatedValuesOutputFormat(this, '\t'),
      "xmlattr", new XmlAttributeOutputFormat(this),
      "xmlelements", new XmlElementOutputFormat(this));

  final CommandHandler[] commandHandlers;

  final CommandHandler[] createCommandHandlers() {
    return new CommandHandler[] {
      new ReflectiveCommandHandler(this, new String[] {"quit", "done", "exit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"connect", "open"},
          new Completer[] {new StringsCompleter(getConnectionURLExamples())}),
      new ReflectiveCommandHandler(this, new String[] {"describe"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"indexes"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"primarykeys"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"exportedkeys"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"manual"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"importedkeys"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"procedures"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"tables"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"typeinfo"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"columns"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"reconnect"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"dropall"},
          new Completer[] {new TableNameCompleter(this)}),
      new ReflectiveCommandHandler(this, new String[] {"history"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"metadata"},
          new Completer[] {new StringsCompleter(getMetadataMethodNames())}),
      new ReflectiveCommandHandler(this, new String[] {"nativesql"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"dbinfo"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"rehash"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"verbose"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"run"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"batch"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"list"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"all"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"go", "#"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"script"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"record"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"brief"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"close"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"closeall"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"isolation"},
          new Completer[] {new StringsCompleter(getIsolationLevels())}),
      new ReflectiveCommandHandler(this, new String[] {"outputformat"},
          new Completer[] {new StringsCompleter(formats.keySet())}),
      new ReflectiveCommandHandler(this, new String[] {"autocommit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"commit"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"properties"},
          new Completer[] {new FileNameCompleter()}),
      new ReflectiveCommandHandler(this, new String[] {"rollback"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"help", "?"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"set"},
          getOpts().optionCompleters()),
      new ReflectiveCommandHandler(this, new String[] {"save"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"scan"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"sql"},
          null),
      new ReflectiveCommandHandler(this, new String[] {"call"},
          null),
    };
  }

  static final SortedSet<String> KNOWN_DRIVERS = new TreeSet<String>(
      Arrays.asList(
          "com.merant.datadirect.jdbc.sqlserver.SQLServerDriver",
          "com.microsoft.jdbc.sqlserver.SQLServerDriver",
          "com.ddtek.jdbc.informix.InformixDriver",
          "org.sourceforge.jxdbcon.JXDBConDriver",
          "com.ddtek.jdbc.oracle.OracleDriver",
          "net.sourceforge.jtds.jdbc.Driver",
          "com.pointbase.jdbc.jdbcDriver",
          "com.internetcds.jdbc.tds.SybaseDriver",
          "org.enhydra.instantdb.jdbc.idbDriver",
          "com.sybase.jdbc2.jdbc.SybDriver",
          "com.ddtek.jdbc.sybase.SybaseDriver",
          "COM.cloudscape.core.JDBCDriver",
          "in.co.daffodil.db.jdbc.DaffodilDBDriver",
          "com.jnetdirect.jsql.JSQLDriver",
          "com.lucidera.jdbc.LucidDbRmiDriver",
          "COM.ibm.db2.jdbc.net.DB2Driver",
          "org.hsqldb.jdbcDriver",
          "com.pointbase.jdbc.jdbcUniversalDriver",
          "com.ddtek.jdbc.sqlserver.SQLServerDriver",
          "com.ddtek.jdbc.db2.DB2Driver",
          "com.merant.datadirect.jdbc.oracle.OracleDriver",
          "oracle.jdbc.OracleDriver",
          "com.informix.jdbc.IfxDriver",
          "com.merant.datadirect.jdbc.informix.InformixDriver",
          "com.ibm.db2.jcc.DB2Driver",
          "com.pointbase.jdbc.jdbcEmbeddedDriver",
          "org.gjt.mm.mysql.Driver",
          "org.postgresql.Driver",
          "com.mysql.jdbc.Driver",
          "oracle.jdbc.driver.OracleDriver",
          "interbase.interclient.Driver",
          "com.mysql.jdbc.NonRegisteringDriver",
          "com.merant.datadirect.jdbc.db2.DB2Driver",
          "com.merant.datadirect.jdbc.sybase.SybaseDriver",
          "com.internetcds.jdbc.tds.Driver",
          "org.hsqldb.jdbcDriver",
          "org.hsql.jdbcDriver",
          "COM.cloudscape.core.JDBCDriver",
          "in.co.daffodil.db.jdbc.DaffodilDBDriver",
          "com.ddtek.jdbc.db2.DB2Driver",
          "interbase.interclient.Driver",
          "com.mysql.jdbc.Driver",
          "com.ddtek.jdbc.oracle.OracleDriver",
          "org.postgresql.Driver",
          "com.pointbase.jdbc.jdbcUniversalDriver",
          "org.sourceforge.jxdbcon.JXDBConDriver",
          "com.ddtek.jdbc.sqlserver.SQLServerDriver",
          "com.jnetdirect.jsql.JSQLDriver",
          "com.microsoft.jdbc.sqlserver.SQLServerDriver",
          "weblogic.jdbc.mssqlserver4.Driver",
          "com.ddtek.jdbc.sybase.SybaseDriver",
          "oracle.jdbc.pool.OracleDataSource",
          "org.axiondb.jdbc.AxionDriver",
          "COM.ibm.db2.jdbc.app.DB2Driver",
          "com.ibm.as400.access.AS400JDBCDriver",
          "COM.FirstSQL.Dbcp.DbcpDriver",
          "COM.ibm.db2.jdbc.net.DB2Driver",
          "org.enhydra.instantdb.jdbc.idbDriver",
          "com.informix.jdbc.IfxDriver",
          "com.microsoft.jdbc.sqlserver.SQLServerDriver",
          "com.imaginary.sql.msql.MsqlDriver",
          "sun.jdbc.odbc.JdbcOdbcDriver",
          "oracle.jdbc.driver.OracleDriver",
          "intersolv.jdbc.sequelink.SequeLinkDriver",
          "openlink.jdbc2.Driver",
          "com.pointbase.jdbc.jdbcUniversalDriver",
          "postgres95.PGDriver",
          "postgresql.Driver",
          "solid.jdbc.SolidDriver",
          "centura.java.sqlbase.SqlbaseDriver",
          "interbase.interclient.Driver",
          "com.mckoi.JDBCDriver",
          "com.inet.tds.TdsDriver",
          "com.microsoft.jdbc.sqlserver.SQLServerDriver",
          "com.thinweb.tds.Driver",
          "weblogic.jdbc.mssqlserver4.Driver",
          "com.mysql.jdbc.DatabaseMetaData",
          "org.gjt.mm.mysql.Driver",
          "com.sap.dbtech.jdbc.DriverSapDB",
          "com.sybase.jdbc2.jdbc.SybDriver",
          "com.sybase.jdbc.SybDriver",
          "com.internetcds.jdbc.tds.Driver",
          "weblogic.jdbc.pool.Driver",
          "com.sqlstream.jdbc.Driver",
          "org.luciddb.jdbc.LucidDbClientDriver",
          "org.apache.hive.jdbc.HiveDriver",
          "org.apache.hadoop.hive.jdbc.HiveDriver"));

  static {
    String testClass = "jline.console.ConsoleReader";
    try {
      Class.forName(testClass);
    } catch (Throwable t) {
      String message =
          locStatic(RESOURCE_BUNDLE, System.err, "jline-missing", testClass);
      throw new ExceptionInInitializerError(message);
    }
  }

  static Manifest getManifest() throws IOException {
    URL base = SqlLine.class.getResource("/META-INF/MANIFEST.MF");
    URLConnection c = base.openConnection();
    if (c instanceof JarURLConnection) {
      return ((JarURLConnection) c).getManifest();
    }
    return null;
  }

  String getManifestAttribute(String name) {
    try {
      Manifest m = getManifest();
      if (m == null) {
        return "??";
      }

      Attributes attrs = m.getAttributes("sqlline");
      if (attrs == null) {
        return "???";
      }

      String val = attrs.getValue(name);
      if (val == null || "".equals(val)) {
        return "????";
      }

      return val;
    } catch (Exception e) {
      e.printStackTrace(errorStream);
      return "?????";
    }
  }

    /** Returns the name of the application.
     *
     * <p>Default is built by substituting artifactId and version from
     * pom.properties into a string, returning something like "sqlline
     * version 1.6". Derived class may override. */
  protected String getApplicationTitle() {
    InputStream inputStream =
        getClass().getResourceAsStream(
            "/META-INF/maven/sqlline/sqlline/pom.properties");
    Properties properties = new Properties();
    properties.put("artifactId", "sqlline");
    properties.put("version", "???");
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      handleException(e);
    }

    return loc(
        "app-introduction",
        properties.getProperty("artifactId"),
        properties.getProperty("version"));
  }

  String getApplicationContactInformation() {
    return getManifestAttribute("Implementation-Vendor");
  }

  String loc(String res) {
    return loc(res, EMPTY_OBJ_ARRAY);
  }

  String loc(String res, int param) {
    try {
      return MessageFormat.format(
          new ChoiceFormat(RESOURCE_BUNDLE.getString(res)).format(param),
          new Object[] {new Integer(param)});
    } catch (Exception e) {
      return res + ": " + param;
    }
  }

  public String loc(String res, Object param1) {
    return loc(res, new Object[] {param1});
  }

  String loc(String res, Object param1, Object param2) {
    return loc(res, new Object[] {param1, param2});
  }

  String loc(String res, Object[] params) {
    return locStatic(RESOURCE_BUNDLE, getErrorStream(), res, params);
  }

  static String locStatic(ResourceBundle resourceBundle, PrintStream err,
      String res, Object... params) {
    try {
      return MessageFormat.format(resourceBundle.getString(res), params);
    } catch (Exception e) {
      e.printStackTrace(err);
      try {
        return res + ": " + Arrays.asList(params);
      } catch (Exception e2) {
        return res;
      }
    }
  }

  protected String locElapsedTime(long milliseconds) {
    return loc("time-ms", new Object[] {milliseconds / 1000d});
  }

  /**
   * Starts the program.
   */
  public static void main(String[] args)
      throws IOException {
    final SqlLine sqlLine = new SqlLine(false, null, null);
    sqlLine.start2(Arrays.asList(args), null, true);
  }

  /**
   * Starts the program with redirected input. For redirected output,
   * System.setOut and System.setErr can be used, but System.setIn will not
   * work.
   *
   * @param args        same as main()
   * @param inputStream redirected input, or null to use standard input
   */
  public static boolean mainWithInputRedirection(
      String[] args,
      InputStream inputStream)
      throws IOException {
    final SqlLine sqlLine = new SqlLine(false, null, null);
    return sqlLine.start2(Arrays.asList(args), inputStream, false);
  }

  protected SqlLine(boolean withSignalHandler, String defaultJdbcDriver,
      String defaultJdbcUrl) {
    this.defaultJdbcDriver = defaultJdbcDriver;
    this.defaultJdbcUrl = defaultJdbcUrl;
    this.opts = createOpts();
    this.commandHandlers = createCommandHandlers();
    sqlLineCommandCompleter = new SqlLineCommandCompleter(this);
    reflector = new Reflector(this);

    // attempt to dynamically load signal handler
    SqlLineSignalHandler signalHandler = null;
    if (withSignalHandler) {
      try {
        Class handlerClass =
            Class.forName("org.apache.hive.sqlline.SunSignalHandler");
        signalHandler = (SqlLineSignalHandler) handlerClass.newInstance();
      } catch (Throwable t) {
        handleException(t);
      }
    }
    this.signalHandler = signalHandler;
  }

  /** Creates and initializes the options object. Derived class may override. */
  protected SqlLineOpts createOpts() {
    return new SqlLineOpts(this, System.getProperties(), "sqlline.properties",
        "sqlline.rcfile");
  }

  /**
   * Backwards compatibility method to allow
   * {@link #mainWithInputRedirection(String[], java.io.InputStream)} proxied
   * calls to keep method signature but add in new behavior of not saving
   * queries.
   *
   * @param args        args[] passed in directly from {@link #main(String[])}
   * @param inputStream Stream to read sql commands from (stdin or a file) or
   *                    null for an interactive shell
   * @param saveHistory whether or not the commands issued will be saved to
   *                    sqlline's history file
   * @throws IOException
   */
  public static boolean start(
      String[] args,
      InputStream inputStream,
      boolean saveHistory) throws IOException {
    return new SqlLine(false, null, null)
        .start2(Arrays.asList(args), inputStream, saveHistory);
  }

  protected boolean start2(
      List<String> args,
      InputStream inputStream,
      boolean saveHistory) throws IOException {
    boolean retVal = begin(args, inputStream, saveHistory);

    // exit the system: useful for Hypersonic and other
    // badly-behaving systems
    if (!Boolean.getBoolean(SqlLineOpts.PROPERTY_NAME_EXIT)) {
      System.exit(retVal ? 1 : 0);
    }

    return retVal;
  }

  DatabaseConnection getDatabaseConnection() {
    return getDatabaseConnections().current();
  }

  Connection getConnection() throws SQLException {
    final DatabaseConnection databaseConnection =
        getDatabaseConnections().current();
    if (databaseConnection == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    final Connection connection = databaseConnection.getConnection();
    if (connection == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    return connection;
  }

  DatabaseMetaData getDatabaseMetaData() {
    final DatabaseConnection databaseConnection =
        getDatabaseConnections().current();
    if (databaseConnection == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    final DatabaseMetaData metaData = databaseConnection.getDatabaseMetaData();
    if (metaData == null) {
      throw new IllegalArgumentException(loc("no-current-connection"));
    }
    return metaData;
  }

  public String[] getIsolationLevels() {
    return new String[] {
      "TRANSACTION_NONE",
      "TRANSACTION_READ_COMMITTED",
      "TRANSACTION_READ_UNCOMMITTED",
      "TRANSACTION_REPEATABLE_READ",
      "TRANSACTION_SERIALIZABLE",
    };
  }

  public String[] getMetadataMethodNames() {
    try {
      TreeSet<String> mnames = new TreeSet<String>();
      Method[] m = DatabaseMetaData.class.getDeclaredMethods();
      for (int i = 0; m != null && i < m.length; i++) {
        mnames.add(m[i].getName());
      }
      return mnames.toArray(new String[mnames.size()]);
    } catch (Throwable t) {
      return new String[0];
    }
  }

  public String[] getConnectionURLExamples() {
    return new String[] {
      "jdbc:JSQLConnect://<hostname>/database=<database>",
      "jdbc:cloudscape:<database>;create=true",
      "jdbc:twtds:sqlserver://<hostname>/<database>",
      "jdbc:daffodilDB_embedded:<database>;create=true",
      "jdbc:datadirect:db2://<hostname>:50000;databaseName=<database>",
      "jdbc:inetdae:<hostname>:1433",
      "jdbc:datadirect:oracle://<hostname>:1521;SID=<database>;MaxPooledStatements=0",
      "jdbc:datadirect:sqlserver://<hostname>:1433;SelectMethod=cursor;DatabaseName=<database>",
      "jdbc:datadirect:sybase://<hostname>:5000",
      "jdbc:db2://<hostname>/<database>",
      "jdbc:hive2://<hostname>",
      "jdbc:hsqldb:<database>",
      "jdbc:idb:<database>.properties",
      "jdbc:informix-sqli://<hostname>:1526/<database>:INFORMIXSERVER=<database>",
      "jdbc:interbase://<hostname>//<database>.gdb",
      "jdbc:luciddb:http://<hostname>",
      "jdbc:microsoft:sqlserver://<hostname>:1433;DatabaseName=<database>;SelectMethod=cursor",
      "jdbc:mysql://<hostname>/<database>?autoReconnect=true",
      "jdbc:oracle:thin:@<hostname>:1521:<database>",
      "jdbc:pointbase:<database>,database.home=<database>,create=true",
      "jdbc:postgresql://<hostname>:5432/<database>",
      "jdbc:postgresql:net//<hostname>/<database>",
      "jdbc:sybase:Tds:<hostname>:4100/<database>?ServiceName=<database>",
      "jdbc:weblogic:mssqlserver4:<database>@<hostname>:1433",
      "jdbc:odbc:<database>",
      "jdbc:sequelink://<hostname>:4003/[Oracle]",
      "jdbc:sequelink://<hostname>:4004/[Informix];Database=<database>",
      "jdbc:sequelink://<hostname>:4005/[Sybase];Database=<database>",
      "jdbc:sequelink://<hostname>:4006/[SQLServer];Database=<database>",
      "jdbc:sequelink://<hostname>:4011/[ODBC MS Access];Database=<database>",
      "jdbc:openlink://<hostname>/DSN=SQLServerDB/UID=sa/PWD=",
      "jdbc:solid://<hostname>:<port>/<UID>/<PWD>",
      "jdbc:dbaw://<hostname>:8889/<database>",
    };
  }

  /**
   * Entry point to creating a {@link ColorBuffer} with color
   * enabled or disabled depending on the value of {@link SqlLineOpts#getColor}.
   */
  ColorBuffer getColorBuffer() {
    return new ColorBuffer(getOpts().getColor());
  }

  /**
   * Entry point to creating a {@link ColorBuffer} with color enabled or
   * disabled depending on the value of {@link SqlLineOpts#getColor}.
   */
  ColorBuffer getColorBuffer(String msg) {
    return new ColorBuffer(msg, getOpts().getColor());
  }

  /**
   * Walk through all the known drivers and try to register them.
   */
  void registerKnownDrivers() {
    for (String className : getKnownDrivers()) {
      try {
        Class.forName(className);
      } catch (Throwable t) {
        // ignore
      }
    }
  }

  /** Returns class names of the JDBC drivers that should be registered by
   * default. Derived class may override. */
  protected Collection<String> getKnownDrivers() {
    return KNOWN_DRIVERS;
  }

  boolean initArgs(List<String> args) {
    List<String> commands = new LinkedList<String>();
    List<String> files = new LinkedList<String>();
    String driver = null;
    String user = null;
    String pass = null;
    String url = null;

    for (int i = 0; i < args.size(); i++) {
      final String arg = args.get(i);
      if (arg.equals("--help") || arg.equals("-h")) {
        usage();
        return false;
      }

      // -- arguments are treated as properties
      if (arg.startsWith("--")) {
        List<String> parts = split(arg.substring(2), "=");
        debug(loc("setting-prop", parts));
        if (parts.size() > 0) {
          boolean ret;

          if (parts.size() >= 2) {
            ret = getOpts().set(parts.get(0), parts.get(1), true);
          } else {
            ret = getOpts().set(parts.get(0), "true", true);
          }

          if (!ret) {
            return false;
          }
        }
        continue;
      }

      if (arg.equals("-d")) {
        driver = args.get(++i);
      } else if (arg.equals("-n")) {
        user = args.get(++i);
      } else if (arg.equals("-p")) {
        pass = args.get(++i);
      } else if (arg.equals("-u")) {
        url = args.get(++i);
      } else if (arg.equals("-e")) {
        commands.add(args.get(++i));
      } else {
        files.add(arg);
      }
    }

    if (url == null) {
      url = defaultJdbcUrl;
    }
    if (driver == null) {
      driver = defaultJdbcDriver;
    }

    if (url != null) {
      String com = COMMAND_PREFIX + "connect "
          + url + " "
          + (user == null || user.length() == 0 ? "''" : user) + " "
          + (pass == null || pass.length() == 0 ? "''" : pass) + " "
          + (driver == null ? "" : driver);
      debug("issuing: " + com);
      dispatch(com, new DispatchCallback());
    }

    // now load properties files
    for (String file : files) {
      dispatch(COMMAND_PREFIX + "properties " + file, new DispatchCallback());
    }

    if (commands.size() > 0) {
      // for single command execute, disable color
      getOpts().setColor(false);
      getOpts().setHeaderInterval(-1);

      for (String command : commands) {
        debug(loc("executing-command", command));
        dispatch(command, new DispatchCallback());
      }
      exit = true; // execute and exit
    }

    // if a script file was specified, run the file and quit
    if (getOpts().getRun() != null) {
      dispatch(COMMAND_PREFIX + "run " + getOpts().getRun(),
          new DispatchCallback());
      dispatch(COMMAND_PREFIX + "quit", new DispatchCallback());
    }
    return true;
  }

  /**
   * Starts accepting input from an input stream, and dispatches commands from
   * that input to the appropriate {@link CommandHandler} until the global
   * variable <code>exit</code> is true.
   *
   * @return Whether successful
   */
  public boolean begin(List<String> args, InputStream inputStream,
      boolean saveHistory)
      throws IOException {
    try {
      // load the options first, so we can override on the command line
      getOpts().load();
    } catch (Exception e) {
      handleException(e);
    }

    FileHistory fileHistory =
        new FileHistory(new File(opts.getHistoryFile()));
    ConsoleReader reader = getConsoleReader(inputStream, fileHistory);
    if (!initArgs(args)) {
      usage();
      return false;
    }

    try {
      info(getApplicationTitle());
    } catch (Exception e) {
      handleException(e);
    }

    // basic setup done. From this point on, honor opts value for showing
    // exception
    initComplete = true;
    DispatchCallback callback = new DispatchCallback();
    while (!exit) {
      try {
        if (signalHandler != null) {
          signalHandler.setCallback(callback);
        }
        dispatch(reader.readLine(getPrompt()), callback);
        if (saveHistory) {
          fileHistory.flush();
        }
      } catch (EOFException eof) {
        // CTRL-D
        commands.quit(null, callback);
      } catch (UserInterruptException ioe) {
        // CTRL-C
        try {
          callback.forceKillSqlQuery();
          callback.setToCancel();
          output(loc("command-canceled"));
        } catch (SQLException sqle) {
          handleException(sqle);
        }
      } catch (Throwable t) {
        handleException(t);
        callback.setToFailure();
      }
    }
    // ### NOTE jvs 10-Aug-2004: Clean up any outstanding
    // connections automatically.
    // nothing is done with the callback beyond
    commands.closeall(null, new DispatchCallback());
    return callback.isSuccess();
  }

  public void close() {
    final DispatchCallback callback = new DispatchCallback();
    commands.quit(null, callback);
    commands.closeall(null, callback);
  }

  public ConsoleReader getConsoleReader(
      InputStream inputStream, FileHistory fileHistory)
      throws IOException {
    Terminal terminal = TerminalFactory.create();
    try {
      terminal.init();
    } catch (Exception e) {
      // For backwards compatibility with code that used to use this lib
      // and expected only IOExceptions, convert back to that. We can't
      // use IOException(Throwable) constructor, which is only JDK 1.6 and
      // later.
      final IOException ioException = new IOException(e.toString());
      ioException.initCause(e);
      throw ioException;
    }
    if (inputStream != null) {
      // ### NOTE:  fix for sf.net bug 879425.
      consoleReader = new ConsoleReader(inputStream, System.out);
    } else {
      consoleReader = new ConsoleReader();
    }

    consoleReader.addCompleter(new SqlLineCompleter(this));
    consoleReader.setHistory(fileHistory);
    consoleReader.setHandleUserInterrupt(true); // CTRL-C handling
    consoleReader.setExpandEvents(false);

    return consoleReader;
  }

  void usage() {
    output(loc("cmd-usage"));
  }

  /**
   * Dispatch the specified line to the appropriate {@link CommandHandler}.
   *
   * @param line the command-line to dispatch
   */
  void dispatch(String line, DispatchCallback callback) {
    if (line == null) {
      // exit
      exit = true;
      return;
    }

    if (line.trim().length() == 0) {
      return;
    }

    if (isComment(line)) {
      return;
    }

    line = line.trim();

    // save it to the current script, if any
    if (scriptOutputFile != null) {
      scriptOutputFile.addLine(line);
    }

    if (isHelpRequest(line)) {
      line = COMMAND_PREFIX + "help";
    }

    if (line.startsWith(COMMAND_PREFIX)) {
      final Map<String, CommandHandler> cmdMap =
          new TreeMap<String, CommandHandler>();
      line = line.substring(1);
      for (CommandHandler commandHandler : commandHandlers) {
        String match = commandHandler.matches(line);
        if (match != null) {
          cmdMap.put(match, commandHandler);
        }
      }

      if (cmdMap.size() == 0) {
        callback.setStatus(DispatchCallback.Status.FAILURE);
        error(loc("unknown-command", line));
      } else if (cmdMap.size() > 1) {
        callback.setStatus(DispatchCallback.Status.FAILURE);
        error(loc("multiple-matches", cmdMap.keySet().toString()));
      } else {
        callback.setStatus(DispatchCallback.Status.RUNNING);
        final CommandHandler commandHandler = cmdMap.values().iterator().next();
        commandHandler.execute(line, callback);
      }
    } else {
      callback.setStatus(DispatchCallback.Status.RUNNING);
      commands.sql(line, callback);
    }
  }

  /**
   * Test whether a line requires a continuation.
   *
   * @param line the line to be tested
   * @return true if continuation required
   */
  boolean needsContinuation(String line) {
    if (null == line) {
      // happens when CTRL-C used to exit a malformed.
      return false;
    }

    if (isHelpRequest(line)) {
      return false;
    }

    if (line.startsWith(COMMAND_PREFIX)) {
      return false;
    }

    if (isComment(line)) {
      return false;
    }

    String trimmed = line.trim();

    if (trimmed.length() == 0) {
      return false;
    }
    return !trimmed.endsWith(";");
  }

  /**
   * Test whether a line is a help request other than !help.
   *
   * @param line the line to be tested
   * @return true if a help request
   */
  boolean isHelpRequest(String line) {
    return line.equals("?") || line.equalsIgnoreCase("help");
  }

  /**
   * Test whether a line is a comment.
   *
   * @param line the line to be tested
   * @return true if a comment
   */
  boolean isComment(String line) {
    // SQL92 comment prefix is "--"
    // sqlline also supports shell-style "#" prefix
    return line.startsWith("#") || line.startsWith("--");
  }

  /**
   * Print the specified message to the console
   *
   * @param msg the message to print
   */
  void output(String msg) {
    output(msg, true);
  }

  void info(String msg) {
    if (!getOpts().isSilent()) {
      output(msg, true, getErrorStream());
    }
  }

  void info(ColorBuffer msg) {
    if (!getOpts().isSilent()) {
      output(msg, true, getErrorStream());
    }
  }

  /**
   * Issue the specified error message.
   *
   * @param msg the message to issue
   * @return false always
   */
  boolean error(String msg) {
    if (initComplete) {
      output(getColorBuffer().red(msg), true, getErrorStream());
    } else {
      // Write to the error stream directly. error() depends
      // on properties like text coloring that may not be set yet.
      getErrorStream().println(msg);
    }
    return false;
  }

  boolean error(Throwable t) {
    handleException(t);
    return false;
  }

  void debug(String msg) {
    if (getOpts().getVerbose()) {
      output(getColorBuffer().blue(msg), true, getErrorStream());
    }
  }

  void output(ColorBuffer msg) {
    output(msg, true);
  }

  void output(String msg, boolean newline, PrintStream out) {
    output(getColorBuffer(msg), newline, out);
  }

  void output(ColorBuffer msg, boolean newline) {
    output(msg, newline, getOutputStream());
  }

  void output(ColorBuffer msg, boolean newline, PrintStream out) {
    if (newline) {
      out.println(msg.getColor());
    } else {
      out.print(msg.getColor());
    }

    if (recordOutputFile == null) {
      return;
    }

    // only write to the record file if we are writing a line ...
    // otherwise we might get garbage from backspaces and such.
    if (newline) {
      recordOutputFile.addLine(msg.getMono()); // always just write mono
    }
  }

  /**
   * Print the specified message to the console
   *
   * @param msg     the message to print
   * @param newline if false, do not append a newline
   */
  void output(String msg, boolean newline) {
    output(getColorBuffer(msg), newline);
  }

  void autocommitStatus(Connection c)
      throws SQLException {
    debug(loc("autocommit-status", c.getAutoCommit() + ""));
  }

  /**
   * Ensure that autocommit is on for the current connection
   *
   * @return true if autocommit is set
   */
  boolean assertAutoCommit() {
    if (!(assertConnection())) {
      return false;
    }
    try {
      if (getDatabaseConnection().getConnection().getAutoCommit()) {
        return error(loc("autocommit-needs-off"));
      }
    } catch (Exception e) {
      return error(e);
    }
    return true;
  }

  /**
   * Assert that we have an active, living connection. Print an error message
   * if we do not.
   *
   * @return true if there is a current, active connection
   */
  boolean assertConnection() {
    try {
      final DatabaseConnection databaseConnection = getDatabaseConnection();
      if (databaseConnection == null) {
        return error(loc("no-current-connection"));
      }
      final Connection connection = databaseConnection.getConnection();
      if (connection == null) {
        return error(loc("no-current-connection"));
      }
      if (connection.isClosed()) {
        return error(loc("connection-is-closed"));
      }
    } catch (SQLException sqle) {
      return error(loc("no-current-connection"));
    }
    return true;
  }

  /**
   * Print out any warnings that exist for the current connection.
   */
  void showWarnings() {
    try {
      final Connection connection = getDatabaseConnection().getConnection();
      if (connection == null) {
        return;
      }

      if (!getOpts().getShowWarnings()) {
        return;
      }

      showWarnings(connection.getWarnings());
    } catch (Exception e) {
      handleException(e);
    }
  }

  /**
   * Print the specified warning on the console, as well as any warnings that
   * are returned from {@link SQLWarning#getNextWarning}.
   *
   * @param warn the {@link SQLWarning} to print
   */
  void showWarnings(SQLWarning warn) {
    if (warn == null) {
      return;
    }

    if (seenWarnings.get(warn) == null) {
      // don't re-display warnings we have already seen
      seenWarnings.put(warn, new java.util.Date());
      handleSQLException(warn);
    }

    SQLWarning next = warn.getNextWarning();
    if (next != warn) {
      showWarnings(next);
    }
  }

  String getPrompt() {
    final DatabaseConnection databaseConnection = getDatabaseConnection();
    if (databaseConnection == null) {
      return "sqlline> ";
    }
    final String url = databaseConnection.getUrl();
    if (url == null) {
      return "sqlline> ";
    } else {
      return getPrompt(connections.getIndex() + ": " + url) + "> ";
    }
  }

  static String getPrompt(String url) {
    if (url == null || url.length() == 0) {
      url = "sqlline";
    }
    final int semicolon = url.indexOf(";");
    if (semicolon > -1) {
      url = url.substring(0, semicolon);
    }
    final int question = url.indexOf("?");
    if (question > -1) {
      url = url.substring(0, question);
    }
    if (url.length() > 45) {
      url = url.substring(0, 45);
    }
    return url;
  }

  /**
   * Try to obtain the current size of the specified {@link ResultSet} by
   * jumping to the last row and getting the row number.
   *
   * @param rs the {@link ResultSet} to get the size for
   * @return the size, or -1 if it could not be obtained
   */
  int getSize(ResultSet rs) {
    try {
      if (rs.getType() == ResultSet.TYPE_FORWARD_ONLY) {
        return -1;
      }
      rs.last();
      int total = rs.getRow();
      rs.beforeFirst();
      return total;
    } catch (SQLException sqle) {
      return -1;
    } catch (AbstractMethodError ame) {
      // JDBC 1 driver error
      return -1;
    }
  }

  ResultSet getColumns(String table) throws SQLException {
    if (!assertConnection()) {
      return null;
    }
    final DatabaseMetaData metaData = getDatabaseConnection().meta;
    final String catalog = metaData.getConnection().getCatalog();
    return metaData.getColumns(catalog, null, table, "%");
  }

  ResultSet getTables() throws SQLException {
    if (!assertConnection()) {
      return null;
    }
    final DatabaseMetaData metaData = getDatabaseConnection().meta;
    final String s = metaData.getConnection().getCatalog();
    return metaData.getTables(s, null, "%", new String[] {"TABLE"});
  }

  String[] getColumnNames(DatabaseMetaData meta) throws SQLException {
    final Set<String> names = new HashSet<String>();
    info(loc("building-tables"));
    try {
      ResultSet columns = getColumns("%");
      try {
        int total = getSize(columns);
        int index = 0;

        while (columns.next()) {
          // add the following strings:
          // 1. column name
          // 2. table name
          // 3. tablename.columnname

          progress(index++, total);
          String name = columns.getString("TABLE_NAME");
          names.add(name);
          names.add(columns.getString("COLUMN_NAME"));
          names.add(columns.getString("TABLE_NAME") + "."
              + columns.getString("COLUMN_NAME"));
        }
        progress(index, index);
      } finally {
        columns.close();
      }
      info(loc("done"));
      return names.toArray(new String[names.size()]);
    } catch (Throwable t) {
      handleException(t);
      return new String[0];
    }
  }


  // //////////////////
  // String utilities
  // //////////////////

  /** Returns an iterable that tokenizes a string. */
  public static Iterable<String> tokenize(final String str,
      final String delim) {
    return new Iterable<String>() {
      final StringTokenizer tokenizer = new StringTokenizer(str, delim);
      public Iterator<String> iterator() {
        return new Iterator<String>() {
          public boolean hasNext() {
            return tokenizer.hasMoreTokens();
          }

          public String next() {
            return tokenizer.nextToken();
          }

          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Split the line into a list by tokenizing on space characters
   *
   * @param line the line to break up
   * @return a list of individual words, never null
   */
  List<String> split(String line) {
    return split(line, " ");
  }

  /**
   * Splits the line into an array of possibly-compound identifiers, observing
   * the database's quoting syntax.
   * <p/>
   * <p>For example, on Oracle, which uses double-quote (&quot;) as quote
   * character,
   * <p/>
   * <blockquote>!tables "My Schema"."My Table"</blockquote>
   * <p/>
   * returns
   * <p/>
   * <blockquote>{ {"!tables"}, {"My Schema", "My Table"} }</blockquote>
   *
   * @param line the line to break up
   * @return an array of compound words
   */
  public String[][] splitCompound(String line) {
    final DatabaseConnection databaseConnection = getDatabaseConnection();
    final Quoting quoting;
    if (databaseConnection == null) {
      quoting = Quoting.DEFAULT;
    } else {
      quoting = databaseConnection.quoting;
    }

    int state = SPACE;
    int idStart = -1;
    final char[] chars = line.toCharArray();
    int n = chars.length;

    // Trim off trailing semicolon and/or whitespace
    while (n > 0
        && (Character.isWhitespace(chars[n - 1])
        || chars[n - 1] == ';')) {
      --n;
    }

    final List<String[]> words = new ArrayList<String[]>();
    final List<String> current = new ArrayList<String>();
    for (int i = 0; i < n;) {
      char c = chars[i];
      switch (state) {
      case SPACE:
      case DOT_SPACE:
        ++i;
        if (Character.isWhitespace(c)) {
          // nothing
        } else if (c == '.') {
          state = DOT_SPACE;
        } else if (c == quoting.start) {
          if (state == SPACE) {
            if (current.size() > 0) {
              words.add(
                  current.toArray(new String[current.size()]));
              current.clear();
            }
          }
          state = QUOTED;
          idStart = i;
        } else {
          if (state == SPACE) {
            if (current.size() > 0) {
              words.add(
                  current.toArray(new String[current.size()]));
              current.clear();
            }
          }
          state = UNQUOTED;
          idStart = i - 1;
        }
        break;
      case QUOTED:
        ++i;
        if (c == quoting.end) {
          if (i < n
              && chars[i] == quoting.end) {
            // Repeated quote character inside a quoted identifier.
            // Elminate one of the repeats, and we remain inside a
            // quoted identifier.
            System.arraycopy(chars, i, chars, i - 1, n - i);
            --n;
          } else {
            state = SPACE;
            final String word =
                new String(chars, idStart, i - idStart - 1);
            current.add(word);
          }
        }
        break;
      case UNQUOTED:
        // We are in an unquoted identifier. Whitespace or dot ends
        // the identifier, anything else extends it.
        ++i;
        if (Character.isWhitespace(c) || c == '.') {
          String word = new String(chars, idStart, i - idStart - 1);
          if (word.equalsIgnoreCase("NULL")) {
            word = null;
          } else if (quoting.upper) {
            word = word.toUpperCase();
          }
          current.add(word);
          state = (c == '.') ? DOT_SPACE : SPACE;
        }
        break;
      default:
        throw new AssertionError("unexpected state " + state);
      }
    }

    switch (state) {
    case SPACE:
    case DOT_SPACE:
      break;
    case QUOTED:
    case UNQUOTED:
      // In the middle of a quoted string. Be lenient, and complete the
      // word.
      String word = new String(chars, idStart, n - idStart);
      if (state == UNQUOTED) {
        if (word.equalsIgnoreCase("NULL")) {
          word = null;
        } else if (quoting.upper) {
          word = word.toUpperCase();
        }
      }
      current.add(word);
      break;
    default:
      throw new AssertionError("unexpected state " + state);
    }

    if (current.size() > 0) {
      words.add(current.toArray(new String[current.size()]));
    }

    return words.toArray(new String[words.size()][]);
  }

  /**
   * In a region of whitespace.
   */
  private static final int SPACE = 0;

  /**
   * In a region of whitespace that contains a dot.
   */
  private static final int DOT_SPACE = 1;

  /**
   * Inside a quoted identifier.
   */
  private static final int QUOTED = 2;

  /**
   * Inside an unquoted identifier.
   */
  private static final int UNQUOTED = 3;

  String dequote(String str) {
    if (str == null) {
      return null;
    }
    while ((str.startsWith("'") && str.endsWith("'"))
        || (str.startsWith("\"") && str.endsWith("\""))) {
      str = str.substring(1, str.length() - 1);
    }
    return str;
  }

  List<String> split(String line, String delim) {
    final List<String> list = new ArrayList<String>();
    for (String t : tokenize(line, delim)) {
      list.add(dequote(t));
    }
    return list;
  }

  static <K, V> Map<K, V> map(Object... obs) {
    Map<K, V> m = new HashMap<K, V>();
    for (int i = 0; i < obs.length - 1; i += 2) {
      //noinspection unchecked
      m.put((K) obs[i], (V) obs[i + 1]);
    }
    return Collections.unmodifiableMap(m);
  }

  static boolean getMoreResults(Statement stmnt) {
    try {
      return stmnt.getMoreResults();
    } catch (Throwable t) {
      return false;
    }
  }

  static String xmlattrencode(String str) {
    str = replace(str, "\"", "&quot;");
    str = replace(str, "<", "&lt;");
    return str;
  }

  static String replace(String source, String from, String to) {
    if (source == null) {
      return null;
    }

    if (from.equals(to)) {
      return source;
    }

    StringBuilder replaced = new StringBuilder();
    int index;
    while ((index = source.indexOf(from)) != -1) {
      replaced.append(source.substring(0, index));
      replaced.append(to);
      source = source.substring(index + from.length());
    }
    replaced.append(source);

    return replaced.toString();
  }

  /**
   * Split the line based on spaces, asserting that the number of words is
   * correct.
   *
   * @param line      the line to split
   * @param assertLen the number of words to assure
   * @param usage     the message to output if there are an incorrect number of
   *                  words.
   * @return the split lines, or null if the assertion failed.
   */
  List<String> split(String line, int assertLen, String usage) {
    List<String> ret = split(line);
    if (ret.size() != assertLen) {
      error(usage);
      return null;
    }
    return ret;
  }

  /**
   * Wrap the specified string by breaking on space characters.
   *
   * @param toWrap the string to wrap
   * @param len    the maximum length of any line
   * @param start  the number of spaces to pad at the beginning of a line
   * @return the wrapped string
   */
  public static String wrap(String toWrap, int len, int start) {
    final StringBuilder buff = new StringBuilder();
    final StringBuilder line = new StringBuilder();
    final String head = spaces(start);

    for (String next : tokenize(toWrap, " ")) {
      if (line.length() + next.length() > len) {
        buff.append(line).append(SEPARATOR).append(head);
        line.setLength(0);
      }

      if (line.length() > 0) {
        line.append(' ');
      }
      line.append(next);
    }

    buff.append(line);
    return buff.toString();
  }

  /**
   * Output a progress indicator to the console.
   *
   * @param cur the current progress
   * @param max the maximum progress, or -1 if unknown
   */
  void progress(int cur, int max) {
    StringBuilder out = new StringBuilder();

    if (lastProgress != null) {
      char[] back = new char[lastProgress.length()];
      Arrays.fill(back, '\b');
      out.append(back);
    }

    String progress =
        cur + "/"
        + (max == -1 ? "?" : "" + max) + " "
        + (max == -1 ? "(??%)"
           : ("(" + (cur * 100 / (max == 0 ? 1 : max)) + "%)"));

    if (cur >= max && max != -1) {
      progress += " " + loc("done") + SEPARATOR;
      lastProgress = null;
    } else {
      lastProgress = progress;
    }

    out.append(progress);

    outputStream.print(out.toString());
    outputStream.flush();
  }

  ///////////////////////////////
  // Exception handling routines
  ///////////////////////////////

  void handleException(Throwable e) {
    while (e instanceof InvocationTargetException) {
      e = ((InvocationTargetException) e).getTargetException();
    }

    if (e instanceof SQLException) {
      handleSQLException((SQLException) e);
    } else if (!initComplete && !getOpts().getVerbose()) {
      // all init errors must be verbose
      if (e.getMessage() == null) {
        error(e.getClass().getName());
      } else {
        error(e.getMessage());
      }
    } else {
      e.printStackTrace(getErrorStream());
    }
  }

  void handleSQLException(SQLException e) {
    // all init errors must be verbose
    final SqlLineOpts opts = getOpts();
    final boolean showWarnings = !initComplete || opts.getShowWarnings();
    final boolean verbose = !initComplete || opts.getVerbose();
    final boolean showNested = !initComplete || opts.getShowNestedErrs();

    if (e instanceof SQLWarning && !showWarnings) {
      return;
    }

    String type = (e instanceof SQLWarning) ? loc("Warning") : loc("Error");

    error(
        loc(
            (e instanceof SQLWarning) ? "Warning" : "Error",
            new Object[] {
              (e.getMessage() == null) ? "" : e.getMessage().trim(),
              (e.getSQLState() == null) ? "" : e.getSQLState().trim(),
              e.getErrorCode()
            }));

    if (verbose) {
      e.printStackTrace(getErrorStream());
    }

    if (!showNested) {
      return;
    }

    for (SQLException nested = e.getNextException();
         nested != null && nested != e;
         nested = nested.getNextException()) {
      handleSQLException(nested);
    }
  }

  boolean scanForDriver(String url) {
    try {
      // already registered
      if (findRegisteredDriver(url) != null) {
        return true;
      }

      // first try known drivers...
      scanDrivers(true);

      if (findRegisteredDriver(url) != null) {
        return true;
      }

      // now really scan...
      scanDrivers(false);

      if (findRegisteredDriver(url) != null) {
        return true;
      }

      return false;
    } catch (Exception e) {
      debug(e.toString());
      return false;
    }
  }

  /** Adapts an {@link Enumeration} into an {@link Iterable}. */
  public static <E> Iterable<E> iterable(final Enumeration<E> enumeration) {
    return new Iterable<E>() {
      public Iterator<E> iterator() {
        return new Iterator<E>() {
          public boolean hasNext() {
            return enumeration.hasMoreElements();
          }

          public E next() {
            return enumeration.nextElement();
          }

          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  private Driver findRegisteredDriver(String url) {
    for (Driver driver : iterable(DriverManager.getDrivers())) {
      try {
        if (driver.acceptsURL(url)) {
          return driver;
        }
      } catch (Exception e) {
        // ignore
      }
    }
    return null;
  }

  List<Driver> scanDrivers(String line) throws IOException {
    return scanDrivers(false);
  }

  List<Driver> scanDrivers(boolean knownOnly) throws IOException {
    final long start = System.currentTimeMillis();
    final Set<String> classNames = new HashSet<String>();
    if (!knownOnly) {
      Collections.addAll(classNames, ClassNameCompleter.getClassNames());
    }
    classNames.addAll(getKnownDrivers());

    final Set<Driver> driverClasses = new HashSet<Driver>();
    for (String className : classNames) {
      if (!className.toLowerCase().contains("driver")) {
        continue;
      }

      try {
        Class c = Class.forName(className, false,
            Thread.currentThread().getContextClassLoader());
        if (!Driver.class.isAssignableFrom(c)) {
          continue;
        }

        if (Modifier.isAbstract(c.getModifiers())) {
          continue;
        }

        // now instantiate and initialize it
        driverClasses.add((Driver) c.newInstance());
      } catch (Throwable t) {
        // ignore
      }
    }
    long end = System.currentTimeMillis();
    info("scan complete in " + (end - start) + "ms");
    return new ArrayList<Driver>(driverClasses);
  }

  ///////////////////////////////////////
  // ResultSet output formatting classes
  ///////////////////////////////////////

  int print(ResultSet rs, DispatchCallback callback) throws SQLException {
    final SqlLineOpts opts = getOpts();
    String format = opts.getOutputFormat();
    OutputFormat f = (OutputFormat) formats.get(format);

    if (f == null) {
      error(loc("unknown-format", new Object[] {format, formats.keySet()}));
      f = new TableOutputFormat(this);
    }

    Rows rows;

    if (opts.getIncremental()) {
      rows = new IncrementalRows(this, rs, callback);
    } else {
      rows = new BufferedRows(this, rs);
    }
    return f.print(rows);
  }

  Statement createStatement() throws SQLException {
    final Connection connection = getDatabaseConnection().getConnection();
    Statement stmnt = connection.createStatement();
    final SqlLineOpts opts = getOpts();
    if (opts.timeout > -1) {
      stmnt.setQueryTimeout(opts.timeout);
    }
    if (opts.rowLimit != 0) {
      stmnt.setMaxRows(opts.rowLimit);
    }
    return stmnt;
  }

  PreparedStatement prepare(String sql) throws SQLException {
    final Connection connection = getDatabaseConnection().getConnection();
    return connection.prepareCall(sql);
  }

  void runBatch(List<String> statements) {
    try {
      Statement stmnt = createStatement();
      try {
        for (String statement : statements) {
          stmnt.addBatch(statement);
        }
        int[] counts = stmnt.executeBatch();

        output(getColorBuffer().pad(getColorBuffer().bold("COUNT"), 8)
            .append(getColorBuffer().bold("STATEMENT")));

        for (int i = 0; counts != null && i < counts.length; i++) {
          output(getColorBuffer().pad(counts[i] + "", 8)
              .append(statements.get(i)));
        }
      } finally {
        try {
          stmnt.close();
        } catch (Exception e) {
          // ignore
        }
      }
    } catch (Exception e) {
      handleException(e);
    }
  }

  /** Returns a string consisting of {@code n} spaces. */
  static String spaces(int n) {
    if (n < SPACES.length) {
      return SPACES[n];
    }
    final char[] chars = new char[n];
    Arrays.fill(chars, ' ');
    return new String(chars);
  }

  /** Pads a string with spaces to at least a given length. */
  protected static String pad(String str, int len) {
    if (str == null) {
      return spaces(len);
    }
    final int v = str.length();
    if (v >= len) {
      return str;
    }
    return str + SqlLine.spaces(len - v);
  }

  protected static String center(String str, int len) {
    int n = len - str.length();
    if (n <= 0) {
      return str;
    }
    final StringBuilder buf = new StringBuilder();
    int left = n / 2;
    if (left > 0) {
      buf.append(SqlLine.spaces(left));
    }
    buf.append(str);
    int right = n - left;
    if (right > 0) {
      buf.append(SqlLine.spaces(right));
    }
    return buf.toString();
  }
  /** Returns the prefix to print before each command. Derived class may
   * override. */
  protected String prefix(int index, int size) {
    return pad(index + "/" + size, 13);
  }

  public int runCommands(List<String> cmds, DispatchCallback callback) {
    int successCount = 0;
    try {
      int index = 1;
      for (String cmd : cmds) {
        info(getColorBuffer().append(prefix(index++, cmds.size())).append(cmd));
        dispatch(cmd, callback);
        boolean success = callback.isSuccess();
        if (success) {
          ++successCount;
        } else {
          // if we do not force script execution, abort
          // when a failure occurs.
          if (!getOpts().getForce()) {
            error(loc("abort-on-error", cmd));
            return successCount;
          }
        }
      }
    } catch (Exception e) {
      handleException(e);
    }
    return successCount;
  }

  // ////////////////////////
  // Command methods follow
  // ////////////////////////

  void setCompletions() throws SQLException, IOException {
    if (getDatabaseConnection() != null) {
      getDatabaseConnection().setCompletions(getOpts().getFastConnect());
    }
  }

  public SqlLineOpts getOpts() {
    return opts;
  }

  DatabaseConnections getDatabaseConnections() {
    return connections;
  }

  public boolean isExit() {
    return exit;
  }

  public void setExit(boolean exit) {
    this.exit = exit;
  }

  Collection<Driver> getDrivers() {
    return drivers;
  }

  void setDrivers(Collection<Driver> drivers) {
    this.drivers = drivers;
  }

  public static String getSeparator() {
    return SEPARATOR;
  }

  Commands getCommands() {
    return commands;
  }

  OutputFile getScriptOutputFile() {
    return scriptOutputFile;
  }

  void setScriptOutputFile(OutputFile script) {
    this.scriptOutputFile = script;
  }

  OutputFile getRecordOutputFile() {
    return recordOutputFile;
  }

  void setRecordOutputFile(OutputFile record) {
    this.recordOutputFile = record;
  }

  public void setOutputStream(PrintStream outputStream) {
    this.outputStream = new PrintStream(outputStream, true);
  }

  PrintStream getOutputStream() {
    return outputStream;
  }

  public void setErrorStream(PrintStream errorStream) {
    this.errorStream = new PrintStream(errorStream, true);
  }

  PrintStream getErrorStream() {
    return errorStream;
  }

  ConsoleReader getConsoleReader() {
    return consoleReader;
  }

  void setConsoleReader(ConsoleReader reader) {
    this.consoleReader = reader;
  }

  List<String> getBatch() {
    return batch;
  }

  void setBatch(List<String> batch) {
    this.batch = batch;
  }

  public Reflector getReflector() {
    return reflector;
  }

  public Completer getCommandCompleter() {
    return sqlLineCommandCompleter;
  }
}

// End SqlLine.java
