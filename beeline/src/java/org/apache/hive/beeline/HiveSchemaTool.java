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
package org.apache.hive.beeline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hive.beeline.HiveSchemaHelper.NestedScriptParser;
import org.apache.hive.sqlline.SqlLine;

public class HiveSchemaTool {
  private String userName = null;
  private String passWord = null;
  private boolean dryRun = false;
  private boolean verbose = false;
  private final HiveConf hiveConf;
  private final String dbType;
  private final MetaStoreSchemaInfo metaStoreSchemaInfo;

  public HiveSchemaTool(String dbType) throws HiveMetaException {
    this(System.getenv("HIVE_HOME"), new HiveConf(HiveSchemaTool.class), dbType);
  }

  public HiveSchemaTool(String hiveHome, HiveConf hiveConf, String dbType)
        throws HiveMetaException {
    if (hiveHome == null || hiveHome.isEmpty()) {
      throw new HiveMetaException("No Hive home directory provided");
    }
    this.hiveConf = hiveConf;
    this.dbType = dbType;
    this.metaStoreSchemaInfo = new MetaStoreSchemaInfo(hiveHome, hiveConf, dbType);
    userName = hiveConf.get(ConfVars.METASTORE_CONNECTION_USER_NAME.varname);
    passWord = hiveConf.get(HiveConf.ConfVars.METASTOREPWD.varname);
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  private static void printAndExit(Options cmdLineOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("schemaTool", cmdLineOptions);
    System.exit(1);
  }

  /***
   * Print Hive version and schema version
   * @throws MetaException
   */
  public void showInfo() throws HiveMetaException {
    Connection metastoreConn = getConnectionToMetastore(true);
    System.out.println("Hive distribution version:\t " +
        MetaStoreSchemaInfo.getHiveSchemaVersion());
    System.out.println("Metastore schema version:\t " +
        getMetaStoreSchemaVersion(metastoreConn));
  }

  // read schema version from metastore
  private String getMetaStoreSchemaVersion(Connection metastoreConn)
        throws HiveMetaException {
    String versionQuery;
    if (HiveSchemaHelper.getDbCommandParser(dbType).needsQuotedIdentifier()) {
      versionQuery = "select t.\"SCHEMA_VERSION\" from \"VERSION\" t";
    } else {
      versionQuery = "select t.SCHEMA_VERSION from VERSION t";
    }
    try {
      Statement stmt = metastoreConn.createStatement();
      ResultSet res = stmt.executeQuery(versionQuery);
      if (!res.next()) {
        throw new HiveMetaException("Didn't find version data in metastore");
      }
      String currentSchemaVersion = res.getString(1);
      metastoreConn.close();
      return currentSchemaVersion;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    }
  }

  // test the connection metastore using the config property
  private void testConnectionToMetastore() throws HiveMetaException {
    Connection conn = getConnectionToMetastore(true);
    try {
      conn.close();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to close metastore connection", e);
    }
  }

  /***
   * get JDBC connection to metastore db
   *
   * @param printInfo print connection parameters
   * @return
   * @throws MetaException
   */
  private Connection getConnectionToMetastore(boolean printInfo)
        throws HiveMetaException {
    try {
      String connectionURL = getValidConfVar(ConfVars.METASTORECONNECTURLKEY);
      String driver = getValidConfVar(ConfVars.METASTORE_CONNECTION_DRIVER);
      if (printInfo) {
        System.out.println("Metastore connection URL:\t " + connectionURL);
        System.out.println("Metastore Connection Driver :\t " + driver);
        System.out.println("Metastore connection User:\t " + userName);
      }
      if ((userName == null) || userName.isEmpty()) {
        throw new HiveMetaException("UserName empty ");
      }

      // load required JDBC driver
      Class.forName(driver);

      // Connect using the JDBC URL and user/pass from conf
      return DriverManager.getConnection(connectionURL, userName, passWord);
    } catch (IOException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    } catch (ClassNotFoundException e) {
      throw new HiveMetaException("Failed to load driver", e);
    }
  }

  /**
   * check if the current schema version in metastore matches the Hive version
   * @throws MetaException
   */
  public void verifySchemaVersion() throws HiveMetaException {
    // don't check version if its a dry run
    if (dryRun) {
      return;
    }
    String newSchemaVersion =
        getMetaStoreSchemaVersion(getConnectionToMetastore(false));
    // verify that the new version is added to schema
    if (!MetaStoreSchemaInfo.getHiveSchemaVersion().equalsIgnoreCase(newSchemaVersion)) {
      throw new HiveMetaException("Found unexpected schema version " + newSchemaVersion);
    }
  }

  /**
   * Perform metastore schema upgrade. extract the current schema version from metastore
   * @throws MetaException
   */
  public void doUpgrade() throws HiveMetaException {
    String fromVersion = getMetaStoreSchemaVersion(getConnectionToMetastore(false));
    if (fromVersion == null || fromVersion.isEmpty()) {
      throw new HiveMetaException("Schema version not stored in the metastore. " +
          "Metastore schema is too old or corrupt. Try specifying the version manually");
    }
    doUpgrade(fromVersion);
  }

  /**
   * Perform metastore schema upgrade
   *
   * @param fromSchemaVer
   *          Existing version of the metastore. If null, then read from the metastore
   * @throws MetaException
   */
  public void doUpgrade(String fromSchemaVer) throws HiveMetaException {
    if (MetaStoreSchemaInfo.getHiveSchemaVersion().equals(fromSchemaVer)) {
      System.out.println("No schema upgrade required from version " + fromSchemaVer);
      return;
    }
    // Find the list of scripts to execute for this upgrade
    List<String> upgradeScripts =
        metaStoreSchemaInfo.getUpgradeScripts(fromSchemaVer);
    testConnectionToMetastore();
    System.out.println("Starting upgrade metastore schema from version " +
        fromSchemaVer + " to " + MetaStoreSchemaInfo.getHiveSchemaVersion());
    String scriptDir = metaStoreSchemaInfo.getMetaStoreScriptDir();
    try {
      for (String scriptFile : upgradeScripts) {
        System.out.println("Upgrade script " + scriptFile);
        if (!dryRun) {
          runBeeLine(scriptDir, scriptFile);
          System.out.println("Completed " + scriptFile);
        }
      }
    } catch (IOException eIO) {
      throw new HiveMetaException(
          "Upgrade FAILED! Metastore state would be inconsistent !!", eIO);
    }

    // Revalidated the new version after upgrade
    verifySchemaVersion();
  }

  /**
   * Initialize the metastore schema to current version
   *
   * @throws MetaException
   */
  public void doInit() throws HiveMetaException {
    doInit(MetaStoreSchemaInfo.getHiveSchemaVersion());

    // Revalidated the new version after upgrade
    verifySchemaVersion();
  }

  /**
   * Initialize the metastore schema
   *
   * @param toVersion
   *          If null then current hive version is used
   * @throws MetaException
   */
  public void doInit(String toVersion) throws HiveMetaException {
    testConnectionToMetastore();
    System.out.println("Starting metastore schema initialization to " + toVersion);

    String initScriptDir = metaStoreSchemaInfo.getMetaStoreScriptDir();
    String initScriptFile = metaStoreSchemaInfo.generateInitFileName(toVersion);

    try {
      System.out.println("Initialization script " + initScriptFile);
      if (!dryRun) {
        runBeeLine(initScriptDir, initScriptFile);
        System.out.println("Initialization script completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("Schema initialization FAILED!" +
      		" Metastore state would be inconsistent !!", e);
    }
  }

  // Flatten the nested upgrade script into a buffer
  public static String buildCommand(NestedScriptParser dbCommandParser,
        String scriptDir, String scriptFile) throws IllegalFormatException, IOException {

    BufferedReader bfReader =
        new BufferedReader(new FileReader(scriptDir + File.separatorChar + scriptFile));
    String currLine;
    StringBuilder sb = new StringBuilder();
    String currentCommand = null;
    while ((currLine = bfReader.readLine()) != null) {
      currLine = currLine.trim();
      if (currLine.isEmpty()) {
        continue; // skip empty lines
      }

      if (currentCommand == null) {
        currentCommand = currLine;
      } else {
        currentCommand = currentCommand + " " + currLine;
      }
      if (dbCommandParser.isPartialCommand(currLine)) {
        // if its a partial line, continue collecting the pieces
        continue;
      }

      // if this is a valid executable command then add it to the buffer
      if (!dbCommandParser.isNonExecCommand(currentCommand)) {
        currentCommand = dbCommandParser.cleanseCommand(currentCommand);

        if (dbCommandParser.isNestedScript(currentCommand)) {
          // if this is a nested sql script then flatten it
          String currScript = dbCommandParser.getScriptName(currentCommand);
          sb.append(buildCommand(dbCommandParser, scriptDir, currScript));
        } else {
          // Now we have a complete statement, process it
          // write the line to buffer
          sb.append(currentCommand);
          sb.append(System.getProperty("line.separator"));
        }
      }
      currentCommand = null;
    }
    bfReader.close();
    return sb.toString();
  }

  // run beeline on the given metastore scrip, flatten the nested scripts into single file
  private void runBeeLine(String scriptDir, String scriptFile) throws IOException {
    NestedScriptParser dbCommandParser =
        HiveSchemaHelper.getDbCommandParser(dbType);
    // expand the nested script
    String sqlCommands = buildCommand(dbCommandParser, scriptDir, scriptFile);
    File tmpFile = File.createTempFile("schematool", ".sql");
    tmpFile.deleteOnExit();

    // write out the buffer into a file. Add beeline commands for autocommit and close
    FileWriter fstream = new FileWriter(tmpFile.getPath());
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("!autocommit on" + System.getProperty("line.separator"));
    out.write(sqlCommands);
    out.write("!closeall" + System.getProperty("line.separator"));
    out.close();
    runBeeLine(tmpFile.getPath());
  }

  // Generate the beeline args per hive conf and execute the given script
  public void runBeeLine(String sqlScriptFile) throws IOException {
    List<String> argList = new ArrayList<String>();
    argList.add("-u");
    argList.add(getValidConfVar(ConfVars.METASTORECONNECTURLKEY));
    argList.add("-d");
    argList.add(getValidConfVar(ConfVars.METASTORE_CONNECTION_DRIVER));
    argList.add("-n");
    argList.add(userName);
    argList.add("-p");
    argList.add(passWord);
    argList.add("-f");
    argList.add(sqlScriptFile);

    // run the script using Beeline
    BeeLine beeLine = new BeeLine();
    if (!verbose) {
      beeLine.setOutputStream(new PrintStream(new NullOutputStream()));
      beeLine.getOpts().setSilent(true);
    }
    //beeLine.getOpts().setAllowMultiLineCommand(false);
    beeLine.getOpts().setIsolation("TRANSACTION_READ_COMMITTED");
    SqlLine.Status status = beeLine.begin(argList, null, false);
    switch (status) {
    case OK:
      break;
    default:
      throw new IOException("Schema script failed, error code " + status + " ("
          + status.ordinal() + ")");
    }
  }

  private String getValidConfVar(ConfVars confVar) throws IOException {
    String confVarStr = hiveConf.get(confVar.varname);
    if (confVarStr == null || confVarStr.isEmpty()) {
      throw new IOException("Empty " + confVar.varname);
    }
    return confVarStr;
  }

  // Create the required command line options
  @SuppressWarnings("static-access")
  private static void initOptions(Options cmdLineOptions) {
    Option help = new Option("help", "print this message");
    Option upgradeOpt = new Option("upgradeSchema", "Schema upgrade");
    Option upgradeFromOpt = OptionBuilder.withArgName("upgradeFrom").hasArg().
                withDescription("Schema upgrade from a version").
                create("upgradeSchemaFrom");
    Option initOpt = new Option("initSchema", "Schema initialization");
    Option initToOpt = OptionBuilder.withArgName("initTo").hasArg().
                withDescription("Schema initialization to a version").
                create("initSchemaTo");
    Option infoOpt = new Option("info", "Show config and schema details");

    OptionGroup optGroup = new OptionGroup();
    optGroup.addOption(upgradeOpt).addOption(initOpt).
                addOption(help).addOption(upgradeFromOpt).
                addOption(initToOpt).addOption(infoOpt);
    optGroup.setRequired(true);

    Option userNameOpt = OptionBuilder.withArgName("user")
                .hasArgs()
                .withDescription("Override config file user name")
                .create("userName");
    Option passwdOpt = OptionBuilder.withArgName("password")
                .hasArgs()
                 .withDescription("Override config file password")
                 .create("passWord");
    Option dbTypeOpt = OptionBuilder.withArgName("databaseType")
                .hasArgs().withDescription("Metastore database type")
                .create("dbType");
    Option dryRunOpt = new Option("dryRun", "list SQL scripts (no execute)");
    Option verboseOpt = new Option("verbose", "only print SQL statements");

    cmdLineOptions.addOption(help);
    cmdLineOptions.addOption(dryRunOpt);
    cmdLineOptions.addOption(userNameOpt);
    cmdLineOptions.addOption(passwdOpt);
    cmdLineOptions.addOption(dbTypeOpt);
    cmdLineOptions.addOption(verboseOpt);
    cmdLineOptions.addOptionGroup(optGroup);
  }

  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
    CommandLine line = null;
    String dbType = null;
    String schemaVer = null;
    Options cmdLineOptions = new Options();

    // Argument handling
    initOptions(cmdLineOptions);
    try {
      line = parser.parse(cmdLineOptions, args);
    } catch (ParseException e) {
      System.err.println("HiveSchemaTool:Parsing failed.  Reason: " + e.getLocalizedMessage());
      printAndExit(cmdLineOptions);
    }

    if (line.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("schemaTool", cmdLineOptions);
      return;
    }

    if (line.hasOption("dbType")) {
      dbType = line.getOptionValue("dbType");
      if ((!dbType.equalsIgnoreCase(HiveSchemaHelper.DB_DERBY) &&
          !dbType.equalsIgnoreCase(HiveSchemaHelper.DB_MYSQL) &&
          !dbType.equalsIgnoreCase(HiveSchemaHelper.DB_POSTGRACE) && !dbType
          .equalsIgnoreCase(HiveSchemaHelper.DB_ORACLE))) {
        System.err.println("Unsupported dbType " + dbType);
        printAndExit(cmdLineOptions);
      }
    } else {
      System.err.println("no dbType supplied");
      printAndExit(cmdLineOptions);
    }

    System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.varname, "true");
    try {
      HiveSchemaTool schemaTool = new HiveSchemaTool(dbType);

      if (line.hasOption("userName")) {
        schemaTool.setUserName(line.getOptionValue("userName"));
      }
      if (line.hasOption("passWord")) {
        schemaTool.setPassWord(line.getOptionValue("passWord"));
      }
      if (line.hasOption("dryRun")) {
        schemaTool.setDryRun(true);
      }
      if (line.hasOption("verbose")) {
        schemaTool.setVerbose(true);
      }

      if (line.hasOption("info")) {
        schemaTool.showInfo();
      } else if (line.hasOption("upgradeSchema")) {
        schemaTool.doUpgrade();
      } else if (line.hasOption("upgradeSchemaFrom")) {
        schemaVer = line.getOptionValue("upgradeSchemaFrom");
        schemaTool.doUpgrade(schemaVer);
      } else if (line.hasOption("initSchema")) {
        schemaTool.doInit();
      } else if (line.hasOption("initSchemaTo")) {
        schemaVer = line.getOptionValue("initSchemaTo");
        schemaTool.doInit(schemaVer);
      } else {
        System.err.println("no valid option supplied");
        printAndExit(cmdLineOptions);
      }
    } catch (HiveMetaException e) {
      System.err.println(e);
      if (line.hasOption("verbose")) {
        e.printStackTrace();
      }
      System.err.println("*** schemaTool failed ***");
      System.exit(1);
    }
    System.out.println("schemaTool completed");

  }
}
