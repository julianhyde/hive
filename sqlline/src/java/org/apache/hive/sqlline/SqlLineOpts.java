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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import jline.TerminalFactory;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

/** Configuration options for SqlLine. */
public class SqlLineOpts implements Completer {
  public static final String PROPERTY_PREFIX = "sqlline.";
  public static final String PROPERTY_NAME_EXIT =
      PROPERTY_PREFIX + "system.exit";
  public static final String DEFAULT_ISOLATION_LEVEL =
      "TRANSACTION_REPEATABLE_READ";
  public static final String DEFAULT_NULL_STRING = "NULL";

  private final SqlLine sqlLine;
  private boolean autosave = false;
  private boolean silent = false;
  private boolean color = false;
  private boolean showHeader = true;
  private int headerInterval = 100;
  private boolean fastConnect = true;
  private boolean autoCommit = true;
  private boolean verbose = false;
  private boolean force = false;
  private boolean incremental = true;
  private boolean showElapsedTime = true;
  private boolean showWarnings = true;
  private boolean showNestedErrs = false;
  private String numberFormat = "default";
  private int maxWidth = TerminalFactory.get().getWidth();
  private int maxHeight = TerminalFactory.get().getHeight();
  private int maxColumnWidth = 15;
  int rowLimit = 0;
  int timeout = -1;
  private String isolation = DEFAULT_ISOLATION_LEVEL;
  private String outputFormat = "table";
  private boolean trimScripts = true;
  private boolean allowMultiLineCommand = true;
  // This can be set for old behavior of nulls printed as empty strings
  private boolean nullEmptyString = false;

  private final File rcFile;
  private String historyFile;
  private String runFile;
  private String scriptFile;

  public SqlLineOpts(SqlLine sqlLine, Properties props, String propFileName,
      String rcfilePropName) {
    this.sqlLine = sqlLine;
    loadProperties(props);
    final File parent = saveDir(rcfilePropName);
    rcFile = new File(parent, propFileName);
    historyFile = new File(parent, "history").getAbsolutePath();
  }

  public Completer[] optionCompleters() {
    return new Completer[] {this};
  }

  /**
   * The save directory if HOME/.sqlline/ on UNIX, and HOME/sqlline/ on
   * Windows.
   */
  public File saveDir(String rcPropName) {
    String dir = System.getProperty(rcPropName);
    if (dir != null && dir.length() > 0) {
      return new File(dir);
    }

    String baseDir = System.getProperty(SqlLine.SQLLINE_BASE_DIR);
    File f;
    if (baseDir != null && baseDir.length() > 0) {
      f = new File(baseDir);
    } else {
      final boolean windows =
          System.getProperty("os.name").toLowerCase().contains("windows");
      final String home = System.getProperty("user.home");
      f = new File(home, (windows ? "" : ".") + "sqlline");
    }
    f = f.getAbsoluteFile();
    try {
      f.mkdirs();
    } catch (Exception e) {
      // ignore
    }
    return f;
  }

  public int complete(String buf, int pos, List<CharSequence> candidates) {
    try {
      return new StringsCompleter(propertyNames())
          .complete(buf, pos, candidates);
    } catch (Throwable t) {
      return -1;
    }
  }

  public void save() throws IOException {
    OutputStream out = new FileOutputStream(rcFile);
    save(out);
    out.close();
  }

  public void save(OutputStream out) throws IOException {
    try {
      Properties props = toProperties();
      // don't save maxwidth: it is automatically set based on
      // the terminal configuration
      props.remove(PROPERTY_PREFIX + "maxwidth");
      props.store(out, sqlLine.getApplicationTitle());
    } catch (Exception e) {
      sqlLine.handleException(e);
    }
  }

  SortedSet<String> propertyNames()
      throws IllegalAccessException, InvocationTargetException {
    TreeSet<String> names = new TreeSet<String>();
    for (Method method : propertyMethods()) {
      names.add(propertyName(method));
    }
    return names;
  }

  /** Returns the property that is retrieved by a given method, removing the
   * "is" or "get" prefix from the method name. Returns null if it is not a
   * property method. */
  private String propertyName(Method method) {
    if (method.getParameterTypes().length != 0) {
      return null;
    }
    final String name = method.getName();
    // "run" is not a real property
    if (name.startsWith("get") && !name.equals("getRun")) {
      return name.substring(3).toLowerCase();
    }
    if (name.startsWith("is")) {
      return name.substring(2).toLowerCase();
    }
    return null;
  }

  List<Method> propertyMethods()
      throws IllegalAccessException, InvocationTargetException {
    // get all the values from getXXX and isXXX methods
    final List<Method> methods = new ArrayList<Method>();
    for (final Method method : getClass().getDeclaredMethods()) {
      if (propertyName(method) != null) {
        methods.add(method);
      }
    }
    return methods;
  }

  public Properties toProperties()
      throws IllegalAccessException, InvocationTargetException,
      ClassNotFoundException {
    Properties props = new Properties();
    for (Map.Entry<String, String> entry : toMap().entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }
    return props;
  }

  public Map<String, String> toMap()
      throws IllegalAccessException, InvocationTargetException,
      ClassNotFoundException {
    Map<String, String> map = new LinkedHashMap<String, String>();
    for (Method method : propertyMethods()) {
      final Object o = sqlLine.getReflector().invoke(this, method.getName(),
          Collections.emptyList());
      if (o != null) {
        map.put(PROPERTY_PREFIX + propertyName(method), o.toString());
      }
    }
    sqlLine.debug("properties: " + map.toString());
    return map;
  }

  public void load() throws IOException {
    if (!rcFile.exists()) {
      return;
    }
    InputStream in = new FileInputStream(rcFile);
    load(in);
    in.close();
  }

  public void load(InputStream fin) throws IOException {
    Properties p = new Properties();
    p.load(fin);
    loadProperties(p);
  }

  public void loadProperties(Properties props) {
    for (Object element : props.keySet()) {
      String key = element.toString();
      if (key.equals(PROPERTY_NAME_EXIT)) {
        // fix for sf.net bug 879422
        continue;
      }
      if (key.startsWith(PROPERTY_PREFIX)) {
        set(key.substring(PROPERTY_PREFIX.length()),
            props.getProperty(key));
      }
    }
  }

  public void set(String key, String value) {
    set(key, value, false);
  }

  public boolean set(String key, String value, boolean quiet) {
    try {
      sqlLine.getReflector().invoke(this, "set" + key, new Object[] {value});
      return true;
    } catch (Exception e) {
      if (!quiet) {
        sqlLine.error(sqlLine.loc("error-setting", key, e));
      }
      return false;
    }
  }

  public void setFastConnect(boolean fastConnect) {
    this.fastConnect = fastConnect;
  }

  public boolean getFastConnect() {
    return this.fastConnect;
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  public boolean getAutoCommit() {
    return this.autoCommit;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public boolean getVerbose() {
    return this.verbose;
  }

  public void setShowElapsedTime(boolean showElapsedTime) {
    this.showElapsedTime = showElapsedTime;
  }

  public boolean getShowElapsedTime() {
    return this.showElapsedTime;
  }

  public void setShowWarnings(boolean showWarnings) {
    this.showWarnings = showWarnings;
  }

  public boolean getShowWarnings() {
    return this.showWarnings;
  }

  public void setShowNestedErrs(boolean showNestedErrs) {
    this.showNestedErrs = showNestedErrs;
  }

  public boolean getShowNestedErrs() {
    return this.showNestedErrs;
  }

  public void setNumberFormat(String numberFormat) {
    this.numberFormat = numberFormat;
  }

  public String getNumberFormat() {
    return this.numberFormat;
  }

  public void setMaxWidth(int maxWidth) {
    this.maxWidth = maxWidth;
  }

  public int getMaxWidth() {
    return this.maxWidth;
  }

  public void setMaxColumnWidth(int maxColumnWidth) {
    this.maxColumnWidth = maxColumnWidth;
  }

  public int getMaxColumnWidth() {
    return this.maxColumnWidth;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public int getRowLimit() {
    return this.rowLimit;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public int getTimeout() {
    return this.timeout;
  }

  public void setIsolation(String isolation) {
    this.isolation = isolation;
  }

  public String getIsolation() {
    return this.isolation;
  }

  public void setHistoryFile(String historyFile) {
    this.historyFile = historyFile;
  }

  public String getHistoryFile() {
    return this.historyFile;
  }

  public void setScriptFile(String scriptFile) {
    this.scriptFile = scriptFile;
  }

  public String getScriptFile() {
    return scriptFile;
  }

  public void setColor(boolean color) {
    this.color = color;
  }

  public boolean getColor() {
    return this.color;
  }

  public void setShowHeader(boolean showHeader) {
    this.showHeader = showHeader;
  }

  public boolean getShowHeader() {
    return this.showHeader;
  }

  public void setHeaderInterval(int headerInterval) {
    this.headerInterval = headerInterval;
  }

  public int getHeaderInterval() {
    return this.headerInterval;
  }

  public void setForce(boolean force) {
    this.force = force;
  }

  public boolean getForce() {
    return this.force;
  }

  public void setIncremental(boolean incremental) {
    this.incremental = incremental;
  }

  public boolean getIncremental() {
    return this.incremental;
  }

  public void setSilent(boolean silent) {
    this.silent = silent;
  }

  public boolean isSilent() {
    return this.silent;
  }

  public void setAutosave(boolean autosave) {
    this.autosave = autosave;
  }

  public boolean getAutosave() {
    return this.autosave;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public String getOutputFormat() {
    return this.outputFormat;
  }

  public void setTrimScripts(boolean trimScripts) {
    this.trimScripts = trimScripts;
  }

  public boolean getTrimScripts() {
    return this.trimScripts;
  }

  public void setMaxHeight(int maxHeight) {
    this.maxHeight = maxHeight;
  }

  public int getMaxHeight() {
    return this.maxHeight;
  }

  public File getPropertiesFile() {
    return rcFile;
  }

  public void setRun(String runFile) {
    this.runFile = runFile;
  }

  public String getRun() {
    return this.runFile;
  }

  public boolean isCautious() {
    return false;
  }

  public boolean isAllowMultiLineCommand() {
    return allowMultiLineCommand;
  }

  public void setAllowMultiLineCommand(boolean allowMultiLineCommand) {
    this.allowMultiLineCommand = allowMultiLineCommand;
  }

  /**
   * Use getNullString() to get the null string to be used.
   * @return true if null representation should be an empty string
   */
  public boolean getNullEmptyString() {
    return nullEmptyString;
  }

  public void setNullEmptyString(boolean nullStringEmpty) {
    this.nullEmptyString = nullStringEmpty;
  }

  public String getNullString() {
    return nullEmptyString ? "" : DEFAULT_NULL_STRING;
  }
}
