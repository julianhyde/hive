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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.hive.sqlline.SqlLine;
import org.apache.hive.sqlline.SqlLineOpts;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A console SQL shell for Hive, with command completion.
 */
public class BeeLine extends SqlLine {
  public static final int DEFAULT_MAX_WIDTH = 80;
  public static final int DEFAULT_MAX_HEIGHT = 80;

  public static final String BEELINE_DEFAULT_JDBC_DRIVER =
      "org.apache.hive.jdbc.HiveDriver";
  public static final String BEELINE_DEFAULT_JDBC_URL = "jdbc:hive2://";

  private static final String SCRIPT_OUTPUT_PREFIX = ">>>";
  private static final int SCRIPT_OUTPUT_PAD_SIZE = 5;

  static final SortedSet<String> HIVE_DRIVERS =
      new TreeSet<String>(Arrays.asList(
          "org.apache.hive.jdbc.HiveDriver",
          "com.mysql.jdbc.DatabaseMetaData"));

  /** Prefix to the name of properties (passed to getConnection) that specify
   * Hive variables. */
  private static final String HIVE_VAR_PREFIX = "hivevar:";

  /** Prefix to the name of properties (passed to getConnection) that specify
   * Hive configuration variables. */
  private static final String HIVE_CONF_PREFIX = "hiveconf:";

  /** Combined resource bundle that first looks in BeeLine.properties, then in
   * SqlLine.properties. */
  private static final ResourceBundle BEE_LINE_RESOURCE_BUNDLE =
      new ChainResourceBundle(
          Arrays.asList(ResourceBundle.getBundle(BeeLine.class.getName()),
              SqlLine.RESOURCE_BUNDLE));

  private static final Options COMMAND_LINE_OPTIONS = createBeeLineOptions();

  @SuppressWarnings("AccessStaticViaInstance")
  protected static Options createBeeLineOptions() {
    // BeeLine options are an extension to SqlLine.
    final Options options = createSqlLineOptions();

    // Synchronized prevents other threads building options using the same
    // static workspace in OptionBuilder.
    synchronized (OptionBuilder.class) {
      // -a <authType>
      options.addOption(OptionBuilder
          .hasArg()
          .withArgName("authType")
          .withDescription("the authentication type")
          .create('a'));

      // Substitution option --hivevar
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("key=value")
          .withLongOpt("hivevar")
          .withDescription("hive variable name and value")
          .create());

      //hive conf option --hiveconf
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("hiveconf")
          .withDescription("Use value for given property")
          .create());
    }
    return options;
  }

  @Override
  protected Options getCommandLineOptions() {
    return COMMAND_LINE_OPTIONS;
  }

  @Override
  protected boolean isSpecialArg(String arg) {
    return arg.equals(HIVE_VAR_PREFIX)
        || arg.equals(HIVE_CONF_PREFIX)
        || super.isSpecialArg(arg);
  }

  @Override
  protected SqlLineOpts createOpts() {
    final SqlLineOpts opts =
        new BeeLineOpts(BeeLine.this);
    opts.setAutoCommit(false);
    opts.setIncremental(false);
    opts.setShowWarnings(false);

    if (opts.getMaxWidth() <= 0) {
      opts.setMaxWidth(DEFAULT_MAX_WIDTH);
    }
    if (opts.getMaxHeight() <= 0) {
      opts.setMaxHeight(DEFAULT_MAX_HEIGHT);
    }

    opts.loadProperties(System.getProperties());
    return opts;
  }

  @Override
  protected String prefix(int index, int size) {
    // TODO: Make script output prefixing configurable. Had to disable this
    // since it results in lots of test diffs.
    return pad(SCRIPT_OUTPUT_PREFIX, SCRIPT_OUTPUT_PAD_SIZE);
  }

  @Override
  protected Collection<String> getKnownDrivers() {
    return HIVE_DRIVERS;
  }

  @Override
  protected String getApplicationTitle() {
    Package pack = BeeLine.class.getPackage();

    return loc("app-introduction", "Beeline",
        pack.getImplementationVersion() == null ? "???"
            : pack.getImplementationVersion(),
        "Apache Hive");
        // getManifestAttribute ("Specification-Title"),
        // getManifestAttribute ("Implementation-Version"),
        // getManifestAttribute ("Implementation-ReleaseDate"),
        // getManifestAttribute ("Implementation-Vendor"),
        // getManifestAttribute ("Implementation-License"),
  }

  /**
   * Starts the program.
   */
  public static void main(String[] args) throws IOException {
    new BeeLine().start2(Arrays.asList(args), null, true);
  }

  /**
   * Starts the program with redirected input.
   *
   * <p>For redirected output, {@link #setOutputStream} and
   * {@link #setErrorStream} can be used.
   *
   * <p>Exits with 0 on success, 1 on invalid arguments, and 2 on any other
   * error.
   *
   * @param args        same as main()
   * @param inputStream redirected input, or null to use standard input
   */
  public static Status mainWithInputRedirection(
      String[] args,
      InputStream inputStream)
      throws IOException {
    return new BeeLine().start2(Arrays.asList(args), inputStream, false);
  }

  public BeeLine() {
    // disable default driver and URL for easier debugging; TODO: enable
    super(false,
        false ? BEELINE_DEFAULT_JDBC_DRIVER : null,
        false ? BEELINE_DEFAULT_JDBC_URL : null);
  }

  @Override
  protected void assignOptions(CommandLine cl) {
    super.assignOptions(cl);

    Properties hiveVars = cl.getOptionProperties("hivevar");
    for (String key : hiveVars.stringPropertyNames()) {
      getOpts().getHiveVariables().put(key, hiveVars.getProperty(key));
    }

    Properties hiveConfs = cl.getOptionProperties("hiveconf");
    for (String key : hiveConfs.stringPropertyNames()) {
      getOpts().getHiveConfVariables().put(key, hiveConfs.getProperty(key));
    }

    String auth = cl.getOptionValue("a");
    getOpts().setAuthType(auth);
  }

  @Override
  public BeeLineOpts getOpts() {
    return (BeeLineOpts) super.getOpts();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Append hive variables specified on the command line to the connection
   * url (after #). They will be set later on the session on the server side.
   */
  @Override
  public String fixUpUrl(String url, Map<String, String> info) {
    String auth = info.get("auth");
    if (auth == null) {
      auth = getOpts().getAuthType();
      if (auth != null) {
        info.put("auth", auth);
      }
    }

    final Map<String, String> hiveVars = getOpts().getHiveVariables();
    for (Map.Entry<String, String> var : hiveVars.entrySet()) {
      info.put(HIVE_VAR_PREFIX + var.getKey(), var.getValue());
    }

    final Map<String, String> hiveConfVars =
        getOpts().getHiveConfVariables();
    for (Map.Entry<String, String> var : hiveConfVars.entrySet()) {
      info.put(HIVE_CONF_PREFIX + var.getKey(), var.getValue());
    }
    return url;
  }

  @Override
  protected ResourceBundle res() {
    return BEE_LINE_RESOURCE_BUNDLE;
  }

  /** Resource bundle that looks in a list of underlying resource bundles. */
  static class ChainResourceBundle extends ResourceBundle {
    private final List<ResourceBundle> bundles;

    ChainResourceBundle(List<ResourceBundle> bundles) {
      this.bundles = bundles;
    }

    @Override
    protected Object handleGetObject(String key) {
      for (ResourceBundle bundle : bundles) {
        try {
          return bundle.getObject(key);
        } catch (MissingResourceException e) {
          // no resource -- look in next bundle
        }
      }
      return null;
    }

    @Override
    public Enumeration<String> getKeys() {
      final Set<String> keys = new HashSet<String>();
      for (ResourceBundle bundle : bundles) {
        final Enumeration<String> keyEnumeration = bundle.getKeys();
        while (keyEnumeration.hasMoreElements()) {
          keys.add(keyEnumeration.nextElement());
        }
      }
      final Iterator<String> keyIterator = keys.iterator();
      return new Enumeration<String>() {
        public boolean hasMoreElements() {
          return keyIterator.hasNext();
        }

        public String nextElement() {
          return keyIterator.next();
        }
      };
    }
  }
}
