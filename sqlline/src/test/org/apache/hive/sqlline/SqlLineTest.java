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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.StringContains;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * Test cases for SqlLine.
 */
public class SqlLineTest {
  private static final String JDBC_URL = "jdbc:hsqldb:mem:x";
  private static final String HSQLDB_JDBC_DRIVER = "org.hsqldb.jdbcDriver";

  /**
   * Public constructor.
   */
  public SqlLineTest() {}

  private List<String> getBaseArgs(String jdbcUrl) {
    return new ArrayList<String>(
        Arrays.asList("--silent=true", "-d", HSQLDB_JDBC_DRIVER,
            "-u", jdbcUrl));
  }

  /**
   * Attempts to run with a JDBC driver class that does not exist.
   */
  @Test
  public void testBadJdbcDriver() throws Exception {
    assertThat(
        checkScriptFile("show databases;\n",
            Arrays.asList("-d", "com.example.bad.Driver", "-u", JDBC_URL)),
        contains("0: jdbc:hsqldb:mem:x (closed)> show databases;\n"
            + "java.lang.ClassNotFoundException: com.example.bad.Driver\n"));
  }

  /**
   * Attempts to run with a JDBC URL for which there is no registered driver.
   */
  @Test
  public void testNoJdbcDriver() throws Exception {
    assertThat(
        checkScriptFile("show databases;\n",
            Arrays.asList("-d", HSQLDB_JDBC_DRIVER, "-u", "notJdbc:fooBase:")),
        contains("0: notJdbc:fooBase: (closed)> show databases;\n"
            + "No known driver to handle \"notJdbc:fooBase:\". Searching for known drivers...\n"));
  }

  /**
   * Attempt to execute a simple script file with the -f option to SqlLine.
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found.
   */
  @Test
  public void testPositiveScriptFile() throws Throwable {
    assertThat(checkScriptFile("values 1;\n", getBaseArgs(JDBC_URL)),
        contains("0: jdbc:hsqldb:mem:x> values 1;\n"
            + "+-------------+\n"
            + "|     C1      |\n"
            + "+-------------+\n"
            + "| 1           |\n"
            + "+-------------+\n"
            + "0: jdbc:hsqldb:mem:x> "));
  }

  /** Until we upgrade junit and can use CoreMatchers.contains. */
  private static StringContains contains(String substring) {
    return new StringContains(substring);
  }

  /**
   * Attempts to execute a simple script file with the -f option to SqlLine.
   * The first command should fail and the second command should not execute.
   */
  @Test
  public void testBreakOnErrorScriptFile() throws Throwable {
    assertThat(
        checkScriptFile("select * from abcdefg01;\nshow databases;\n",
            getBaseArgs(JDBC_URL)),
        CoreMatchers.not(contains(" default ")));
  }

  /**
   * Select null from table; checks how null is printed
   */
  @Test
  public void testNullDefault() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(
        checkScriptFile(
            "select null from (values 1);\n",
            argList),
        contains(" NULL "));
  }

  /**
   * Selects null from table; checks that default null is printed differently.
   */
  @Test
  public void testNullNonEmpty() throws Throwable {
    assertThat(
        checkScriptFile(
            "!set nullemptystring false\n"
            + "select null as n from (values 1);\n",
            getBaseArgs(JDBC_URL)),
        contains("NULL"));
  }

  @Test
  public void testGetVariableValue() throws Throwable {
    assertThat(
        checkScriptFile("set env:TERM;", getBaseArgs(JDBC_URL)),
        contains("env:TERM"));
  }

  /**
   * Selects null from table; check if setting null to empty string works.
   * Original beeline/sqlline used to print nulls as empty strings.
   */
  @Test
  public void testNullEmpty() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--outputformat=csv");
    assertThat(
        checkScriptFile(
            "!set nullemptystring true\n"
            + "select 'abc',null,'d ef','g,h' from (values 1);\n",
            argList),
        contains("abc,,d ef,\"g,h\""));
  }

  /**
   * Selects null from table;
   * check if setting null to empty string works;
   * using sqlline cmd line argument.
   * Original sqlline used to print nulls as empty strings.
   */
  @Test
  public void testNullEmptyCmdArg() throws Exception {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--nullemptystring=true");
    argList.add("--outputformat=csv");
    assertThat(
        checkScriptFile("select 'abc',null,'def' from (values 1);\n", argList),
        contains("abc,,def"));
  }

  /**
   * Delimiter-separated format.
   */
  @Test
  public void testDsv() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--outputformat=dsv");
    argList.add("--delimiterForDsv=:");
    assertThat(
        checkScriptFile(
            "select 'a|b c',null,'d:ef' from (values 1);\n",
            argList),
        contains(
            "C1:C2:C3\n"
                + "a|b c:NULL:\"d:ef\"\n"));
  }

  /**
   * Delimiter-separated format with default delimiter and with null as empty
   * string.
   */
  @Test
  public void testDsvDefaultNullEmpty() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--outputformat=dsv");
    argList.add("--nullemptystring=true");
    assertThat(
        checkScriptFile(
            "select 'a|b c',null,'d:ef' from (values 1);\n",
            argList),
        contains(
            "C1|C2|C3\n"
            + "\"a|b c\"||d:ef\n"));
  }

  /**
   * Attempt to execute a missing script file with the -f option to SqlLine
   */
  @Test
  public void testNegativeScriptFile() throws Exception {
    // Create and delete a temp file
    File scriptFile = makeFile("dummy");
    scriptFile.delete();

    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("-f");
    argList.add(scriptFile.getAbsolutePath());

    assertThat(checkCommandLineScript(argList),
        contains("FileNotFoundException"));
  }

  /**
   * Launches sqlline with "-r propertiesFile".
   */
  @Test
  public void testPropertiesFile() throws Exception {
    // Create and delete a temp file
    File propertiesFile = File.createTempFile("props", ".properties");
    final FileOutputStream fos = new FileOutputStream(propertiesFile);
    final PrintStream ps = new PrintStream(fos);
    ps.println("driver=" + HSQLDB_JDBC_DRIVER);
    ps.println("url=" + JDBC_URL + "yyy");
    ps.println("user=sa");
    ps.println("password=");
    ps.close();

    final List<String> argList = new ArrayList<String>();
    argList.add("--silent=true");
    argList.add("-r");
    argList.add(propertiesFile.getAbsolutePath());

    assertThat(checkScriptFile("values (1)", argList),
        contains("jdbc:hsqldb:mem:xyyy> values"));
    propertiesFile.delete();
  }

  /**
   * Tests that a bad command-line argument gives an error.
   */
  @Test
  public void testBadArg() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("-badarg");

    assertThat(checkScriptFile("values (1)", argList),
        contains(
            "Unrecognized option: -badarg\n"
            + "Usage: java org.apache.hive.sqlline.SqlLine \n"
            + "   -u <database url>               the JDBC URL"));
  }

  /**
   * Tests the "close" command.
   */
  @Test
  public void testClose() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(
        checkScriptFile(
            "values (1);\n"
            + "!close\n"
            + "values (1);\n"
            + "!close\n",
            argList),
        contains(
            "0: jdbc:hsqldb:mem:x> values (1);\n"
            + "+-------------+\n"
            + "|     C1      |\n"
            + "+-------------+\n"
            + "| 1           |\n"
            + "+-------------+\n"
            + "0: jdbc:hsqldb:mem:x> !close\n"
            + "sqlline> values (1);\n"
            + "No current connection\n"));
  }

  /**
   * Tests various kinds of comments: '--' and '#', with and without preceding
   * spaces.
   */
  @Test
  public void testComments() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(checkScriptFile("-- a comment\n", argList),
        contains(
            "0: jdbc:hsqldb:mem:x> -- a comment\n"
            + "0: jdbc:hsqldb:mem:x> "));
    assertThat(checkScriptFile(" -- a comment\n", argList),
        contains(
            "0: jdbc:hsqldb:mem:x>  -- a comment\n"
            + "0: jdbc:hsqldb:mem:x> "));
    assertThat(checkScriptFile(" ## a -- comment ##  \n", argList),
        contains(
            "0: jdbc:hsqldb:mem:x>  ## a -- comment ##  \n"
            + "0: jdbc:hsqldb:mem:x> "));
    assertThat(checkScriptFile("# a comment\n", argList),
        contains(
            "0: jdbc:hsqldb:mem:x> # a comment\n"
            + "0: jdbc:hsqldb:mem:x> "));
  }

  /**
   * Test that BeeLine will read comment lines that start with whitespace.
   */
  @Test
  public void testWhitespaceBeforeCommentScriptFile() throws Exception {
    List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(
        checkScriptFile(
            "\t\t \t \t-- comment has spaces and tabs before it\n"
            + "   \t # comment has spaces and tabs before it\n"
            + "!set color true\n",
            argList),
        allOf(
            not(contains("cannot recognize input near '<EOF>'")),
            contains("jdbc:hsqldb:mem:x> !set color true\n")));
  }

  /**
   * HIVE-4566
   * @throws java.io.UnsupportedEncodingException
   */
  @Test
  public void testNPE() throws UnsupportedEncodingException {
    SqlLine sqlLine = new SqlLine(false, false, null, null);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream sqllineOutputStream = new PrintStream(os);
    sqlLine.setOutputStream(sqllineOutputStream);
    sqlLine.setErrorStream(sqllineOutputStream);

    final DispatchCallback callback = new DispatchCallback();
    sqlLine.runCommands(Arrays.asList("!typeinfo"), callback);
    String output = os.toString("UTF8");
    Assert.assertFalse(output.contains("java.lang.NullPointerException"));
    Assert.assertTrue(output.contains("No current connection"));

    sqlLine.runCommands(Arrays.asList("!nativesql"), callback);
    output = os.toString("UTF8");
    Assert.assertFalse(output.contains("java.lang.NullPointerException"));
    Assert.assertTrue(output.contains("No current connection"));
  }

  /**
   * Unit test for {@link org.apache.hive.sqlline.SqlLine#splitCompound(String)}.
   */
  @Test public void testSplitCompound() {
    final SqlLine line = new SqlLine(false, false, null, null);
    String[][] strings;

    // simple line
    strings = line.splitCompound("abc de  fgh");
    assertEquals(new String[][] {{"ABC"}, {"DE"}, {"FGH"}}, strings);

    // line with double quotes
    strings = line.splitCompound("abc \"de fgh\" ijk");
    assertEquals(new String[][] {{"ABC"}, {"de fgh"}, {"IJK"}}, strings);

    // line with double quotes as first and last
    strings = line.splitCompound("\"abc de\"  fgh \"ijk\"");
    assertEquals(new String[][] {{"abc de"}, {"FGH"}, {"ijk"}}, strings);

    // escaped double quotes, and dots inside quoted identifiers
    strings = line.splitCompound("\"ab.c \"\"de\"  fgh.ij");
    assertEquals(new String[][] {{"ab.c \"de"}, {"FGH", "IJ"}}, strings);

    // single quotes do not affect parsing
    strings = line.splitCompound("'abc de'  fgh");
    assertEquals(new String[][] {{"'ABC"}, {"DE'"}, {"FGH"}}, strings);

    // incomplete double-quoted identifiers are implicitly completed
    strings = line.splitCompound("abcdefgh   \"ijk");
    assertEquals(new String[][] {{"ABCDEFGH"}, {"ijk"}}, strings);

    // dot at start of line is illegal, but we are lenient and ignore it
    strings = line.splitCompound(".abc def.gh");
    assertEquals(new String[][] {{"ABC"}, {"DEF", "GH"}}, strings);

    // spaces around dots are fine
    strings = line.splitCompound("abc de .  gh .i. j");
    assertEquals(new String[][] {{"ABC"}, {"DE", "GH", "I", "J"}}, strings);

    // double-quote inside an unquoted identifier is treated like a regular
    // character; should be an error, but we are lenient
    strings = line.splitCompound("abc\"de \"fg\"");
    assertEquals(new String[][] {{"ABC\"DE"}, {"fg"}}, strings);

    // null value only if unquoted
    strings = line.splitCompound("abc null");
    assertEquals(new String[][] {{"ABC"}, {null}}, strings);
    strings = line.splitCompound("abc foo.null.bar");
    assertEquals(new String[][] {{"ABC"}, {"FOO", null, "BAR"}}, strings);
    strings = line.splitCompound("abc foo.\"null\".bar");
    assertEquals(new String[][] {{"ABC"}, {"FOO", "null", "BAR"}}, strings);
    strings = line.splitCompound("abc foo.\"NULL\".bar");
    assertEquals(new String[][] {{"ABC"}, {"FOO", "NULL", "BAR"}}, strings);

    // trim trailing whitespace and semicolon
    strings = line.splitCompound("abc ;\t     ");
    assertEquals(new String[][] {{"ABC"}}, strings);
    // keep semicolon inside line
    strings = line.splitCompound("abc\t;def");
    assertEquals(new String[][] {{"ABC"}, {";DEF"}}, strings);
  }

  void assertEquals(String[][] expectedses, String[][] actualses) {
    assertThat(actualses.length, equalTo(expectedses.length));
    for (int i = 0; i < expectedses.length; ++i) {
      String[] expecteds = expectedses[i];
      String[] actuals = actualses[i];
      assertThat(actuals.length, equalTo(expecteds.length));
      for (int j = 0; j < expecteds.length; ++j) {
        String expected = expecteds[j];
        String actual = actuals[j];
        assertThat(actual, equalTo(expected));
      }
    }
  }

  @Test public void testManual() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final SqlLine line = new SqlLine(false, false, null, null);
    final PrintStream ps = new PrintStream(out);
    line.setOutputStream(ps);
    line.setErrorStream(ps);
    line.runCommands(Collections.singletonList("!manual"),
        new DispatchCallback());
    ps.flush();
    assertThat(out.toString().contains("License and Terms of Use"), is(true));
  }

  /** Tests that usage is printed exactly once. */
  @Test public void testHelp() throws Exception {
    final String out = checkCommandLineScript(Arrays.asList("--help"));
    assertThat(out,
        out.startsWith("Usage: java org.apache.hive.sqlline.SqlLine"),
        is(true));
    int n = 0;
    int i = -1;
    while ((i = out.indexOf("the JDBC URL", i + 1)) >= 0) {
      ++n;
    }
    assertThat(n, equalTo(1));
  }

  /** Tests '-e' option.
   *
   * <p>[HIVE-5765] - Beeline throws NPE when -e option is used. */
  @Test public void testE() throws Exception {
    final String out = checkCommandLineScript(Arrays.asList("-e", "!closeall"));
    assertThat(out, out.startsWith("sqlline version"), is(true));
  }

  @Test public void testError() throws IOException {
    // as file, quits and returns error
    checkFile("!bad-command\n!set color\n",
        "sqlline> !bad-command\nUnknown command: bad-command\n\n",
        SqlLine.Status.OTHER);

    // as input, carries on, and returns OK
    checkIn("!bad-command\n!set color\n",
        "sqlline> !bad-command\n"
        + "Unknown command: bad-command\n"
        + "sqlline> !set color\n"
        + "Usage: set <key> <value>\n"
        + "sqlline> ");
  }

  @Test public void testSet() throws IOException {
    checkInOut(true,
        "!set color red\n"
            + "!set\n",
        allOf(
            contains("sqlline> !set color red\n"),
            contains("\ncolor               false\n")),
        SqlLine.Status.OK);
  }

  @Test public void testNullEmptyString() throws IOException {
    checkIn(
        "!nullemptystring true\n"
        + "!nullemptystring false\n"
        + "!nullemptystring 1\n"
        + "!nullemptystring yes\n"
        + "!nullemptystring\n"
        + "!nullemptystring \n"
        + "!nullemptystring foo\n"
        + "!set nullemptystring true\n"
        + "!set nullemptystring foo\n",
        "sqlline> !nullemptystring true\n"
        + "sqlline> !nullemptystring false\n"
        + "sqlline> !nullemptystring 1\n"
        + "sqlline> !nullemptystring yes\n"
        + "sqlline> !nullemptystring\n"
        + "Usage: set <key> <value>\n"
        + "sqlline> !nullemptystring \n"
        + "Usage: set <key> <value>\n"
        + "sqlline> !nullemptystring foo\n"
        + "sqlline> !set nullemptystring true\n"
        + "sqlline> !set nullemptystring foo\n"
        + "sqlline> ");

    checkFile("!set nullemptystring\n",
        "sqlline> !set nullemptystring\n"
        + "Usage: set <key> <value>\n\n",
        SqlLine.Status.OTHER);
  }

  private void checkFile(String in, String expectedOut,
      SqlLine.Status expectedStatus) throws IOException {
    checkInOut(true, in, equalTo(expectedOut), expectedStatus);
  }

  private void checkIn(String in, String expectedOut) throws IOException {
    checkInOut(false, in, equalTo(expectedOut), SqlLine.Status.OK);
  }

  private void checkInOut(boolean asFile,
      String in, Matcher<String> outputMatcher,
      SqlLine.Status expectedStatus) throws IOException {
    final SqlLine line = new SqlLine(false, false, null, null);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PrintStream ps = new PrintStream(out);
    line.setOutputStream(ps);
    line.setErrorStream(ps);
    final ByteArrayInputStream inputStream;
    final List<String> args;
    File scriptFile;
    if (asFile) {
      inputStream = new ByteArrayInputStream(new byte[0]);
      scriptFile = makeFile(in);
      args = Arrays.asList("-f", scriptFile.getAbsolutePath());
    } else {
      inputStream = new ByteArrayInputStream(in.getBytes());
      args = Arrays.asList("--silent");
      scriptFile = null;
    }
    SqlLine.Status status =
        line.begin(args, inputStream);
    assertThat(out.toString(), outputMatcher);
    assertThat(status, equalTo(expectedStatus));
    if (scriptFile != null) {
      scriptFile.delete();
    }
  }

  private File makeFile(String in) throws IOException {
    final File scriptFile = File.createTempFile("sqlline", ".sql");
    final FileOutputStream fos = new FileOutputStream(scriptFile);
    fos.write(in.getBytes());
    fos.close();
    return scriptFile;
  }

  /** Tests a run that has both an init file and a script file. */
  @Test public void testInitAndScript() throws IOException {
    final SqlLine line = new SqlLine(false, false, null, null);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PrintStream ps = new PrintStream(out);
    line.setOutputStream(ps);
    line.setErrorStream(ps);
    File initFile = makeFile("!set color true");
    File scriptFile = makeFile("!set color false");
    final List<String> args = Arrays.asList(
        "-i", initFile.getAbsolutePath(),
        "-f", scriptFile.getAbsolutePath(),
        "-d", HSQLDB_JDBC_DRIVER,
        "-u", JDBC_URL);

    SqlLine.Status status = line.begin(args, null);
    assertThat(out.toString(), new BaseMatcher<String>() {
      @Override
      public boolean matches(Object o) {
        return ((String) o).matches(
            "Running init script .*.sql\n"
            + "0: jdbc:hsqldb:mem:x> !set color true\n"
            + "0: jdbc:hsqldb:mem:x> !set color false\n"
            + "Closing: 0: jdbc:hsqldb:mem:x\n");
      }

      @Override
      public void describeTo(Description description) {
      }
    });
    assertThat(status, equalTo(SqlLine.Status.OK));
    initFile.delete();
    scriptFile.delete();
  }

  @Test public void testShell() throws IOException {
    checkFile("!sh echo 123 45\n",
        "sqlline> !sh echo 123 45\n123 45\nsqlline> \n",
        SqlLine.Status.OK);
    checkFile("!sh false\n",
        "sqlline> !sh false\nCommand failed with exit code = 1\n\n",
        SqlLine.Status.OTHER);
    checkFile("!sh\n",
        "sqlline> !sh\n\n",
        SqlLine.Status.OTHER);
  }

  @Test public void testWrap() {
    assertThat(SqlLine.wrap("ab cd ef", 1, 0), equalTo("\nab\ncd\nef"));
    assertThat(SqlLine.wrap("ab cd ef", 2, 0), equalTo("ab\ncd\nef"));
    assertThat(SqlLine.wrap("ab cd ef", 3, 0), equalTo("ab\ncd\nef"));
    assertThat(SqlLine.wrap("ab cd ef", 4, 0), equalTo("ab cd\nef"));
    assertThat(SqlLine.wrap("ab cd ef", 5, 0), equalTo("ab cd\nef"));
    assertThat(SqlLine.wrap("ab cd ef", 6, 0), equalTo("ab cd\nef"));

    assertThat(SqlLine.wrap("ab cde e f", 3, 0), equalTo("ab\ncde\ne f"));
    assertThat(SqlLine.wrap("ab cde e f", 3, 2), equalTo("ab\n  cde\n  e f"));
  }

  @Test public void testCenter() {
    assertThat(SqlLine.center("abc", 1), equalTo("abc"));
    assertThat(SqlLine.center("abc", 3), equalTo("abc"));
    assertThat(SqlLine.center("abc", 5), equalTo(" abc "));
    assertThat(SqlLine.center("abc", 6), equalTo(" abc  "));
    assertThat(SqlLine.center("abc", 7), equalTo("  abc  "));
    assertThat(SqlLine.center("abc", 8), equalTo("  abc   "));
  }

  /**
   * Executes a script.
   */
  private String checkCommandLineScript(List<String> argList) throws Exception {
    SqlLine sqlLine = new SqlLine(false, false, null, null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    sqlLine.setOutputStream(ps);
    sqlLine.setErrorStream(ps);
    sqlLine.begin(argList, null);
    return os.toString("UTF8");
  }

  /**
   * Attempts to execute a simple script file with the -f option to SqlLine.
   *
   * <p>Tests for presence of an expected pattern
   * in the output (stdout or stderr), fails if not found.
   *
   * @throws Exception on command execution error
   */
  private String checkScriptFile(String scriptText, List<String> argList)
      throws Exception {
    // Put the script content in a temp file
    File scriptFile = File.createTempFile("sqllinetest", ".sql");
    scriptFile.deleteOnExit();
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    os.print(scriptText);
    os.close();

    final ArrayList<String> argList2 = new ArrayList<String>(argList);
    argList2.add("-f");
    argList2.add(scriptFile.getAbsolutePath());

    String output = checkCommandLineScript(argList2);
    scriptFile.delete();
    return output;
  }

  /** Tabs should be treated as whitespace in script files; they should not
   * trigger completion. */
  @Test public void testTab() throws Exception {
    assertThat(
        checkScriptFile("!set color\t red\n", Arrays.<String>asList()),
        equalTo("sqlline> !set color red\n"
            + "sqlline> \n"));
  }

  /** Output with header. */
  @Test public void testHeader() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(
        checkScriptFile(
            "!set showheader true\n"
            + "!set headerinterval 2\n"
            + "select * from (values (1, 'a'), (2, 'b'), (3, 'c'))\n"
            + "  as t(x, y);\n",
            argList),
        equalTo(
            "0: jdbc:hsqldb:mem:x> !set showheader true\n"
            + "0: jdbc:hsqldb:mem:x> !set headerinterval 2\n"
            + "0: jdbc:hsqldb:mem:x> select * from (values (1, 'a'), (2, 'b'), (3, 'c'))\n"
            + ". . . . . . . . . . >   as t(x, y);\n"
            + "+-------------+---+\n"
            + "|      X      | Y |\n"
            + "+-------------+---+\n"
            + "| 1           | a |\n"
            + "| 2           | b |\n"
            + "+-------------+---+\n"
            + "|      X      | Y |\n"
            + "+-------------+---+\n"
            + "| 3           | c |\n"
            + "+-------------+---+\n"
            + "0: jdbc:hsqldb:mem:x> \n"));
  }

  /** Output with headers disabled. */
  @Test public void testNoHeader() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(
        checkScriptFile(
            "!set showheader false\n"
            + "!set headerinterval 2\n"
            + "select * from (values (1, 'a'), (2, 'b'), (3, 'c'))\n"
            + "  as t(x, y);\n",
            argList),
        equalTo(
            "0: jdbc:hsqldb:mem:x> !set showheader false\n"
            + "0: jdbc:hsqldb:mem:x> !set headerinterval 2\n"
            + "0: jdbc:hsqldb:mem:x> select * from (values (1, 'a'), (2, 'b'), (3, 'c'))\n"
            + ". . . . . . . . . . >   as t(x, y);\n"
            + "+-------------+---+\n"
            + "| 1           | a |\n"
            + "| 2           | b |\n"
            + "| 3           | c |\n"
            + "+-------------+---+\n"
            + "0: jdbc:hsqldb:mem:x> \n"));
  }

  /** Output a wide table with truncateTable=false. */
  @Test public void testTruncate() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--truncateTable=false");
    argList.add("--maxWidth=20");
    assertThat(
        checkScriptFile(
            "select * from (values ('abcdefghij', 'abcdef', 1234567))\n"
            + "  as t(x, y, z);\n",
            argList),
        equalTo(
            "0: jdbc:hsqldb:mem:x> select * from (values ('abcdefghij', 'abcdef', 1234567))\n"
            + ". . . . . . . . . . >   as t(x, y, z);\n"
            + "+------------+--------+-------------+\n"
            + "|     X      |   Y    |      Z      |\n"
            + "+------------+--------+-------------+\n"
            + "| abcdefghij | abcdef | 1234567     |\n"
            + "+------------+--------+-------------+\n"
            + "0: jdbc:hsqldb:mem:x> \n"));
  }

  /** Output a wide table with truncateTable=true. */
  @Test public void testNoTruncate() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--truncateTable=true");
    argList.add("--maxWidth=20");
    assertThat(
        checkScriptFile(
            "select * from (values ('abcdefghij', 'abcdef', 1234567))\n"
            + "  as t(x, y, z);\n",
            argList),
        equalTo(
            "0: jdbc:hsqldb:mem:x> select * from (values ('abcdefghij', 'abcdef', 1234567))\n"
            + ". . . . . . . . . . >   as t(x, y, z);\n"
            + "+------------+-----+\n"
            + "|     X      |   Y |\n"
            + "+------------+-----+\n"
            + "| abcdefghij | abc |\n"
            + "+------------+-----+\n"
            + "0: jdbc:hsqldb:mem:x> \n"));
  }

  /** After closing a connection, prompt changes to "sqlline> ". */
  @Test public void testPromptOnClosedConnection() throws Exception {
    final List<String> argList = getBaseArgs(JDBC_URL);
    assertThat(
        checkScriptFile(
            "values 1;\n"
            + "!close\n"
            + "values 2;\n",
            argList),
        equalTo(
            "0: jdbc:hsqldb:mem:x> values 1;\n"
            + "+-------------+\n"
            + "|     C1      |\n"
            + "+-------------+\n"
            + "| 1           |\n"
            + "+-------------+\n"
            + "0: jdbc:hsqldb:mem:x> !close\n"
            + "sqlline> values 2;\n"
            + "No current connection\n\n"));
  }
}

// End SqlLineTest.java
