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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.internal.matchers.StringContains;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test cases for SqlLine.
 */
public class SqlLineTest {
  /**
   * Public constructor.
   */
  public SqlLineTest() {}

  /**
   * Runs a script.
   *
   * @throws java.lang.Exception Any exception while executing
   * @return The stderr and stdout from running the script
   */
  private static String runScript(List<String> argList) throws Exception {
    SqlLine beeLine = new SqlLine(false, null, null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    beeLine.setOutputStream(ps);
    beeLine.setErrorStream(ps);
    beeLine.begin(argList, null, false);
    return os.toString("UTF8");
  }

  /**
   * Unit test for {@link org.apache.hive.sqlline.SqlLine#splitCompound(String)}.
   */
  @Test public void testSplitCompound() {
    final SqlLine line = new SqlLine(false, null, null);
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
    final SqlLine line = new SqlLine(false, null, null);
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
    final String out = runScript(Arrays.asList("--help"));
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
    final String out = runScript(Arrays.asList("-e", "!closeall"));
    assertThat(out, out.startsWith("sqlline version"), is(true));
  }

  @Test public void testError() throws IOException {
    checkInOut("!bad-command\n",
        "sqlline> !bad-command\nUnknown command: bad-command\nsqlline> ",
        SqlLine.Status.OTHER);
  }

  @Test public void testSet() throws IOException {
    checkInOut("!set color red\n"
            + "!set\n",
        CoreMatchers.allOf(
            new StringContains("sqlline> !set color red\n"),
            new StringContains("\ncolor               false\n")),
        SqlLine.Status.OK);
  }

  @Test public void testNullEmptyString() throws IOException {
    checkInOut("!nullemptystring true\n"
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
        + "sqlline> ",
        SqlLine.Status.OK);

    checkInOut("!set nullemptystring\n",
        "sqlline> !set nullemptystring\n"
        + "Usage: set <key> <value>\n"
        + "sqlline> ",
        SqlLine.Status.OTHER);
  }

  private void checkInOut(String in, String expectedOut,
      SqlLine.Status expectedStatus) throws IOException {
    checkInOut(in, equalTo(expectedOut), expectedStatus);
  }

  private void checkInOut(String in, Matcher<String> outputMatcher,
      SqlLine.Status expectedStatus) throws IOException {
    final SqlLine line = new SqlLine(false, null, null);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final PrintStream ps = new PrintStream(out);
    line.setOutputStream(ps);
    line.setErrorStream(ps);
    final ByteArrayInputStream inputStream =
        new ByteArrayInputStream(in.getBytes());
    SqlLine.Status status =
        line.begin(Arrays.asList("--silent"), inputStream, false);
    assertThat(out.toString(), outputMatcher);
    assertThat(status, equalTo(expectedStatus));
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
    SqlLine sqlLine = new SqlLine(false, null, null);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    sqlLine.setOutputStream(ps);
    sqlLine.setErrorStream(ps);
    sqlLine.begin(argList, null, false);
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
        equalTo("sqlline version ???\nsqlline> !set color red\nsqlline> "));
  }
}

// End SqlLineTest.java
