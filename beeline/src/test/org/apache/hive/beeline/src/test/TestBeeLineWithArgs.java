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
package org.apache.hive.beeline.src.test;

import java.io.File;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hive.sqlline.DispatchCallback;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hive.beeline.BeeLine;
import org.apache.hive.sqlline.SqlLine;
import org.apache.hive.service.server.HiveServer2;

/**
 * TestSqlLineWithArgs - executes tests of the command-line arguments to SqlLine
 *
 */
public class TestBeeLineWithArgs {

  // Default location of HiveServer2
  final static String BASE_JDBC_URL = BeeLine.BEELINE_DEFAULT_JDBC_URL + "localhost:10000";
  //set JDBC_URL to something else in test case, if it needs to be customized
  String JDBC_URL = BASE_JDBC_URL;

  private static HiveServer2 hiveServer2;

  private List<String> getBaseArgs(String jdbcUrl) {
    List<String> argList = new ArrayList<String>(8);
    argList.add("-d");
    argList.add(BeeLine.BEELINE_DEFAULT_JDBC_DRIVER);
    argList.add("-u");
    argList.add(jdbcUrl);
    return argList;
  }

  /**
   * Start up a local Hive Server 2 for these tests
   */
  @BeforeClass
  public static void preTests() throws Exception {
    HiveConf hiveConf = new HiveConf();
    //  hiveConf.logVars(System.err);
    // System.err.flush();

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    System.err.println("Starting HiveServer2...");
    hiveServer2.start();
    Thread.sleep(1000);
  }

  /**
   * Shut down a local Hive Server 2 for these tests
   */
  @AfterClass
  public static void postTests() {
    try {
      if (hiveServer2 != null) {
        System.err.println("Stopping HiveServer2...");
        hiveServer2.stop();
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * Execute a script.
   *
   * @throws Throwable Any exception while executing
   * @return The stderr and stdout from running the script
   */
  private String testCommandLineScript(List<String> argList) throws Throwable {
    SqlLine beeLine = new BeeLine();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream sqllineOutputStream = new PrintStream(os);
    beeLine.setOutputStream(sqllineOutputStream);
    beeLine.setErrorStream(sqllineOutputStream);
    beeLine.begin(argList, null, false);
    String output = os.toString("UTF8");

    return output;
  }

  /**
   * Attempt to execute a simple script file with the -f option to SqlLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   *
   * @param testName Name of test to print
   * @param expectedPattern Text to look for in command output
   * @param shouldMatch true if the pattern should be found, false if it should not
   * @throws Exception on command execution error
   */
  private void testScriptFile(String testName, String scriptText, String expectedPattern,
      boolean shouldMatch, List<String> argList) throws Throwable {

    long startTime = System.currentTimeMillis();
    System.out.println(">>> STARTED " + testName);

    // Put the script content in a temp file
    File scriptFile = File.createTempFile(testName, "temp");
    scriptFile.deleteOnExit();
    PrintStream os = new PrintStream(new FileOutputStream(scriptFile));
    os.print(scriptText);
    os.close();

    argList.add("-f");
    argList.add(scriptFile.getAbsolutePath());

    if(shouldMatch) {
      try {
        String output = testCommandLineScript(argList);
        long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
        String time = "(" + elapsedTime + "s)";
        if (output.contains(expectedPattern)) {
          System.out.println(">>> PASSED " + testName + " " + time);
        } else {
          System.err.println("Output: " + output);
          System.err.println(">>> FAILED " + testName + " (ERROR) " + time);
          Assert.fail(testName);
        }
      } catch (Throwable e) {
        e.printStackTrace();
        throw e;
      }
    } else {
      try {
        String output = testCommandLineScript(argList);
        long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
        String time = "(" + elapsedTime + "s)";
        if (output.contains(expectedPattern)) {
          System.err.println("Output: " + output);
          System.err.println(">>> FAILED " + testName + " (ERROR) " + time);
          Assert.fail(testName);
        } else {
          System.out.println(">>> PASSED " + testName + " " + time);
        }
      } catch (Throwable e) {
        System.err.println("Exception: " + e.toString());
        e.printStackTrace();
        throw e;
      }
    }
    scriptFile.delete();
  }

  /**
   * Attempt to execute a simple script file with the -f option to SqlLine
   * Test for presence of an expected pattern
   * in the output (stdout or stderr), fail if not found
   * Print PASSED or FAILED
   */
  @Test
  public void testPositiveScriptFile() throws Throwable {
    final String TEST_NAME = "testPositiveScriptFile";
    final String SCRIPT_TEXT = "show databases;\n";
    final String EXPECTED_PATTERN = " default ";
    List<String> argList = getBaseArgs(JDBC_URL);
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Test Beeline -hivevar option. User can specify --hivevar name=value on Beeline command line.
   * In the script, user should be able to use it in the form of ${name}, which will be substituted with
   * the value.
   * @throws Throwable
   */
  @Test
  public void testBeelineCommandLineHiveVariable() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("--hivevar");
    argList.add("DUMMY_TBL=dummy");
    final String TEST_NAME = "testHiveCommandLineHiveVariable";
    final String SCRIPT_TEXT = "create table ${DUMMY_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }

  /**
   * Attempt to execute a simple script file with the -f option to SqlLine
   * The first command should fail and the second command should not execute
   * Print PASSED or FAILED
   */
  @Test
  public void testBreakOnErrorScriptFile() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL);
    final String TEST_NAME = "testBreakOnErrorScriptFile";
    final String SCRIPT_TEXT = "select * from abcdefg01;\nshow databases;\n";
    final String EXPECTED_PATTERN = " default ";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, false, argList);
  }

  /**
   * Attempt to execute a missing script file with the -f option to SqlLine
   * Print PASSED or FAILED
   */
  @Test
  public void testNegativeScriptFile() throws Throwable {
    final String TEST_NAME = "testNegativeScriptFile";
    final String EXPECTED_PATTERN = " default ";

    long startTime = System.currentTimeMillis();
    System.out.println(">>> STARTED " + TEST_NAME);

    // Create and delete a temp file
    File scriptFile = File.createTempFile("sqllinenegative", "temp");
    scriptFile.delete();

    List<String> argList = getBaseArgs(JDBC_URL);
    argList.add("-f");
    argList.add(scriptFile.getAbsolutePath());

    try {
      String output = testCommandLineScript(argList);
      long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
      String time = "(" + elapsedTime + "s)";
      if (output.contains(EXPECTED_PATTERN)) {
        System.err.println("Output: " + output);
        System.err.println(">>> FAILED " + TEST_NAME + " (ERROR) " + time);
        Assert.fail(TEST_NAME);
      } else {
        System.out.println(">>> PASSED " + TEST_NAME + " " + time);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * HIVE-4566
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testNPE() throws UnsupportedEncodingException {
    SqlLine sqlLine = new BeeLine();

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream sqllineOutputStream = new PrintStream(os);
    sqlLine.setOutputStream(sqllineOutputStream);
    sqlLine.setErrorStream(sqllineOutputStream);

    final DispatchCallback callback = new DispatchCallback();
    sqlLine.runCommands(Arrays.asList("!typeinfo"), callback);
    String output = os.toString("UTF8");
    Assert.assertFalse( output.contains("java.lang.NullPointerException") );
    Assert.assertTrue( output.contains("No current connection") );

    sqlLine.runCommands(Arrays.asList("!nativesql"), callback);
    output = os.toString("UTF8");
    Assert.assertFalse(output.contains("java.lang.NullPointerException"));
    Assert.assertTrue( output.contains("No current connection") );
    System.out.println(">>> PASSED " + "testNPE");
  }

  @Test
  public void testHiveVarSubstitution() throws Throwable {
    List<String> argList = getBaseArgs(JDBC_URL + "#D_TBL=dummy_t");
    final String TEST_NAME = "testHiveVarSubstitution";
    final String SCRIPT_TEXT = "create table ${D_TBL} (d int);\nshow tables;\n";
    final String EXPECTED_PATTERN = "dummy_t";
    testScriptFile(TEST_NAME, SCRIPT_TEXT, EXPECTED_PATTERN, true, argList);
  }
}
