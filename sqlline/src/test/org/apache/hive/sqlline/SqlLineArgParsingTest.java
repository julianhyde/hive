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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit test for how SqlLine parses command-line arguments.
 */
public class SqlLineArgParsingTest {
  @Test
  public void testSimpleArgs() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("-u", "url", "-n", "name",
        "-p", "password", "-d", "driver");
    Assert.assertTrue(bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
  }

  /**
   * The first flag is taken by the parser.
   */
  @Test
  public void testDuplicateArgs() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("-u", "url", "-u", "url2", "-n", "name",
        "-p", "password", "-d", "driver");
    Assert.assertTrue(bl.initArgs(args));
    assertThat(bl.connectArgs, equalTo("url name password driver"));
  }

  @Test
  public void testQueryScripts() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("-u", "url", "-n", "name",
      "-p", "password", "-d", "driver", "-e", "select1", "-e", "select2");
    Assert.assertTrue(bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.queries.contains("select1"));
    Assert.assertTrue(bl.queries.contains("select2"));
  }

  @Test
  public void testSqlLineOpts() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("-u", "url", "-n", "name",
        "-p", "password", "-d", "driver", "--autoCommit=true", "--verbose",
        "--truncateTable");
    Assert.assertTrue(bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getAutoCommit());
    Assert.assertTrue(bl.getOpts().getVerbose());
    Assert.assertTrue(bl.getOpts().getTruncateTable());
  }

  /**
   * Test setting script file with -f option.
   */
  @Test
  public void testScriptFile() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("-u", "url", "-n", "name",
      "-p", "password", "-d", "driver", "-f", "myscript");
    Assert.assertTrue(bl.initArgs(args));
    Assert.assertTrue(bl.connectArgs.equals("url name password driver"));
    Assert.assertTrue(bl.getOpts().getScriptFile().equals("myscript"));
  }

  /**
   * Displays the usage.
   */
  @Test
  public void testHelp() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("--help");
    Assert.assertFalse(bl.initArgs(args));
  }

  /**
   * Displays the usage.
   */
  @Test
  public void testUnmatchedArgs() throws Exception {
    ArgCapturingSqlLine bl = new ArgCapturingSqlLine();
    List<String> args = Arrays.asList("-u", "url", "-n");
    Assert.assertFalse(bl.initArgs(args));
  }

  /** Sub-class of SqlLine that merely parses arguments. */
  public static class ArgCapturingSqlLine extends SqlLine {
    String connectArgs = null;
    List<String> properties = new ArrayList<String>();
    List<String> queries = new ArrayList<String>();

    protected ArgCapturingSqlLine() {
      super(false, false, null, null);
    }

    @Override
    protected void dispatch(String command, DispatchCallback callback) {
      String connectCommand = "!connect";
      String propertyCommand = "!property";
      if (command.startsWith(connectCommand)) {
        this.connectArgs =
            command.substring(connectCommand.length() + 1, command.length());
      } else if (command.startsWith(propertyCommand)) {
        this.properties.add(
            command.substring(propertyCommand.length() + 1, command.length()));
      } else {
        this.queries.add(command);
      }
    }
  }
}
