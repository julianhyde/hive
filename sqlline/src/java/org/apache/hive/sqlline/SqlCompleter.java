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
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import jline.console.completer.StringsCompleter;

/** Suggests completions for SQL statements. */
class SqlCompleter extends StringsCompleter {
  public SqlCompleter(Set<String> completions) {
    super(completions);
  }

  public static SqlCompleter create(SqlLine sqlLine, boolean skipMeta)
      throws IOException, SQLException {
    return new SqlCompleter(getCompletions(sqlLine, skipMeta));
  }

  public static Set<String> getCompletions(SqlLine sqlLine, boolean skipMeta)
      throws IOException, SQLException {
    Set<String> completions = new TreeSet<String>();

    // add the default SQL completions
    StringBuilder keywords = new StringBuilder();
    keywords.append(
        new BufferedReader(
            new InputStreamReader(
                SqlCompleter.class.getResourceAsStream(
                    "sql-keywords.properties"))).readLine());

    // now add the keywords from the current connection
    final DatabaseMetaData metaData = sqlLine.getDatabaseConnection().meta;
    try {
      keywords.append(",").append(metaData.getSQLKeywords());
    } catch (Exception e) {
      debug(sqlLine, e, "getSQLKeywords");
    }
    try {
      keywords.append(",").append(metaData.getStringFunctions());
    } catch (Exception e) {
      debug(sqlLine, e, "getStringFunctions");
    }
    try {
      keywords.append(",").append(metaData.getNumericFunctions());
    } catch (Exception e) {
      debug(sqlLine, e, "getNumericFunctions");
    }
    try {
      keywords.append(",").append(metaData.getSystemFunctions());
    } catch (Exception e) {
      debug(sqlLine, e, "getSystemFunctions");
    }
    try {
      keywords.append(",").append(metaData.getTimeDateFunctions());
    } catch (Exception e) {
      debug(sqlLine, e, "getDateFunctions");
    }

    // also allow lower-case versions of all the keywords
    keywords.append(",").append(keywords.toString().toLowerCase());

    for (String keyword : SqlLine.tokenize(keywords.toString(), ", ")) {
      if (keyword.length() > 0) {
        completions.add(keyword);
      }
    }

    // now add the tables and columns from the current connection
    if (!skipMeta) {
      Collections.addAll(completions, sqlLine.getColumnNames(metaData));
    }

    return completions;
  }

  private static void debug(SqlLine sqlLine, Exception e, String method) {
    sqlLine.error(
        new RuntimeException("Method '" + method
            + "' failed while populating SQL completions", e));
  }
}
