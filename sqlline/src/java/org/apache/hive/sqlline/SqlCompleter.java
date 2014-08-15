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
import java.util.Set;
import java.util.TreeSet;

import jline.console.completer.StringsCompleter;

/** Suggests completions for SQL statements. */
class SqlCompleter extends StringsCompleter {
  public SqlCompleter(SqlLine sqlLine, boolean skipMeta)
      throws IOException, SQLException {
    super(new String[0]);

    Set<String> completions = new TreeSet<String>();

    // add the default SQL completions
    String keywords =
        new BufferedReader(
            new InputStreamReader(
                SqlCompleter.class.getResourceAsStream(
                    "sql-keywords.properties"))).readLine();

    // now add the keywords from the current connection
    final DatabaseMetaData metaData = sqlLine.getDatabaseConnection().meta;
    try {
      keywords += "," + metaData.getSQLKeywords();
    } catch (Throwable t) {
      // ignore
    }
    try {
      keywords += "," + metaData.getStringFunctions();
    } catch (Throwable t) {
      // ignore
    }
    try {
      keywords += "," + metaData.getNumericFunctions();
    } catch (Throwable t) {
      // ignore
    }
    try {
      keywords += "," + metaData.getSystemFunctions();
    } catch (Throwable t) {
      // ignore
    }
    try {
      keywords += "," + metaData.getTimeDateFunctions();
    } catch (Throwable t) {
      // ignore
    }

    // also allow lower-case versions of all the keywords
    keywords += "," + keywords.toLowerCase();

    for (String keyword : SqlLine.tokenize(keywords, ", ")) {
      completions.add(keyword);
    }

    // now add the tables and columns from the current connection
    if (!skipMeta) {
      String[] columns = sqlLine.getColumnNames(metaData);
      for (String column : columns) {
        completions.add(column);
      }
    }

    // set the Strings that will be completed
    getStrings().addAll(completions);
  }
}
