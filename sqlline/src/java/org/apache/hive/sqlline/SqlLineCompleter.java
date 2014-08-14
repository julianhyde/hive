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

import java.util.List;

import jline.console.completer.Completer;

/**
 * Completer for SqlLine. It dispatches to sub-completers based on the
 * current arguments.
 */
class SqlLineCompleter implements Completer {
  private final SqlLine sqlLine;

  public SqlLineCompleter(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int complete(String buf, int pos, List<CharSequence> candidates) {
    if (buf != null
        && buf.startsWith(SqlLine.COMMAND_PREFIX)
        && !buf.startsWith(SqlLine.COMMAND_PREFIX + "all")
        && !buf.startsWith(SqlLine.COMMAND_PREFIX + "sql")) {
      return sqlLine.getCommandCompleter().complete(buf, pos, candidates);
    } else {
      final DatabaseConnection databaseConnection =
          sqlLine.getDatabaseConnection();
      if (databaseConnection == null) {
        return -1;
      }
      final Completer sqlCompleter = databaseConnection.getSqlCompleter();
      if (sqlCompleter == null) {
        return -1;
      }
      return sqlCompleter.complete(buf, pos, candidates);
    }
  }
}

// End SqlLineCompleter.java
