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
import jline.console.completer.StringsCompleter;

/** Suggests table names. */
class TableNameCompleter implements Completer {
  private final SqlLine sqlLine;

  public TableNameCompleter(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int complete(String buf, int pos, List<CharSequence> candidates) {
    final DatabaseConnection databaseConnection =
        sqlLine.getDatabaseConnection();
    if (databaseConnection == null) {
      return -1;
    }
    return new StringsCompleter(databaseConnection.getTableNames(true))
        .complete(buf, pos, candidates);
  }
}
