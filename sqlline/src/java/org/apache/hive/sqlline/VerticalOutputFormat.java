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

/**
 * OutputFormat for vertical column name: value format.
 */
class VerticalOutputFormat implements OutputFormat {
  private final SqlLine sqlLine;

  public VerticalOutputFormat(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int print(Rows rows) {
    int count = 0;
    Rows.Row header = rows.next();

    while (rows.hasNext()) {
      printRow(rows, header, rows.next());
      count++;
    }
    return count;
  }

  public void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;
    int headWidth = 0;
    final int length = Math.min(head.length, vals.length);
    for (int i = 0; i < length; i++) {
      headWidth = Math.max(headWidth, head[i].length());
    }

    headWidth += 2;

    for (int i = 0; i < length; i++) {
      sqlLine.output(
          sqlLine.getColorBuffer()
              .bold(SqlLine.pad(head[i], headWidth))
              .appendIf(vals[i] != null, vals[i]));
    }

    sqlLine.output(""); // spacing
  }
}
