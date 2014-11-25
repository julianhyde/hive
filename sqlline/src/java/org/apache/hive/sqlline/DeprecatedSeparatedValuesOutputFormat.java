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
 * OutputFormat for values separated by a delimiter.
 *
 * <p>Note that this does not handle escaping of the quote char.  The new
 * {@link SeparatedValuesOutputFormat} supports that. The formats supported by
 * this class are deprecated.
 */
class DeprecatedSeparatedValuesOutputFormat implements OutputFormat {

  private final SqlLine sqlLine;
  private char separator;

  public DeprecatedSeparatedValuesOutputFormat(SqlLine sqlLine, char separator) {
    this.sqlLine = sqlLine;
    setSeparator(separator);
  }

  @Override
  public int print(Rows rows) {
    int count = 0;
    while (rows.hasNext()) {
      printRow(rows, rows.next());
      count++;
    }
    return count - 1; // sans header row
  }

  public void printRow(Rows rows, Rows.Row row) {
    String[] vals = row.values;
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < vals.length; i++) {
      buf.append(buf.length() == 0 ? "" : "" + getSeparator())
          .append('\'')
          .append(vals[i] == null ? "" : vals[i])
          .append('\'');
    }
    sqlLine.output(buf.toString());
  }

  public void setSeparator(char separator) {
    this.separator = separator;
  }

  public char getSeparator() {
    return this.separator;
  }
}
