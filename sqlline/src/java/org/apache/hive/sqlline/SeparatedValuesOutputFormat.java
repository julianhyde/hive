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

import java.io.IOException;
import java.io.StringWriter;

import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * OutputFormat for values separated by a delimiter.
 */
class SeparatedValuesOutputFormat implements OutputFormat {
  private final SqlLine sqlLine;
  private final StringWriter strWriter = new StringWriter();
  private final CsvListWriter writer;

  SeparatedValuesOutputFormat(SqlLine sqlLine, char separator) {
    this.sqlLine = sqlLine;
    CsvPreference csvPreference =
        new CsvPreference.Builder('"', separator, "").build();
    this.writer = new CsvListWriter(strWriter, csvPreference);
  }

  public int print(Rows rows) {
    int count = 0;
    while (rows.hasNext()) {
      printRow(rows, rows.next());
      count++;
    }
    return count - 1; // sans header row
  }

  public void printRow(Rows rows, Rows.Row row) {
    String formattedStr = getFormattedStr(row.values);
    sqlLine.output(formattedStr);
  }

  private String getFormattedStr(String[] vals) {
    if (vals.length == 0) {
      return "";
    }
    try {
      writer.write(vals);
      writer.flush();
      final String s = strWriter.toString();
      strWriter.getBuffer().setLength(0);
      return s;
    } catch (IOException e) {
      sqlLine.error(e);
      return "";
    }
  }
}
