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
 * OutputFormat for a pretty, table-like format.
 */
class TableOutputFormat implements OutputFormat {
  private final SqlLine sqlLine;
  private final StringBuilder sb = new StringBuilder();

  public TableOutputFormat(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int print(Rows rows) {
    int index = 0;
    ColorBuffer header = null;
    ColorBuffer headerCols = null;
    final int width = sqlLine.getOpts().getMaxWidth() - 4;

    // normalize the columns sizes
    rows.normalizeWidths();

    for (; rows.hasNext();) {
      Rows.Row row = rows.next();
      ColorBuffer cbuf = getOutputString(rows, row);
      if (sqlLine.getOpts().getTruncateTable()) {
        cbuf = cbuf.truncate(width);
      }

      if (index == 0)  {
        sb.setLength(0);
        for (int j = 0; j < row.sizes.length; j++) {
          if (j > 0) {
            sb.append("-+-");
          }
          for (int k = 0; k < row.sizes[j]; k++) {
            sb.append('-');
          }
        }

        headerCols = cbuf;
        header = sqlLine.getColorBuffer()
            .green(sb.toString());
        if (sqlLine.getOpts().getTruncateTable()) {
          header = header.truncate(headerCols.getVisibleLength());
        }
      }

      if (sqlLine.getOpts().getShowHeader()) {
        if (index == 0
            || (index - 1 > 0
            && (index - 1) % sqlLine.getOpts().getHeaderInterval() == 0)) {
          printRow(header, true);
          printRow(headerCols, false);
          printRow(header, true);
        }
      } else if (index == 0) {
        printRow(header, true);
      }

      if (index != 0) {
        // don't output the header twice
        printRow(cbuf, false);
      }
      index++;
    }

    if (header != null) {
      printRow(header, true);
    }

    return index - 1;
  }

  void printRow(ColorBuffer cbuff, boolean header) {
    if (header) {
      sqlLine.output(sqlLine.getColorBuffer()
          .green("+-")
          .append(cbuff)
          .green("-+"));
    } else {
      sqlLine.output(sqlLine.getColorBuffer()
          .green("| ")
          .append(cbuff)
          .green(" |"));
    }
  }

  public ColorBuffer getOutputString(Rows rows, Rows.Row row) {
    return getOutputString(rows, row, " | ");
  }

  ColorBuffer getOutputString(Rows rows, Rows.Row row, String delim) {
    ColorBuffer buf = sqlLine.getColorBuffer();

    for (int i = 0; i < row.values.length; i++) {
      if (buf.getVisibleLength() > 0) {
        buf.green(delim);
      }

      String v;
      if (row.isMeta) {
        v = SqlLine.center(row.values[i], row.sizes[i]);
        if (rows.isPrimaryKey(i)) {
          buf.cyan(v);
        } else {
          buf.bold(v);
        }
      } else {
        v = SqlLine.pad(row.values[i], row.sizes[i]);
        if (rows.isPrimaryKey(i)) {
          buf.cyan(v);
        } else {
          buf.append(v);
        }
      }
    }

    if (row.deleted) {
      // make deleted rows red
      buf = sqlLine.getColorBuffer().red(buf.getMono());
    } else if (row.updated) {
      // make updated rows blue
      buf = sqlLine.getColorBuffer().blue(buf.getMono());
    } else if (row.inserted) {
      // make new rows green
      buf = sqlLine.getColorBuffer().green(buf.getMono());
    }
    return buf;
  }
}
