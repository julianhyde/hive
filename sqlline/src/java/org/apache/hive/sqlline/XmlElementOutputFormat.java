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

/** Output format that formats records in element-oriented XML. */
class XmlElementOutputFormat extends AbstractOutputFormat {
  public XmlElementOutputFormat(SqlLine sqlLine) {
    super(sqlLine);
  }

  @Override
  public void printHeader(Rows.Row header) {
    sqlLine.output("<resultset>");
  }

  @Override
  public void printFooter(Rows.Row header) {
    sqlLine.output("</resultset>");
  }

  @Override
  public void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    final String[] head = header.values;
    final String[] values = row.values;
    final int n = Math.min(head.length, values.length);
    final StringBuilder buf = new StringBuilder("  <result>");
    for (int i = 0; i < head.length && i < values.length; i++) {
      buf.append("    <")
          .append(head[i])
          .append(">")
          .append(SqlLine.xmlattrencode(values[i]))
          .append("</")
          .append(head[i])
          .append(">");
    }
    buf.append("  </result>");
    sqlLine.output(buf.toString());
  }
}
