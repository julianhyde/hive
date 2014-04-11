package org.apache.hive.sqlline;

class XmlElementOutputFormat
    extends AbstractOutputFormat {
  public XmlElementOutputFormat(SqlLine sqlLine) {
    super(sqlLine);
  }

  public void printHeader(Rows.Row header) {
    sqlLine.output("<resultset>");
  }

  public void printFooter(Rows.Row header) {
    sqlLine.output("</resultset>");
  }

  public void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;

    sqlLine.output("  <result>");
    for (int i = 0; (i < head.length) && (i < vals.length); i++) {
      sqlLine.output(
          "    <" + head[i] + ">"
              + (SqlLine.xmlattrencode(vals[i]))
              + "</" + head[i] + ">");
    }

    sqlLine.output("  </result>");
  }
}
