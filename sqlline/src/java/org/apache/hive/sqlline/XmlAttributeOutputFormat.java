package org.apache.hive.sqlline;

class XmlAttributeOutputFormat extends AbstractOutputFormat {
  public XmlAttributeOutputFormat(SqlLine sqlLine) {
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

    StringBuffer result = new StringBuffer("  <result");

    for (int i = 0; (i < head.length) && (i < vals.length); i++) {
      result.append(' ').append(head[i]).append("=\"").append(
          SqlLine.xmlattrencode(vals[i])).append('"');
    }

    result.append("/>");

    sqlLine.output(result.toString());
  }
}
