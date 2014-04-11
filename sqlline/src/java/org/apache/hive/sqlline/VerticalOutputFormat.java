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
    Rows.Row header = (Rows.Row) rows.next();

    while (rows.hasNext()) {
      printRow(rows, header, (Rows.Row) rows.next());
      count++;
    }

    return count;
  }

  public void printRow(Rows rows, Rows.Row header, Rows.Row row) {
    String[] head = header.values;
    String[] vals = row.values;
    int headWidth = 0;
    for (int i = 0; (i < head.length) && (i < vals.length); i++) {
      headWidth = Math.max(headWidth, head[i].length());
    }

    headWidth += 2;

    for (int i = 0; (i < head.length) && (i < vals.length); i++) {
      sqlLine.output(
          sqlLine.getColorBuffer()
              .bold(sqlLine.getColorBuffer().pad(head[i], headWidth).getMono())
              .append((vals[i] == null) ? "" : vals[i]));
    }

    sqlLine.output(""); // spacing
  }
}
