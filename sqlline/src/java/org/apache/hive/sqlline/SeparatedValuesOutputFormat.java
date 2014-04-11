package org.apache.hive.sqlline;

/**
 * OutputFormat for values separated by a delimiter.
 *
 * <p><strong>TODO</strong>:
 * Handle character escaping
 */
class SeparatedValuesOutputFormat implements OutputFormat {
  private final SqlLine sqlLine;
  private char separator;

  public SeparatedValuesOutputFormat(SqlLine sqlLine, char separator) {
    this.sqlLine = sqlLine;
    setSeparator(separator);
  }

  public int print(Rows rows) {
    int count = 0;
    while (rows.hasNext()) {
      printRow(rows, (Rows.Row) rows.next());
      count++;
    }

    return count - 1; // sans header row
  }

  public void printRow(Rows rows, Rows.Row row) {
    String[] vals = row.values;
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < vals.length; i++) {
      buf.append((buf.length() == 0) ? "" : ("" + getSeparator()))
          .append('\'').append((vals[i] == null) ? "" : vals[i]).append(
          '\'');
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
