package org.apache.hive.sqlline;

/**
 * Abstract OutputFormat.
 */
abstract class AbstractOutputFormat implements OutputFormat {
  protected final SqlLine sqlLine;

  public AbstractOutputFormat(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int print(Rows rows) {
    int count = 0;
    Rows.Row header = (Rows.Row) rows.next();

    printHeader(header);

    while (rows.hasNext()) {
      printRow(rows, header, (Rows.Row) rows.next());
      count++;
    }

    printFooter(header);

    return count;
  }

  abstract void printHeader(Rows.Row header);

  abstract void printFooter(Rows.Row header);

  abstract void printRow(Rows rows, Rows.Row header, Rows.Row row);
}
