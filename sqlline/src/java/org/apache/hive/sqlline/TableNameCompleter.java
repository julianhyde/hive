package org.apache.hive.sqlline;

import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import java.util.List;

class TableNameCompleter implements Completer {
  private SqlLine sqlLine;

  public TableNameCompleter(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int complete(String buf, int pos, List<CharSequence> candidates) {
    if (sqlLine.getDatabaseConnection() == null) {
      return -1;
    }

    return new StringsCompleter(sqlLine.getDatabaseConnection().getTableNames(true))
        .complete(buf, pos, candidates);
  }
}
