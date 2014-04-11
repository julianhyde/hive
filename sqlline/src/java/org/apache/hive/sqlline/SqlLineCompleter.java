package org.apache.hive.sqlline;

import jline.console.completer.Completer;

import java.util.List;

/**
 * Completer for SqlLine. It dispatches to sub-completers based on the
 * current arguments.
 */
class SqlLineCompleter
    implements Completer {
  private SqlLine sqlLine;

  public SqlLineCompleter(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public int complete(String buf, int pos, List<CharSequence> candidates) {
    if (buf != null
        && buf.startsWith(SqlLine.COMMAND_PREFIX)
        && !buf.startsWith(SqlLine.COMMAND_PREFIX + "all")
        && !buf.startsWith(SqlLine.COMMAND_PREFIX + "sql")) {
      return sqlLine.getCommandCompleter().complete(buf, pos, candidates);
    } else {
      if (sqlLine.getDatabaseConnection() != null && (sqlLine.getDatabaseConnection().getSQLCompletor() != null)) {
        return sqlLine.getDatabaseConnection().getSQLCompletor().complete(buf, pos, candidates);
      } else {
        return -1;
      }
    }
  }
}
