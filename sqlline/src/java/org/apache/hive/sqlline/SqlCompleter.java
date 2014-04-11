package org.apache.hive.sqlline;

import jline.console.completer.StringsCompleter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

class SqlCompleter extends StringsCompleter {
  public SqlCompleter(SqlLine sqlLine, boolean skipMeta) throws IOException, SQLException {
    super(new String[0]);

    Set<String> completions = new TreeSet<String>();

    // add the default SQL completions
    String keywords =
        new BufferedReader(
            new InputStreamReader(
                SqlCompleter.class.getResourceAsStream(
                    "sql-keywords.properties"))).readLine();

    // now add the keywords from the current connection

    try {
      keywords += "," + sqlLine.getDatabaseConnection().meta.getSQLKeywords();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + sqlLine.getDatabaseConnection().meta.getStringFunctions();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + sqlLine.getDatabaseConnection().meta.getNumericFunctions();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + sqlLine.getDatabaseConnection().meta.getSystemFunctions();
    } catch (Throwable t) {
    }
    try {
      keywords += "," + sqlLine.getDatabaseConnection().meta.getTimeDateFunctions();
    } catch (Throwable t) {
    }

    // also allow lower-case versions of all the keywords
    keywords += "," + keywords.toLowerCase();

    for (StringTokenizer tok = new StringTokenizer(keywords, ", ");
        tok.hasMoreTokens();
        completions.add(tok.nextToken())) {
      ;
    }

    // now add the tables and columns from the current connection
    if (!(skipMeta)) {
      String[] columns = sqlLine.getColumnNames(sqlLine.getDatabaseConnection().meta);
      for (
          int i = 0;
          (columns != null)
              && (i < columns.length);
          i++) {
        completions.add(columns[i++]);
      }
    }

    // set the Strings that will be completed
    getStrings().addAll(completions);
  }
}
