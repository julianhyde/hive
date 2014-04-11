package org.apache.hive.sqlline;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;

/**
 * Abstract base class representing a set of rows to be displayed.
 */
abstract class Rows implements Iterator {
  protected final SqlLine sqlLine;
  final ResultSetMetaData rsMeta;
  final Boolean[] primaryKeys;
  final NumberFormat numberFormat;

  Rows(SqlLine sqlLine, ResultSet rs) throws SQLException {
    this.sqlLine = sqlLine;
    rsMeta = rs.getMetaData();
    int count = rsMeta.getColumnCount();
    primaryKeys = new Boolean[count];
    if (sqlLine.getOpts().getNumberFormat().equals("default")) {
      numberFormat = null;
    } else {
      numberFormat = new DecimalFormat(sqlLine.getOpts().getNumberFormat());
    }
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Update all of the rows to have the same size, set to the maximum
   * length of each column in the Rows.
   */
  abstract void normalizeWidths();

  /**
   * Return whether the specified column (0-based index) is a primary key.
   * Since this method depends on whether the JDBC driver property
   * implements {@link java.sql.ResultSetMetaData#getTableName} (many do not), it
   * is not reliable for all databases.
   */
  boolean isPrimaryKey(int col) {
    if (primaryKeys[col] != null) {
      return primaryKeys[col].booleanValue();
    }

    try {
      // this doesn't always work, since some JDBC drivers (e.g.,
      // Oracle's) return a blank string from getTableName.
      String table = rsMeta.getTableName(col + 1);
      String column = rsMeta.getColumnName(col + 1);

      if ((table == null)
          || (table.length() == 0)
          || (column == null)
          || (column.length() == 0)) {
        return (primaryKeys[col] = new Boolean(false))
            .booleanValue();
      }

      ResultSet pks =
          sqlLine.getDatabaseConnection().meta.getPrimaryKeys(
              sqlLine.getDatabaseConnection().meta.getConnection().getCatalog(),
              null,
              table);

      try {
        while (pks.next()) {
          if (column.equalsIgnoreCase(
              pks.getString("COLUMN_NAME"))) {
            return (primaryKeys[col] = new Boolean(true))
                .booleanValue();
          }
        }
      } finally {
        pks.close();
      }

      return (primaryKeys[col] = new Boolean(false)).booleanValue();
    } catch (SQLException sqle) {
      return (primaryKeys[col] = new Boolean(false)).booleanValue();
    }
  }

  class Row {
    final String[] values;
    final boolean isMeta;
    protected boolean deleted;
    protected boolean inserted;
    protected boolean updated;
    protected int[] sizes;

    Row(int size)
        throws SQLException {
      isMeta = true;
      values = new String[size];
      sizes = new int[size];
      for (int i = 0; i < size; i++) {
        values[i] = rsMeta.getColumnLabel(i + 1);
        sizes[i] = (values[i] == null) ? 1 : values[i].length();
      }

      deleted = false;
      updated = false;
      inserted = false;
    }

    Row(int size, ResultSet rs)
        throws SQLException {
      isMeta = false;
      values = new String[size];
      sizes = new int[size];

      try {
        deleted = rs.rowDeleted();
      } catch (Throwable t) {
      }
      try {
        updated = rs.rowUpdated();
      } catch (Throwable t) {
      }
      try {
        inserted = rs.rowInserted();
      } catch (Throwable t) {
      }

      for (int i = 0; i < size; i++) {
        if (numberFormat != null) {
          Object o = rs.getObject(i + 1);
          if (o == null) {
            values[i] = null;
          } else if (o instanceof Number) {
            values[i] = numberFormat.format(o);
          } else {
            values[i] = o.toString();
          }
        } else {
          values[i] = String.valueOf(rs.getObject(i + 1));
        }
        sizes[i] = (values[i] == null) ? 1 : values[i].length();
      }
    }
  }
}
