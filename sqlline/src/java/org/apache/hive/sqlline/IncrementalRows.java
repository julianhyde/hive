package org.apache.hive.sqlline;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * Rows implementation which returns rows incrementally from result set
 * without any buffering.
 */
class IncrementalRows extends Rows {
  private final ResultSet rs;
  private Row labelRow;
  private Row maxRow;
  private Row nextRow;
  private boolean endOfResult;
  private boolean normalizingWidths;
  private DispatchCallback dispatchCallback;

  IncrementalRows(SqlLine sqlLine, ResultSet rs, DispatchCallback dispatchCallback)
      throws SQLException {
    super(sqlLine, rs);
    this.rs = rs;
    this.dispatchCallback = dispatchCallback;

    labelRow = new Row(rsMeta.getColumnCount());
    maxRow = new Row(rsMeta.getColumnCount());

    // pre-compute normalization so we don't have to deal
    // with SQLExceptions later
    for (int i = 0; i < maxRow.sizes.length; ++i) {
      // normalized display width is based on maximum of display size
      // and label size
      maxRow.sizes[i] =
          Math.max(
              maxRow.sizes[i],
              rsMeta.getColumnDisplaySize(i + 1));
    }

    nextRow = labelRow;
    endOfResult = false;
  }

  public boolean hasNext() {
    if (endOfResult || dispatchCallback.isCanceled()) {
      return false;
    }

    if (nextRow == null) {
      try {
        if (rs.next()) {
          nextRow = new Row(labelRow.sizes.length, rs);

          if (normalizingWidths) {
            // perform incremental normalization
            nextRow.sizes = labelRow.sizes;
          }
        } else {
          endOfResult = true;
        }
      } catch (SQLException ex) {
        throw new RuntimeException(ex.toString());
      }
    }

    return (nextRow != null);
  }

  public Object next() {
    if (!hasNext() && !dispatchCallback.isCanceled()) {
      throw new NoSuchElementException();
    }

    Object ret = nextRow;
    nextRow = null;
    return ret;
  }

  void normalizeWidths() {
    // normalize label row
    labelRow.sizes = maxRow.sizes;

    // and remind ourselves to perform incremental normalization
    // for each row as it is produced
    normalizingWidths = true;
  }
}
