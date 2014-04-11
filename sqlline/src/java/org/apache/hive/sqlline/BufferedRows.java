package org.apache.hive.sqlline;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Rows implementation which buffers all rows in a linked list.
 */
class BufferedRows extends Rows {
  private final List<Row> list;

  private final Iterator<Row> iterator;

  BufferedRows(SqlLine sqlLine, ResultSet rs) throws SQLException {
    super(sqlLine, rs);

    list = new LinkedList<Row>();

    int count = rsMeta.getColumnCount();

    list.add(new Row(count));

    while (rs.next()) {
      list.add(new Row(count, rs));
    }

    iterator = list.iterator();
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Object next() {
    return iterator.next();
  }

  void normalizeWidths() {
    int[] max = null;
    for (int i = 0; i < list.size(); i++) {
      Row row = list.get(i);
      if (max == null) {
        max = new int[row.values.length];
      }

      for (int j = 0; j < max.length; j++) {
        max[j] = Math.max(max[j], row.sizes[j] + 1);
      }
    }

    for (int i = 0; i < list.size(); i++) {
      Row row = list.get(i);
      row.sizes = max;
    }
  }
}
