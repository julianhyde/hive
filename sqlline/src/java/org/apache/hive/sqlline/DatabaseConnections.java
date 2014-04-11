package org.apache.hive.sqlline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class DatabaseConnections {
  private final List<DatabaseConnection> connections = new ArrayList<DatabaseConnection>();
  private int index = -1;

  public DatabaseConnection current() {
    if (index != -1) {
      return connections.get(index);
    }

    return null;
  }

  public int size() {
    return connections.size();
  }

  public Iterator<DatabaseConnection> iterator() {
    return connections.iterator();
  }

  public void remove() {
    if (index != -1) {
      connections.remove(index);
    }

    while (index >= connections.size()) {
      index--;
    }
  }

  public void setConnection(DatabaseConnection connection) {
    if (connections.indexOf(connection) == -1) {
      connections.add(connection);
    }

    index = connections.indexOf(connection);
  }

  public int getIndex() {
    return index;
  }

  public boolean setIndex(int index) {
    if (index < 0 || index >= connections.size()) {
      return false;
    }

    this.index = index;
    return true;
  }
}
