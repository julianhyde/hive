package org.apache.hive.sqlline;

interface OutputFormat {
  int print(Rows rows);
}
