package org.apache.hive.sqlline;

/**
 * Quoting strategy.
 */
class Quoting {
  final char start;
  final char end;
  final boolean upper;

  Quoting(char start, char end, boolean upper) {
    this.start = start;
    this.end = end;
    this.upper = upper;
  }

  public static final Quoting DEFAULT = new Quoting('"', '"', true);
}
