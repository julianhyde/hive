/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.sqlline;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A buffer that can output segments using ANSI color.
 */
final class ColorBuffer implements Comparable<ColorBuffer> {

  private final List<Object> parts = new LinkedList<Object>();
  private int visibleLength = 0;
  private final boolean useColor;

  public ColorBuffer(boolean useColor) {
    this("", useColor);
  }

  public ColorBuffer(String str, boolean useColor) {
    this.useColor = useColor;
    append(str);
  }

  /**
   * Pad the specified String with spaces to the indicated length
   *
   * @param str the String to pad
   * @param len the length we want the return String to be
   * @return the passed in String with spaces appended until the length
   * matches the specified length.
   */
  ColorBuffer pad(ColorBuffer str, int len) {
    final int v = str.getVisibleLength();
    if (v < len) {
      str.append(SqlLine.spaces(len - v));
    }
    return append(str);
  }

  ColorBuffer center(String str, int len) {
    final int n = len - str.length();
    final int right = n / 2;
    final int left = n - right;
    if (left > 0) {
      append(SqlLine.spaces(left));
    }
    append(str);
    if (right > 0) {
      append(SqlLine.spaces(right));
    }
    return this;
  }

  ColorBuffer pad(String str, int len) {
    return append(SqlLine.pad(str, len));
  }

  public String getColor() {
    return getBuffer(useColor);
  }

  public String getMono() {
    return getBuffer(false);
  }

  String getBuffer(boolean color) {
    StringBuilder buf = new StringBuilder();
    for (Object part : parts) {
      if (!color && part instanceof ColorAttr) {
        continue;
      }
      buf.append(part.toString());
    }
    return buf.toString();
  }

  /**
   * Truncate the ColorBuffer to the specified length and return the new
   * ColorBuffer. Any open color tags will be closed.
   * Do nothing if the specified length &le; 0.
   */
  public ColorBuffer truncate(int len) {
    if (len <= 0) {
      return this;
    }
    ColorBuffer cbuff = new ColorBuffer(useColor);
    ColorAttr lastAttr = null;
    for (Iterator<Object> i = parts.iterator();
        cbuff.getVisibleLength() < len && i.hasNext();) {
      Object next = i.next();
      if (next instanceof ColorAttr) {
        lastAttr = (ColorAttr) next;
        cbuff.append((ColorAttr) next);
        continue;
      }
      String val = next.toString();
      if (cbuff.getVisibleLength() + val.length() > len) {
        int partLen = len - cbuff.getVisibleLength();
        val = val.substring(0, partLen);
      }
      cbuff.append(val);
    }

    // close off the buffer with a normal tag
    if (lastAttr != null && lastAttr != ColorAttr.NORMAL) {
      cbuff.append(ColorAttr.NORMAL);
    }

    return cbuff;
  }


  @Override
  public String toString() {
    return getColor();
  }

  public ColorBuffer appendIf(boolean condition, String str) {
    if (condition) {
      append(str);
    }
    return this;
  }

  public ColorBuffer append(String str) {
    parts.add(str);
    visibleLength += str.length();
    return this;
  }

  public ColorBuffer append(ColorBuffer buf) {
    parts.addAll(buf.parts);
    visibleLength += buf.getVisibleLength();
    return this;
  }

  public ColorBuffer append(ColorAttr attr) {
    parts.add(attr);
    return this;
  }

  public ColorBuffer append(ColorAttr attr, String val) {
    parts.add(attr);
    parts.add(val);
    parts.add(ColorAttr.NORMAL);
    visibleLength += val.length();
    return this;
  }

  public int getVisibleLength() {
    return visibleLength;
  }

  public ColorBuffer bold(String str) {
    return append(ColorAttr.BOLD, str);
  }

  public ColorBuffer lined(String str) {
    return append(ColorAttr.LINED, str);
  }

  public ColorBuffer grey(String str) {
    return append(ColorAttr.GREY, str);
  }

  public ColorBuffer red(String str) {
    return append(ColorAttr.RED, str);
  }

  public ColorBuffer blue(String str) {
    return append(ColorAttr.BLUE, str);
  }

  public ColorBuffer green(String str) {
    return append(ColorAttr.GREEN, str);
  }

  public ColorBuffer cyan(String str) {
    return append(ColorAttr.CYAN, str);
  }

  public ColorBuffer yellow(String str) {
    return append(ColorAttr.YELLOW, str);
  }

  public ColorBuffer magenta(String str) {
    return append(ColorAttr.MAGENTA, str);
  }

  public int compareTo(ColorBuffer other) {
    return getMono().compareTo(other.getMono());
  }

  /** Style attribute. */
  enum ColorAttr {
    BOLD("\033[1m"),
    NORMAL("\033[m"),
    REVERS("\033[7m"),
    LINED("\033[4m"),
    GREY("\033[1;30m"),
    RED("\033[1;31m"),
    GREEN("\033[1;32m"),
    BLUE("\033[1;34m"),
    CYAN("\033[1;36m"),
    YELLOW("\033[1;33m"),
    MAGENTA("\033[1;35m"),
    INVISIBLE("\033[8m");

    private final String style;

    ColorAttr(String style) {
      this.style = style;
    }

    @Override
    public String toString() {
      return style;
    }
  }
}
