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

package org.apache.hadoop.hive.cli;

import jline.console.completer.Completer;

import java.util.List;
import java.util.SortedSet;

/** Similar to jline 1.0's SimpleCompletor. */
class SimpleCompleter implements Completer {
  final SortedSet<String> candidates;
  final String delimiter;

  public SimpleCompleter(SortedSet<String> candidateStrings, String delimiter) {
    this.candidates = candidateStrings;
    this.delimiter = delimiter;
  }

  public int complete(String buffer, final int cursor,
      final List<CharSequence> clist) {
    if (buffer == null) {
      buffer = "";
    }

    for (String can : candidates.tailSet(buffer)) {
      if (!can.startsWith(buffer)) {
        break;
      }

      if (delimiter != null) {
        int index = can.indexOf(delimiter, cursor);

        if (index != -1) {
          can = can.substring(0, index + 1);
        }
      }

      clist.add(can);
    }

    if (clist.size() == 1) {
      clist.set(0, clist.get(0) + " ");
    }

    // the index of the completion is always from the beginning of
    // the buffer.
    return clist.size() == 0 ? -1 : 0;
  }
}
