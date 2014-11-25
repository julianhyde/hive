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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

/**
 * An abstract implementation of CommandHandler.
 */
public abstract class AbstractCommandHandler implements CommandHandler {
  public static final List<Completer> NULL_COMPLETER_LIST =
      Collections.<Completer>singletonList(new NullCompleter());

  protected final SqlLine sqlLine;
  private final String name;
  private final String[] names;
  private final String helpText;
  private final List<Completer> parameterCompleters;

  public AbstractCommandHandler(SqlLine sqlLine, String[] names,
      String helpText, Completer[] completers) {
    assert names != null;
    this.sqlLine = sqlLine;
    this.name = names[0];
    this.names = names;
    this.helpText = helpText;
    if (completers == null || completers.length == 0) {
      this.parameterCompleters = NULL_COMPLETER_LIST;
    } else {
      List<Completer> c = new LinkedList<Completer>();
      Collections.addAll(c, completers);
      c.add(new NullCompleter());
      this.parameterCompleters = c;
    }
  }

  @Override
  public String getHelpText() {
    return helpText;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String[] getNames() {
    return this.names;
  }

  @Override
  public String matches(String line) {
    if (line == null || line.length() == 0) {
      return null;
    }

    List<String> parts = sqlLine.split(line);
    if (parts.isEmpty()) {
      return null;
    }

    final String part0 = parts.get(0);
    for (String name2 : names) {
      if (name2.startsWith(part0)) {
        return name2;
      }
    }
    return null;
  }

  @Override
  public List<Completer> getParameterCompleters() {
    return this.parameterCompleters;
  }
}
