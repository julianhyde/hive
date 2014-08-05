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

import java.util.LinkedList;
import java.util.List;

import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;

/** Suggests completions for commands. */
class SqlLineCommandCompleter extends AggregateCompleter {
  public SqlLineCommandCompleter(SqlLine sqlLine) {
    List<ArgumentCompleter> completers = new LinkedList<ArgumentCompleter>();

    for (CommandHandler commandHandler : sqlLine.commandHandlers) {
      for (String cmd : commandHandler.getNames()) {
        final List<Completer> completers2 = new LinkedList<Completer>();
        completers2.add(new StringsCompleter(SqlLine.COMMAND_PREFIX + cmd));
        completers2.addAll(commandHandler.getParameterCompleters());
        completers2.add(new NullCompleter()); // last param no complete

        completers.add(new ArgumentCompleter(completers2));
      }
    }

    getCompleters().addAll(completers);
  }
}
