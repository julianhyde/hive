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

import jline.console.completer.Completer;

/**
 * A {@link CommandHandler} implementation that
 * uses reflection to determine the method to dispatch the command.
 */
public class ReflectiveCommandHandler extends AbstractCommandHandler {
  /**
   * Creates a ReflectiveCommandHandler.
   *
   * @param sqlLine Shell instance
   * @param cmds    Array of alternative names for the same command. The
   *                first one is always chosen for display purposes and to
   *                lookup help documentation from SqlLine.properties file.
   * @param completers Completers
   */
  public ReflectiveCommandHandler(SqlLine sqlLine, String[] cmds,
      Completer[] completers) {
    super(sqlLine, cmds, sqlLine.loc("help-" + cmds[0]), completers);
  }

  public void execute(String line, DispatchCallback callback) {
    try {
      sqlLine.getCommands().getClass()
          .getMethod(getName(), String.class, DispatchCallback.class)
          .invoke(sqlLine.getCommands(), line, callback);
    } catch (Throwable e) {
      callback.setToFailure();
      sqlLine.error(e);
    }
  }
}
