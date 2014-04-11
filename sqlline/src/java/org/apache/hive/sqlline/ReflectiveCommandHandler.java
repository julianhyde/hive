package org.apache.hive.sqlline;

import jline.console.completer.Completer;

/**
 * A {@link CommandHandler} implementation that
 * uses reflection to determine the method to dispatch the command.
 */
public class ReflectiveCommandHandler extends AbstractCommandHandler {
  public ReflectiveCommandHandler(SqlLine sqlLine, String[] cmds,
      Completer[] completer) {
    super(sqlLine, cmds, sqlLine.loc("help-" + cmds[0]), completer);
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
