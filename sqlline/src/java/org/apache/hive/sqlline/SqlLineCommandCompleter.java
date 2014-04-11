package org.apache.hive.sqlline;

import jline.console.completer.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

class SqlLineCommandCompleter extends AggregateCompleter {
  public SqlLineCommandCompleter(SqlLine sqlLine) {
    List<ArgumentCompleter> completers = new LinkedList<ArgumentCompleter>();

    for (int i = 0; i < sqlLine.commandHandlers.length; i++) {
      String[] cmds = sqlLine.commandHandlers[i].getNames();
      for (int j = 0; cmds != null && j < cmds.length; j++) {
        Completer[] comps = sqlLine.commandHandlers[i].getParameterCompleters();
        List<Completer> compl = new LinkedList<Completer>();
        compl.add(new StringsCompleter(SqlLine.COMMAND_PREFIX + cmds[j]));
        compl.addAll(Arrays.asList(comps));
        compl.add(new NullCompleter()); // last param no complete

        completers.add(new ArgumentCompleter(compl));
      }
    }

    getCompleters().addAll(completers);
  }
}
