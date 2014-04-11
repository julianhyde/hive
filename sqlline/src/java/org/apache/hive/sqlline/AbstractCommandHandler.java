package org.apache.hive.sqlline;

import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * An abstract implementation of CommandHandler.
 */
public abstract class AbstractCommandHandler implements CommandHandler {
  protected final SqlLine sqlLine;
  private final String name;
  private final String[] names;
  private final String helpText;
  private Completer[] parameterCompleters = new Completer[0];

  public AbstractCommandHandler(SqlLine sqlLine, String[] names,
      String helpText, Completer[] completers) {
    this.sqlLine = sqlLine;
    this.name = names[0];
    this.names = names;
    this.helpText = helpText;
    if ((completers == null) || (completers.length == 0)) {
      this.parameterCompleters =
          new Completer[]{
              new NullCompleter()
          };
    } else {
      List<Completer> c = new LinkedList<Completer>(Arrays.asList(completers));
      c.add(new NullCompleter());
      this.parameterCompleters = c.toArray(new Completer[0]);
    }
  }

  public String getHelpText() {
    return helpText;
  }

  public String getName() {
    return this.name;
  }

  public String[] getNames() {
    return this.names;
  }

  public String matches(String line) {
    if ((line == null) || (line.length() == 0)) {
      return null;
    }

    String[] parts = sqlLine.split(line);
    if ((parts == null) || (parts.length == 0)) {
      return null;
    }

    for (int i = 0; i < names.length; i++) {
      if (names[i].startsWith(parts[0])) {
        return names[i];
      }
    }

    return null;
  }

  public void setParameterCompleters(Completer[] parameterCompleters) {
    this.parameterCompleters = parameterCompleters;
  }

  public Completer[] getParameterCompleters() {
    return this.parameterCompleters;
  }
}
