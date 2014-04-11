package org.apache.hive.sqlline;

import jline.console.completer.Completer;

/**
 * A generic command to be executed. Execution of the command should be
 * dispatched to the
 * {@link #execute(String, org.apache.hive.sqlline.DispatchCallback)} method after
 * determining that the command is appropriate with the
 * {@link #matches(String)} method.
 */
interface CommandHandler {
  /**
   * @return the name of the command
   */
  public String getName();

  /**
   * @return all the possible names of this command.
   */
  public String[] getNames();

  /**
   * @return the short help description for this command.
   */
  public String getHelpText();

  /**
   * Check to see if the specified string can be dispatched to this
   * command.
   *
   * @param line the command line to check.
   * @return the command string that matches, or null if it no match
   */
  public String matches(String line);

  /**
   * Execute the specified command.
   *
   * @param line             the full command line to execute.
   * @param dispatchCallback the callback to check or interrupt the action
   */
  public void execute(String line, DispatchCallback dispatchCallback);

  /**
   * Returns the completers that can handle parameters.
   */
  public Completer[] getParameterCompleters();
}
