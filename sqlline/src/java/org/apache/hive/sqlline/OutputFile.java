package org.apache.hive.sqlline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class OutputFile {
  final File file;
  final PrintWriter out;

  public OutputFile(String filename) throws IOException {
    file = new File(filename);
    out = new PrintWriter(new FileWriter(file));
  }

  @Override
  public String toString() {
    return file.getAbsolutePath();
  }

  public void addLine(String command) {
    out.println(command);
  }

  public void print(String command) {
    out.print(command);
  }

  public void close() throws IOException {
    out.close();
  }
}
