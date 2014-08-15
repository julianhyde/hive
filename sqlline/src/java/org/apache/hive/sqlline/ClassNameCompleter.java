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

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import jline.console.completer.StringsCompleter;

/**
 * An implementation of {@link jline.console.completer.Completer} that completes
 * java class names. By default, it scans the java class path to locate all the
 * classes.
 */
public class ClassNameCompleter extends StringsCompleter {
  /**
   * Complete candidates using all the classes available in the
   * java <em>CLASSPATH</em>.
   */
  public ClassNameCompleter() throws IOException {
    super(getClassNames());
    getStrings().add(".");
  }

  public static String[] getClassNames() throws IOException {
    Set<URL> urls = new HashSet<URL>();

    for (ClassLoader loader = ClassNameCompleter.class.getClassLoader();
         loader != null;
         loader = loader.getParent()) {
      if (!(loader instanceof URLClassLoader)) {
        continue;
      }

      Collections.addAll(urls, ((URLClassLoader) loader).getURLs());
    }

    // Now add the URL that holds java.lang.String. This is because
    // some JVMs do not report the core classes jar in the list of
    // class loaders.
    Class[] systemClasses = {
      String.class, javax.swing.JFrame.class
    };

    for (Class systemClass : systemClasses) {
      URL classURL = systemClass.getResource("/"
          + systemClass.getName().replace('.', '/') + ".class");

      if (classURL != null) {
        URLConnection uc = classURL.openConnection();
        if (uc instanceof JarURLConnection) {
          urls.add(((JarURLConnection) uc).getJarFileURL());
        }
      }
    }

    Set<String> classes = new HashSet<String>();
    for (URL url : urls) {
      File file = new File(url.getFile());

      if (file.isDirectory()) {
        Set<String> files = getClassFiles(
            file.getAbsolutePath(),
            new HashSet<String>(), file, new int[]{200});
        classes.addAll(files);

        continue;
      }

      if (!file.isFile()) {
        // TODO: handle directories
        continue;
      }
      if (!file.toString().endsWith(".jar")) {
        continue;
      }

      JarFile jf = new JarFile(file);

      for (JarEntry entry : SqlLine.iterable(jf.entries())) {
        if (entry == null) {
          continue;
        }

        String name = entry.getName();

        if (!name.endsWith(".class")) {
          // only use class files
          continue;
        }

        classes.add(name);
      }
    }

    // now filter classes by changing "/" to "." and trimming the
    // trailing ".class"
    Set classNames = new TreeSet();

    for (String name : classes) {
      classNames.add(name.replace('/', '.').substring(0, name.length() - 6));
    }

    return (String[]) classNames.toArray(new String[classNames.size()]);
  }

  private static Set<String> getClassFiles(
      String root,
      Set<String> holder,
      File directory,
      int[] maxDirectories) {
    // we have passed the maximum number of directories to scan
    if (maxDirectories[0]-- < 0) {
      return holder;
    }

    File[] files = directory.listFiles();
    if (files == null) {
      files = new File[0];
    }

    for (File file : files) {
      String name = file.getAbsolutePath();

      if (!name.startsWith(root)) {
        continue;
      } else if (file.isDirectory()) {
        getClassFiles(root, holder, file, maxDirectories);
      } else if (file.getName().endsWith(".class")) {
        holder.add(file.getAbsolutePath().substring(root.length() + 1));
      }
    }

    return holder;
  }
}

// End ClassNameCompleter.java
