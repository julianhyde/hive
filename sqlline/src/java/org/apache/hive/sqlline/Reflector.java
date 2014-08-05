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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/** Reflector. */
class Reflector {
  private final SqlLine sqlLine;

  public Reflector(SqlLine sqlLine) {
    this.sqlLine = sqlLine;
  }

  public Object invoke(Object on, String method, Object[] args)
      throws InvocationTargetException, IllegalAccessException,
      ClassNotFoundException {
    return invoke(on, method, Arrays.asList(args));
  }

  public Object invoke(Object on, String method, List args)
      throws InvocationTargetException, IllegalAccessException,
      ClassNotFoundException {
    return invoke(on, on == null ? null : on.getClass(), method, args);
  }

  public Object invoke(Object on, Class defClass,
      String method, List args)
      throws InvocationTargetException, IllegalAccessException,
      ClassNotFoundException {
    Class c = defClass != null ? defClass : on.getClass();
    List<Method> candidateMethods = new LinkedList<Method>();

    for (Method candidateMethod : c.getMethods()) {
      if (candidateMethod.getName().equalsIgnoreCase(method)) {
        candidateMethods.add(candidateMethod);
      }
    }

    if (candidateMethods.size() == 0) {
      throw new IllegalArgumentException(sqlLine.loc("no-method",
          new Object[] {method, c.getName()}));
    }

    for (Method candidateMethod : candidateMethods) {
      Class[] ptypes = candidateMethod.getParameterTypes();
      if (!(ptypes.length == args.size())) {
        continue;
      }

      Object[] converted = convert(args, ptypes);
      if (converted == null) {
        continue;
      }

      if (!Modifier.isPublic(candidateMethod.getModifiers())) {
        continue;
      }
      return candidateMethod.invoke(on, converted);
    }

    return null;
  }

  public static Object[] convert(List objects, Class[] toTypes)
      throws ClassNotFoundException {
    Object[] converted = new Object[objects.size()];
    for (int i = 0; i < converted.length; i++) {
      converted[i] = convert(objects.get(i), toTypes[i]);
    }
    return converted;
  }

  public static Object convert(Object ob, Class toType)
      throws ClassNotFoundException {
    if (ob == null) {
      return null;
    }
    final String s = ob.toString();
    if (s.equals("null")) {
      return null;
    }
    if (toType == String.class) {
      return s;
    } else if (toType == Byte.class || toType == byte.class) {
      return new Byte(s);
    } else if (toType == Character.class || toType == char.class) {
      return s.charAt(0);
    } else if (toType == Short.class || toType == short.class) {
      return new Short(s);
    } else if (toType == Integer.class || toType == int.class) {
      return new Integer(s);
    } else if (toType == Long.class || toType == long.class) {
      return new Long(s);
    } else if (toType == Double.class || toType == double.class) {
      return new Double(s);
    } else if (toType == Float.class || toType == float.class) {
      return new Float(s);
    } else if (toType == Boolean.class || toType == boolean.class) {
      return s.equals("true")
          || s.equals(true + "")
          || s.equals("1")
          || s.equals("on")
          || s.equals("yes");
    } else if (toType == Class.class) {
      return Class.forName(s);
    }
    return null;
  }
}
