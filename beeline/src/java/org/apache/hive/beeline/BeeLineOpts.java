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
package org.apache.hive.beeline;

import org.apache.hive.sqlline.SqlLineOpts;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Configuration options for BeeLine. */
class BeeLineOpts extends SqlLineOpts {
  private Map<String, String> hiveVariables = new HashMap<String, String>();
  private Map<String, String> hiveConfVariables = new HashMap<String, String>();
  private String authType;

  public BeeLineOpts(BeeLine beeLine) {
    super(beeLine,
        new Properties(),
        "beeline.properties",
        "beeline.rcfile");
  }

  public boolean isCautious() {
    // Bug-compatibility mode.
    return true;
  }

  public Map<String, String> getHiveVariables() {
    return hiveVariables;
  }

  public void setHiveVariables(Map<String, String> hiveVariables) {
    this.hiveVariables = hiveVariables;
  }

  public Map<String, String> getHiveConfVariables() {
    return hiveConfVariables;
  }

  public void setHiveConfVariables(Map<String, String> hiveConfVariables) {
    this.hiveConfVariables = hiveConfVariables;
  }

  public String getAuthType() {
    return authType;
  }

  public void setAuthType(String authType) {
    this.authType = authType;
  }
}
