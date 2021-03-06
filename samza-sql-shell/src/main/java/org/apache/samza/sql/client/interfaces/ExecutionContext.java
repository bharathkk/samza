/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.client.interfaces;

import java.util.Map;

/**
 * Whenever the shell calls the executor to execute a SQL statement, an object of ExecutionContext is passed.
 */
public class ExecutionContext {
  private Map<String, String> m_configs;

  public ExecutionContext(Map<String, String> config) {
    m_configs = config;
  }

  /**
  * @return The Map storing all configuration pairs. Note that the set map is the same as the one used by
  * ExecutionContext, so changes to the map are reflected in ExecutionContext, and vice-versa.
  */
  public Map<String, String> getConfigMap() {
    return m_configs;
  }
}
