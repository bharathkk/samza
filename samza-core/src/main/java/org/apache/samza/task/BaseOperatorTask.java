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
package org.apache.samza.task;

import org.apache.samza.config.Config;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.impl.OperatorImplGraph;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;


public class BaseOperatorTask implements InitableTask, WindowableTask, ClosableTask {
  private final OperatorSpecGraph specGraph;
  // TODO: to be replaced by proper scope of shared context factory in SAMZA-1714
  private final ContextManager contextManager;
  private final Clock clock;

  protected OperatorImplGraph operatorImplGraph;

  public BaseOperatorTask(OperatorSpecGraph specGraph, ContextManager contextManager, Clock clock) {
    this.specGraph = specGraph;
    this.contextManager = contextManager;
    this.clock = clock;
  }

  public BaseOperatorTask(OperatorSpecGraph specGraph, ContextManager contextManager) {
    this(specGraph, contextManager, SystemClock.instance());
  }

  /**
   * Initializes this task during startup.
   * <p>
   * Implementation: Initializes the runtime {@link OperatorImplGraph} according to user-defined {@link OperatorSpecGraph}.
   * The {@link org.apache.samza.operators.StreamGraphSpec} sets the input and output streams and the task-wide
   * context manager using the {@link org.apache.samza.operators.StreamGraph} APIs,
   * and the logical transforms using the {@link org.apache.samza.operators.MessageStream} APIs. After the
   * {@link org.apache.samza.operators.StreamGraphSpec} is initialized once by the application, it then creates
   * an immutable {@link OperatorSpecGraph} accordingly, which is passed in to this class to create the {@link OperatorImplGraph}
   * corresponding to the logical DAG.
   *
   * @param config allows accessing of fields in the configuration files that this StreamTask is specified in
   * @param context allows initializing and accessing contextual data of this StreamTask
   * @throws Exception in case of initialization errors
   */
  @Override
  public final void init(Config config, TaskContext context) throws Exception {

    // get the user-implemented per task context manager and initialize it
    if (this.contextManager != null) {
      this.contextManager.init(config, context);
    }

    // create the operator impl DAG corresponding to the logical operator spec DAG
    this.operatorImplGraph = new OperatorImplGraph(specGraph, config, context, clock);
  }

  @Override
  public final void window(MessageCollector collector, TaskCoordinator coordinator)  {
    operatorImplGraph.getAllInputOperators()
        .forEach(inputOperator -> inputOperator.onTimer(collector, coordinator));
  }

  @Override
  public void close() throws Exception {
    if (this.contextManager != null) {
      this.contextManager.close();
    }
    operatorImplGraph.close();
  }

  /* package private for testing */
  OperatorImplGraph getOperatorImplGraph() {
    return this.operatorImplGraph;
  }
}
