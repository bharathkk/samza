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
package org.apache.samza.operators.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.AsyncOperator;
import org.apache.samza.operators.functions.AsyncSinkFunction;
import org.apache.samza.operators.spec.AsyncSinkOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * An operator that sends incoming messages to an arbitrary output system using the provided {@link AsyncSinkFunction}.
 */
class AsyncSinkOperatorImpl<M> extends OperatorImpl<M, Void> implements AsyncOperator<M, Void> {

  private final AsyncSinkOperatorSpec<M> sinkOpSpec;
  private final AsyncSinkFunction<M> asyncSinkFn;

  AsyncSinkOperatorImpl(AsyncSinkOperatorSpec<M> sinkOpSpec, Config config, TaskContext context) {
    this.sinkOpSpec = sinkOpSpec;
    this.asyncSinkFn = sinkOpSpec.getSinkFn();
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    this.asyncSinkFn.init(config, context);
  }

  @Override
  public Collection<Void> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    try {
      handle(message, collector, coordinator).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException(e);
    }
    // there should be no further chained operators since this is a terminal operator.
    return Collections.emptyList();
  }

  @Override
  public CompletableFuture<Collection<Void>> handleMessage(M message, MessageCollector collector,
      TaskCoordinator coordinator, TaskCallback callback) {
    CompletableFuture<Collection<Void>> results =
        handle(message, collector, coordinator)
            .handle((val, ex) -> {
                if (ex != null) {
                  callback.failure(ex);
                }

                return Collections.emptyList();
              });

    return results;
  }

  final CompletableFuture<Void> handle(M message, MessageCollector collector, TaskCoordinator coordinator) {
    return asyncSinkFn.apply(message, collector, coordinator);
  }

  @Override
  protected void handleClose() {
    this.asyncSinkFn.close();
  }

  protected OperatorSpec<M, Void> getOperatorSpec() {
    return sinkOpSpec;
  }
}
