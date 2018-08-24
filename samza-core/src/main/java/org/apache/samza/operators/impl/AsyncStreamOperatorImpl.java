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
import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.operators.spec.AsyncStreamOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


public class AsyncStreamOperatorImpl<M, RM> extends OperatorImpl<M, RM> {
  private final AsyncStreamOperatorSpec<M, RM> streamOpSpec;
  private final AsyncFlatMapFunction<M, RM> transformFn;

  public AsyncStreamOperatorImpl(AsyncStreamOperatorSpec<M, RM> streamOpSpec) {
    this.streamOpSpec = streamOpSpec;
    transformFn = streamOpSpec.getTransformFn();
  }
  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  protected Collection<RM> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    try {
      handle(message, collector, coordinator).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SamzaException(e);
    }

    return Collections.emptyList();
  }

  final CompletableFuture<Collection<RM>> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator, TaskCallback callback) {
    CompletableFuture<Collection<RM>> results = handle(message, collector, coordinator);

    results.exceptionally(ex -> {
        callback.failure(ex);
        return Collections.emptyList();
      });

    return results;
  }

  final CompletableFuture<Collection<RM>> handle(M message, MessageCollector collector, TaskCoordinator coordinator) {
    return transformFn.apply(message);
  }

  @Override
  protected void handleClose() {

  }

  @Override
  protected OperatorSpec<M, RM> getOperatorSpec() {
    return streamOpSpec;
  }
}
