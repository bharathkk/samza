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
package org.apache.samza.operators.spec;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.config.Config;
import org.apache.samza.operators.functions.AsyncFlatMapFunction;
import org.apache.samza.operators.functions.AsyncMapFunction;
import org.apache.samza.operators.functions.TimerFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.task.TaskContext;


/**
 * The spec for an operator that transforms each input message to a single output message.
 *
 * @param <M> type of input message
 * @param <OM> type of output messages
 */
class AsyncMapOperatorSpec<M, OM> extends AsyncStreamOperatorSpec<M, OM> {

  private final AsyncMapFunction<M, OM> mapFn;

  AsyncMapOperatorSpec(AsyncMapFunction<M, OM> mapFn, String opId) {
    super(new AsyncFlatMapFunction<M, OM>() {
      @Override
      public CompletableFuture<Collection<OM>> apply(M message) {
        return mapFn.apply(message)
            .thenApply(m -> {
                if (m != null) {
                  return Collections.singleton(m);
                }

                return Collections.emptyList();
              });
      }

      @Override
      public void init(Config config, TaskContext context) {
        mapFn.init(config, context);
      }

      @Override
      public void close() {
        mapFn.close();
      }
    }, OpCode.MAP, opId);
    this.mapFn = mapFn;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return this.mapFn instanceof WatermarkFunction ? (WatermarkFunction) this.mapFn : null;
  }

  @Override
  public TimerFunction getTimerFn() {
    return this.mapFn instanceof TimerFunction ? (TimerFunction) this.mapFn : null;
  }
}
