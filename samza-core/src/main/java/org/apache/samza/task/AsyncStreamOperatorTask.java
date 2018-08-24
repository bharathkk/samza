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

import java.util.concurrent.CompletableFuture;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.impl.InputOperatorImpl;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.MessageType;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.util.Clock;


public class AsyncStreamOperatorTask extends BaseOperatorTask implements AsyncStreamTask {

  public AsyncStreamOperatorTask(OperatorSpecGraph specGraph, ContextManager contextManager, Clock clock) {
    super(specGraph, contextManager, clock);
  }

  public AsyncStreamOperatorTask(OperatorSpecGraph specGraph, ContextManager contextManager) {
    super(specGraph, contextManager);
  }

  @Override
  public void processAsync(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator,
      TaskCallback callback) {
    SystemStream systemStream = ime.getSystemStreamPartition().getSystemStream();
    InputOperatorImpl inputOpImpl = operatorImplGraph.getInputOperator(systemStream);
    if (inputOpImpl != null) {
      switch (MessageType.of(ime.getMessage())) {
        case USER_MESSAGE:
          CompletableFuture<Void>
              future = inputOpImpl.onMessage(KV.of(ime.getKey(), ime.getMessage()), collector, coordinator, callback);

          future.whenComplete((val, ex) -> {
              if (ex != null) {
                callback.failure(ex);
              } else {
                callback.complete();
              }
            });

          break;

        case END_OF_STREAM:
          EndOfStreamMessage eosMessage = (EndOfStreamMessage) ime.getMessage();
          inputOpImpl.aggregateEndOfStream(eosMessage, ime.getSystemStreamPartition(), collector, coordinator);
          break;

        case WATERMARK:
          WatermarkMessage watermarkMessage = (WatermarkMessage) ime.getMessage();
          inputOpImpl.aggregateWatermark(watermarkMessage, ime.getSystemStreamPartition(), collector, coordinator);
          break;
      }
    }
  }
}
