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

import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.MessageType;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.impl.InputOperatorImpl;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamTask} implementation that brings all the operator API implementation components together and
 * feeds the input messages into the user-defined transformation chains in {@link OperatorSpecGraph}.
 */
public class StreamOperatorTask extends BaseOperatorTask implements StreamTask {
  private static final Logger LOG = LoggerFactory.getLogger(StreamOperatorTask.class);

  /**
   * Constructs an adaptor task to run the user-implemented {@link OperatorSpecGraph}.
   * @param specGraph the serialized version of user-implemented {@link OperatorSpecGraph}
   *                  that includes the logical DAG
   * @param contextManager the {@link ContextManager} used to set up the shared context used by operators in the DAG
   * @param clock the {@link Clock} to use for time-keeping
   */
  public StreamOperatorTask(OperatorSpecGraph specGraph, ContextManager contextManager, Clock clock) {
    super(specGraph, contextManager, clock);
  }

  public StreamOperatorTask(OperatorSpecGraph specGraph, ContextManager contextManager) {
    super(specGraph, contextManager, SystemClock.instance());
  }

  /**
   * Passes the incoming message envelopes along to the {@link InputOperatorImpl} node
   * for the input {@link SystemStream}.
   * <p>
   * From then on, each {@link org.apache.samza.operators.impl.OperatorImpl} propagates its transformed output to
   * its chained {@link org.apache.samza.operators.impl.OperatorImpl}s itself.
   *
   * @param ime incoming message envelope to process
   * @param collector the collector to send messages with
   * @param coordinator the coordinator to request commits or shutdown
   */
  @Override
  public final void process(IncomingMessageEnvelope ime, MessageCollector collector, TaskCoordinator coordinator) {
    SystemStream systemStream = ime.getSystemStreamPartition().getSystemStream();
    InputOperatorImpl inputOpImpl = operatorImplGraph.getInputOperator(systemStream);
    if (inputOpImpl != null) {
      switch (MessageType.of(ime.getMessage())) {
        case USER_MESSAGE:
          inputOpImpl.onMessage(KV.of(ime.getKey(), ime.getMessage()), collector, coordinator);
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
