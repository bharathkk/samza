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

import org.apache.samza.operators.functions.AsyncFlatMapFunction;


public abstract class AsyncStreamOperatorSpec<M, OM> extends OperatorSpec<M, OM> {
  protected final AsyncFlatMapFunction<M, OM> transformFn;

  /**
   * Constructor for a {@link StreamOperatorSpec}.
   *
   * @param transformFn  the transformation function
   * @param opCode  the {@link OpCode} for this {@link StreamOperatorSpec}
   * @param opId  the unique ID for this {@link StreamOperatorSpec}
   */
  protected AsyncStreamOperatorSpec(AsyncFlatMapFunction<M, OM> transformFn, OperatorSpec.OpCode opCode, String opId) {
    super(opCode, opId);
    this.transformFn = transformFn;
  }

  public AsyncFlatMapFunction<M, OM> getTransformFn() {
    return this.transformFn;
  }
}
