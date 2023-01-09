/*
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.Core;

import java.util.concurrent.Executor;

public class StellarNebulaContext {
  private final StellarNebulaConnection connection;
  private final Executor executor;
  private final Core core;

  public StellarNebulaContext(StellarNebulaConnection connection, Executor executor, Core core) {
    this.connection = connection;
    this.executor = executor;
    this.core = core;
  }

  public StellarNebulaConnection connection() {
    return connection;
  }

  public Executor executor() {
    return executor;
  }

  public Core core() {
    return core;
  }
}
