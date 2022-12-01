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
