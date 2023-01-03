/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.util.Set;

/**
 * WIP experimental, trying out a Protostellar-specific version of Core
 */
@Stability.Internal
public class CoreProtostellar {
  /**
   * The default port used for Protostellar.
   */
  private static final int DEFAULT_PROTOSTELLAR_TLS_PORT = 18098;


  private ProtostellarEndpoint endpoint;
  private final Set<SeedNode> seedNodes;
  private final Core core;


  /**
   * Creates a new CoreProtostellar.
   *
   * @param environment the environment used.
   * @param authenticator the authenticator used for kv and http authentication.
   * @param seedNodes the seed nodes to initially connect to.
   * @param connectionString if provided, the original connection string from the user.
   */
  protected CoreProtostellar(final Core core, final Authenticator authenticator, final Set<SeedNode> seedNodes) {
    if (core.context().environment().securityConfig().tlsEnabled() && !authenticator.supportsTls()) {
      throw new InvalidArgumentException("TLS enabled but the Authenticator does not support TLS!", null, null);
    } else if (!core.context().environment().securityConfig().tlsEnabled() && !authenticator.supportsNonTls()) {
      throw new InvalidArgumentException("TLS not enabled but the Authenticator does only support TLS!", null, null);
    }

    if (seedNodes.isEmpty()) {
      throw new IllegalStateException("Have no seed nodes");
    }

    this.seedNodes = seedNodes;
    this.core = core;
  }

  public void shutdown(final Duration timeout) {
    endpoint.shutdown(timeout);
  }

  public synchronized ProtostellarEndpoint endpoint() {
    // todo sn this is synchronized only because we're deferring creating the endpoint until we have a request, and we're only doing that to support the "com.couchbase.protostellar.overrideHostname" temporary hack
    // (which is set after the creation of Core).
    // Will go back to initialising the endpoint in the ctor at some point.
    if (endpoint == null) {
      SeedNode first = seedNodes.stream().findFirst().get();
      this.endpoint = new ProtostellarEndpoint(core.context(), core.context().environment(), first.address(), first.protostellarPort().orElse(DEFAULT_PROTOSTELLAR_TLS_PORT));
    }
    return endpoint;
  }
}
