/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.CoreEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains a pool of ProtostellarEndpoints.
 *
 * Experimenting with this for performance reasons.  Not sure if we'll keep it.
 *
 * https://github.com/grpc/grpc/issues/21386 indicates limit is quite low, maybe 100 concurrent rpcs
 * inspiration: https://github.com/googleapis/gax-java/blob/main/gax-grpc/src/main/java/com/google/api/gax/grpc/ChannelPool.java
 */
public class ProtostellarPool {
  private final Logger logger = LoggerFactory.getLogger(ProtostellarPool.class);

  private final List<ProtostellarEndpoint> endpoints;
  private final AtomicLong lastUsed = new AtomicLong(0);

  public ProtostellarPool(Core core, String hostname, final int port) {
    int numEndpoints = Integer.parseInt(System.getProperty("com.couchbase.protostellar.numEndpoints", "8"));
    logger.info("creating with endpoints {}", numEndpoints);
    endpoints = new ArrayList<>(numEndpoints);
    for(int i = 0; i < numEndpoints; i ++) {
      endpoints.add(new ProtostellarEndpoint(core, hostname, port));
    }
  }

  public void shutdown(Duration timeout) {
    endpoints.forEach(endpoint -> endpoint.shutdown(timeout));
  }

  public ProtostellarEndpoint endpoint() {
    // Just using a basic roundrobin strategy for now
    int index = (int) (lastUsed.getAndIncrement() % endpoints.size());
    // logger.info("Using endpoint {}", index);
    return endpoints.get(index);
  }
}
