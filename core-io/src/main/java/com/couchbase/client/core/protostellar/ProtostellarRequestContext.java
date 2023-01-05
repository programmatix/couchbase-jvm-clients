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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Stability.Internal
public class ProtostellarRequestContext extends AbstractContext {
  public static final String REQUEST_KV_INSERT = "insert";
  public static final String REQUEST_KV_REMOVE = "remove";
  public static final String REQUEST_KV_GET = "get";
  public static final String REQUEST_QUERY = "query";

  private final Core core;
  private final long createdAt;
  private long logicallyCompletedAt;
  private final ServiceType serviceType;
  private final String requestName;
  private int retryAttempts;
  private Set<RetryReason> retryReasons;
  private final Duration timeout;

  /**
   * The time it took to encode the payload (if any).
   */
  // todo sn set this
  private long encodeLatency;

  public ProtostellarRequestContext(Core core,
                                    ServiceType serviceType,
                                    String requestName,
                                    Duration timeout) {
    this.core = core;
    this.serviceType = serviceType;
    this.requestName = requestName;
    this.timeout = timeout;
    this.createdAt = System.nanoTime();
  }

  public long encodeLatency() {
    return encodeLatency;
  }
//
//  public ProtostellarRequestContext<TGrpcRequest> encodeLatency(long encodeLatency) {
//    this.encodeLatency = encodeLatency;
//    return this;
//  }


  /**
   * Returns the request latency once logically completed (includes potential "inner" operations like observe
   * calls).
   */
  public long logicalRequestLatency() {
    if (logicallyCompletedAt == 0 || logicallyCompletedAt <= createdAt) {
      return 0;
    }
    return logicallyCompletedAt - createdAt;
  }
//
  public void incrementRetryAttempts(Duration duration, RetryReason reason) {
    retryAttempts += 1;
    if (retryReasons == null) {
      retryReasons = new HashSet<>();
    }
    retryReasons.add(reason);
  }

  public void injectExportableParams(final Map<String, Object> input) {
//    ProtostellarErrorContext context = ProtostellarErrorContext.create();
//    Map<String, Object> input = new HashMap<>();
//    context.injectExportableParams(input);

    // todo sn is id important?
    // context.put("requestId", request.id());
    // todo sn track idempotency
    // context.put("idempotent", request.idempotent());
    input.put("requestName", requestName);
    input.put("retried", retryAttempts);
    // todo sn track completion
    // context.put("completed", request.completed());
    input.put("timeoutMs", timeout.toMillis());
    // todo sn track cancellation
//    if (request.cancelled()) {
//      context.put("cancelled", true);
//      context.put("reason", request.cancellationReason());
//    }
    // todo sn clientContext
//    if (clientContext != null) {
//      context.put("clientContext", clientContext);
//    }
    // todo sn is serviceContext important?
//    Map<String, Object> serviceContext = request.serviceContext();
//    if (serviceContext != null) {
//      context.put("service", serviceContext);
//    }
    if (retryReasons != null) {
      input.put("retryReasons", retryReasons);
    }
    long logicalLatency = logicalRequestLatency();
    // todo sn track timings
//    if (dispatchLatency != 0 || logicalLatency != 0 || encodeLatency != 0 || serverLatency != 0) {
//      HashMap<String, Long> timings = new HashMap<>();
//      if (dispatchLatency != 0) {
//        timings.put("dispatchMicros", TimeUnit.NANOSECONDS.toMicros(dispatchLatency));
//      }
//
//      if (totalDispatchLatency.get() != 0) {
//        timings.put("totalDispatchMicros", TimeUnit.NANOSECONDS.toMicros(totalDispatchLatency.get()));
//      }
//      if (serverLatency != 0) {
//        timings.put("serverMicros", TimeUnit.NANOSECONDS.toMicros(serverLatency));
//      }
//      if (totalServerLatency.get() != 0) {
//        timings.put("totalServerMicros", TimeUnit.NANOSECONDS.toMicros(totalServerLatency.get()));
//      }
//      if (logicalLatency != 0) {
//        timings.put("totalMicros", TimeUnit.NANOSECONDS.toMicros(logicalLatency));
//      }
//      if (encodeLatency != 0) {
//        timings.put("encodingMicros", TimeUnit.NANOSECONDS.toMicros(encodeLatency));
//      }
//      context.put("timings", timings);
//    }
//    return input;
  }

  public ServiceType serviceType() {
    return serviceType;
  }

  public long logicallyCompletedAt() {
    return logicallyCompletedAt;
  }

  public String requestName() {
    return requestName;
  }


  public Duration timeout() {
    return timeout;
  }

  public int retryAttempts() {
    return retryAttempts;
  }
}
