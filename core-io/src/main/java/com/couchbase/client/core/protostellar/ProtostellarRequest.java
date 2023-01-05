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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Think we need this as there's so much to hold onto outside of the basic GRPC request.
 * However, cannot extend Request since that requires its response derive from Response, which a GRPC response doesn't.
 * Could create Response wrappers, but seeing how far we get without that...
 */
@Stability.Internal
public class ProtostellarRequest<TGrpcRequest> {
  private final Core core;
  private TGrpcRequest request;
  private final ProtostellarRequestContext context;
  private @Nullable RequestSpan span;

  /**
   * The time it took to encode the payload (if any).
   */
  private long encodeLatency;

  public ProtostellarRequest(Core core,
                             RequestSpan span,
                             ProtostellarRequestContext context) {
    this.core = core;
    this.span = span;
    this.context = context;
  }

  public ProtostellarRequest<TGrpcRequest> request(TGrpcRequest request) {
    this.request = request;
    return this;
  }

  public TGrpcRequest request() {
    return request;
  }

  public ProtostellarRequestContext context() {
    return context;
  }

  public long encodeLatency() {
    return encodeLatency;
  }

  public ProtostellarRequest<TGrpcRequest> encodeLatency(long encodeLatency) {
    this.encodeLatency = encodeLatency;
    return this;
  }

//  public long logicallyCompletedAt() {
//    return logicallyCompletedAt;
//  }
//
//  public ProtostellarRequest<TGrpcRequest> logicallyCompletedAt(long logicallyCompletedAt) {
//    this.logicallyCompletedAt = logicallyCompletedAt;
//    return this;
//  }

  public RequestSpan span() {
    return span;
  }

  public ProtostellarRequest<TGrpcRequest> span(RequestSpan span) {
    this.span = span;
    return this;
  }

  // todo sn throw FeatureUnavailableException on most management APIs - or should we fallback?
  // todo sn have another go at finding number of underlying streams and HTTP2 connections

  /**
   * Returns the request latency once logically completed (includes potential "inner" operations like observe
   * calls).
   */
//  public long logicalRequestLatency() {
//    if (logicallyCompletedAt == 0 || logicallyCompletedAt <= createdAt) {
//      return 0;
//    }
//    return logicallyCompletedAt - createdAt;
//  }

  public void logicallyComplete(@Nullable Throwable err) {
    if (span != null) {
      if (!CbTracing.isInternalSpan(span)) {
        span.attribute(TracingIdentifiers.ATTR_RETRIES, context.retryAttempts());
        if (err != null) {
          span.recordException(err);
          span.status(RequestSpan.StatusCode.ERROR);
        }
      }
      span.end();
    }

    if (!(core.context().environment().meter() instanceof NoopMeter)) {
      long latency = context.logicalRequestLatency();
      if (latency > 0) {
        Core.ResponseMetricIdentifier rmi = new Core.ResponseMetricIdentifier(context.serviceType().ident(), context.requestName());
        core.responseMetric(rmi).recordValue(latency);
      }
    }
  }

  public void incrementRetryAttempts(Duration duration, RetryReason reason) {
    context.incrementRetryAttempts(duration, reason);
  }


  public Duration timeout() {
    return context.timeout();
  }
}
