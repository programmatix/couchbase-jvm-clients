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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.io.netty.util.Timeout;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Current plan is to avoid having ProtostellarRequest extend Request, but there are places in the public API that require a Request.
 * So, one will be created on-demand.
 *
 * Most of these methods are left empty as they should not be actually used.
 */
@Stability.Volatile
public class ProtostellarBaseRequest implements Request<ProtostellarBaseRequest.ProtostellarResponse> {
  private final ProtostellarRequest<?> request;

  @Stability.Internal
  public ProtostellarBaseRequest(ProtostellarRequest<?> request) {
    this.request = request;
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("This method should not be called");
  }

  @Override
  public long id() {
    throw unsupported();
  }

  @Override
  public CompletableFuture<ProtostellarResponse> response() {
    throw unsupported();
  }

  @Override
  public void succeed(ProtostellarResponse result) {
    throw unsupported();
  }

  @Override
  public void fail(Throwable error) {
    throw unsupported();
  }

  @Override
  public void cancel(CancellationReason reason, Function<Throwable, Throwable> exceptionTranslator) {
    throw unsupported();
  }

  @Override
  public void timeoutRegistration(Timeout registration) {
    throw unsupported();
  }

  @Override
  public RequestContext context() {
    throw unsupported();
  }

  @Override
  public Duration timeout() {
    return request.timeout();
  }

  @Override
  public boolean timeoutElapsed() {
    throw unsupported();
  }

  @Override
  public boolean completed() {
    throw unsupported();
  }

  @Override
  public boolean succeeded() {
    throw unsupported();
  }

  @Override
  public boolean failed() {
    throw unsupported();
  }

  @Override
  public boolean cancelled() {
    throw unsupported();
  }

  @Override
  public CancellationReason cancellationReason() {
    throw unsupported();
  }

  @Override
  public ServiceType serviceType() {
    throw unsupported();
  }

  @Override
  public Map<String, Object> serviceContext() {
    throw unsupported();
  }

  @Override
  public RetryStrategy retryStrategy() {
    throw unsupported();
  }

  @Override
  public RequestSpan requestSpan() {
    throw unsupported();
  }

  @Override
  public long createdAt() {
    throw unsupported();
  }

  @Override
  public long absoluteTimeout() {
    // todo sn some of these fields, such as timeout, would be reasonably useful to e.g. RetryStrategy implementations, and we should supply
    return request.absoluteTimeout();
  }

  static class ProtostellarResponse implements Response {
    @Override
    public ResponseStatus status() {
      throw unsupported();
    }
  }
}
