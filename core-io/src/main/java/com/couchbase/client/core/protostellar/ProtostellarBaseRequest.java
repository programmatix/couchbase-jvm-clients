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
  private static final RuntimeException UNSUPPORTED = new UnsupportedOperationException("This method should not be called");

  private final ProtostellarRequest<?> request;

  @Stability.Internal
  public ProtostellarBaseRequest(ProtostellarRequest<?> request) {
    this.request = request;
  }

  @Override
  public long id() {
    throw UNSUPPORTED;
  }

  @Override
  public CompletableFuture<ProtostellarResponse> response() {
    throw UNSUPPORTED;
  }

  @Override
  public void succeed(ProtostellarResponse result) {
    throw UNSUPPORTED;
  }

  @Override
  public void fail(Throwable error) {
    throw UNSUPPORTED;
  }

  @Override
  public void cancel(CancellationReason reason, Function<Throwable, Throwable> exceptionTranslator) {
    throw UNSUPPORTED;
  }

  @Override
  public void timeoutRegistration(Timeout registration) {
    throw UNSUPPORTED;
  }

  @Override
  public RequestContext context() {
    throw UNSUPPORTED;
  }

  @Override
  public Duration timeout() {
    return request.timeout();
  }

  @Override
  public boolean timeoutElapsed() {
    throw UNSUPPORTED;
  }

  @Override
  public boolean completed() {
    throw UNSUPPORTED;
  }

  @Override
  public boolean succeeded() {
    throw UNSUPPORTED;
  }

  @Override
  public boolean failed() {
    throw UNSUPPORTED;
  }

  @Override
  public boolean cancelled() {
    throw UNSUPPORTED;
  }

  @Override
  public CancellationReason cancellationReason() {
    throw UNSUPPORTED;
  }

  @Override
  public ServiceType serviceType() {
    throw UNSUPPORTED;
  }

  @Override
  public Map<String, Object> serviceContext() {
    throw UNSUPPORTED;
  }

  @Override
  public RetryStrategy retryStrategy() {
    throw UNSUPPORTED;
  }

  @Override
  public RequestSpan requestSpan() {
    throw UNSUPPORTED;
  }

  @Override
  public long createdAt() {
    throw UNSUPPORTED;
  }

  @Override
  public long absoluteTimeout() {
    // todo sn some of these fields, such as timeout, would be reasonably useful to e.g. RetryStrategy implementations, and we should supply
    return request.absoluteTimeout();
  }

  static class ProtostellarResponse implements Response {
    @Override
    public ResponseStatus status() {
      throw UNSUPPORTED;
    }
  }
}
