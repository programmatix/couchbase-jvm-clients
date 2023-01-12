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
package com.couchbase.client.java.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.io.grpc.stub.StreamObserver;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequestContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.protostellar.query.v1.QueryRequest;
import com.couchbase.client.protostellar.query.v1.QueryResponse;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownBlocking;
import static com.couchbase.client.core.protostellar.ProtostellarRequestContext.REQUEST_QUERY;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;


@Stability.Internal
public class QueryAccessorProtostellar {
  public static QueryResult blocking(Core core,
                                     QueryOptions.Built opts,
                                     ProtostellarRequest<QueryRequest> request,
                                     JsonSerializer serializer) {
    handleShutdownBlocking(core, request.context());
    List<QueryResponse> responses = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    StreamObserver<QueryResponse> response = new StreamObserver<QueryResponse>() {
      @Override
      public void onNext(QueryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        throw convertException(throwable);
      }

      @Override
      public void onCompleted() {
        latch.countDown();
      }
    };

    core.protostellar().endpoint().queryStub()
      .withDeadline(convertTimeout(request.timeout()))
      .query(request.request(), response);

    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return new QueryResultProtostellar(responses, serializer);
  }

  public static CompletableFuture<QueryResult> async(Core core,
                                                     QueryOptions.Built opts,
                                                     ProtostellarRequest<QueryRequest> request,
                                                     JsonSerializer serializer) {
    CompletableFuture<QueryResult> ret = new CompletableFuture<>();
    if (handleShutdownAsync(core, ret, request.context())) {
      return ret;
    }
    List<QueryResponse> responses = new ArrayList<>();

    StreamObserver<QueryResponse> response = new StreamObserver<QueryResponse>() {
      @Override
      public void onNext(QueryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        ret.completeExceptionally(convertException(throwable));
      }

      @Override
      public void onCompleted() {
        ret.complete(new QueryResultProtostellar(responses, serializer));
      }
    };

    core.protostellar().endpoint().queryStub()
      .withDeadline(convertTimeout(request.timeout()))
      .query(request.request(), response);

    return ret;
  }

  public static Mono<ReactiveQueryResultProtostellar> reactive(Core core,
                                                               QueryOptions.Built opts,
                                                               ProtostellarRequest<QueryRequest> request,
                                                               JsonSerializer serializer) {
    // todo sn
//    if (handleShutdownReactive(core, () -> { return null; })) {
//      return;
//    }

    Sinks.Many<QueryResponse> responses = Sinks.many().replay().latest();

    StreamObserver<QueryResponse> response = new StreamObserver<QueryResponse>() {
      @Override
      public void onNext(QueryResponse response) {
        responses.tryEmitNext(response).orThrow();
      }

      @Override
      public void onError(Throwable throwable) {
        responses.tryEmitError(convertException(throwable)).orThrow();
      }

      @Override
      public void onCompleted() {
        responses.tryEmitComplete().orThrow();
      }
    };

    core.protostellar().endpoint().queryStub()
      .withDeadline(convertTimeout(request.timeout()))
      .query(request.request(), response);

    return Mono.just(new ReactiveQueryResultProtostellar(responses.asFlux(), serializer));
  }

  private static Duration timeout(Core core) {
    return core.context().environment().timeoutConfig().queryTimeout();
  }

  private static RuntimeException convertException(Throwable throwable) {
    // todo snbrett query error handling
    if (throwable instanceof RuntimeException) {
      return (RuntimeException) throwable;
    }
    return new RuntimeException(throwable);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.query.v1.QueryRequest> request(Core core,
                                                                                                     String statement,
                                                                                                     QueryOptions.Built opts,
                                                                                                     ClusterEnvironment environment,
                                                                                                     @Nullable String bucketName,
                                                                                                     @Nullable String scopeName) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedAnalyticsErrorContext(statement));

    Duration timeout = opts.timeout().orElse(core.context().environment().timeoutConfig().queryTimeout());
    RequestSpan span = createSpan(core, TracingIdentifiers.SPAN_REQUEST_QUERY, CoreDurability.NONE, opts.parentSpan().orElse(null));
    span.attribute(TracingIdentifiers.ATTR_STATEMENT, statement);
    ProtostellarRequest<com.couchbase.client.protostellar.query.v1.QueryRequest> out = new ProtostellarRequest<>(core,
      span,
      new ProtostellarRequestContext(core, ServiceType.QUERY, REQUEST_QUERY, timeout, opts.readonly()),
      timeout,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    com.couchbase.client.protostellar.query.v1.QueryRequest.Builder request = com.couchbase.client.protostellar.query.v1.QueryRequest.newBuilder()
      .setStatement(statement);
    if (bucketName != null) {
      request.setBucketName(bucketName);
    }
    if (scopeName != null) {
      request.setScopeName(scopeName);
    }
    opts.injectParams(request);

    out.request(request.build());

    // todo sn request.context().clientContext(opts.clientContext());
    return out;
  }
}
