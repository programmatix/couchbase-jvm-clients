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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.deps.io.grpc.stub.StreamObserver;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryRequest;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * Internal helper to map the results from the analytics requests.
 *
 * @since 3.0.0
 */
public class AnalyticsAccessorProtostellar {

  public static CompletableFuture<AnalyticsResult> analyticsQueryAsync(final CoreProtostellar core,
                                                                       final AnalyticsQueryRequest request,
                                                                       final JsonSerializer serializer) {
    List<AnalyticsQueryResponse> responses = new ArrayList<>();
    CompletableFuture<AnalyticsResult> ret = new CompletableFuture<>();

    StreamObserver<AnalyticsQueryResponse> response = new StreamObserver<AnalyticsQueryResponse>() {
      @Override
      public void onNext(AnalyticsQueryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable throwable) {
        ret.completeExceptionally(throwable);
      }

      @Override
      public void onCompleted() {
        ret.complete(new AnalyticsResultProtostellar(responses, serializer));
      }
    };

    core.endpoint().analyticsStub().analyticsQuery(request, response);

    return ret;
  }

  public static ReactiveAnalyticsResultProtostellar analyticsQueryReactive(final CoreProtostellar core, final AnalyticsQueryRequest request, final JsonSerializer serializer) {
    Sinks.Many<AnalyticsQueryResponse> responses = Sinks.many().replay().latest();

    CompletableFuture<AnalyticsResult> ret = new CompletableFuture<>();

    StreamObserver<AnalyticsQueryResponse> response = new StreamObserver<AnalyticsQueryResponse>() {
      @Override
      public void onNext(AnalyticsQueryResponse response) {
        responses.tryEmitNext(response).orThrow();
      }

      @Override
      public void onError(Throwable throwable) {
        responses.tryEmitError(throwable).orThrow();
      }

      @Override
      public void onCompleted() {
        responses.tryEmitComplete().orThrow();
      }
    };

    core.endpoint().analyticsStub().analyticsQuery(request, response);

    return new ReactiveAnalyticsResultProtostellar(responses.asFlux(), serializer);
  }
//
//  private static Flux<AnalyticsResponseProtostellar> analyticsQueryInternal(final Core core, final AnalyticsRequestProtostellar request) {
//    core.send(request);
//    return Reactor
//      .wrap(request, request.response(), true)
//      .doOnNext(ignored -> request.context().logicallyComplete())
//      .doOnError(err -> request.context().logicallyComplete(err));
//  }

}
