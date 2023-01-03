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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.FutureCallback;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.Futures;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.error.context.ErrorContext;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownAsync;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownBlocking;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.handleShutdownReactive;

public class AccessorKeyValueProtostellar {

  /**
   * @param <TSdkResult> e.g. MutationResult
   * @param <TGrpcResponse> e.g. com.couchbase.client.protostellar.kv.v1.InsertResponse
   */
  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  TSdkResult blocking(Core core,
                      ProtostellarRequest<TGrpcRequest>     request,
                      Supplier<TGrpcResponse>               executeBlockingGrpcCall,
                      Function<TGrpcResponse, TSdkResult>   convertResponse,
                      Function<Throwable, ProtostellarFailureBehaviour> convertException) {
    handleShutdownBlocking(core, request.context());
    final RequestSpan dispatchSpan = createDispatchSpan(core, request);
    try {
      // Make the Protostellar call.
      // todo sn check this is blocking just this user thread, not also an executor thread
      TGrpcResponse response = executeBlockingGrpcCall.get();

      if (dispatchSpan != null) {
        dispatchSpan.end();
      }
      TSdkResult result = convertResponse.apply(response);
      request.logicallyComplete(null);
      return result;
    } catch (Throwable t) {
      ProtostellarFailureBehaviour converted = convertException.apply(t);
      if (dispatchSpan != null) {
        dispatchSpan.recordException(converted.exception());
        dispatchSpan.status(RequestSpan.StatusCode.ERROR);
        dispatchSpan.end();
      }
      if (converted.shouldRetry()) {
        // todo snbrett what kind of backoff do we want
        Duration backoff = Duration.ofMillis(50);
        try {
          Thread.sleep(backoff.toMillis());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        request.incrementRetryAttempts(backoff, converted.retry());
        return blocking(core, request, executeBlockingGrpcCall, convertResponse, convertException);
      }
      else {
        request.logicallyComplete(converted.exception());
        throw converted.exception();
      }
    }
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  CompletableFuture<TSdkResult> async(Core core,
                                      ProtostellarRequest<TGrpcRequest>         request,
                                      Supplier<ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                                      Function<TGrpcResponse, TSdkResult>       convertResponse,
                                      Function<Throwable, ProtostellarFailureBehaviour>     convertException) {

    CompletableFuture<TSdkResult> ret = new CompletableFuture<>();
    asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
    return ret;
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  void asyncInternal(CompletableFuture<TSdkResult> ret,
                    Core core,
                    ProtostellarRequest<TGrpcRequest>         request,
                    Supplier<ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                    Function<TGrpcResponse, TSdkResult>       convertResponse,
                    Function<Throwable, ProtostellarFailureBehaviour>     convertException) {
    if (handleShutdownAsync(core, ret, request.context())) {
      return;
    }
    final RequestSpan dispatchSpan = createDispatchSpan(core, request);

    // Make the Protostellar call.
    ListenableFuture<TGrpcResponse> response = executeFutureGrpcCall.get();

    Futures.addCallback(response, new FutureCallback<TGrpcResponse>() {
      @Override
      public void onSuccess(TGrpcResponse response) {
        if (dispatchSpan != null) {
          dispatchSpan.end();
        }

        TSdkResult result = convertResponse.apply(response);
        request.logicallyComplete(null);
        ret.complete(result);
      }

      @Override
      public void onFailure(Throwable t) {
        ProtostellarFailureBehaviour converted = convertException.apply(t);
        if (dispatchSpan != null) {
          dispatchSpan.recordException(converted.exception());
          dispatchSpan.status(RequestSpan.StatusCode.ERROR);
          dispatchSpan.end();
        }
        if (converted.shouldRetry()) {
          // todo sn CancellationReason.TOO_MANY_REQUESTS_IN_RETRY
          // todo sn more generally, do we need to be able to cancel at any point?  If so, need to put the CompletableFuture/Sink inside the ProtostellarRequest. How would blocking work?
          // todo sn even more generally, do we need to be able to retry at any point?  If so, need to teach the ProtostellarRequest how to execute itself.
          Duration backoff = Duration.ofMillis(50);
          request.incrementRetryAttempts(backoff, converted.retry());
          core.context().environment().timer().schedule(() -> {
            // note this won't work - it'll create a new CompletableFuture - request.send().accept(request);
            asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
          }, backoff);
        }
        else {
          request.logicallyComplete(converted.exception());
          ret.completeExceptionally(converted.exception());
        }
      }
    }, core.context().environment().executor());
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  Mono<TSdkResult> reactive(Core core,
                            ProtostellarRequest<TGrpcRequest>         request,
                            Supplier<ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                            Function<TGrpcResponse, TSdkResult>       convertResponse,
                            Function<Throwable, ProtostellarFailureBehaviour>     convertException) {
    return Mono.defer(() -> {
      Sinks.One<TSdkResult> ret = Sinks.one();
      reactiveInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
      return ret.asMono();
    });
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  void reactiveInternal(Sinks.One<TSdkResult> ret,
                        Core core,
                        ProtostellarRequest<TGrpcRequest>         request,
                        Supplier<ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                        Function<TGrpcResponse, TSdkResult>       convertResponse,
                        Function<Throwable, ProtostellarFailureBehaviour>     convertException) {
    if (handleShutdownReactive(ret, core, request.context())) {
      return;
    }

    final RequestSpan dispatchSpan = createDispatchSpan(core, request);

    // Make the Protostellar call.
    ListenableFuture<TGrpcResponse> response = executeFutureGrpcCall.get();

    Futures.addCallback(response, new FutureCallback<TGrpcResponse>() {
      @Override
      public void onSuccess(TGrpcResponse response) {
        if (dispatchSpan != null) {
          dispatchSpan.end();
        }
        TSdkResult result = convertResponse.apply(response);
        request.logicallyComplete(null);
        ret.tryEmitValue(result).orThrow();
      }

      @Override
      public void onFailure(Throwable t) {
        ProtostellarFailureBehaviour converted = convertException.apply(t);
        if (dispatchSpan != null) {
          dispatchSpan.recordException(converted.exception());
          dispatchSpan.status(RequestSpan.StatusCode.ERROR);
          dispatchSpan.end();
        }
        if (converted.shouldRetry()) {
          Duration backoff = Duration.ofMillis(50);
          request.incrementRetryAttempts(backoff, converted.retry());
          core.context().environment().timer().schedule(() -> {
            reactiveInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
          }, backoff);
        }
        else {
          request.logicallyComplete(converted.exception());
          ret.tryEmitError(converted.exception()).orThrow();
        }
      }
    }, core.context().environment().executor());
  }

  private static <TGrpcRequest> @Nullable RequestSpan createDispatchSpan(Core core, ProtostellarRequest<TGrpcRequest> request) {
    RequestTracer tracer = core.context().environment().requestTracer();
    final RequestSpan dispatchSpan;
    if (!CbTracing.isInternalTracer(tracer)) {
      dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.span());
      // todo sn setCommonDispatchSpanAttributes
    } else {
      dispatchSpan = null;
    }
    return dispatchSpan;
  }
}
