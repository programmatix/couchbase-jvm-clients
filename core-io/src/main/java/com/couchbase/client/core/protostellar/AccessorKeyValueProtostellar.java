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
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.FutureCallback;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.Futures;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.io.netty.TracingUtils;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.core.retry.RetryOrchestratorProtostellar;
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
                      Function<ProtostellarEndpoint, TGrpcResponse> executeBlockingGrpcCall,
                      Function<TGrpcResponse, TSdkResult>   convertResponse,
                      Function<Throwable, ProtostellarRequestBehaviour> convertException) {
    while (true) {
      handleShutdownBlocking(core, request.context());
      ProtostellarEndpoint endpoint = core.protostellar().endpoint();
      RequestSpan dispatchSpan = createDispatchSpan(core, request, endpoint);
      try {
        // Make the Protostellar call.
        // todo sn check this is blocking just this user thread, not also an executor thread
        TGrpcResponse response = executeBlockingGrpcCall.apply(endpoint);

        if (dispatchSpan != null) {
          dispatchSpan.end();
        }
        TSdkResult result = convertResponse.apply(response);
        request.logicallyComplete(null);
        return result;
      } catch (Throwable t) {
        ProtostellarRequestBehaviour behaviour = convertException.apply(t);
        handleDispatchSpan(behaviour, dispatchSpan);
        if (behaviour.retryDuration() != null) {
          try {
            Thread.sleep(behaviour.retryDuration().toMillis());
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          // todo sn do we want to handle CancellationReason.TOO_MANY_REQUESTS_IN_RETRY here?
          // Loop round again for a retry.
        } else {
          request.logicallyComplete(behaviour.exception());
          throw behaviour.exception();
        }
      }
    }
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  CoreAsyncResponse<TSdkResult> asyncCore(Core core,
                                          ProtostellarRequest<TGrpcRequest>         request,
                                          Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                                          Function<TGrpcResponse, TSdkResult>       convertResponse,
                                          Function<Throwable, ProtostellarRequestBehaviour>     convertException) {

    CompletableFuture<TSdkResult> ret = new CompletableFuture<>();
    CoreAsyncResponse<TSdkResult> response = new CoreAsyncResponse<>(ret, () -> {
      // todo sn what to do here?
    });
    asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
    return response;
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  CompletableFuture<TSdkResult> async(Core core,
                                      ProtostellarRequest<TGrpcRequest>         request,
                                      Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                                      Function<TGrpcResponse, TSdkResult>       convertResponse,
                                      Function<Throwable, ProtostellarRequestBehaviour>     convertException) {

    CompletableFuture<TSdkResult> ret = new CompletableFuture<>();
    asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
    return ret;
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  void asyncInternal(CompletableFuture<TSdkResult> ret,
                    Core core,
                    ProtostellarRequest<TGrpcRequest>         request,
                    Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                    Function<TGrpcResponse, TSdkResult>       convertResponse,
                    Function<Throwable, ProtostellarRequestBehaviour>     convertException) {
    if (handleShutdownAsync(core, ret, request.context())) {
      return;
    }
    ProtostellarEndpoint endpoint = core.protostellar().endpoint();
    RequestSpan dispatchSpan = createDispatchSpan(core, request, endpoint);

    // Make the Protostellar call.
    ListenableFuture<TGrpcResponse> response = executeFutureGrpcCall.apply(endpoint);

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
        ProtostellarRequestBehaviour behaviour = convertException.apply(t);
        handleDispatchSpan(behaviour, dispatchSpan);
        if (behaviour.retryDuration() != null) {
          boolean unableToSchedule = core.context().environment().timer().schedule(() -> {
            asyncInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
          }, behaviour.retryDuration(), true) == null;

          if (unableToSchedule) {
            RuntimeException err = request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY).exception();
            request.logicallyComplete(err);
            ret.completeExceptionally(err);
          }
        }
        else {
          request.logicallyComplete(behaviour.exception());
          ret.completeExceptionally(behaviour.exception());
        }
      }
    }, core.context().environment().executor());
  }

  public static <TSdkResult, TGrpcRequest, TGrpcResponse>
  Mono<TSdkResult> reactive(Core core,
                            ProtostellarRequest<TGrpcRequest>         request,
                            Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                            Function<TGrpcResponse, TSdkResult>       convertResponse,
                            Function<Throwable, ProtostellarRequestBehaviour>     convertException) {
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
                        Function<ProtostellarEndpoint, ListenableFuture<TGrpcResponse>> executeFutureGrpcCall,
                        Function<TGrpcResponse, TSdkResult>       convertResponse,
                        Function<Throwable, ProtostellarRequestBehaviour>     convertException) {
    if (handleShutdownReactive(ret, core, request.context())) {
      return;
    }

    ProtostellarEndpoint endpoint = core.protostellar().endpoint();
    RequestSpan dispatchSpan = createDispatchSpan(core, request, endpoint);

    // Make the Protostellar call.
    ListenableFuture<TGrpcResponse> response = executeFutureGrpcCall.apply(endpoint);

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
        ProtostellarRequestBehaviour behaviour = convertException.apply(t);
        handleDispatchSpan(behaviour, dispatchSpan);
        if (behaviour.retryDuration() != null) {
          boolean unableToSchedule = core.context().environment().timer().schedule(() -> {
            reactiveInternal(ret, core, request, executeFutureGrpcCall, convertResponse, convertException);
          }, behaviour.retryDuration(), true) == null;

          if (unableToSchedule) {
            RuntimeException err = request.cancel(CancellationReason.TOO_MANY_REQUESTS_IN_RETRY).exception();
            request.logicallyComplete(err);
            ret.tryEmitError(err).orThrow();
          }
        }
        else {
          request.logicallyComplete(behaviour.exception());
          ret.tryEmitError(behaviour.exception()).orThrow();
        }
      }
    }, core.context().environment().executor());
  }

  private static void handleDispatchSpan(ProtostellarRequestBehaviour behaviour, @Nullable RequestSpan dispatchSpan) {
    if (dispatchSpan != null) {
      if (behaviour.exception() != null) {
        dispatchSpan.recordException(behaviour.exception());
      }
      dispatchSpan.status(RequestSpan.StatusCode.ERROR);
      dispatchSpan.end();
    }
  }

  private static <TGrpcRequest> @Nullable RequestSpan createDispatchSpan(Core core,
                                                                         ProtostellarRequest<TGrpcRequest> request,
                                                                         ProtostellarEndpoint endpoint) {
    RequestTracer tracer = core.context().environment().requestTracer();
    final RequestSpan dispatchSpan;
    if (!CbTracing.isInternalTracer(tracer)) {
      dispatchSpan = tracer.requestSpan(TracingIdentifiers.SPAN_DISPATCH, request.span());
      // todo snbrett do we want to provide localId and operationId for Protostellar
      TracingUtils.setCommonDispatchSpanAttributes(dispatchSpan, null, null, 0, endpoint.hostname(), endpoint.port(), null);
    } else {
      dispatchSpan = null;
    }
    return dispatchSpan;
  }
}
