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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.protostellar.AccessorKeyValueProtostellar;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequestContext;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequestContext;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.util.ProtostellarUtil;
import com.couchbase.client.protostellar.kv.v1.RemoveRequest;
import com.couchbase.client.protostellar.kv.v1.RemoveResponse;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convert;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.ProtostellarRequestContext.REQUEST_KV_REMOVE;

// todo sn move this and other java-client accessors into core-io, similar to InsertAccessorProtostellar

@Stability.Internal
public class RemoveAccessorProtostellar {
  private RemoveAccessorProtostellar() {}

  public static MutationResult blocking(Core core,
                                        RemoveOptions.Built opts,
                                        ProtostellarRequest<RemoveRequest> req) {
    com.couchbase.client.protostellar.kv.v1.RemoveRequest request = req.request();
    return AccessorKeyValueProtostellar.blocking(core,
      req,
      (endpoint) -> endpoint.kvBlockingStub().withDeadline(convertTimeout(req.timeout())).remove(request),
      (response) -> convertResponse(response),
      (err) ->      convertException(core, req, err));
  }

  public static CompletableFuture<MutationResult> async(Core core,
                                                        RemoveOptions.Built opts,
                                                        ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> req) {
    com.couchbase.client.protostellar.kv.v1.RemoveRequest request = req.request();
    return AccessorKeyValueProtostellar.async(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(convertTimeout(req.timeout())).remove(request),
      (response) -> convertResponse(response),
      (err) ->      convertException(core, req, err));
  }

  public static Mono<MutationResult> reactive(Core core,
                                              RemoveOptions.Built opts,
                                              ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> req) {
    com.couchbase.client.protostellar.kv.v1.RemoveRequest request = req.request();
    return AccessorKeyValueProtostellar.reactive(core,
      req,
      (endpoint) -> endpoint.kvStub().withDeadline(convertTimeout(req.timeout())).remove(request),
      (response) -> convertResponse(response),
      (err) ->      convertException(core, req, err));
  }

  private static MutationResult convertResponse(RemoveResponse response) {
    return ProtostellarUtil.convertMutationResult(response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  private static ProtostellarRequestBehaviour convertException(Core core, ProtostellarRequest<RemoveRequest> req, Throwable t) {
    return CoreProtostellarUtil.convertKeyValueException(core, req, t);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> request(String id,
                                                                                                   RemoveOptions.Built opts,
                                                                                                   Core core,
                                                                                                   CollectionIdentifier collectionIdentifier) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), opts.durabilityLevel(), core);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.RemoveRequest> out = new ProtostellarRequest<>(core,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_REMOVE, opts.durabilityLevel(), opts.parentSpan().orElse(null)),
      new ProtostellarKeyValueRequestContext(core, ServiceType.KV, REQUEST_KV_REMOVE, timeout, id, collectionIdentifier, false),
      timeout,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    com.couchbase.client.protostellar.kv.v1.RemoveRequest.Builder request = com.couchbase.client.protostellar.kv.v1.RemoveRequest.newBuilder()
      .setBucketName(collectionIdentifier.bucket())
      .setScopeName(collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE))
      .setCollectionName(collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION))
      .setKey(id)
      .setCas(opts.cas());

    if (opts.durabilityLevel().isPresent() && opts.durabilityLevel().get() != DurabilityLevel.NONE) {
      request.setDurabilityLevel(convert(opts.durabilityLevel().get()));
    }

    out.request(request.build());
    return out;
  }
}
