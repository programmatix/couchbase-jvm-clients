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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.protostellar.CoreInsertAccessorProtostellar;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequestContext;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequestContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.ProtostellarUtil;
import com.couchbase.client.protostellar.kv.v1.InsertRequest;
import com.couchbase.client.protostellar.kv.v1.InsertResponse;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertFromFlags;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.ProtostellarRequestContext.REQUEST_KV_INSERT;

@Stability.Internal
public class InsertAccessorProtostellar {
  private InsertAccessorProtostellar() {}

  public static MutationResult blocking(Core core,
                                        InsertOptions.Built opts,
                                        ProtostellarRequest<InsertRequest> req) {
    return CoreInsertAccessorProtostellar.blocking(core,
      req.timeout(),
      req,
      InsertAccessorProtostellar::convertResponse);
  }

  public static CompletableFuture<MutationResult> async(Core core,
                                                        InsertOptions.Built opts,
                                                        ProtostellarRequest<InsertRequest> req) {
    return CoreInsertAccessorProtostellar.async(core,
      req.timeout(),
      req,
      InsertAccessorProtostellar::convertResponse);
  }

  public static Mono<MutationResult> reactive(Core core,
                                              InsertOptions.Built opts,
                                              ProtostellarRequest<InsertRequest> req) {
    return CoreInsertAccessorProtostellar.reactive(core,
      req.timeout(),
      req,
      InsertAccessorProtostellar::convertResponse);
  }

  private static MutationResult convertResponse(InsertResponse response) {
    return ProtostellarUtil.convertMutationResult(response.getCas(), response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static ProtostellarRequest<InsertRequest> request(String id,
                                                           Object content,
                                                           InsertOptions.Built opts,
                                                           Core core,
                                                           ClusterEnvironment environment,
                                                           CollectionIdentifier collectionIdentifier) {

    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), opts.durabilityLevel(), core);
    ProtostellarRequest<InsertRequest> out = new ProtostellarRequest<>(core,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_INSERT, opts.durabilityLevel(), opts.parentSpan().orElse(null)),
      new ProtostellarKeyValueRequestContext(core, ServiceType.KV, REQUEST_KV_INSERT, timeout, id, collectionIdentifier, false),
      timeout,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

      Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    final RequestSpan encodeSpan = CbTracing.newSpan(core.context(), TracingIdentifiers.SPAN_REQUEST_ENCODING, out.span());
    long start = System.nanoTime();
    Transcoder.EncodedValue encoded;
    try {
      encoded = transcoder.encode(content);
    } finally {
      encodeSpan.end();
    }

    out.encodeLatency(System.nanoTime() - start);

    InsertRequest.Builder request = InsertRequest.newBuilder()
      .setBucketName(collectionIdentifier.bucket())
      .setScopeName(collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE))
      .setCollectionName(collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION))
      .setKey(id)
      .setContent(ByteString.copyFrom(encoded.encoded()))
      .setContentType(convertFromFlags(encoded.flags()));

    if (opts.expiry() != Expiry.none()) {
      request.setExpiry(ProtostellarUtil.convertExpiry(opts.expiry()));
    }
    if (opts.durabilityLevel().isPresent() && opts.durabilityLevel().get() != DurabilityLevel.NONE) {
      request.setDurabilityLevel(CoreProtostellarUtil.convert(opts.durabilityLevel().get()));
    }

    out.request(request.build());

    return out;
  }
}
