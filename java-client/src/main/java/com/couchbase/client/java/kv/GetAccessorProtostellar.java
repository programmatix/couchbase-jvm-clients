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
import com.couchbase.client.core.protostellar.AccessorKeyValueProtostellar;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarFailureBehaviour;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequestContext;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequestContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.util.ProtostellarUtil;
import com.couchbase.client.protostellar.kv.v1.GetRequest;
import com.couchbase.client.protostellar.kv.v1.GetResponse;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertToFlags;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.ProtostellarRequestContext.REQUEST_KV_GET;

@Stability.Internal
public class GetAccessorProtostellar {
  private GetAccessorProtostellar() {}

  public static GetResult blocking(Core core,
                                   GetOptions.Built opts,
                                   ProtostellarRequest<GetRequest> req,
                                   Transcoder transcoder) {
    com.couchbase.client.protostellar.kv.v1.GetRequest request = req.request();
    return AccessorKeyValueProtostellar.blocking(core,
      req,
      () ->         core.protostellar().endpoint().kvBlockingStub().withDeadline(convertTimeout(req.timeout())).get(request),
      (response) -> convertResponse(response, transcoder),
      (err) ->      convertException(err, req));
  }

  public static CompletableFuture<GetResult> async(Core core,
                                                   GetOptions.Built opts,
                                                   ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req,
                                                   Transcoder transcoder) {
    com.couchbase.client.protostellar.kv.v1.GetRequest request = req.request();
    return AccessorKeyValueProtostellar.async(core,
      req,
      () ->         core.protostellar().endpoint().kvStub().withDeadline(convertTimeout(req.timeout())).get(request),
      (response) -> convertResponse(response, transcoder),
      (err) ->      convertException(err, req));
  }

  public static Mono<GetResult> reactive(Core core,
                                         GetOptions.Built opts,
                                         ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req,
                                         Transcoder transcoder) {
    com.couchbase.client.protostellar.kv.v1.GetRequest request = req.request();
    return AccessorKeyValueProtostellar.reactive(core,
      req,
      () ->         core.protostellar().endpoint().kvStub().withDeadline(convertTimeout(req.timeout())).get(request),
      (response) -> convertResponse(response, transcoder),
      (err) ->      convertException(err, req));
  }

  private static GetResult convertResponse(GetResponse response, Transcoder transcoder) {
    return new GetResult(response.getContent().toByteArray(),
      convertToFlags(response.getContentType()),
      response.getCas(),
      ProtostellarUtil.convertExpiry(response.hasExpiry(), response.getExpiry()),
      transcoder);
  }

  private static ProtostellarFailureBehaviour convertException(Throwable t, ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req) {
    return CoreProtostellarUtil.convertKeyValueException(t, req);
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> request(Core core,
                                                                                                GetOptions.Built opts,
                                                                                                CollectionIdentifier collectionIdentifier,
                                                                                                String id) {
    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> out = new ProtostellarRequest<>(core,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_GET, Optional.empty(), opts.parentSpan().orElse(null)),
      new ProtostellarKeyValueRequestContext(core, ServiceType.KV, REQUEST_KV_GET, timeout, id, collectionIdentifier),
      timeout,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    out.request(com.couchbase.client.protostellar.kv.v1.GetRequest.newBuilder()
      .setBucketName(collectionIdentifier.bucket())
      .setScopeName(collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE))
      .setCollectionName(collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION))
      .setKey(id)
      .build());

    return out;
  }
}
