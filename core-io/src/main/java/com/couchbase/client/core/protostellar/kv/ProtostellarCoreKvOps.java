/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.protostellar.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.protostellar.AccessorKeyValueProtostellar;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequestContext;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.kv.v1.GetResponse;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertTimeout;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertToFlags;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.ProtostellarRequestContext.REQUEST_KV_GET;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ProtostellarCoreKvOps implements CoreKvOps {
  private final Core core;
  private final CoreContext ctx;
  // todo sn use these
  private final Duration defaultKvTimeout;
  private final RetryStrategy defaultRetryStrategy;
  private final CollectionIdentifier collectionIdentifier;
  private final CoreKeyspace keyspace;
  private final RequestTracer requestTracer;

  public ProtostellarCoreKvOps(Core core, CoreKeyspace keyspace) {
    this.core = requireNonNull(core);
    this.ctx = core.context();
    this.defaultKvTimeout = ctx.environment().timeoutConfig().kvTimeout();
    this.defaultRetryStrategy = ctx.environment().retryStrategy();
    this.requestTracer = ctx.environment().requestTracer();
    this.keyspace = requireNonNull(keyspace);
    this.collectionIdentifier = keyspace.toCollectionIdentifier();
  }

  @Override
  public CoreGetResult getBlocking(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = request(core, common, collectionIdentifier, key);
    com.couchbase.client.protostellar.kv.v1.GetRequest request = req.request();

    return AccessorKeyValueProtostellar.blocking(core,
      req,
      // todo sn withDeadline creates a new stub and Google performance docs advise reusing stubs as much as possible
      // Measure the impact to decide if it's worth tracking if it's a non-default timeout
      () ->         core.protostellar().endpoint().kvBlockingStub().withDeadline(convertTimeout(req.timeout())).get(request),
      (response) -> this.convertGetResponse(key, response),
      (err) ->      CoreProtostellarUtil.convertKeyValueException(err, req));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAsync(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    // todo sn DRY these checks
    notNullOrEmpty(key, "Document ID");

    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = request(core, common, collectionIdentifier, key);
    com.couchbase.client.protostellar.kv.v1.GetRequest request = req.request();

    return AccessorKeyValueProtostellar.asyncCore(core,
      req,
      () ->         core.protostellar().endpoint().kvStub().withDeadline(convertTimeout(req.timeout())).get(request),
      (response) -> this.convertGetResponse(key, response),
      (err) ->      CoreProtostellarUtil.convertKeyValueException(err, req));
  }

  @Override
  public Mono<CoreGetResult> getReactive(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> req = request(core, common, collectionIdentifier, key);
    com.couchbase.client.protostellar.kv.v1.GetRequest request = req.request();

    return AccessorKeyValueProtostellar.reactive(core,
      req,
      () ->         core.protostellar().endpoint().kvStub().withDeadline(convertTimeout(req.timeout())).get(request),
      (response) -> this.convertGetResponse(key, response),
      (err) ->      CoreProtostellarUtil.convertKeyValueException(err, req));
  }

  private CoreGetResult convertGetResponse(String key, GetResponse response) {
    return new CoreGetResult(CoreKvResponseMetadata.NONE,
      keyspace,
      key,
      response.getContent().toByteArray(),
      convertToFlags(response.getContentType()),
      response.getCas(),
      // todo sn expiry
      null);
      // ProtostellarUtil.convertExpiry(response.hasExpiry(), response.getExpiry()));
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndLockAsync(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    throw new UnsupportedOperationException("Not currently supported");
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndTouchAsync(
      CoreCommonOptions common,
      String key,
      long expiration
  ) {
    throw new UnsupportedOperationException("Not currently supported");
  }

  @Override
  public CoreAsyncResponse<CoreSubdocGetResult> subdocGet(
      CoreCommonOptions common,
      String key,
      byte flags,
      List<SubdocGetRequest.Command> commands
  ) {
    throw new UnsupportedOperationException("Not currently supported");
  }

  public static ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> request(Core core,
                                                                                                CoreCommonOptions opts,
                                                                                                CollectionIdentifier collectionIdentifier,
                                                                                                String id) {
    Duration timeout = CoreProtostellarUtil.kvTimeout(opts.timeout(), core);
    ProtostellarRequest<com.couchbase.client.protostellar.kv.v1.GetRequest> out = new ProtostellarRequest<>(core,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_KV_GET, Optional.empty(), opts.parentSpan().orElse(null)),
      new ProtostellarKeyValueRequestContext(core, ServiceType.KV, REQUEST_KV_GET, timeout, id, collectionIdentifier));

    out.request(com.couchbase.client.protostellar.kv.v1.GetRequest.newBuilder()
      .setBucketName(collectionIdentifier.bucket())
      .setScopeName(collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE))
      .setCollectionName(collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION))
      .setKey(id)
      .build());

    return out;
  }

}
