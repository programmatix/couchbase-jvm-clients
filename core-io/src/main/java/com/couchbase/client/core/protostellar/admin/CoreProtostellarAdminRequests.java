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
package com.couchbase.client.core.protostellar.admin;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertFromFlags;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.ProtostellarRequest.REQUEST_KV_GET;
import static com.couchbase.client.core.protostellar.ProtostellarRequest.REQUEST_KV_INSERT;
import static com.couchbase.client.core.protostellar.ProtostellarRequest.REQUEST_KV_REMOVE;

import java.time.Duration;
import java.util.function.Supplier;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreEncodedContent;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarAdminRequest;
import com.couchbase.client.core.protostellar.ProtostellarKeyValueRequest;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.ListCollectionsRequest;
import com.couchbase.client.protostellar.kv.v1.GetRequest;
import com.couchbase.client.protostellar.kv.v1.InsertRequest;
import com.couchbase.client.protostellar.kv.v1.RemoveRequest;

/**
 * For creating Protostellar GRPC requests.
 */
@Stability.Internal
public class CoreProtostellarAdminRequests {
  private CoreProtostellarAdminRequests() {}

  public static ProtostellarRequest<CreateCollectionRequest> createCollectionRequest(Core core,
                                                                                     String bucketName,
                                                                                     String scopeName,
                                                                                     String collectionName,
                                                                                     Duration maxTTL,
                                                                                     CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), CoreDurability.NONE, core);
    ProtostellarRequest<CreateCollectionRequest> out = new ProtostellarAdminRequest<>(core,
      bucketName, scopeName, collectionName,
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    CreateCollectionRequest.Builder request = CreateCollectionRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .setCollectionName(collectionName);

    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<DeleteCollectionRequest> deleteCollectionRequest(Core core,
                                                                                     String bucketName,
                                                                                     String scopeName,
                                                                                     String collectionName,
                                                                                     CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), CoreDurability.NONE, core);
    ProtostellarRequest<DeleteCollectionRequest> out = new ProtostellarAdminRequest<>(core,
      bucketName, scopeName, collectionName,
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    DeleteCollectionRequest.Builder request = DeleteCollectionRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName)
      .setCollectionName(collectionName);

    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<CreateScopeRequest> createScopeRequest(Core core,
                                                                           String bucketName,
                                                                           String scopeName,
                                                                           CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), CoreDurability.NONE, core);
    ProtostellarRequest<CreateScopeRequest> out = new ProtostellarAdminRequest<>(core,
      bucketName, scopeName, null,
      TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    CreateScopeRequest.Builder request = CreateScopeRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName);
    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<DeleteScopeRequest> deleteScopeRequest(Core core,
                                                                           String bucketName,
                                                                           String scopeName,
                                                                           CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), CoreDurability.NONE, core);
    ProtostellarRequest<DeleteScopeRequest> out = new ProtostellarAdminRequest<>(core,
      bucketName, scopeName, null,
      TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    DeleteScopeRequest.Builder request = DeleteScopeRequest.newBuilder()
      .setBucketName(bucketName)
      .setScopeName(scopeName);
    out.request(request.build());
    return out;
  }

  public static ProtostellarRequest<ListCollectionsRequest> listCollectionsRequest(Core core,
                                                                                   String bucketName,
                                                                                   CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.kvDurableTimeout(opts.timeout(), CoreDurability.NONE, core);
    ProtostellarRequest<ListCollectionsRequest> out = new ProtostellarAdminRequest<>(core,
      bucketName, null, null,
      TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()));

    ListCollectionsRequest.Builder request = ListCollectionsRequest.newBuilder()
      .setBucketName(bucketName);
    out.request(request.build());
    return out;
  }
}
