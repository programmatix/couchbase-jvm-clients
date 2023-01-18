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

import static com.couchbase.client.core.protostellar.admin.CoreProtostellarAdminRequests.createCollectionRequest;
import static com.couchbase.client.core.protostellar.admin.CoreProtostellarAdminRequests.createScopeRequest;
import static com.couchbase.client.core.protostellar.admin.CoreProtostellarAdminRequests.deleteCollectionRequest;
import static com.couchbase.client.core.protostellar.admin.CoreProtostellarAdminRequests.deleteScopeRequest;
import static com.couchbase.client.core.protostellar.admin.CoreProtostellarAdminRequests.listCollectionsRequest;
import static com.couchbase.client.core.protostellar.admin.CoreProtostellarAdminResponses.convertResponse;
import static java.util.Objects.requireNonNull;

import reactor.core.publisher.Mono;

import java.time.Duration;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.admin.CoreAdminOps;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessors;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.CreateScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteCollectionRequest;
import com.couchbase.client.protostellar.admin.collection.v1.DeleteScopeRequest;
import com.couchbase.client.protostellar.admin.collection.v1.ListCollectionsRequest;

@Stability.Internal
public final class ProtostellarCoreAdminOps implements CoreAdminOps {
	private final Core core;
	private final String bucketName;

	public ProtostellarCoreAdminOps(Core core, String bucketName) {
		this.core = requireNonNull(core);
		this.bucketName = requireNonNull(bucketName);
	}

	@Override
	public void createCollectionBlocking(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {
		ProtostellarRequest<CreateCollectionRequest> request = createCollectionRequest(core, bucketName, scopeName,
				collectionName, maxTTL, options);
		CoreProtostellarAccessors.blocking(core, request, (endpoint) -> {
			return endpoint.collectionAdminBlockingStub().withDeadline(request.deadline())
					.createCollection(request.request());
		}, (response) -> { convertResponse(bucketName, scopeName, collectionName, response); return null;});
	}

	@Override
	public CoreAsyncResponse<Void> createCollectionAsync(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {
		ProtostellarRequest<CreateCollectionRequest> request = createCollectionRequest(core, bucketName, scopeName,
				collectionName, maxTTL, options);
		return CoreProtostellarAccessors.async(core, request, (endpoint) -> endpoint.collectionAdminStub()
				.withDeadline(request.deadline()).createCollection(request.request()),
				(response) -> { convertResponse(bucketName, scopeName, collectionName, response); return null;});
	}

	@Override
	public Mono<Void> createCollectionReactive(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {
		ProtostellarRequest<CreateCollectionRequest> request = createCollectionRequest(core, bucketName, scopeName,
				collectionName, maxTTL, options);
		return CoreProtostellarAccessors.reactive(core, request, (endpoint) -> endpoint.collectionAdminStub()
				.withDeadline(request.deadline()).createCollection(request.request()),
				(response) -> { convertResponse(bucketName, scopeName, collectionName, response); return null;});
	}

	@Override
	public CoreAsyncResponse<Void> createScopeAsync(String scopeName, CoreCommonOptions options) {
		ProtostellarRequest<CreateScopeRequest> request = createScopeRequest(core, bucketName, scopeName, options);
		return CoreProtostellarAccessors.async(core, request,
				(endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).createScope(request.request()),
				(response) -> { convertResponse(bucketName, scopeName, null, response); return null; });
	}

	@Override
	public CoreAsyncResponse<Void> dropCollectionAsync(String scopeName, String collectionName,
			CoreCommonOptions options) {
		ProtostellarRequest<DeleteCollectionRequest> request = deleteCollectionRequest(core, bucketName, scopeName,
				collectionName, options);
		return CoreProtostellarAccessors.async(core, request, (endpoint) -> endpoint.collectionAdminStub()
				.withDeadline(request.deadline()).deleteCollection(request.request()),
				(response) -> { convertResponse(bucketName, scopeName, collectionName, response); return null; });
	}

	@Override
	public CoreAsyncResponse<Void> dropScopeAsync(String scopeName, CoreCommonOptions options) {
		ProtostellarRequest<DeleteScopeRequest> request = deleteScopeRequest(core, bucketName, scopeName, options);
		return CoreProtostellarAccessors.async(core, request,
				(endpoint) -> endpoint.collectionAdminStub().withDeadline(request.deadline()).deleteScope(request.request()),
				(response) -> { convertResponse(bucketName, scopeName, null, response); return null;});
	}

	@Override
	public CoreAsyncResponse<CollectionsManifest> getAllScopesAsync(CoreCommonOptions options) {
		ProtostellarRequest<ListCollectionsRequest> request = listCollectionsRequest(core, bucketName, options);
		return CoreProtostellarAccessors.async(core, request, (endpoint) -> endpoint.collectionAdminStub()
				.withDeadline(request.deadline()).listCollections(request.request()),
				(response) -> convertResponse(bucketName, null, null, response));
	}

	private static RuntimeException unsupported() {
		return new UnsupportedOperationException("Not currently supported");
	}
}
