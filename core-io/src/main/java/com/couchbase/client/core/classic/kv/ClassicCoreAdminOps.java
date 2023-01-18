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
package com.couchbase.client.core.classic.kv;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.endpoint.http.CoreHttpRequest.Builder.newForm;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbThrowables.propagate;
import static java.util.Objects.requireNonNull;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.admin.CoreAdminOps;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UrlQueryStringBuilder;

@Stability.Internal
public final class ClassicCoreAdminOps implements CoreAdminOps {
	private final Core core;
	private final String bucketName;
	private final CoreHttpClient httpClient;

	public ClassicCoreAdminOps(Core core, String bucketName) {
		this.core = requireNonNull(core);
		this.bucketName = bucketName;
		this.httpClient = core.httpClient(RequestTarget.manager());
	}

	@Override
	public void createCollectionBlocking(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {
		createCollectionAsync(scopeName, collectionName, maxTTL, options).toBlocking();
	}

	@Override
	public CoreAsyncResponse<Void> createCollectionAsync(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {

		UrlQueryStringBuilder form = newForm().set("name", collectionName);
		if (maxTTL != null && !maxTTL.isZero()) {
			form.set("maxTTL", maxTTL.getSeconds());
		}

		CompletableFuture<Object> r = httpClient.post(pathForCollections(bucketName, scopeName), options)
				.trace(TracingIdentifiers.SPAN_REQUEST_MC_CREATE_COLLECTION).traceBucket(bucketName).traceScope(scopeName)
				.traceCollection(collectionName).form(form).exec(core).exceptionally(translateErrors(scopeName, collectionName))
				.thenApply(resp -> null);
		return (CoreAsyncResponse) new CoreAsyncResponse<>(r, () -> {});
	}

	@Override
	public Mono<Void> createCollectionReactive(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {
		return createCollectionAsync(scopeName, collectionName, maxTTL, options).toMono();
	}

	@Override
	public CoreAsyncResponse<Void> createScopeAsync(String scopeName, CoreCommonOptions options) {
		CompletableFuture<Object> r = httpClient.post(pathForScopes(bucketName), options)
				.trace(TracingIdentifiers.SPAN_REQUEST_MC_CREATE_SCOPE).traceBucket(bucketName).traceScope(scopeName)
				.form(newForm().add("name", scopeName)).exec(core).exceptionally(translateErrors(scopeName, null))
				.thenApply(response -> null);
		return (CoreAsyncResponse) new CoreAsyncResponse<>(r, () -> {});
	}

	@Override
	public CoreAsyncResponse<Void> dropCollectionAsync(String scopeName, String collectionName,
			CoreCommonOptions options) {
		CompletableFuture<Object> r = httpClient.delete(pathForCollection(bucketName, scopeName, collectionName), options)
				.trace(TracingIdentifiers.SPAN_REQUEST_MC_DROP_COLLECTION).traceBucket(bucketName).traceScope(scopeName)
				.traceCollection(collectionName).exec(core).exceptionally(translateErrors(scopeName, collectionName))
				.thenApply(response -> null);
		return (CoreAsyncResponse) new CoreAsyncResponse<>(r, () -> {});
	}

	@Override
	public CoreAsyncResponse<Void> dropScopeAsync(String scopeName, CoreCommonOptions options) {
		CompletableFuture<Object> r = httpClient.delete(pathForScope(bucketName, scopeName), options)
				.trace(TracingIdentifiers.SPAN_REQUEST_MC_DROP_SCOCPE).traceBucket(bucketName).traceScope(scopeName).exec(core)
				.exceptionally(translateErrors(scopeName, null)).thenApply(response -> null);
		return (CoreAsyncResponse) new CoreAsyncResponse<>(r, () -> {});
	}

	@Override
	public CoreAsyncResponse<CollectionsManifest> getAllScopesAsync(CoreCommonOptions options) {
		CompletableFuture<Object> r = httpClient.get(pathForScopes(bucketName), options).trace(TracingIdentifiers.SPAN_REQUEST_MC_GET_ALL_SCOPES)
			.traceBucket(bucketName).exec(core).exceptionally(translateErrors(null, null))
			.thenApply(response -> Mapper.decodeInto(response.content(), CollectionsManifest.class));
		return (CoreAsyncResponse) new CoreAsyncResponse<>(r, () -> {});
	}

	private static CoreHttpPath pathForScopes(String bucketName) {
		return path("/pools/default/buckets/{bucketName}/scopes", mapOf("bucketName", bucketName));
	}

	private static CoreHttpPath pathForScope(String bucketName, String scopeName) {
		return path("/pools/default/buckets/{bucketName}/scopes/{scopeName}",
				mapOf("bucketName", bucketName, "scopeName", scopeName));
	}

	private static CoreHttpPath pathForCollections(String bucketName, String scopeName) {
		return path("/pools/default/buckets/{bucketName}/scopes/{scopeName}/collections",
				mapOf("bucketName", bucketName, "scopeName", scopeName));
	}

	private static CoreHttpPath pathForCollection(String bucketName, String scopeName, String collectionName) {
		return path("/pools/default/buckets/{bucketName}/scopes/{scopeName}/collections/{collectionName}",
				mapOf("bucketName", bucketName, "scopeName", scopeName, "collectionName", collectionName));
	}

	/**
	 * Helper method to check for common errors and raise the right exceptions in those cases.
	 *
	 * @param scopeName (nullable)
	 * @param collectionName (nullable)
	 */
	private static Function<Throwable, CoreHttpResponse> translateErrors(String scopeName, String collectionName) {
		return t -> {
			String error = HttpStatusCodeException.httpResponseBody(t);
			ResponseStatus responseStatus = HttpStatusCodeException.couchbaseResponseStatus(t);

			if (responseStatus == ResponseStatus.NOT_FOUND) {
				if (error.contains("Not found.") || error.contains("Requested resource not found.")) {
					// This happens on pre 6.5 clusters (i.e. 5.5)
					throw FeatureNotAvailableException.collections();
				}

				if (error.matches(".*Scope.+not found.*") || error.contains("scope_not_found")) {
					throw ScopeNotFoundException.forScope(scopeName);
				}

				if (error.matches(".*Collection.+not found.*")) {
					throw CollectionNotFoundException.forCollection(collectionName);
				}
			}

			if (responseStatus == ResponseStatus.INVALID_ARGS) {
				if (error.matches(".*Scope.+already exists.*")) {
					throw ScopeExistsException.forScope(scopeName);
				}
				if (error.contains("scope_not_found")) {
					throw ScopeNotFoundException.forScope(scopeName);
				}
				if (error.matches(".*Collection.+already exists.*")) {
					throw CollectionExistsException.forCollection(collectionName);
				}
				if (error.contains("Not allowed on this version of cluster")) {
					// This happens on 6.5 if collections dev preview is not enabled
					throw FeatureNotAvailableException.collections();
				}
				if (error.contains("Not allowed on this type of bucket")) {
					// This happens on 7.0 and later under memcached buckets
					throw FeatureNotAvailableException.collectionsForMemcached();
				}

				if (error.matches(".*Collection.+not found.*") || error.contains("collection_not_found")) {
					throw CollectionNotFoundException.forCollection(collectionName);
				}
			}

			if (error.contains("Method Not Allowed")) {
				// Happens on pre 6.5 clusters on i.e. dropScope
				throw FeatureNotAvailableException.collections();
			}

			throw propagate(t);
		};
	}

	private static RuntimeException unsupported() {
		return new UnsupportedOperationException("Not currently supported");
	}
}
