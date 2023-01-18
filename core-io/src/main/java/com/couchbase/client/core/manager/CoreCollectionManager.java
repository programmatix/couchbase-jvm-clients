/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.manager;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.admin.CoreAdminOps;
import com.couchbase.client.core.classic.kv.ClassicCoreAdminOps;
import com.couchbase.client.core.config.CollectionsManifest;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.protostellar.admin.ProtostellarCoreAdminOps;

@Stability.Internal
public class CoreCollectionManager {
	private final CoreAdminOps ops;

	public CoreCollectionManager(Core core, String bucketName) {
		this.ops = core.isProtostellar() ? new ProtostellarCoreAdminOps(core, bucketName)
				: new ClassicCoreAdminOps(core, bucketName);
	}

	public CompletableFuture<Void> createCollection(String scopeName, String collectionName, Duration maxTTL,
			CoreCommonOptions options) {
		return ops.createCollectionAsync(scopeName, collectionName, maxTTL, options).toFuture();
	}

	public CompletableFuture<Void> createScope(String scopeName, CoreCommonOptions options) {
		return ops.createScopeAsync(scopeName, options).toFuture();
	}

	public CompletableFuture<Void> dropCollection(String scopeName, String collectionName, CoreCommonOptions options) {
		return ops.dropCollectionAsync(scopeName, collectionName, options).toFuture();
	}

	public CompletableFuture<Void> dropScope(String scopeName, CoreCommonOptions options) {
		return ops.dropScopeAsync(scopeName, options).toFuture();
	}

	public CompletableFuture<CollectionsManifest> getAllScopes(CoreCommonOptions options) {
		return ops.getAllScopesAsync(options).toFuture();
	}

}
