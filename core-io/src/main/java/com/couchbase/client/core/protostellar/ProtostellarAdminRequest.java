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

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

@Stability.Internal
public class ProtostellarAdminRequest<TGrpcRequest> extends ProtostellarRequest<TGrpcRequest> {
	/**
	 * 
	 */
	private final String bucketName;
	private final String scopeName;
	private final String collectionName;

	public ProtostellarAdminRequest(Core core, String bucketName, String scopeName, String collectionName,
			String requestName, RequestSpan span, Duration timeout, boolean idempotent, RetryStrategy retryStrategy) {
		super(core, ServiceType.KV, requestName, span, timeout, idempotent, retryStrategy);
		this.bucketName = bucketName;
		this.scopeName = scopeName;
		this.collectionName = collectionName;
	}

	@Override
	protected Map<String, Object> serviceContext() {
		Map<String, Object> ctx = new HashMap<>();

		ctx.put("type", serviceType.ident());

		if (bucketName != null) {
			ctx.put("bucket", redactMeta(bucketName));
		}
		if (scopeName != null) {
			ctx.put("scope", redactMeta(scopeName));
		}
		if (collectionName != null) {
			ctx.put("collection", redactMeta(collectionName));
		}

		return ctx;
	}
}
