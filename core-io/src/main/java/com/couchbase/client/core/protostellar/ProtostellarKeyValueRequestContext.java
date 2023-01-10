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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Map;

@Stability.Internal
public class ProtostellarKeyValueRequestContext extends ProtostellarRequestContext {
  private final String id;
  private final CollectionIdentifier collectionIdentifier;


  public ProtostellarKeyValueRequestContext(Core core,
                                            ServiceType serviceType,
                                            String requestName,
                                            Duration timeout,
                                            String id,
                                            CollectionIdentifier collectionIdentifier,
                                            boolean idempotent) {
    super(core, serviceType, requestName, timeout, idempotent);
    this.id = id;
    this.collectionIdentifier = collectionIdentifier;
  }

  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    input.put("id", id);
    input.put("bucket", collectionIdentifier.bucket());
    input.put("scope", collectionIdentifier.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE));
    input.put("collection", collectionIdentifier.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION));
  }
}
