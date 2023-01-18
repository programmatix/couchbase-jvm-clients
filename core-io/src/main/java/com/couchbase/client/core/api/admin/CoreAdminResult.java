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

package com.couchbase.client.core.api.admin;

import reactor.util.annotation.Nullable;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;

@Stability.Internal
public abstract class CoreAdminResult {
  private final CoreAdminResponseMetadata meta;
  private final String bucketName;

  public CoreAdminResult(@Nullable CoreAdminResponseMetadata meta, String bucketName) {
    this.meta = meta == null ? CoreAdminResponseMetadata.NONE : meta;
    this.bucketName = bucketName;
  }

  public CoreAdminResponseMetadata meta() {
    return meta;
  }
}
