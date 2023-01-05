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

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.util.CbCollections.listOf;

@Stability.Internal
public final class CoreSubdocGetResult extends CoreKvResult {
  private final List<SubDocumentField> values;
  private final long cas;
  @Nullable private final CouchbaseException error;
  private final boolean isDeleted;

  public CoreSubdocGetResult(
      @Nullable CoreKvResponseMetadata meta,
      SubDocumentField[] values,
      long cas,
      @Nullable CouchbaseException error,
      boolean isDeleted
  ) {
    super(meta);
    this.values = listOf(values);
    this.cas = cas;
    this.error = error;
    this.isDeleted = isDeleted;
  }

  public List<SubDocumentField> values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  public Optional<CouchbaseException> error() {
    return Optional.ofNullable(error);
  }

  public boolean isDeleted() {
    return isDeleted;
  }
}
