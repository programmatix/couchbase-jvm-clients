/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Allows customizing how query indexes are loaded.
 */
public class GetAllQueryIndexesOptions extends CommonOptions<GetAllQueryIndexesOptions> {

  private String scopeName;
  private String collectionName;

  private GetAllQueryIndexesOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static GetAllQueryIndexesOptions getAllQueryIndexesOptions() {
    return new GetAllQueryIndexesOptions();
  }

  /**
   * Sets the scope name for this query management operation.
   * <p>
   * If the scope name is set but the {@link #collectionName(String)} (String)} is not, then all indexes within
   * a scope (for all the collections inside) will be returned.
   *
   * @param scopeName the name of the scope.
   * @return this options class for chaining purposes.
   */
  public GetAllQueryIndexesOptions scopeName(final String scopeName) {
    this.scopeName = notNullOrEmpty(scopeName, "ScopeName");
    return this;
  }

  /**
   * Sets the collection name for this query management operation.
   * <p>
   * Please note that if the collection name is set, the {@link #scopeName(String)} must also be set.
   *
   * @param collectionName the name of the collection.
   * @return this options class for chaining purposes.
   */
  public GetAllQueryIndexesOptions collectionName(final String collectionName) {
    this.collectionName = notNullOrEmpty(collectionName, "CollectionName");
    return this;
  }

  @Stability.Internal
  public Built build() {
    if (collectionName != null && scopeName == null) {
      throw InvalidArgumentException.fromMessage("If a collectionName is provided, a scopeName must also be provided");
    }
    return new Built();
  }

  public class Built extends BuiltCommonOptions {
    Built() { }

    public Optional<String> scopeName() {
      return Optional.ofNullable(scopeName);
    }

    public Optional<String> collectionName() {
      return Optional.ofNullable(collectionName);
    }
  }

}
