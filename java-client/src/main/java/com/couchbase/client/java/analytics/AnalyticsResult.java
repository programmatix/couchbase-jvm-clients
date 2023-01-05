/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The result of an analytics query, including rows and associated metadata.
 *
 * @since 3.0.0
 */
public abstract class AnalyticsResult {
  /**
   * The default serializer to use.
   */
  protected final JsonSerializer serializer;

  /**
   * Creates a new AnalyticsResult.
   */
  AnalyticsResult(final JsonSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Returns all rows, converted into instances of the target class.
   *
   * @param target the target class to deserialize into.
   * @param <T> the generic type to cast the rows into.
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   * @return the Rows as a list of the generic target type.
   */
  public abstract <T> List<T> rowsAs(final Class<T> target);

  /**
   * Returns all rows, converted into instances of the target type.
   *
   * @param target the target type to deserialize into.
   * @param <T> the generic type to cast the rows into.
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   * @return the Rows as a list of the generic target type.
   */
  public abstract <T> List<T> rowsAs(final TypeRef<T> target);

  /**
   * Returns all rows, converted into {@link JsonObject}s.
   *
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   * @return the Rows as a list of JsonObjects.
   */
  public List<JsonObject> rowsAsObject() {
    return rowsAs(JsonObject.class);
  }

  /**
   * Returns the {@link AnalyticsMetaData} giving access to the additional metadata associated with this analytics
   * query.
   *
   * @return the analytics metadata.
   */
  public abstract AnalyticsMetaData metaData();
}

