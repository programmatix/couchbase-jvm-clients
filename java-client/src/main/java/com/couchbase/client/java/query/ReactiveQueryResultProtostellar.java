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
package com.couchbase.client.java.query;

import com.couchbase.client.java.analytics.AnalyticsMetaData;
import com.couchbase.client.java.analytics.AnalyticsMetaDataProtostellar;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse;
import com.couchbase.client.protostellar.query.v1.QueryResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveQueryResultProtostellar extends ReactiveQueryResult {

  private final Flux<QueryResponse> responses;

  ReactiveQueryResultProtostellar(Flux<QueryResponse> responses, final JsonSerializer serializer) {
    super(serializer);
    this.responses = responses;
  }

  public <T> Flux<T> rowsAs(final Class<T> target) {
    return responses.flatMap(response -> Flux.fromIterable(response.getRowsList())
      .map(row -> serializer.deserialize(target, row.toByteArray())));
  }

  public <T> Flux<T> rowsAs(final TypeRef<T> target) {
    return responses.flatMap(response -> Flux.fromIterable(response.getRowsList())
      .map(row -> serializer.deserialize(target, row.toByteArray())));
  }

  public Mono<QueryMetaData> metaData() {
    return responses.takeUntil(response -> response.hasMetaData())
      .single()
      .map(response -> new QueryMetaDataProtostellar(response.getMetaData()));
  }
}
