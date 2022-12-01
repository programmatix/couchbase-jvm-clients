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
