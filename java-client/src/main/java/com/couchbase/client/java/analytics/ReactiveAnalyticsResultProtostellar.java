package com.couchbase.client.java.analytics;

import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveAnalyticsResultProtostellar extends ReactiveAnalyticsResult {

  private final Flux<AnalyticsQueryResponse> responses;

  ReactiveAnalyticsResultProtostellar(Flux<AnalyticsQueryResponse> responses, final JsonSerializer serializer) {
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

  public Mono<AnalyticsMetaData> metaData() {
    return responses.takeUntil(response -> response.hasMetaData())
      .single()
      .map(response -> new AnalyticsMetaDataProtostellar(response.getMetaData()));
  }
}
