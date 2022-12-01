package com.couchbase.client.java.analytics;

import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveAnalyticsResultHttp extends ReactiveAnalyticsResult {

  private final AnalyticsResponse response;

  ReactiveAnalyticsResultHttp(final AnalyticsResponse response, final JsonSerializer serializer) {
    super(serializer);
    this.response = response;
  }

  public <T> Flux<T> rowsAs(final Class<T> target) {
    return response.rows().map(row -> serializer.deserialize(target, row.data()));
  }

  public <T> Flux<T> rowsAs(final TypeRef<T> target) {
    return response.rows().map(row -> serializer.deserialize(target, row.data()));
  }

  public Mono<AnalyticsMetaData> metaData() {
    return response.trailer().map(t -> AnalyticsMetaDataHttp.from(response.header(), t));
  }
}
