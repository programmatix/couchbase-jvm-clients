package com.couchbase.client.java.analytics;

import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse;

import java.util.List;
import java.util.stream.Collectors;

public class AnalyticsResultProtostellar extends AnalyticsResult {
  private final List<AnalyticsQueryResponse> responses;

  public AnalyticsResultProtostellar(List<AnalyticsQueryResponse> responses, JsonSerializer serializer) {
    super(serializer);
    this.responses = responses;
  }

  @Override
  public <T> List<T> rowsAs(Class<T> target) {
    return responses.stream()
      .flatMap(response -> response.getRowsList().stream())
      .map(row -> serializer.deserialize(target, row.toByteArray()))
      .collect(Collectors.toList());
  }

  @Override
  public <T> List<T> rowsAs(TypeRef<T> target) {
    return responses.stream()
      .flatMap(response -> response.getRowsList().stream())
      .map(row -> serializer.deserialize(target, row.toByteArray()))
      .collect(Collectors.toList());
  }

  @Override
  public AnalyticsMetaData metaData() {
    return responses.stream()
      .filter(v -> v.hasMetaData())
      .map(v -> new AnalyticsMetaDataProtostellar(v.getMetaData()))
      .findFirst()
      .get();
  }
}
