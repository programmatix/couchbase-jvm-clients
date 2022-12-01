package com.couchbase.client.java.query;

import com.couchbase.client.java.analytics.AnalyticsMetaData;
import com.couchbase.client.java.analytics.AnalyticsMetaDataProtostellar;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.util.List;
import java.util.stream.Collectors;

public class QueryResultProtostellar extends QueryResult {
  private final List<QueryResponse> responses;

  public QueryResultProtostellar(List<QueryResponse> responses, JsonSerializer serializer) {
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
  public QueryMetaData metaData() {
    return responses.stream()
      .filter(v -> v.hasMetaData())
      .map(v -> new QueryMetaDataProtostellar(v.getMetaData()))
      .findFirst()
      .get();
  }
}
