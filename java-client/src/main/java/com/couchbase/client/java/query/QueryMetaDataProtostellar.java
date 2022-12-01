package com.couchbase.client.java.query;

import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.java.analytics.AnalyticsWarning;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryMetaDataProtostellar extends QueryMetaData {
  private final QueryResponse.MetaData metaData;

  public QueryMetaDataProtostellar(QueryResponse.MetaData metaData) {
    this.metaData = metaData;
  }

  @Override
  public String requestId() {
    return metaData.getRequestId();
  }

  @Override
  public String clientContextId() {
    return metaData.getClientContextId();
  }

  @Override
  public QueryStatus status() {
    switch (metaData.getStatus()) {
      case RUNNING:
        return QueryStatus.RUNNING;
      case SUCCESS:
        return QueryStatus.SUCCESS;
      case ERRORS:
        return QueryStatus.ERRORS;
      case COMPLETED:
        return QueryStatus.COMPLETED;
      case STOPPED:
        return QueryStatus.STOPPED;
      case TIMEOUT:
        return QueryStatus.TIMEOUT;
      case CLOSED:
        return QueryStatus.CLOSED;
      case FATAL:
        return QueryStatus.FATAL;
      case ABORTED:
        return QueryStatus.ABORTED;
    }
    return QueryStatus.UNKNOWN;
  }

  @Override
  public Optional<JsonObject> signature() {
    return Optional.of(JsonObject.fromJson(metaData.getSignature().toByteArray()));
  }

  @Override
  public Optional<JsonObject> profile() {
    if (metaData.hasProfile()) {
      return Optional.of(JsonObject.fromJson(metaData.getProfile().toByteArray()));
    }
    return Optional.empty();
  }

  @Override
  public Optional<QueryMetrics> metrics() {
    if (metaData.hasMetrics()) {
      return Optional.of(new QueryMetricsProtostellar(metaData.getMetrics()));
    }
    return Optional.empty();
  }

  @Override
  public List<QueryWarning> warnings() {
    return metaData.getWarningsList()
      .stream()
      .map(warning -> new QueryWarning(new ErrorCodeAndMessage(warning.getCode(), warning.getMessage(), false, null)))
      .collect(Collectors.toList());
  }
}
