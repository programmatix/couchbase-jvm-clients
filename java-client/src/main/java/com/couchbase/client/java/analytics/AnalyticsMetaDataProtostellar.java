package com.couchbase.client.java.analytics;

import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AnalyticsMetaDataProtostellar extends AnalyticsMetaData {
  com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse.MetaData md;

  AnalyticsMetaDataProtostellar(com.couchbase.client.protostellar.analytics.v1.AnalyticsQueryResponse.MetaData md) {
    this.md = md;
  }

  @Override
  public String requestId() {
    return md.getRequestId();
  }

  @Override
  public String clientContextId() {
    return md.getClientContextId();
  }

  @Override
  public AnalyticsStatus status() {
    return AnalyticsStatus.from(md.getStatus());
  }

  @Override
  public Optional<JsonObject> signature() {
    return Optional.of(JsonObject.fromJson(md.getSignature().toByteArray()));
  }

  @Override
  public AnalyticsMetrics metrics() {
    return null; // todo sn
  }

  @Override
  public List<AnalyticsWarning> warnings() {
    return md.getWarningsList()
      .stream()
      .map(warning -> new AnalyticsWarning(new ErrorCodeAndMessage(warning.getCode(), warning.getMessage(), false, null)))
      .collect(Collectors.toList());
  }

  @Override
  public Optional<JsonObject> plans() {
    // todo sn?
    return Optional.empty();
  }
}
