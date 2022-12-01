package com.couchbase.client.java.query;

import com.couchbase.client.core.util.ProtostellarUtil;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.time.Duration;

public class QueryMetricsProtostellar extends QueryMetrics {
  private final QueryResponse.MetaData.Metrics metrics;

  public QueryMetricsProtostellar(QueryResponse.MetaData.Metrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public Duration elapsedTime() {
    return ProtostellarUtil.convert(metrics.getElapsedTime());
  }

  @Override
  public Duration executionTime() {
    return ProtostellarUtil.convert(metrics.getExecutionTime());
  }

  @Override
  public long sortCount() {
    return metrics.getSortCount();
  }

  @Override
  public long resultCount() {
    return metrics.getResultCount();
  }

  @Override
  public long resultSize() {
    return metrics.getResultSize();
  }

  @Override
  public long mutationCount() {
    return metrics.getMutationCount();
  }

  @Override
  public long errorCount() {
    return metrics.getErrorCount();
  }

  @Override
  public long warningCount() {
    return metrics.getWarningCount();
  }
}
