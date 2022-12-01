package com.couchbase.client.java.analytics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AnalyticsMetaDataHttp extends AnalyticsMetaData {

  private final AnalyticsChunkHeader header;
  private final AnalyticsChunkTrailer trailer;

  private AnalyticsMetaDataHttp(final AnalyticsChunkHeader header, final AnalyticsChunkTrailer trailer) {
    this.header = header;
    this.trailer = trailer;
  }

  @Stability.Internal
  static AnalyticsMetaData from(final AnalyticsChunkHeader header, final AnalyticsChunkTrailer trailer) {
    return new AnalyticsMetaDataHttp(header, trailer);
  }

  /**
   * Get the request identifier of the query request
   *
   * @return request identifier
   */
  public String requestId() {
    return header.requestId();
  }

  /**
   * Get the client context identifier as set by the client
   *
   * @return client context identifier
   */
  public String clientContextId() {
    return header.clientContextId().orElse("");
  }

  /**
   * Get the status of the response.
   *
   * @return the status of the response.
   */
  public AnalyticsStatus status() {
    return AnalyticsStatus.from(trailer.status());
  }

  /**
   * Get the signature as the target type, if present.
   *
   * @return the decoded signature if present.
   */
  public Optional<JsonObject> signature() {
    return header.signature().map(bytes -> {
      try {
        return JacksonTransformers.MAPPER.readValue(bytes, JsonObject.class);
      } catch (IOException e) {
        throw new DecodingFailureException("Could not decode Analytics signature", e);
      }
    });
  }

  /**
   * Get the associated metrics for the response.
   *
   * @return the metrics for the analytics response.
   */
  public AnalyticsMetrics metrics() {
    return new AnalyticsMetrics(trailer.metrics());
  }

  /**
   * Returns warnings if present.
   *
   * @return warnings, if present.
   */
  public List<AnalyticsWarning> warnings() {
    return this.trailer.warnings().map(warnings ->
      ErrorCodeAndMessage.fromJsonArray(warnings).stream().map(AnalyticsWarning::new).collect(Collectors.toList())
    ).orElse(Collections.emptyList());
  }

  /**
   * Returns plan information if present.
   *
   * @return plan information if present.
   */
  @Stability.Internal
  public Optional<JsonObject> plans() {
    return trailer.plans().map(bytes -> {
      try {
        return JacksonTransformers.MAPPER.readValue(bytes, JsonObject.class);
      } catch (IOException e) {
        throw new DecodingFailureException("Could not decode Analytics plan information", e);
      }
    });
  }

  @Override
  public String toString() {
    return "AnalyticsMetaData{" +
      "header=" + header +
      ", trailer=" + trailer +
      '}';
  }
}
