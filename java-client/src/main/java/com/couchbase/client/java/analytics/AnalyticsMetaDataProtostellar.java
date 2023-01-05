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
