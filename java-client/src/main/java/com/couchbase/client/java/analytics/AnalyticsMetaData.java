/*
 * Copyright (c) 2018 Couchbase, Inc.
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

/**
 * Holds associated metadata returned by the server for the performed analytics request.
 */
public abstract class AnalyticsMetaData {
    /**
     * Get the request identifier of the query request
     *
     * @return request identifier
     */
    public abstract String requestId();

    /**
     * Get the client context identifier as set by the client
     *
     * @return client context identifier
     */
    public abstract String clientContextId();

    /**
     * Get the status of the response.
     *
     * @return the status of the response.
     */
    public abstract AnalyticsStatus status();

    /**
     * Get the signature as the target type, if present.
     *
     * @return the decoded signature if present.
     */
    public abstract Optional<JsonObject> signature();

    /**
     * Get the associated metrics for the response.
     *
     * @return the metrics for the analytics response.
     */
    public abstract AnalyticsMetrics metrics();

    /**
     * Returns warnings if present.
     *
     * @return warnings, if present.
     */
    public abstract List<AnalyticsWarning> warnings();

    /**
     * Returns plan information if present.
     *
     * @return plan information if present.
     */
    @Stability.Internal
    public abstract Optional<JsonObject> plans();
}

