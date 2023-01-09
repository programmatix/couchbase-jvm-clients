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

import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class ReactiveAnalyticsResult {

    /**
     * The default serializer to use.
     */
    protected final JsonSerializer serializer;

    ReactiveAnalyticsResult(final JsonSerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Get a {@link Flux} which publishes the rows that were fetched by the query which are then decoded to
     * {@link JsonObject}
     *
     * @return {@link Flux}
     */
    public Flux<JsonObject> rowsAsObject() {
        return rowsAs(JsonObject.class);
    }

    abstract public <T> Flux<T> rowsAs(final Class<T> target);

    abstract public <T> Flux<T> rowsAs(final TypeRef<T> target);

    abstract public Mono<AnalyticsMetaData> metaData();
}

