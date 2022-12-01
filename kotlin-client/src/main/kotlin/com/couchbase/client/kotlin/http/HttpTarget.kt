/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.http

import com.couchbase.client.core.msg.RequestTarget
import com.couchbase.client.core.service.ServiceCoordinate

/**
 * Determines which host and port an HTTP request is dispatched to.
 * Use the companion factory methods to create new instances.
 */
public class HttpTarget internal constructor(
        serviceType: ServiceCoordinate,
) {
    internal val coreTarget = RequestTarget(serviceType, null, null)

    override fun toString(): String = coreTarget.toString()

    public companion object {
        /**
         * Target the Analytics service (port 8095 by default).
         */
        public fun analytics(): HttpTarget = HttpTarget(ServiceCoordinate.ANALYTICS)

        /**
         * Target the Backup service (port 8097 by default).
         */
        public fun backup(): HttpTarget = HttpTarget(ServiceCoordinate.BACKUP)

        /**
         * Target the Eventing service (port 8096 by default).
         */
        public fun eventing(): HttpTarget = HttpTarget(ServiceCoordinate.EVENTING)

        /**
         * Target the Cluster Management service (port 8091 by default).
         */
        public fun manager(): HttpTarget = HttpTarget(ServiceCoordinate.MANAGER)

        /**
         * Target the N1QL Query service (port 8093 by default).
         */
        public fun query(): HttpTarget = HttpTarget(ServiceCoordinate.QUERY)

        /**
         * Target the Full-Text Search service (port 8094 by default).
         */
        public fun search(): HttpTarget = HttpTarget(ServiceCoordinate.SEARCH)
    }
}
