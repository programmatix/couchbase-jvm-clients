/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty.manager;

import io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.endpoint.BaseEndpoint;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.error.QuotaLimitedException;
import com.couchbase.client.core.error.RateLimitedException;
import com.couchbase.client.core.error.context.ManagerErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.io.netty.NonChunkedHttpMessageHandler;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.service.ServiceType;

class NonChunkedManagerMessageHandler extends NonChunkedHttpMessageHandler {
  NonChunkedManagerMessageHandler(BaseEndpoint endpoint) {
    super(endpoint, ServiceType.MANAGER);
  }

  @Override
  protected Exception failRequestWith(HttpResponseStatus status, String content, NonChunkedHttpRequest<Response> request) {
    ManagerErrorContext errorContext = new ManagerErrorContext(
      HttpProtocol.decodeStatus(status),
      request.context(),
      status.code(),
      content
    );

    if (status.equals(HttpResponseStatus.BAD_REQUEST)) {
      if (content.contains("Magma is supported in enterprise edition only")) {
        return FeatureNotAvailableException.communityEdition("Storage Backend: Magma");
      }
      if (content.contains("Compression mode is supported in enterprise edition only")) {
        return FeatureNotAvailableException.communityEdition("Compression Mode");
      }
      if (content.contains("This http API endpoint requires enterprise edition")){
        return FeatureNotAvailableException.communityEdition("HTTP API Feature");
      }
    }

    if (status.equals(HttpResponseStatus.TOO_MANY_REQUESTS)) {
      if (content.contains("num_concurrent_requests")
        || content.contains("ingress")
        || content.contains("egress")) {
        return new RateLimitedException(errorContext);
      } else if (content.contains("Maximum number of collections has been reached for scope")) {
        return new QuotaLimitedException(errorContext);
      }
    }

    return new HttpStatusCodeException(status, content, request, errorContext);
  }

}
