/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.classic;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

import java.util.concurrent.CompletableFuture;

@Stability.Internal
public class ClassicHelper {
  private ClassicHelper() {
    throw new AssertionError("not instantiable");
  }

  public static void setClientContext(CoreCommonOptions common, Request<?> request) {
    request.context().clientContext(common.clientContext());
  }

  /**
   * Returns a new async response whose cancellation task calls {@link Request#cancel(CancellationReason)}.
   */
  public static <T, RES extends Response> CoreAsyncResponse<T> newAsyncResponse(
      Request<RES> request,
      CompletableFuture<T> future
  ) {
    return new CoreAsyncResponse<>(
        future,
        () -> request.cancel(CancellationReason.STOPPED_LISTENING)
    );
  }
}
