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

package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Stability.Internal
public interface CoreKvOps {

  CoreAsyncResponse<CoreGetResult> getAsync(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  );

  default CoreGetResult getBlocking(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  ) {
    return getAsync(common, key, projections, withExpiry).toBlocking();
  }

  default Mono<CoreGetResult> getReactive(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  ) {
    return Mono.defer(() -> getAsync(common, key, projections, withExpiry).toMono());
  }

  default void checkProjectionLimits(
      List<String> projections,
      boolean withExpiry
  ) {
    if (withExpiry) {
      if (projections.size() > 15) {
        throw InvalidArgumentException.fromMessage("Only a maximum of 16 fields can be "
            + "projected per request due to a server limitation (includes the expiration macro as one field).");
      }
    } else if (projections.size() > 16) {
      throw InvalidArgumentException.fromMessage("Only a maximum of 16 fields can be "
          + "projected per request due to a server limitation.");
    }
  }

  CoreAsyncResponse<CoreGetResult> getAndLockAsync(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  );

  default CoreGetResult getAndLockBlocking(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    return getAndLockAsync(common, key, lockTime).toBlocking();
  }

  default Mono<CoreGetResult> getAndLockReactive(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    return Mono.defer(() -> getAndLockAsync(common, key, lockTime).toMono());
  }

  CoreAsyncResponse<CoreGetResult> getAndTouchAsync(
      CoreCommonOptions common,
      String key,
      long expiration
  );

  default CoreGetResult getAndTouchBlocking(
      CoreCommonOptions common,
      String key,
      long expiration
  ) {
    return getAndTouchAsync(common, key, expiration).toBlocking();
  }

  default Mono<CoreGetResult> getAndTouchReactive(
      CoreCommonOptions common,
      String key,
      long expiration
  ) {
    return Mono.defer(() -> getAndTouchAsync(common, key, expiration).toMono());
  }

  CoreAsyncResponse<CoreSubdocGetResult> subdocGet(
      CoreCommonOptions common,
      String key,
      byte flags,
      List<SubdocGetRequest.Command> commands
  );
}
