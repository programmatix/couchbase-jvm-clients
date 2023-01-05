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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.deps.com.google.common.util.concurrent.FutureCallback;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.Futures;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import reactor.core.publisher.Mono;

import java.util.concurrent.Executor;

public class ListenableFutureUtil {
  private ListenableFutureUtil() {}

  public static <V> Mono<V> toMono(ListenableFuture<V> input, Executor executor) {
    return Mono.create(sink -> {
      Futures.addCallback(input, new FutureCallback<V>() {
        @Override
        public void onSuccess(V result) {
          sink.success(result);
        }

        @Override
        public void onFailure(Throwable t) {
          if (t instanceof RuntimeException) {
            sink.error((RuntimeException) t);
          } else {
            sink.error(new RuntimeException(t));
          }
        }
      }, executor);
    });
  }
}
