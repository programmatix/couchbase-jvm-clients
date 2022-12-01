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
