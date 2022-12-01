package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.util.concurrent.TimeUnit;

@Stability.Internal
public class ProtostellarUtil {
  private ProtostellarUtil() {}

  public static com.couchbase.client.core.deps.com.google.protobuf.Duration convert(java.time.Duration input) {
    return com.couchbase.client.core.deps.com.google.protobuf.Duration.newBuilder()
      .setSeconds(input.getSeconds())
      .setNanos(input.getNano())
      .build();
  }

  public static java.time.Duration convert(com.couchbase.client.core.deps.com.google.protobuf.Duration input) {
    return java.time.Duration.ofNanos(TimeUnit.SECONDS.toNanos(input.getSeconds()) + input.getNanos());
  }
}
