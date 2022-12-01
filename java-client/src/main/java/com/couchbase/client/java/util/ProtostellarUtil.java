package com.couchbase.client.java.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.deps.com.google.protobuf.Timestamp;
import com.couchbase.client.core.deps.io.grpc.Deadline;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.java.kv.Expiry;
import com.couchbase.client.java.kv.MutationResult;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ProtostellarUtil {
  private ProtostellarUtil() {
  }

  public static Timestamp convertExpiry(Expiry expiry) {
    // todo snbrett expiry is going to change anyway
    return Timestamp.newBuilder()
//        .setSeconds()
      .build();
//    expiry.encode()
  }

  public static Optional<Instant> convertExpiry(boolean hasExpiry, Timestamp expiry) {
    if (hasExpiry) {
      return Optional.of(Instant.ofEpochSecond(expiry.getSeconds()));
    }
    return Optional.empty();
  }

  public static MutationResult convertMutationResult(long cas, @Nullable com.couchbase.client.protostellar.kv.v1.MutationToken mt) {
    Optional<MutationToken> mutationToken;
    if (mt != null) {
      mutationToken = Optional.of(new MutationToken((short) mt.getVbucketId(), mt.getVbucketId(), mt.getSeqNo(), mt.getBucketName()));
    } else {
      mutationToken = Optional.empty();
    }
    return new MutationResult(cas, mutationToken);
  }
}
