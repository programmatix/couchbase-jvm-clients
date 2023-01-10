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
    return Timestamp.newBuilder().setSeconds(expiry.encode()).build();
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
