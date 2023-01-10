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
package com.couchbase.client.scala.protostellar

import com.couchbase.client.core.Core
import com.couchbase.client.core.deps.com.google.protobuf.Timestamp
import com.couchbase.client.core.deps.io.grpc.Deadline
import com.couchbase.client.core.msg.kv.{DurabilityLevel, MutationToken}
import com.couchbase.client.protostellar.kv.v1
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv.MutationResult

import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

object ProtostellarUtil {
  def durabilityToCore(durability: Durability): Optional[DurabilityLevel] = {
    durability match {
      case Durability.Disabled => Optional.empty()
      case Durability.ClientVerified(_, _) => Optional.empty()
      case Durability.Majority => Optional.of(DurabilityLevel.MAJORITY)
      case Durability.MajorityAndPersistToActive => Optional.of(DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE)
      case Durability.PersistToMajority => Optional.of(DurabilityLevel.PERSIST_TO_MAJORITY)
    }
  }

  def convert(durability: Durability): v1.DurabilityLevel = {
    durability match {
      case Durability.Majority => v1.DurabilityLevel.MAJORITY
      case Durability.MajorityAndPersistToActive => v1.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
      case Durability.PersistToMajority => v1.DurabilityLevel.PERSIST_TO_MAJORITY
    }
    throw new IllegalArgumentException("Unsupported durability type " + durability)
  }

  def convertExpiry(expiryEpochTimeSecs: Long): Timestamp = {
    // todo snbrett expiry is going to change anyway
    Timestamp.newBuilder()
            .setSeconds(expiryEpochTimeSecs)
            .build();
  }

  def convertKvDurableTimeout(customTimeout: Duration, dl: Durability, core: Core): Deadline = {
    if (customTimeout != Duration.MinusInf) Deadline.after(customTimeout.toMillis, TimeUnit.MILLISECONDS)
    else if (dl != Durability.Disabled) Deadline.after(core.context.environment.timeoutConfig.kvDurableTimeout.toMillis, TimeUnit.MILLISECONDS)
    else Deadline.after(core.context.environment.timeoutConfig.kvTimeout.toMillis, TimeUnit.MILLISECONDS)
  }

  def convertKvTimeout(customTimeout: Duration, core: Core): Deadline = {
    if (customTimeout != Duration.MinusInf) Deadline.after(customTimeout.toMillis, TimeUnit.MILLISECONDS)
    else Deadline.after(core.context.environment.timeoutConfig.kvTimeout.toMillis, TimeUnit.MILLISECONDS)
  }

  def convertMutationResult(cas: Long, mutToken: Option[com.couchbase.client.protostellar.kv.v1.MutationToken]): MutationResult = {
    val mutationToken = mutToken.map(mt => new MutationToken(mt.getVbucketId.toShort, mt.getVbucketId, mt.getSeqNo, mt.getBucketName))
    new MutationResult(cas, mutationToken)
  }

}
