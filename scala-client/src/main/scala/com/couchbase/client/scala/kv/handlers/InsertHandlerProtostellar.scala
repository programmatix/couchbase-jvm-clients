/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.scala.kv.handlers

import com.couchbase.client.core.Core
import com.couchbase.client.core.cnc.{CbTracing, RequestSpan, TracingIdentifiers}
import com.couchbase.client.core.deps.com.google.protobuf.ByteString
import com.couchbase.client.core.error.EncodingFailureException
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.protostellar.CoreProtostellarUtil.convertFromFlags
import com.couchbase.client.core.protostellar.{CoreInsertAccessorProtostellar, CoreProtostellarUtil, ProtostellarKeyValueRequestContext, ProtostellarRequest, ProtostellarRequestContext}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.protostellar.kv.v1.{InsertRequest, InsertResponse}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.kv.{InsertOptions, MutationResult}
import com.couchbase.client.scala.protostellar.ProtostellarUtil
import com.couchbase.client.scala.protostellar.ProtostellarUtil.convertKvDurableTimeout
import com.couchbase.client.scala.util.Validate

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.TimeoutUtil.kvTimeout

/**
  * Handles requests and responses for KV insert operations over Protostellar.
  *
  * @author Graham Pople
  */
private[scala] object InsertHandlerProtostellar {

  def blocking(core: Core, req: ProtostellarRequest[InsertRequest]): Try[MutationResult] = {
    Try(CoreInsertAccessorProtostellar.blocking(core,
      req.timeout(),
      req,
      convertResponse
    ))
  }

  private def convertResponse(response: InsertResponse): MutationResult = {
    ProtostellarUtil.convertMutationResult(response.getCas, if (response.hasMutationToken) Some(response.getMutationToken) else None)
  }

  def request[T](
      core: Core,
      id: String,
      content: T,
      timeout: Duration,
      durability: Durability,
      expiryEpochTimeSecs: Long,
      retryStrategy: RetryStrategy,
      transcoder: Transcoder,
      serializer: JsonSerializer[T],
      parentSpan: Option[RequestSpan],
      collectionIdentifier: CollectionIdentifier
  ): Try[ProtostellarRequest[InsertRequest]] = {

    val validations = for {
      _ <- Validate.notNullOrEmpty(id, "id")
      _ <- Validate.notNull(content, "content")
      _ <- Validate.notNull(timeout, "timeout")
      _ <- Validate.notNull(durability, "durability")
      _ <- Validate.notNull(retryStrategy, "retryStrategy")
      _ <- Validate.notNull(transcoder, "transcoder")
      _ <- Validate.notNull(serializer, "serializer")
      _ <- Validate.notNull(parentSpan, "parentSpan")
      _ <- Validate.notNull(collectionIdentifier, "collectionIdentifier")
    } yield null

    if (validations.isFailure) {
      validations
    } else {
      val actualTimeout: Duration = if (timeout == Duration.MinusInf) kvTimeout(core, durability) else timeout
      val out = new ProtostellarRequest[InsertRequest](core,
        // todo sn create this span correctly
        core.context.environment.requestTracer.requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_INSERT, parentSpan.orNull),
        new ProtostellarKeyValueRequestContext(core, ServiceType.KV, "insert", actualTimeout, id, collectionIdentifier))

      val encodeSpan = CbTracing.newSpan(core.context.environment.requestTracer, TracingIdentifiers.SPAN_REQUEST_ENCODING, out.span)
      val start      = System.nanoTime()

      val encoded = transcoder match {
        case x: TranscoderWithSerializer    => x.encode(content, serializer)
        case x: TranscoderWithoutSerializer => x.encode(content)
      }

      encodeSpan.end()
      out.encodeLatency(System.nanoTime - start)

      encoded match {
        case Success(en) =>

          val request: InsertRequest.Builder = InsertRequest.newBuilder
                  .setBucketName(collectionIdentifier.bucket)
                  .setScopeName(collectionIdentifier.scope.orElse(CollectionIdentifier.DEFAULT_SCOPE))
                  .setCollectionName(collectionIdentifier.collection.orElse(CollectionIdentifier.DEFAULT_COLLECTION))
                  .setKey(id)
                  .setContent(ByteString.copyFrom(en.encoded))
                  .setContentType(convertFromFlags(en.flags))

          if (expiryEpochTimeSecs != 0) {
            request.setExpiry(ProtostellarUtil.convertExpiry(expiryEpochTimeSecs))
          }
          if (durability != Durability.Disabled) {
            request.setDurabilityLevel(ProtostellarUtil.convert(durability))
          }

          out.request(request.build)

          Success(out)

        case Failure(err) =>
          Failure(new EncodingFailureException(err))
      }
    }
  }
}
