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

package com.couchbase.client.core.classic.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreLookupInMacro;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.SubDocumentField;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.projections.ProjectionsApplier;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.classic.ClassicHelper.newAsyncResponse;
import static com.couchbase.client.core.classic.ClassicHelper.setClientContext;
import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ClassicCoreKvOps implements CoreKvOps {
  private final Core core;
  private final CoreContext ctx;
  private final Duration defaultKvTimeout;
  private final RetryStrategy defaultRetryStrategy;
  private final CollectionIdentifier collectionIdentifier;
  private final CoreKeyspace keyspace;
  private final RequestTracer requestTracer;

  public ClassicCoreKvOps(Core core, CoreKeyspace keyspace) {
    this.core = requireNonNull(core);
    this.ctx = core.context();
    this.defaultKvTimeout = ctx.environment().timeoutConfig().kvTimeout();
    this.defaultRetryStrategy = ctx.environment().retryStrategy();
    this.requestTracer = ctx.environment().requestTracer();
    this.keyspace = requireNonNull(keyspace);
    this.collectionIdentifier = keyspace.toCollectionIdentifier();
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAsync(CoreCommonOptions common, String key, List<String> projections, boolean withExpiry) {
    notNullOrEmpty(key, "Document ID");

    Duration timeout = common.timeout().orElse(defaultKvTimeout);
    RetryStrategy retryStrategy = common.retryStrategy().orElse(defaultRetryStrategy);

    if (!withExpiry && projections.isEmpty()) {
      RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_GET);
      GetRequest request = new GetRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, span);
      setClientContext(common, request);

      return newAsyncResponse(
          request,
          execute(request).thenApply(it -> new CoreGetResult(
              CoreKvResponseMetadata.from(it.flexibleExtras()),
              keyspace,
              key,
              it.content(),
              it.flags(),
              it.cas(),
              null)
          )
      );
    }

    SubdocGetRequest request = getWithProjectionsOrExpiryRequest(common, key, projections, withExpiry);
    core.send(request);
    return newAsyncResponse(
        request,
        request.response()
            .thenApply(response -> {
              if (response.status().success() || response.status() == ResponseStatus.SUBDOC_FAILURE) {
                return parseGetWithProjectionsOrExpiry(key, response);
              }
              throw keyValueStatusToException(request, response);
            })
            .whenComplete((response, failure) -> markComplete(request, failure))
    );
  }

  private SubdocGetRequest getWithProjectionsOrExpiryRequest(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  ) {
    notNullOrEmpty(key, "Document ID");
    checkProjectionLimits(projections, withExpiry);

    Duration timeout = common.timeout().orElse(defaultKvTimeout);
    RetryStrategy retryStrategy = common.retryStrategy().orElse(defaultRetryStrategy);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN);
    List<SubdocGetRequest.Command> commands = new ArrayList<>(16);

    if (!projections.isEmpty()) {
      for (String projection : projections) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, projection, false, commands.size()));
      }
    } else {
      commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false, commands.size()));
    }

    if (withExpiry) {
      // xattrs must go first
      commands.add(0, new SubdocGetRequest.Command(SubdocCommandType.GET, CoreLookupInMacro.EXPIRY_TIME, true, commands.size()));

      // If we have projections, there is no need to fetch the flags
      // since only JSON is supported that implies the flags.
      // This will also "force" the transcoder on the read side to be
      // JSON aware since the flags are going to be hard-set to the
      // JSON compat flags.
      if (projections.isEmpty()) {
        commands.add(1, new SubdocGetRequest.Command(SubdocCommandType.GET, CoreLookupInMacro.FLAGS, true, commands.size()));
      }
    }

    return new SubdocGetRequest(
        timeout,
        ctx,
        collectionIdentifier,
        retryStrategy,
        key,
        (byte) 0x00,
        commands,
        span
    );
  }

  private CoreGetResult parseGetWithProjectionsOrExpiry(String key, SubdocGetResponse response) {
    if (response.error().isPresent()) {
      throw response.error().get();
    }

    long cas = response.cas();

    byte[] exptime = null;
    byte[] content = null;
    byte[] flags = null;

    for (SubDocumentField value : response.values()) {
      if (value != null) {
        if (CoreLookupInMacro.EXPIRY_TIME.equals(value.path())) {
          exptime = value.value();
        } else if (CoreLookupInMacro.FLAGS.equals(value.path())) {
          flags = value.value();
        } else if (value.path().isEmpty()) {
          content = value.value();
        }
      }
    }

    int convertedFlags = flags == null || flags.length == 0
        ? CodecFlags.JSON_COMPAT_FLAGS
        : Integer.parseInt(new String(flags, UTF_8));

    if (content == null) {
      try {
        content = ProjectionsApplier.reconstructDocument(response);
      } catch (Exception e) {
        throw new CouchbaseException("Unexpected Exception while decoding Sub-Document get", e);
      }
    }

    Optional<Instant> expiration = Optional.empty();
    if (exptime != null && exptime.length > 0) {
      long parsed = Long.parseLong(new String(exptime, UTF_8));
      if (parsed > 0) {
        expiration = Optional.of(Instant.ofEpochSecond(parsed));
      }
    }

    return new CoreGetResult(
        CoreKvResponseMetadata.from(response.flexibleExtras()),
        keyspace,
        key,
        content,
        convertedFlags,
        cas,
        expiration.orElse(null)
    );
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndLockAsync(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    notNull(lockTime, "lockTime");
    notNullOrEmpty(key, "Document ID");

    Duration timeout = common.timeout().orElse(defaultKvTimeout);
    RetryStrategy retryStrategy = common.retryStrategy().orElse(defaultRetryStrategy);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_LOCK);

    GetAndLockRequest request = new GetAndLockRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, lockTime, span);
    setClientContext(common, request);

    return newAsyncResponse(request, execute(request)
        .thenApply(it -> new CoreGetResult(
                CoreKvResponseMetadata.from(it.flexibleExtras()),
                keyspace,
                key,
                it.content(),
                it.flags(),
                it.cas(),
                null
            )
        )
    );
  }

  @Override
  public CoreAsyncResponse<CoreGetResult> getAndTouchAsync(
      CoreCommonOptions common,
      String key,
      long expiration
  ) {
    notNullOrEmpty(key, "Document ID");

    Duration timeout = common.timeout().orElse(defaultKvTimeout);
    RetryStrategy retryStrategy = common.retryStrategy().orElse(defaultRetryStrategy);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_TOUCH);

    GetAndTouchRequest request = new GetAndTouchRequest(key, timeout, ctx, collectionIdentifier, retryStrategy, expiration, span);
    setClientContext(common, request);

    return newAsyncResponse(request, execute(request)
        .thenApply(it ->
            new CoreGetResult(
                CoreKvResponseMetadata.from(it.flexibleExtras()),
                keyspace,
                key,
                it.content(),
                it.flags(),
                it.cas(),
                null
            )
        )
    );
  }

  @Override
  public CoreAsyncResponse<CoreSubdocGetResult> subdocGet(
      CoreCommonOptions common,
      String key,
      byte flags,
      List<SubdocGetRequest.Command> commands
  ) {
    notNullOrEmpty(key, "Document ID");

    Duration timeout = common.timeout().orElse(defaultKvTimeout);
    RetryStrategy retryStrategy = common.retryStrategy().orElse(defaultRetryStrategy);
    RequestSpan span = span(common, TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN);

    SubdocGetRequest request = new SubdocGetRequest(timeout, ctx, collectionIdentifier, retryStrategy, key, flags, commands, span);
    setClientContext(common, request);

    core.send(request);
    return newAsyncResponse(request, request
        .response()
        .thenApply(response -> {
          if (response.status().success() || response.status() == ResponseStatus.SUBDOC_FAILURE) {
            return new CoreSubdocGetResult(
                CoreKvResponseMetadata.from(response.flexibleExtras()),
                response.values(),
                response.cas(),
                response.error().orElse(null),
                response.isDeleted()
            );
          }
          throw keyValueStatusToException(request, response);
        })
        .whenComplete((response, failure) -> markComplete(request, failure))
    );
  }

  private <T extends BaseResponse> CompletableFuture<T> execute(KeyValueRequest<T> request) {
    core.send(request);
    return handleResponse(request);
  }

  private static <T extends BaseResponse> CompletableFuture<T> handleResponse(KeyValueRequest<T> request) {
    return request
        .response()
        .thenApply(response -> {
          if (response.status().success()) {
            return response;
          }
          throw keyValueStatusToException(request, response);
        })
        .whenComplete((response, failure) -> markComplete(request, failure));
  }

  private static void markComplete(KeyValueRequest<?> request, Throwable failure) {
    if (failure == null || failure instanceof DocumentNotFoundException) {
      request.context().logicallyComplete();
    } else {
      request.context().logicallyComplete(failure);
    }
  }

  private RequestSpan span(CoreCommonOptions common, String spanName) {
    return requestTracer.requestSpan(spanName, common.parentSpan().orElse(null));
  }
}
