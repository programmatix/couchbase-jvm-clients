/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.kv.CoreRangeScanItem;
import com.couchbase.client.core.kv.RangeScanOrchestrator;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetMetaRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.ReplaceRequest;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.kv.ReplicaHelper;
import com.couchbase.client.core.util.BucketConfigUtil;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.Expiry;
import com.couchbase.client.java.kv.GetAllReplicasOptions;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetAnyReplicaOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetReplicaResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupInAccessor;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInAccessor;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RangeScan;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.SamplingScan;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.ScanType;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_EXISTS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_ALL_REPLICAS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_LOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_ANY_REPLICA_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_INSERT_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_LOOKUP_IN_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_MUTATE_IN_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_REMOVE_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_REPLACE_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_UNLOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_UPSERT_OPTIONS;

/**
 * The {@link AsyncCollection} provides basic asynchronous access to all collection APIs.
 *
 * <p>This type of API provides asynchronous support through the concurrency mechanisms
 * that ship with Java 8 and later, notably the async {@link CompletableFuture}. It is the
 * async mechanism with the lowest overhead (best performance) but also comes with less
 * bells and whistles as the {@link ReactiveCollection} for example.</p>
 *
 * <p>Most of the time we recommend using the {@link ReactiveCollection} unless you need the
 * last drop of performance or if you are implementing higher level primitives on top of this
 * one.</p>
 *
 * @since 3.0.0
 */
public class AsyncCollection {

  /**
   * Holds the underlying core which is used to dispatch operations.
   */
  private final Core core;

  /**
   * Holds the core context of the attached core.
   */
  private final CoreContext coreContext;

  /**
   * Holds the environment for this collection.
   */
  private final ClusterEnvironment environment;

  /**
   * The name of the collection.
   */
  private final String name;

  /**
   * The name of the bucket.
   */
  private final String bucket;

  /**
   * The name of the associated scope.
   */
  private final String scopeName;

  /**
   * Holds the async binary collection object.
   */
  private final AsyncBinaryCollection asyncBinaryCollection;

  /**
   * Stores information about the collection.
   */
  private final CollectionIdentifier collectionIdentifier;

  /**
   * Holds the orchestrator for range scans.
   */
  private final RangeScanOrchestrator rangeScanOrchestrator;

  /**
   * Strategy for performing KV operations.
   */
  final CoreKvOps kvOps;

  /**
   * Creates a new {@link AsyncCollection}.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the scope associated.
   * @param core the core into which ops are dispatched.
   * @param environment the surrounding environment for config options.
   */
  AsyncCollection(final String name, final String scopeName, final String bucket,
                  final Core core, final ClusterEnvironment environment) {
    this.name = name;
    this.scopeName = scopeName;
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
    this.bucket = bucket;
    this.collectionIdentifier = new CollectionIdentifier(bucket, Optional.of(scopeName), Optional.of(name));
    this.asyncBinaryCollection = new AsyncBinaryCollection(core, environment, collectionIdentifier);
    this.rangeScanOrchestrator = new RangeScanOrchestrator(core, collectionIdentifier);
    this.kvOps = core.kvOps(CoreKeyspace.from(collectionIdentifier));
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  @Stability.Volatile
  public Core core() {
    return core;
  }

  @Stability.Internal
  RangeScanOrchestrator rangeScanOrchestrator() {
    return rangeScanOrchestrator;
  }

  /**
   * Provides access to the underlying {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return environment;
  }

  /**
   * The name of the collection in use.
   *
   * @return the name of the collection.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the name of the bucket associated with this collection.
   */
  public String bucketName() {
    return bucket;
  }

  /**
   * Returns the name of the scope associated with this collection.
   */
  public String scopeName() {
    return scopeName;
  }

  /**
   * Provides access to the binary APIs, not used for JSON documents.
   *
   * @return the {@link AsyncBinaryCollection}.
   */
  public AsyncBinaryCollection binary() {
    return asyncBinaryCollection;
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} indicating once loaded or failed.
   */
  public CompletableFuture<GetResult> get(final String id) {
    return get(id, DEFAULT_GET_OPTIONS);
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> get(final String id, final GetOptions options) {
    notNull(options, "GetOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    final GetOptions.Built opts = options.build();

    final Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return kvOps.getAsync(opts, id, opts.projections(), opts.withExpiry())
      .thenApply(coreGetResult -> new GetResult(coreGetResult, transcoder));
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndLock(final String id, Duration lockTime) {
    return getAndLock(id, lockTime, DEFAULT_GET_AND_LOCK_OPTIONS);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndLock(final String id, final Duration lockTime,
                                                 final GetAndLockOptions options) {
    notNull(options, "GetAndLockOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    GetAndLockOptions.Built opts = options.build();
    final Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return kvOps.getAndLockAsync(opts, id, lockTime)
      .thenApply(coreGetResult -> new GetResult(coreGetResult, transcoder));
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndTouch(final String id, final Duration expiry) {
    return getAndTouch(id, expiry, DEFAULT_GET_AND_TOUCH_OPTIONS);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with custom
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndTouch(final String id, final Duration expiry,
                                                  final GetAndTouchOptions options) {
    notNull(expiry, "Expiry", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(options, "GetAndTouchOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    GetAndTouchOptions.Built opts = options.build();
    final Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return kvOps.getAndTouchAsync(opts, id, Expiry.relative(expiry).encode())
      .thenApply(coreGetResult -> new GetResult(coreGetResult, transcoder));
  }

  /**
   * Reads from all available replicas and the active node and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  public CompletableFuture<List<CompletableFuture<GetReplicaResult>>> getAllReplicas(final String id) {
    return getAllReplicas(id, DEFAULT_GET_ALL_REPLICAS_OPTIONS);
  }

  /**
   * Reads from replicas or the active node based on the options and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  public CompletableFuture<List<CompletableFuture<GetReplicaResult>>> getAllReplicas(final String id,
                                                                                     final GetAllReplicasOptions options) {
    notNull(options, "GetAllReplicasOptions");
    GetAllReplicasOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();

    return ReplicaHelper.getAllReplicasAsync(
        core,
        collectionIdentifier,
        id,
        opts.timeout().orElse(environment.timeoutConfig().kvTimeout()),
        opts.retryStrategy().orElse(environment().retryStrategy()),
        opts.clientContext(),
        opts.parentSpan().orElse(null),
        response -> GetReplicaResult.from(response, transcoder));
  }

  /**
   * Reads all available replicas, and returns the first found.
   *
   * @param id the document id.
   * @return a future containing the first available replica.
   */
  public CompletableFuture<GetReplicaResult> getAnyReplica(final String id) {
    return getAnyReplica(id, DEFAULT_GET_ANY_REPLICA_OPTIONS);
  }

  /**
   * Reads all available replicas, and returns the first found.
   *
   * @param id the document id.
   * @param options the custom options.
   * @return a future containing the first available replica.
   */
  public CompletableFuture<GetReplicaResult> getAnyReplica(final String id, final GetAnyReplicaOptions options) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(options, "GetAnyReplicaOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    GetAnyReplicaOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();

    return ReplicaHelper.getAnyReplicaAsync(
        core,
        collectionIdentifier,
        id,
        opts.timeout().orElse(environment.timeoutConfig().kvTimeout()),
        opts.retryStrategy().orElse(environment().retryStrategy()),
        opts.clientContext(),
        opts.parentSpan().orElse(null),
        response -> GetReplicaResult.from(response, transcoder));
  }

  /**
   * Checks if the given document ID exists on the active partition with default options.
   *
   * @param id the document ID
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<ExistsResult> exists(final String id) {
    return exists(id, DEFAULT_EXISTS_OPTIONS);
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document ID
   * @param options to modify the default behavior
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<ExistsResult> exists(final String id, final ExistsOptions options) {
    ExistsOptions.Built opts = notNull(options, "options").build();
    return kvOps.existsAsync(opts, id).toFuture()
      .thenApply(ExistsResult::from);
  }

  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link CompletableFuture} completing once removed or failed.
   */
  public CompletableFuture<MutationResult> remove(final String id) {
    return remove(id, DEFAULT_REMOVE_OPTIONS);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once removed or failed.
   */
  public CompletableFuture<MutationResult> remove(final String id, final RemoveOptions options) {
    notNull(options, "RemoveOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    RemoveOptions.Built opts = options.build();

    return kvOps.removeAsync(
        opts,
        id,
        opts.cas(),
        opts.toCoreDurability()
      )
      .toFuture().thenApply(MutationResult::new);
  }

  /**
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @return a {@link CompletableFuture} completing once inserted or failed.
   */
  public CompletableFuture<MutationResult> insert(final String id, Object content) {
    return insert(id, content, DEFAULT_INSERT_OPTIONS);
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link CompletableFuture} completing once inserted or failed.
   */
  public CompletableFuture<MutationResult> insert(final String id, Object content, final InsertOptions options) {
    notNull(options, "InsertOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    InsertOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return kvOps.insertAsync(
        opts,
        id,
        () -> transcoder.encode(content),
        opts.toCoreDurability(),
        opts.expiry().encode()
      )
      .toFuture().thenApply(MutationResult::new);
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content) {
    return upsert(id, content, DEFAULT_UPSERT_OPTIONS);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content, final UpsertOptions options) {
    notNull(options, "UpsertOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    UpsertOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return kvOps.upsertAsync(
        opts,
        id,
        () -> transcoder.encode(content),
        opts.toCoreDurability(),
        opts.expiry().encode(),
        opts.preserveExpiry()
      )
      .toFuture().thenApply(MutationResult::new);
  }

  /**
   * Replaces a full document which already exists with default options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @return a {@link CompletableFuture} completing once replaced or failed.
   */
  public CompletableFuture<MutationResult> replace(final String id, Object content) {
    return replace(id, content, DEFAULT_REPLACE_OPTIONS);
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link CompletableFuture} completing once replaced or failed.
   */
  public CompletableFuture<MutationResult> replace(final String id, Object content, final ReplaceOptions options) {
    notNull(options, "ReplaceOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    ReplaceOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return kvOps.replaceAsync(
        opts,
        id,
        () -> transcoder.encode(content),
        opts.cas(),
        opts.toCoreDurability(),
        opts.expiry().encode(),
        opts.preserveExpiry()
      )
      .toFuture().thenApply(MutationResult::new);
  }

  /**
   * Updates the expiry of the document with the given id with default options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @return a {@link MutationResult} once the operation completes.
   */
  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry) {
    return touch(id, expiry, DEFAULT_TOUCH_OPTIONS);
  }

  /**
   * Updates the expiry of the document with the given id with custom options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return a {@link MutationResult} once the operation completes.
   */
  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry, final TouchOptions options) {
    notNull(options, "TouchOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(expiry, "Expiry", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    TouchOptions.Built opts = options.build();
    return kvOps.touchAsync(opts, id, Expiry.relative(expiry).encode())
      .toFuture().thenApply(MutationResult::new);
  }

  /**
   * Unlocks a document if it has been locked previously, with default options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @return the future which completes once a response has been received.
   */
  public CompletableFuture<Void> unlock(final String id, final long cas) {
    return unlock(id, cas, DEFAULT_UNLOCK_OPTIONS);
  }

  /**
   * Unlocks a document if it has been locked previously, with custom options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @return the future which completes once a response has been received.
   */
  public CompletableFuture<Void> unlock(final String id, final long cas, final UnlockOptions options) {
    notNull(options, "UnlockOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    UnlockOptions.Built opts = options.build();
    return kvOps.unlockAsync(opts, id, cas).toFuture();
  }

  /**
   * Performs lookups to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public CompletableFuture<LookupInResult> lookupIn(final String id, final List<LookupInSpec> specs) {
    return lookupIn(id, specs, DEFAULT_LOOKUP_IN_OPTIONS);
  }

  /**
   * Performs lookups to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param options custom options to modify the lookup options.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public CompletableFuture<LookupInResult> lookupIn(final String id, final List<LookupInSpec> specs,
                                                    final LookupInOptions options) {
    notNull(options, "LookupInOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    LookupInOptions.Built opts = options.build();
    final JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return LookupInAccessor.lookupInAccessor(core, lookupInRequest(id, specs, opts), serializer);
  }

  /**
   * Helper method to create the underlying lookup subdoc request.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param opts custom options to modify the lookup options.
   * @return the subdoc lookup request.
   */
  SubdocGetRequest lookupInRequest(final String id, final List<LookupInSpec> specs, final LookupInOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNullOrEmpty(specs, "LookupInSpecs", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    ArrayList<SubdocGetRequest.Command> commands = new ArrayList<>(specs.size());

    for (int i = 0; i < specs.size(); i ++) {
      LookupInSpec spec = specs.get(i);
      commands.add(spec.export(i));
    }

    // xattrs come first
    commands.sort(Comparator.comparing(v -> !v.xattr()));

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    byte flags = 0;
    if (opts.accessDeleted()) {
      flags |= SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED;
    }

    RequestSpan span = environment.requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN, opts.parentSpan().orElse(null));
    SubdocGetRequest request = new SubdocGetRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
      flags, commands, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutateInResult> mutateIn(final String id,
                                                    final List<MutateInSpec> specs) {
    return mutateIn(id, specs, DEFAULT_MUTATE_IN_OPTIONS);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutateInResult> mutateIn(final String id,
                                                    final List<MutateInSpec> specs,
                                                    final MutateInOptions options) {
    notNull(options, "MutateInOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    MutateInOptions.Built opts = options.build();
    Duration timeout = decideKvTimeout(opts, environment.timeoutConfig());
    return mutateInRequest(id, specs, opts, timeout)
            .thenCompose(request -> MutateInAccessor.mutateIn(
                      core,
                      request,
                      id,
                      opts.persistTo(),
                      opts.replicateTo(),
                      opts.storeSemantics() == StoreSemantics.INSERT,
                      environment.jsonSerializer()
              ));
  }


  /**
   * Returns a stream of {@link ScanResult ScanResults} performing a Key-Value range scan with default options.
   *
   * @param scanType the type or range scan to perform.
   * @return a CompletableFuture of a list of {@link ScanResult ScanResults} (potentially empty).
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Stability.Volatile
  public CompletableFuture<List<ScanResult>> scan(final ScanType scanType) {
    return scan(scanType, ScanOptions.scanOptions());
  }

  /**
   * Returns a stream of {@link ScanResult ScanResults} performing a Key-Value range scan with custom options.
   *
   * @param scanType the type or range scan to perform.
   * @param options a {@link ScanOptions} to customize the behavior of the scan operation.
   * @return a CompletableFuture of a list of {@link ScanResult ScanResults} (potentially empty).
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Stability.Volatile
  public CompletableFuture<List<ScanResult>> scan(final ScanType scanType, final ScanOptions options) {
    return scanRequest(scanType, options).collectList().toFuture();
  }

  /**
   * Internal helper method to create and run the KV range scan request.
   *
   * @param scanType the type or range scan to perform.
   * @param options a {@link ScanOptions} to customize the behavior of the scan operation.
   * @return the flux stream of converted scan results.
   */
  @SuppressWarnings("resource")
  Flux<ScanResult> scanRequest(final ScanType scanType, final ScanOptions options) {
    notNull(scanType, "ScanType");
    notNull(options, "Options");

    ScanOptions.Built opts = options.build();
    Duration timeout = opts.timeout().orElse(environment().timeoutConfig().kvScanTimeout());

    Map<Short, MutationToken> consistencyTokens = opts.consistentWith().map(ms -> {
      Map<Short, MutationToken> tokens = new HashMap<>();
      for (MutationToken mt : ms) {
        tokens.put(mt.partitionID(), mt);
      }
      return tokens;
    }).orElse(Collections.emptyMap());

    Flux<CoreRangeScanItem> coreScanStream;

    if (scanType instanceof RangeScan) {
      RangeScan rs = (RangeScan) scanType;
      coreScanStream = rangeScanOrchestrator.rangeScan(rs.from().id(), rs.from().exclusive(), rs.to().id(),
        rs.to().exclusive(), timeout, opts.batchItemLimit(), opts.batchByteLimit(), opts.idsOnly(),
        opts.sort().intoCore(), opts.parentSpan(), consistencyTokens
      );
    } else if (scanType instanceof SamplingScan) {
      SamplingScan ss = (SamplingScan) scanType;
      coreScanStream = rangeScanOrchestrator.samplingScan(ss.limit(), ss.seed(), timeout, opts.batchItemLimit(),
        opts.batchByteLimit(), opts.idsOnly(), opts.sort().intoCore(), opts.parentSpan(), consistencyTokens
      );
    } else {
      return Flux.error(InvalidArgumentException.fromMessage("Unsupported ScanType: " + scanType));
    }

    final Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();

    return coreScanStream.map(item -> new ScanResult(opts.idsOnly(), item.key(), item.value(), item.flags(),
      item.cas(), Optional.ofNullable(item.expiry()), transcoder)
    );
  }

  /**
   * Helper method to create the underlying subdoc mutate request.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param opts custom options to modify the mutation options.
   * @return the subdoc mutate request.
   */
  CompletableFuture<SubdocMutateRequest> mutateInRequest(final String id, final List<MutateInSpec> specs,
                                                         final MutateInOptions.Built opts, final Duration timeout) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNullOrEmpty(specs, "MutateInSpecs", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    if (specs.isEmpty()) {
      throw SubdocMutateRequest.errIfNoCommands(ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    } else if (specs.size() > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      throw SubdocMutateRequest.errIfTooManyCommands(ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    }

    final boolean requiresBucketConfig = opts.createAsDeleted() || opts.storeSemantics() == StoreSemantics.REVIVE ;
    CompletableFuture<BucketConfig> bucketConfigFuture;

    if (requiresBucketConfig) {
      bucketConfigFuture = BucketConfigUtil.waitForBucketConfig(core, bucketName(), timeout).toFuture();
    }
    else {
      // Nothing will be using the bucket config so just provide null
      bucketConfigFuture = CompletableFuture.completedFuture(null);
    }

    return bucketConfigFuture.thenCompose(bucketConfig -> {
        RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
        JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();

        final RequestSpan span = environment
          .requestTracer()
          .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN, opts.parentSpan().orElse(null));

        ArrayList<SubdocMutateRequest.Command> commands = new ArrayList<>(specs.size());

      final RequestSpan encodeSpan = CbTracing.newSpan(coreContext, TracingIdentifiers.SPAN_REQUEST_ENCODING, span);
      long start = System.nanoTime();

      try {
          for (int i = 0; i < specs.size(); i++) {
            MutateInSpec spec = specs.get(i);
            commands.add(spec.encode(serializer, i));
          }
        } finally {
          encodeSpan.end();
        }
      long end = System.nanoTime();

      // xattrs come first
      commands.sort(Comparator.comparing(v -> !v.xattr()));

      long expiry = opts.expiry().encode();
      SubdocMutateRequest request = new SubdocMutateRequest(timeout, coreContext, collectionIdentifier, bucketConfig, retryStrategy, id,
          opts.storeSemantics() == StoreSemantics.INSERT, opts.storeSemantics() == StoreSemantics.UPSERT,
              opts.storeSemantics() == StoreSemantics.REVIVE,
          opts.accessDeleted(), opts.createAsDeleted(), commands, expiry, opts.preserveExpiry(), opts.cas(),
          opts.durabilityLevel(), span
        );
        request.context()
          .clientContext(opts.clientContext())
          .encodeLatency(end - start);
        final CompletableFuture<SubdocMutateRequest> future = new CompletableFuture<>();
        future.complete(request);
        return future;
    });
  }

  /**
   * Helper method to decide if the user timeout, the kv timeout or the durable kv timeout should be used.
   *
   * @param opts the built opts from the command.
   * @param config the env timeout config to use if not overridden by the user.
   * @return the timeout to use for the op.
   */
  @SuppressWarnings("unchecked")
  static Duration decideKvTimeout(CommonDurabilityOptions.BuiltCommonDurabilityOptions opts, TimeoutConfig config) {
    Optional<Duration> userTimeout = opts.timeout();
    if (userTimeout.isPresent()) {
      return userTimeout.get();
    }

    boolean syncDurability = opts.durabilityLevel().isPresent() && (
      opts.durabilityLevel().get() == DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
      || opts.durabilityLevel().get() == DurabilityLevel.PERSIST_TO_MAJORITY);
    boolean pollDurability = opts.persistTo() != PersistTo.NONE;

    if (syncDurability || pollDurability) {
      return config.kvDurableTimeout();
    } else {
      return config.kvTimeout();
    }
  }

  CollectionIdentifier collectionIdentifier() {
    return collectionIdentifier;
  }

}
