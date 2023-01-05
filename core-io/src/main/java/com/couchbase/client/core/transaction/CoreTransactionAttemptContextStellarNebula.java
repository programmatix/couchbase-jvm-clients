/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.transaction;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.FutureCallback;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.Futures;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.deps.com.google.protobuf.ByteString;
import com.couchbase.client.core.deps.io.grpc.CallOptions;
import com.couchbase.client.core.deps.io.grpc.StatusRuntimeException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.error.transaction.PreviousOperationFailedException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.DocumentGetterStellarNebula;
import com.couchbase.client.core.transaction.components.OperationTypes;
import com.couchbase.client.core.transaction.components.StellarNebulaConnection;
import com.couchbase.client.core.transaction.components.StellarNebulaContext;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.ListenableFutureUtil;
import com.couchbase.client.core.transaction.util.WaitGroup;
import com.couchbase.client.protostellar.transactions.v1.TransactionBeginAttemptRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionBeginAttemptResponse;
import com.couchbase.client.protostellar.transactions.v1.TransactionCommitRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionCommitResponse;
import com.couchbase.client.protostellar.transactions.v1.TransactionInsertRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionInsertResponse;
import com.couchbase.client.protostellar.transactions.v1.TransactionRemoveRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionRemoveResponse;
import com.couchbase.client.protostellar.transactions.v1.TransactionReplaceRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionReplaceResponse;
import com.couchbase.client.protostellar.transactions.v1.TransactionRollbackRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionRollbackResponse;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;
import static com.couchbase.client.core.transaction.support.OptionsUtil.kvTimeoutMutating;

//class BookClientInterceptor implements ClientInterceptor {
//  @Override
//  public <ReqT, RespT> ClientCall<ReqT, RespT>
//  interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
//    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
//      @Override
//      public void start(Listener<RespT> responseListener, Metadata headers) {
//        headers.put(Metadata.Key.of("hooks-id", ASCII_STRING_MARSHALLER), "MY_HOST");
//        super.start(responseListener, headers);
//      }
//    };
//  }
//}

/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
 * as commit or rollback the transaction.
 */
@Stability.Internal
public class CoreTransactionAttemptContextStellarNebula extends CoreTransactionAttemptContext {

  static class AvailableAfterBeginTransaction {
    public final String attemptId;
    public final String bucketName;
    public final NodeIdentifier target;

    public AvailableAfterBeginTransaction(String attemptId, String bucketName, NodeIdentifier target) {
      this.attemptId = attemptId;
      this.bucketName = bucketName;
      this.target = target;
    }
  }

  private final StellarNebulaContext sn;

  private final boolean lockDebugging = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.debug.lock", "true"));
  private final WaitGroup kvOps = new WaitGroup(this, lockDebugging);
  private volatile @Nullable AvailableAfterBeginTransaction afterBegin = null;


  public CoreTransactionAttemptContextStellarNebula(Core core, CoreTransactionContext overall, CoreMergedTransactionConfig config,
                                                    CoreTransactionsReactive parent, Optional<SpanWrapper> parentSpan, CoreTransactionAttemptContextHooks hooks) {
    super(core, config, overall, parentSpan.orElse(null));
    this.sn = new StellarNebulaContext(StellarNebulaConnection.INSTANCE, core.context().environment().transactionsSchedulers().executor(), core);
  }

  @Override
  public String transactionId() {
    return overall.transactionId();
  }


  public Core core() {
    return core;
  }

  public Scheduler scheduler() {
    return core.context().environment().transactionsSchedulers().schedulerBlocking();
  }

  /**
   * Returns the globally unique ID of this attempt, which may be useful for debugging and logging purposes.
   */
  public String attemptId() {
    // Any function that requires this info to be thread-safe should lock.  Most functions just need it for logging.
    if (afterBegin != null) {
      return afterBegin.attemptId;
    }
    return "";
  }


  /**
   * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>bucket</code>.
   * <p>
   *
   * @param collection the Couchbase collection the document exists on
   * @param id         the document's ID
   * @return a <code>CoreTransactionGetResult</code> containing the document
   */
  public ListenableFuture<CoreTransactionGetResult> get(CollectionIdentifier collection, String id) {
    // todo sntxn sort out span lifetimes
    SpanWrapper pspan = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_GET, attemptSpan);

    LOGGER.info(attemptId(), "getting doc %s", DebugUtil.docId(collection, id));

    return doKVOperation("get " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_GET, collection, id,
            (operationId, span) -> getInternal(collection, id, span));

  }

  private <V> ListenableFuture<V> wrap(ListenableFuture<V> result, SpanWrapper span, Object dbg, @Nullable OperationTypes opType) {
    Futures.addCallback(result, new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        long elapsed = span.finish();
        logger().info(attemptId(), "success on %s in %dus", dbg, elapsed);
      }

      @Override
      public void onFailure(Throwable t) {
      }
    }, sn.executor());

    return Futures.catching(result, RuntimeException.class, (t) -> {
      if (t instanceof StatusRuntimeException) {
        t = convertTransactionOperationFailed(t, opType);
      }

      long elapsed = span.finishWithErrorStatus();
      logger().info(attemptId(), "failure on %s after %dus, with error %s", dbg, elapsed, DebugUtil.dbg(t));

      throw t;
    }, sn.executor());
  }

  private RuntimeException convertTransactionOperationFailed(RuntimeException t, OperationTypes opType) {
    try {
      String desc = ((StatusRuntimeException) t).getStatus().getDescription();

      // transaction operation failed | shouldRetry:false, shouldRollback:true, shouldRaise:0, class:2 | doc was not found: document not found
      String[] split = desc.split(" \\| ");

      if (split[0].trim().equals("transaction operation failed")) {
        String[] split2 = split[1].split(",");
        boolean shouldRetry = Boolean.parseBoolean(split2[0].split(":")[1]);
        boolean shouldRollback = Boolean.parseBoolean(split2[1].split(":")[1]);
        int shouldRaiseRaw = Integer.parseInt(split2[2].split(":")[1]);
        TransactionOperationFailedException.FinalErrorToRaise toRaise = TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED;
        switch (shouldRaiseRaw) {
          case 0:
            toRaise = TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED;
            break;
          case 1:
            toRaise = TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED;
            break;
          case 2:
            toRaise = TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS;
            break;
          case 3:
            toRaise = TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT;
            break;
        }

        String causeRaw = split[2].trim();
        RuntimeException cause;
        boolean raiseCauseDirectly = false;

        // doc was a staged remove: document not found
        if (causeRaw.startsWith("doc was not found") || causeRaw.contains("document not found")) {
          cause = new DocumentNotFoundException(null);
          if (opType == OperationTypes.GET) {
            raiseCauseDirectly = true;
          }
        }
        else if (causeRaw.startsWith("found existing document")) {
          cause = new DocumentExistsException(null);
          if (opType == OperationTypes.INSERT) {
            raiseCauseDirectly = true;
          }
        }
        else if (causeRaw.contains("illegal State")) {
          cause = new IllegalStateException();
        }
        else if (causeRaw.contains("previous operation failed")) {
          cause = new PreviousOperationFailedException();
        }
        else {
          // Redact as can contain document keys
          cause = new RuntimeException(RedactableArgument.redactUser(causeRaw).toString());
        }

        if (raiseCauseDirectly) {
          t = cause;
        }
        else {
          TransactionOperationFailedException.Builder builder = TransactionOperationFailedException.Builder.createError()
                  .cause(cause)
                  .raiseException(toRaise);

          if (!shouldRollback) {
            builder.doNotRollbackAttempt();
          }
          if (shouldRetry) {
            builder.retryTransaction();
          }

          t = operationFailed(builder.build());
        }
        logger().info(attemptId(), "converted error '%s' to %s", RedactableArgument.redactUser(desc), DebugUtil.dbg(t));
      }
    }
    catch (Throwable err) {
      logger().info(attemptId(), "failed to convert error %s: %s", DebugUtil.dbg(t), DebugUtil.dbg(err));
    }
    return t;
  }

  public ListenableFuture<CoreTransactionGetResult> getInternal(CollectionIdentifier collection, String id, SpanWrapper span) {
    logger().info(attemptId(), "fetching doc %s", DebugUtil.docId(collection, id));

    ListenableFuture<Optional<CoreTransactionGetResult>> result = DocumentGetterStellarNebula.getAsync(sn,
            LOGGER,
            collection,
            config,
            id,
            transactionId(),
            attemptId(),
            span,
            meteringUnitsBuilder);

    result = wrap(result, span,"got doc " + DebugUtil.docId(collection, id), OperationTypes.GET);

//    Futures.addCallback(result, new FutureCallback<Optional<CoreTransactionGetResult>>() {
//      @Override
//      public void onSuccess(@NullableDecl Optional<CoreTransactionGetResult> result) {
//        long elapsed = span.finish();
//        logger().info(attemptId(), "got doc %s in %dus", DebugUtil.docId(collection, id), elapsed);
//      }
//
//      @Override
//      public void onFailure(Throwable t) {
//        long elapsed = span.finishWithErrorStatus();
//        logger().info(attemptId(), "failed to get doc %s after %dus, with error %s", DebugUtil.docId(collection, id), elapsed, DebugUtil.dbg(t));
//      }
//    }, sn.executor());

    return Futures.transform(result, doc -> {
      if (doc.isPresent()) {
        return doc.get();
      } else {
        throw new DocumentNotFoundException(ReducedKeyValueErrorContext.create(id));
      }
    }, sn.executor());
  }

  boolean hasExpiredClientSide(String place, Optional<String> docId) {
    boolean over = overall.hasExpiredClientSide();

    if (over) LOGGER.info(attemptId(), "expired in %s", place);

    return over;
  }


  @Override
  TransactionOperationFailedException canPerformOperation(String dbg, boolean canPerformPendingCheck) {
    // todo sntxn
    return null;
  }

  @Override
  public CoreTransactionResult transactionEnd(Throwable err, boolean singleQueryTransactionMode) {
    return transactionEnd(err, singleQueryTransactionMode, true);
  }

  public ListenableFuture<CoreTransactionGetResult> insert(CollectionIdentifier collection, String id, byte[] content, SpanWrapper pspan) {

    return doKVOperation("insert " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_INSERT, collection, id,
            (operationId, span) -> insertInternal(operationId, collection, id, content, span));
  }

  private ListenableFuture<CoreTransactionGetResult> insertInternal(String operationId, CollectionIdentifier collection, String id, byte[] content,
                                                                    SpanWrapper pspan) {
    SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.SPAN_DISPATCH, pspan);

    logger().info(attemptId(), "inserting doc %s", DebugUtil.docId(collection, id));

    TransactionInsertRequest request = TransactionInsertRequest.newBuilder()
            .setBucketName(collection.bucket())
            .setScopeName(collection.scope().orElse(DEFAULT_SCOPE))
            .setCollectionName(collection.collection().orElse(DEFAULT_COLLECTION))
            .setTransactionId(transactionId())
            .setAttemptId(attemptId())
            .setKey(id)
            .setValue(ByteString.copyFrom(content))
            .build();

    ListenableFuture<TransactionInsertResponse> result = sn.connection().stubFuture()
            .withOption(CallOptions.Key.create("hooks-id"), "ABCD")
            .withDeadlineAfter(kvTimeoutMutating(sn.core()).toMillis(), TimeUnit.MILLISECONDS)
            .transactionInsert(request);

//    TransactionInsertResponse resultaw = sn.connection().stubBlocking().transactionInsert(request);

//    ListenableFuture<TransactionInsertResponse> result = Futures.immediateFuture(resultaw);

    result = wrap(result, span,"insert doc " + DebugUtil.docId(collection, id), OperationTypes.INSERT);

    return Futures.transform(result,
            v -> CoreTransactionGetResult.createFrom(collection, id, content, v),
            sn.executor());
  }

  private ListenableFuture<Void> waitForAllOps(String dbg) {
    logger().info(attemptId(), "waiting for %d KV ops in %s", kvOps.waitingCount(), dbg);
    return kvOps.await(Duration.ofMillis(expiryRemainingMillis()));
  }

  public ListenableFuture<CoreTransactionGetResult> replace(CoreTransactionGetResult doc, byte[] content, SpanWrapper pspan) {
    return doKVOperation("replace " + DebugUtil.docId(doc), pspan, CoreTransactionAttemptContextHooks.HOOK_REPLACE, doc.collection(), doc.id(),
            (operationId, span) -> replaceInternal(operationId, doc, content, span));
  }

  public ListenableFuture<CoreTransactionGetResult> remove(CoreTransactionGetResult doc, SpanWrapper pspan) {
    return doKVOperation("replace " + DebugUtil.docId(doc), pspan, CoreTransactionAttemptContextHooks.HOOK_REMOVE, doc.collection(), doc.id(),
            (operationId, span) -> removeInternal(operationId, doc, span));
  }

  /**
   * Provide generic support functionality around KV operations (including those done in queryMode), including locking.
   * <p>
   * The callback is required to unlock the mutex iff it returns without error. This method will handle unlocking on errors.
   */
  private <T> ListenableFuture<T> doKVOperation(String lockDebugOrig, SpanWrapper span, String stageName, CollectionIdentifier docCollection, String docId,
                                                BiFunction<String, SpanWrapper, ListenableFuture<T>> op) {
    String operationId = UUID.randomUUID().toString();
    // If two operations on the same doc are done concurrently it can be unclear, so include a partial of the operation id
    String lockDebug = lockDebugOrig + " - " + operationId.substring(0, TransactionLogEvent.CHARS_TO_LOG);
    SpanWrapperUtil.setAttributes(span, this, docCollection, docId);
    // We don't attach the opid to the span, it's too low cardinality to be useful

    WaitGroup.Waiter waiter;
    synchronized (this) {
      waiter = kvOps.add(lockDebugOrig);
      beginTransactionIfNeededLocked(docCollection.bucket(), span);
    }

    TransactionOperationFailedException returnEarly = canPerformOperation(lockDebug);
    if (returnEarly != null) {
      return Futures.immediateFailedFuture(returnEarly);
    }

    ListenableFuture<T> result = op.apply(operationId, span);

    Futures.addCallback(result, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        kvOps.done(waiter);
      }

      @Override
      public void onFailure(Throwable t) {
        span.setErrorStatus();
        kvOps.done(waiter);
      }
    }, sn.executor());

    return result;
  }

  private ListenableFuture<CoreTransactionGetResult> replaceInternal(String operationId,
                                                                     CoreTransactionGetResult doc,
                                                                     byte[] content,
                                                                     SpanWrapper pspan) {
    SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), doc.collection(), doc.id(), TracingIdentifiers.SPAN_DISPATCH, pspan);

    LOGGER.info(attemptId(), "replace doc %s, operationId = %s", doc, operationId);

    TransactionReplaceRequest request = TransactionReplaceRequest.newBuilder()
            .setBucketName(doc.collection().bucket())
            .setScopeName(doc.collection().scope().orElse(DEFAULT_SCOPE))
            .setCollectionName(doc.collection().collection().orElse(DEFAULT_COLLECTION))
            .setTransactionId(transactionId())
            .setAttemptId(attemptId())
            .setKey(doc.id())
            .setCas(doc.cas())
            .setValue(ByteString.copyFrom(content))
            .build();

    ListenableFuture<TransactionReplaceResponse> result = sn.connection().stubFuture()
            .withDeadlineAfter(kvTimeoutMutating(sn.core()).toMillis(), TimeUnit.MILLISECONDS)
            .transactionReplace(request);

    result = wrap(result, span,"replace doc " + DebugUtil.docId(doc.collection(), doc.id()), OperationTypes.REPLACE);

    return Futures.transform(result, v -> {
      doc.cas(v.getCas());
      return doc;
    }, sn.executor());
  }

  private ListenableFuture<CoreTransactionGetResult> removeInternal(String operationId,
                                                                    CoreTransactionGetResult doc,
                                                                    SpanWrapper pspan) {
    SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), doc.collection(), doc.id(), TracingIdentifiers.SPAN_DISPATCH, pspan);

    LOGGER.info(attemptId(), "remove doc %s, operationId = %s", doc, operationId);

    TransactionRemoveRequest request = TransactionRemoveRequest.newBuilder()
            .setBucketName(doc.collection().bucket())
            .setScopeName(doc.collection().scope().orElse(DEFAULT_SCOPE))
            .setCollectionName(doc.collection().collection().orElse(DEFAULT_COLLECTION))
            .setTransactionId(transactionId())
            .setAttemptId(attemptId())
            .setKey(doc.id())
            .setCas(doc.cas())
            .build();

    ListenableFuture<TransactionRemoveResponse> result = sn.connection().stubFuture()
            .withDeadlineAfter(kvTimeoutMutating(sn.core()).toMillis(), TimeUnit.MILLISECONDS)
            .transactionRemove(request);

    result = wrap(result, span, "remove doc " + DebugUtil.docId(doc.collection(), doc.id()), OperationTypes.REMOVE);

    return Futures.transform(result, v -> {
      doc.cas(v.getCas());
      return doc;
    }, sn.executor());
  }


  private void beginTransactionIfNeededLocked(String bucketName, SpanWrapper pspan) {

    if (this.afterBegin == null) {
      SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, "transaction_begin_attempt", pspan)
              .attribute(TracingIdentifiers.ATTR_NAME, bucketName);

      try {
        logger().info(attemptId(), "Calling begin transaction existing transactionId=%s", transactionId());

        TransactionBeginAttemptRequest.Builder builder = TransactionBeginAttemptRequest.newBuilder()
                .setBucketName(bucketName);

        // todo sntxn
//        if (transactionId() != null) {
//          builder.setTransactionId(transactionId());
//        }

        TransactionBeginAttemptRequest request = builder.build();

        TransactionBeginAttemptResponse result = sn.connection().stubBlocking().transactionBeginAttempt(request);

        // todo sntxn provide and use node target
        this.afterBegin = new AvailableAfterBeginTransaction(result.getAttemptId(), bucketName, null);
        this.overall.transactionId(result.getTransactionId());
        long elapsedMicros = span.finish();
        logger().info("", "TransactionBeginRequest completed in %dus returning transactionId %s attemptId %s",
                elapsedMicros, result.getTransactionId(), afterBegin.attemptId);
      }
      catch (Throwable err) {
        long elapsedMicros = span.finish(err);
        logger().info("", "TransactionBeginRequest failed with %s in %dus", DebugUtil.dbg(err), elapsedMicros);
      }
    }
  }


  long expiryRemainingMillis() {
    long nowNanos = System.nanoTime();
    long expiredMillis = overall.timeSinceStartOfTransactionsMillis(nowNanos);
    long remainingMillis = config.expirationTime().toMillis() - expiredMillis;

    // This bounds the value to [0-expirationTime].  It should always be in this range, this is just to protect
    // against the application clock changing.
    return Math.max(Math.min(remainingMillis, config.expirationTime().toMillis()), 0);
  }

  private RequestTracer tracer() {
    // Will go to the ThresholdRequestTracer by default.  In future, may want our own default tracer.
    return core.context().environment().requestTracer();
  }


  @Override
  Mono<Void> commitInternal() {
    return ListenableFutureUtil.toMono(commit(), sn.executor());
  }

  ListenableFuture<Void> commit() {
    SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_COMMIT, attemptSpan);
    waitForAllOps("commit");
    synchronized (this) {
      if (afterBegin == null) {
        logger().info("Commit has no work to do");
        return Futures.immediateVoidFuture();
      }

      // There's a race window between locking and checking the waiting count where ops can be added.  If this happens
      // we need to unlock and try again.
      if (kvOps.waitingCount() > 0) {
        logger().info("Commit still waiting for " + kvOps.waitingCount() + " ops, retrying");
        return Futures.transformAsync(Futures.immediateVoidFuture(),
                v -> commit(),
                sn.executor());
      }
      ListenableFuture<Void> out = commitInternalLocked(span);
      Futures.addCallback(out, new FutureCallback<Void>() {
        @Override
        public void onSuccess(Void result) {
          span.finish();
        }

        @Override
        public void onFailure(Throwable t) {
          span.finish(t);
        }
      }, sn.executor());
      return out;
    }
  }

  private ListenableFuture<Void> commitInternalLocked(SpanWrapper span) {
    LOGGER.info(attemptId(), "commit %s", this);

    TransactionCommitRequest request = TransactionCommitRequest.newBuilder()
            .setBucketName(afterBegin.bucketName)
            .setTransactionId(transactionId())
            .setAttemptId(afterBegin.attemptId)
            .build();

    ListenableFuture<TransactionCommitResponse> result = sn.connection().stubFuture()
            .withDeadlineAfter(expiryRemainingMillis(), TimeUnit.MILLISECONDS)
            .transactionCommit(request);

    result = wrap(result, span,"commit", null);

    return Futures.transform(result, v -> null, sn.executor());
  }

  public RequestSpan span() {
    return attemptSpan.span();
  }


  public ListenableFuture<Void> rollback() {
    SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_ROLLBACK, attemptSpan);
    waitForAllOps("rollback");
    synchronized (this) {
      if (afterBegin == null) {
        logger().info("Rollback has no work to do");
        return Futures.immediateVoidFuture();
      }

      if (kvOps.waitingCount() > 0) {
        logger().info("Rollback still waiting for " + kvOps.waitingCount() + " ops, retrying");
        return Futures.transformAsync(Futures.immediateVoidFuture(),
                v -> rollback(),
                sn.executor());
      }

      TransactionRollbackRequest request = TransactionRollbackRequest.newBuilder()
              .setBucketName(afterBegin.bucketName)
              .setTransactionId(transactionId())
              .setAttemptId(afterBegin.attemptId)
              .build();

      ListenableFuture<TransactionRollbackResponse> result = sn.connection().stubFuture()
              .withDeadlineAfter(expiryRemainingMillis(), TimeUnit.MILLISECONDS)
              .transactionRollback(request);

      result = wrap(result, span,"rollback", null);

      ListenableFuture<Void> out = Futures.transform(result, v -> null, sn.executor());

//      Futures.addCallback(out, new FutureCallback<Void>() {
//        @Override
//        public void onSuccess(@NullableDecl Void result) {
//          span.finish();
//        }
//
//        @Override
//        public void onFailure(Throwable t) {
//          span.finish(t);
//        }
//      }, sn.executor());

      return out;
    }
  }

  Mono<Void> rollbackAuto() {
    return Mono.defer(() -> {
      return ListenableFutureUtil.toMono(rollback(), sn.executor());
    });
  }

  @Override
  protected CleanupRequest createCleanupRequestIfNeeded(CoreTransactionsCleanup cleanup) {
    return null;
  }


  public CoreTransactionLogger logger() {
    return LOGGER;
  }
}
