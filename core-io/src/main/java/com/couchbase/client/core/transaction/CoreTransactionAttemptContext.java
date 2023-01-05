package com.couchbase.client.core.transaction;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.transaction.CommitNotPermittedException;
import com.couchbase.client.core.error.transaction.PreviousOperationFailedException;
import com.couchbase.client.core.error.transaction.RetryTransactionException;
import com.couchbase.client.core.error.transaction.RollbackNotPermittedException;
import com.couchbase.client.core.error.transaction.TransactionAlreadyAbortedException;
import com.couchbase.client.core.error.transaction.TransactionAlreadyCommittedException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.error.transaction.internal.WrappedTransactionOperationFailedException;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.Builder.createError;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.STATE_BITS_MASK_BITS;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.STATE_BITS_MASK_FINAL_ERROR;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.STATE_BITS_POSITION_FINAL_ERROR;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY;
import static com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic.TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK;

// Note this has to be named CoreTransactionAttemptContext as that's what Spring expects.
// todo sntxn make sure we're compatible with Spring still and maybe expose a minimal interface for that
// todo sntxn move this into a CoreLoop
@Stability.Internal
abstract public class CoreTransactionAttemptContext {

    // todo sntxn have definitely borked some changes during the merge - maybe QueryContext

    protected final AtomicInteger stateBits = new AtomicInteger(0);
    protected final Core core;
    protected final CoreMergedTransactionConfig config;

    protected final CoreTransactionContext overall;
    // The time this particular attempt started
    protected final long startTimeClient;
    protected final SpanWrapper attemptSpan;
    protected final CoreTransactionLogger LOGGER;
    protected final MeteringUnits.MeteringUnitsBuilder meteringUnitsBuilder = new MeteringUnits.MeteringUnitsBuilder();

    protected CoreTransactionAttemptContext(Core core, CoreMergedTransactionConfig config, CoreTransactionContext overall, @Nullable SpanWrapper parentSpan) {
        this.core = core;
        this.config = config;
        this.overall = overall;
        this.startTimeClient = System.nanoTime();
        this.attemptSpan = SpanWrapperUtil.createOp(this, core.context().environment().requestTracer(), null, null, TracingIdentifiers.TRANSACTION_OP_ATTEMPT, parentSpan);
        LOGGER = overall.LOGGER;
    }

    // These can both be null on SN prior to begin attempt
    public @Nullable String transactionId() {
        return overall.transactionId();
    }

    abstract public @Nullable String attemptId();

    @Stability.Internal
    public abstract CoreTransactionLogger logger();

    public abstract Scheduler scheduler();

    //  abstract TransactionOperationFailedException operationFailed(TransactionOperationFailedException err);
    public abstract Core core();

    public abstract RequestSpan span();


//  abstract CoreTransactionResult transactionEnd(@Nullable Throwable err, boolean singleQueryTransactionMode);

    abstract Mono<Void> rollbackAuto();


    /**
     * Rollback errors rules:
     * Errors during auto-rollback: do not update internal state, as the user cares more about the original error that provoked the rollback
     * Errors during app-rollback:  do update internal state.  Nothing else has gone wrong with the transaction, and the user will care about rollback problems.
     * <p>
     * If !updateInternalState, the internal state bits are not changed.
     */
    public TransactionOperationFailedException operationFailed(boolean updateInternalState, TransactionOperationFailedException err) {
        if (updateInternalState) {
            return operationFailed(err);
        }

        return err;
    }

    public TransactionOperationFailedException operationFailed(TransactionOperationFailedException err) {
        int sb = TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED;

        if (!err.autoRollbackAttempt()) {
            sb |= TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK;
        }

        if (!err.retryTransaction()) {
            sb |= TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY;
        }

        setStateBits("operationFailed", sb, err.toRaise().ordinal());

        return err;
    }

    @Stability.Internal
    Mono<Void> lambdaEnd(@Nullable CoreTransactionsCleanup cleanup,
                         @Nullable Throwable err,
                         boolean singleQueryTransactionMode) {
        return Mono.defer(() -> {
            int sb = stateBits.get();
            boolean shouldNotRollback = (sb & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
            int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
            TransactionOperationFailedException.FinalErrorToRaise finalError = TransactionOperationFailedException.FinalErrorToRaise.values()[maskedFinalError];
            boolean rollbackNeeded = finalError != TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_SUCCESS && !shouldNotRollback && !singleQueryTransactionMode;

            logger().info(attemptId(), "reached post-lambda in %dus, shouldNotRollback=%s finalError=%s rollbackNeeded=%s, err (only cause of this will be used)=%s tximplicit=%s%s",
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTimeClient), shouldNotRollback,
                    finalError, rollbackNeeded, err, singleQueryTransactionMode,
                    ""
                    // todo units?
                    // Don't display the aggregated units if queryMode as it won't be accurate
                    // queryModeUnlocked() ? "" : meteringUnitsBuilder.toString()
            );

            core().transactionsContext().counters().attempts().incrementBy(1);

            return Mono.defer(() -> {
                        if (rollbackNeeded) {
                            return rollbackAuto()
                                    .onErrorResume(er -> {
                                        // If rollback fails, want to raise original error as cause, but with retry=false.
                                        logger().info(attemptId(), "rollback failed with %s. Original error will be raised as cause, and retry should be disabled", DebugUtil.dbg(er));
                                        setStateBits("lambdaEnd", TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY, 0);
                                        return Mono.empty();
                                    });
                        }

                        return Mono.empty();
                    })
                    // Only want to add the attempt after doing the rollback, so the attempt has the correct state (hopefully
                    // ROLLED_BACK)
                    .then(Mono.fromRunnable(() -> {
                        if (cleanup != null) {
                            CleanupRequest cleanupRequest = createCleanupRequestIfNeeded(cleanup);
                            if (cleanupRequest != null) {
                                cleanup.add(cleanupRequest);
                            }
                        }
                    }))
                    .doOnTerminate(() -> {
                        if (err != null) {
                            attemptSpan.finishWithErrorStatus();
                        } else {
                            attemptSpan.finish();
                        }
                    })
                    .then(Mono.defer(() -> retryIfRequired(err)));
        });
    }

    protected abstract CleanupRequest createCleanupRequestIfNeeded(@Nullable CoreTransactionsCleanup cleanup);

    abstract boolean hasExpiredClientSide(String place, Optional<String> docId);

    private Mono<Void> retryIfRequired(Throwable err) {
        int sb = stateBits.get();
        boolean shouldNotRetry = (sb & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
        TransactionOperationFailedException.FinalErrorToRaise finalError = TransactionOperationFailedException.FinalErrorToRaise.values()[maskedFinalError];
        boolean retryNeeded = finalError != TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_SUCCESS && !shouldNotRetry;

        if (retryNeeded) {
            if (hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_BEFORE_RETRY, Optional.empty())) {
                // This will set state bits and raise a cause for transactionEnd
                return Mono.error(operationFailed(createError()
                        .doNotRollbackAttempt()
                        .raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .build()));
            }
        }

        logger().info(attemptId(), "reached end of lambda post-rollback (if needed), shouldNotRetry=%s finalError=%s retryNeeded=%s",
                shouldNotRetry, finalError, retryNeeded);

        if (retryNeeded) {
            return Mono.error(new RetryTransactionException());
        }

        if (err != null) {
            return Mono.error(err);
        }

        return Mono.empty();
    }

    @Stability.Internal
    public CoreTransactionResult transactionEnd(@Nullable Throwable err,
                                                boolean singleQueryTransactionMode,
                                                // todo remove
                                                boolean unstagingComplete) {

        int sb = stateBits.get();
        int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
        TransactionOperationFailedException.FinalErrorToRaise finalError = TransactionOperationFailedException.FinalErrorToRaise.values()[maskedFinalError];

        // todo this is getting called at start of txn also
        overall.LOGGER.info(attemptId(), "reached end of transaction, toRaise=%s, err=%s", finalError, err);

        core().transactionsContext().counters().transactions().incrementBy(1);

        Throwable cause = null;
        if (err != null) {
            if (!(err instanceof TransactionOperationFailedException)) {
                if (!singleQueryTransactionMode) {
                    // A bug.  Only TransactionOperationFailedException is allowed to reach here.
                    logger().info(attemptId(), "Non-TransactionOperationFailedException '" + DebugUtil.dbg(err) + "' received, this is a bug");
                }
            } else {
                TransactionOperationFailedException e = (TransactionOperationFailedException) err;
                cause = e.getCause();
            }
        }

        RuntimeException ret = null;

        switch (finalError) {
            case TRANSACTION_FAILED_POST_COMMIT:
                unstagingComplete = false;
                break;

            case TRANSACTION_SUCCESS:
                if (singleQueryTransactionMode) {
                    if (ret instanceof RuntimeException) {
                        ret = (RuntimeException) err;
                    } else {
                        ret = new RuntimeException(err);
                    }
                }
                break;

            case TRANSACTION_EXPIRED: {
                String msg = "Transaction has expired configured timeout of " + overall.expirationTime().toMillis() + "ms.  The transaction is not committed.";
                ret = new CoreTransactionExpiredException(cause, logger(), overall.transactionId(), msg);
                break;
            }
            case TRANSACTION_COMMIT_AMBIGUOUS: {
                String msg = "It is ambiguous whether the transaction committed";
                ret = new CoreTransactionCommitAmbiguousException(cause, logger(), overall.transactionId(), msg);
                break;
            }
            default:
                ret = new CoreTransactionFailedException(cause, logger(), overall.transactionId());
                break;
        }

        CoreTransactionResult result = new CoreTransactionResult(overall.LOGGER,
                Duration.ofNanos(System.nanoTime() - overall.startTimeClient()),
                overall.transactionId(),
                unstagingComplete);

        if (ret != null) {
            throw ret;
        }

        return result;
    }

    @Stability.Internal
    Throwable convertToOperationFailedIfNeeded(Throwable e, boolean singleQueryTransactionMode) {
        // If it's an TransactionOperationFailedException, the error originator has already chosen the error handling behaviour.  All
        // transaction internals will only raise this.
        if (e instanceof TransactionOperationFailedException) {
            return (TransactionOperationFailedException) e;
        } else if (e instanceof WrappedTransactionOperationFailedException) {
            return ((WrappedTransactionOperationFailedException) e).wrapped();
        } else if (singleQueryTransactionMode) {
            logger().info(attemptId(), "Caught exception from application's lambda %s, not converting", DebugUtil.dbg(e));
            return e;
        } else {
            // If we're here, it's an error thrown by the application's lambda, e.g. not from transactions internals
            // (these only raise TransactionOperationFailedException).
            // All such errors should try to rollback, and then fail the transaction.
            TransactionOperationFailedException.Builder builder = TransactionOperationFailedException.Builder.createError().cause(e);

            // This is the method for the lambda to explicitly request an auto-rollback-and-retry
            if (e instanceof RetryTransactionException) {
                builder.retryTransaction();
            }
            TransactionOperationFailedException out = builder.build();

            // Do not use getSimpleName() here (or anywhere)!  TXNJ-237
            // We redact the exception's name and message to be on the safe side
            logger().info(attemptId(), "Caught exception from application's lambda %s, converted it to %s",
                    DebugUtil.dbg(e),
                    DebugUtil.dbg(out));

            attemptSpan.recordExceptionAndSetErrorStatus(e);

            // Pass it through operationFailed to account for cases like insert raising DocExists, or query SLA errors
            return operationFailed(out);
        }
    }

    protected boolean hasStateBit(int stateBit) {
        return (stateBits.get() & stateBit) != 0;
    }

    Mono<Void> implicitCommit(boolean singleQueryTransactionMode) {
        return Mono.defer(() -> {

            // May have done an explicit commit already, or the attempt may have failed.
            if (hasStateBit(TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED)) {
                return Mono.just(this);
            } else if (singleQueryTransactionMode) {
                return Mono.just(this);
            } else {
                logger().info(attemptId(), "doing implicit commit");

                return commitInternal();
            }
        }).then();
    }

    protected void setStateBits(String dbg, int newBehaviourFlags, int newFinalErrorToRaise) {
        int oldValue = stateBits.get();
        int newValue = oldValue | newBehaviourFlags;
        // Only save the new ToRaise if it beats what's there now
        if (newFinalErrorToRaise > ((oldValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR)) {
            newValue = (newValue & STATE_BITS_MASK_BITS) | (newFinalErrorToRaise << STATE_BITS_POSITION_FINAL_ERROR);
        }
        while (!stateBits.compareAndSet(oldValue, newValue)) {
            oldValue = stateBits.get();
            newValue = oldValue | newBehaviourFlags;
            if (newFinalErrorToRaise > ((oldValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR)) {
                newValue = (newValue & STATE_BITS_MASK_BITS) | (newFinalErrorToRaise << STATE_BITS_POSITION_FINAL_ERROR);
            }
        }

        boolean wasShouldNotRollback = (oldValue & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
        boolean wasShouldNotRetry = (oldValue & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        boolean wasShouldNotCommit = (oldValue & TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED) != 0;
        boolean wasAppRollbackNotAllowed = (oldValue & TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED) != 0;
        TransactionOperationFailedException.FinalErrorToRaise wasToRaise = TransactionOperationFailedException.FinalErrorToRaise.values()[(oldValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR];

        boolean shouldNotRollback = (newValue & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
        boolean shouldNotRetry = (newValue & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        boolean shouldNotCommit = (newValue & TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED) != 0;
        boolean appRollbackNotAllowed = (newValue & TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED) != 0;
        TransactionOperationFailedException.FinalErrorToRaise toRaise = TransactionOperationFailedException.FinalErrorToRaise.values()[(newValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR];

        StringBuilder sb = new StringBuilder("changed state bits in ").append(dbg).append(", changed");

        if (wasShouldNotRollback != shouldNotRollback) sb.append(" shouldNotRollback to ").append(shouldNotRollback);
        if (wasShouldNotRetry != shouldNotRetry) sb.append(" shouldNotRetry to ").append(shouldNotRetry);
        if (wasShouldNotCommit != shouldNotCommit) sb.append(" shouldNotCommit to ").append(shouldNotCommit);
        if (wasAppRollbackNotAllowed != appRollbackNotAllowed)
            sb.append(" appRollbackNotAllowed to ").append(appRollbackNotAllowed);
        if (wasToRaise != toRaise) sb.append(" toRaise from ").append(wasToRaise).append(" to ").append(toRaise);

        logger().info(attemptId(), sb.toString());
    }

    abstract Mono<Void> commitInternal();

    @Nullable
    protected TransactionOperationFailedException canPerformOperation(String dbg) {
        return canPerformOperation(dbg, true);
    }

    /**
     * @param canPerformPendingCheck permits rollback
     */
    abstract @Nullable TransactionOperationFailedException canPerformOperation(String dbg, boolean canPerformPendingCheck);

    @Nullable
    TransactionOperationFailedException canPerformRollback(String dbg, boolean appRollback) {
        if (appRollback && hasStateBit(TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED)) {
            logger().info(attemptId(), "state bits indicate app-rollback is not allowed");
            // Does not pass through operationFailed as informational
            return createError()
                    .cause(new RollbackNotPermittedException())
                    .doNotRollbackAttempt()
                    .build();
        }

        TransactionOperationFailedException out = canPerformOperation(dbg, false);
        if (out != null) {
            return out;
        }

        return null;
    }

    @Nullable
    TransactionOperationFailedException canPerformCommit(String dbg) {
        if (hasStateBit(TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED)) {
            logger().info(attemptId(), "state bits indicate commit is not allowed");
            // Does not pass through operationFailed as informational
            return createError()
                    .cause(new CommitNotPermittedException())
                    .doNotRollbackAttempt()
                    .build();
        }

        TransactionOperationFailedException out = canPerformOperation(dbg);
        if (out != null) {
            return out;
        }

        return null;
    }

    public abstract CoreTransactionResult transactionEnd(@Nullable Throwable err, boolean singleQueryTransactionMode);
}

