package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.retry.RetryReason;
import reactor.util.annotation.Nullable;

public class ProtostellarFailureBehaviour {
  private final @Nullable RetryReason retry;
  private final RuntimeException exception;
  private final ProtostellarErrorContext errorContext;

  public ProtostellarFailureBehaviour(@Nullable RetryReason retry,
                                      RuntimeException exception,
                                      ProtostellarErrorContext errorContext) {
    this.retry = retry;
    this.exception = exception;
    this.errorContext = errorContext;
  }

  public boolean shouldRetry() {
    return retry != null;
  }

  @Nullable
  public RetryReason retry() {
    return retry;
  }

  public RuntimeException exception() {
    return exception;
  }
}
