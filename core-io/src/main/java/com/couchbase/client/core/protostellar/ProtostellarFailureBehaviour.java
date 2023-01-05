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
