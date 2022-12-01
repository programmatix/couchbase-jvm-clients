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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.ProtostellarErrorContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.protostellar.ProtostellarRequest;

import java.util.function.Function;
import java.util.function.Supplier;

public class RequestCanceledException extends CouchbaseException {

  private final CancellationReason reason;

  public RequestCanceledException(String message, CancellationReason reason, CancellationErrorContext ctx) {
    super(message, ctx);
    this.reason = reason;
  }

  public static <TGrpcRequest> RequestCanceledException shuttingDown(AbstractContext context) {
    CancellationErrorContext ctx = new CancellationErrorContext(context);
    throw new RequestCanceledException("Request cancelled as in the process of shutting down", CancellationReason.SHUTDOWN, ctx);
  }

  @Override
  @Stability.Uncommitted
  public CancellationErrorContext context() {
    return (CancellationErrorContext) super.context();
  }

  @Stability.Uncommitted
  public CancellationReason reason() {
    return reason;
  }
}
