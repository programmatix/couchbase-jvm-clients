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
package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContextStellarNebula;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionQueryResult;
import reactor.util.annotation.Nullable;

import java.util.concurrent.ExecutionException;

import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

public class TransactionAttemptContextOperationsStellarNebula implements TransactionAttemptContextOperations {
  private final CoreTransactionAttemptContextStellarNebula internal;
  private final JsonSerializer serializer;

  public TransactionAttemptContextOperationsStellarNebula(CoreTransactionAttemptContextStellarNebula internal,
                                                          JsonSerializer serializer) {
    this.internal = internal;
    this.serializer = serializer;
  }

  @Override
  public TransactionGetResult get(Collection collection, String id) {
    CoreTransactionGetResult result = wrap(internal.get(makeCollectionIdentifier(collection.async()), id));
    return new TransactionGetResult(result, serializer);
  }

  @Override
  public TransactionGetResult replace(CoreTransactionGetResult doc, byte[] encoded, SpanWrapper span) {
    CoreTransactionGetResult result = wrap(internal.replace(doc, encoded, span));
    return new TransactionGetResult(result, serializer());
  }

  @Override
  public TransactionGetResult insert(CollectionIdentifier collection, String id, byte[] encoded, SpanWrapper span) {
    CoreTransactionGetResult result = wrap(internal.insert(collection, id, encoded, span));
    return new TransactionGetResult(result, serializer());
  }

  @Override
  public void remove(CoreTransactionGetResult doc, SpanWrapper span) {
    wrap(internal.remove(doc, span));
  }

  @Override
  public TransactionQueryResult queryBlocking(String statement, @Nullable String bucketName, @Nullable String scopeName, ObjectNode options, boolean singleQueryTransaction) {
    return null;
  }

  @Override
  public JsonSerializer serializer() {
    return serializer;
  }

  private <V> V wrap(ListenableFuture<V> future) {
    try {
      // todo sntxn figure out blocking execution model
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() != null) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }
}
