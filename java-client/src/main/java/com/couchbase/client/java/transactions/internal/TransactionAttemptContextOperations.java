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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionQueryResult;
import reactor.util.annotation.Nullable;

public interface TransactionAttemptContextOperations {
  TransactionGetResult get(Collection collection, String id);
  TransactionGetResult replace(CoreTransactionGetResult doc, byte[] encoded, SpanWrapper span);
  TransactionGetResult insert(CollectionIdentifier collection, String id, byte[] encoded, SpanWrapper span);
  void remove(CoreTransactionGetResult internal, SpanWrapper spanWrapper);
  public TransactionQueryResult queryBlocking(String statement, @Nullable String bucketName, @Nullable String scopeName, ObjectNode options, boolean singleQueryTransaction);
  JsonSerializer serializer();
}


