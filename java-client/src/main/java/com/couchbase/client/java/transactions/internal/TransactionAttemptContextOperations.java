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


