package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContextClassic;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionQueryResult;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.java.transactions.internal.ConverterUtil.makeCollectionIdentifier;

public class TransactionAttemptContextOperationsClassic implements TransactionAttemptContextOperations {
  private final CoreTransactionAttemptContextClassic internal;
  private final JsonSerializer serializer;

  public TransactionAttemptContextOperationsClassic(CoreTransactionAttemptContextClassic internal,
                                                    JsonSerializer serializer) {
    this.internal = internal;
    this.serializer = serializer;
  }

  @Override
  public TransactionGetResult get(Collection collection, String id) {
    return internal.get(makeCollectionIdentifier(collection.async()), id)
            .map(result -> new TransactionGetResult(result, serializer()))
            .block();
  }

  @Override
  public TransactionGetResult replace(CoreTransactionGetResult doc, byte[] encoded, SpanWrapper span) {
    return internal.replace(doc, encoded, span)
            .map(result -> new TransactionGetResult(result, serializer()))
            .block();
  }

  @Override
  public TransactionGetResult insert(CollectionIdentifier collection, String id, byte[] encoded, SpanWrapper span) {
    return internal.insert(collection, id, encoded, span)
            .map(result -> new TransactionGetResult(result, serializer()))
            .block();
  }

  @Override
  public void remove(CoreTransactionGetResult doc, SpanWrapper spanWrapper) {
    this.internal.remove(doc, spanWrapper).block();
  }

  @Override
  public TransactionQueryResult queryBlocking(String statement, @Nullable String bucketName, @Nullable String scopeName, ObjectNode options, boolean singleQueryTransaction) {
    return internal.queryBlocking(statement, bucketName, scopeName, options, singleQueryTransaction)
            .publishOn(internal.core().context().environment().transactionsSchedulers().schedulerBlocking())
            .map(response -> new TransactionQueryResult(response.header, response.rows, response.trailer, serializer()))
            .block();
  }

  @Override
  public JsonSerializer serializer() {
    return serializer;
  }
}
