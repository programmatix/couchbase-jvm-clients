package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.deps.io.grpc.ManagedChannel;
import com.couchbase.client.core.deps.io.grpc.ManagedChannelBuilder;
import com.couchbase.client.protostellar.transactions.v1.TransactionsGrpc;

public class StellarNebulaConnection {
  private final TransactionsGrpc.TransactionsFutureStub stubFuture;
  private final TransactionsGrpc.TransactionsStub stub;
  private final TransactionsGrpc.TransactionsBlockingStub stubBlocking;

  public StellarNebulaConnection() {
    ManagedChannel channelBlocking = ManagedChannelBuilder.forAddress("localhost", 18098).usePlaintext().build();

    stubFuture = TransactionsGrpc.newFutureStub(channelBlocking);
    stub = TransactionsGrpc.newStub(channelBlocking);
    stubBlocking = TransactionsGrpc.newBlockingStub(channelBlocking);
  }

  public TransactionsGrpc.TransactionsFutureStub stubFuture() {
    return stubFuture;
  }

  public TransactionsGrpc.TransactionsStub stub() {
    return stub;
  }

  public TransactionsGrpc.TransactionsBlockingStub stubBlocking() {
    return stubBlocking;
  }

  public static StellarNebulaConnection INSTANCE = new StellarNebulaConnection();

//  public static <T> CompletableFuture<T> buildCompletableFuture(
//          final ListenableFuture<T> listenableFuture
//  ) {
//    //create an instance of CompletableFuture
//    CompletableFuture<T> completable = new CompletableFuture<T>() {
//      @Override
//      public boolean cancel(boolean mayInterruptIfRunning) {
//        // propagate cancel to the listenable future
//        boolean result = listenableFuture.cancel(mayInterruptIfRunning);
//        super.cancel(mayInterruptIfRunning);
//        return result;
//      }
//    };
//
//    listenableFuture.addListener(
//
//    // add callback
//    listenableFuture.addCallback(new ListenableFutureCallback<T>() {
//      @Override
//      public void onSuccess(T result) {
//        completable.complete(result);
//      }
//
//      @Override
//      public void onFailure(Throwable t) {
//        completable.completeExceptionally(t);
//      }
//    });
//    return completable;
//  }

}
