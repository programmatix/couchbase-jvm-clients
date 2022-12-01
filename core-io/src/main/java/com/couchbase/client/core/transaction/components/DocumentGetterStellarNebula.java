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

package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.Futures;
import com.couchbase.client.core.deps.com.google.common.util.concurrent.ListenableFuture;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import com.couchbase.client.protostellar.transactions.v1.TransactionGetRequest;
import com.couchbase.client.protostellar.transactions.v1.TransactionGetResponse;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.transaction.support.OptionsUtil.kvTimeoutNonMutating;

/**
 * Responsible for doing transaction-aware get()s.
 */
@Stability.Internal
public class DocumentGetterStellarNebula {
    private DocumentGetterStellarNebula() {}

    public static ListenableFuture<Optional<CoreTransactionGetResult>> getAsync(StellarNebulaContext sn,
                                                                    CoreTransactionLogger LOGGER,
                                                                    CollectionIdentifier collection,
                                                                    CoreMergedTransactionConfig config,
                                                                    String docId,
                                                                    String transactionId,
                                                                    String attemptId,
                                                                    @Nullable SpanWrapper span,
                                                                    MeteringUnits.MeteringUnitsBuilder units) {
        return justGetDoc(sn, collection, docId, kvTimeoutNonMutating(sn.core()), transactionId, attemptId, span, LOGGER, units);
    }

    public static ListenableFuture<Optional<CoreTransactionGetResult>>
    justGetDoc(StellarNebulaContext sn,
               CollectionIdentifier collection,
               String docId,
               Duration timeout,
               String transactionId,
               String attemptId,
               @Nullable SpanWrapper span,
               CoreTransactionLogger logger,
               MeteringUnits.MeteringUnitsBuilder units) {
//      long start = System.nanoTime();

      TransactionGetRequest req = TransactionGetRequest.newBuilder()
              .setBucketName(collection.bucket())
              .setScopeName(collection.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE))
              .setCollectionName(collection.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION))
              .setTransactionId(transactionId)
              .setAttemptId(attemptId)
              .setKey(docId)
              .build();

      ListenableFuture<TransactionGetResponse> response = sn.connection().stubFuture()
              .withDeadlineAfter(kvTimeoutNonMutating(sn.core()).toMillis(), TimeUnit.MILLISECONDS)
              .transactionGet(req);

      ListenableFuture<Optional<CoreTransactionGetResult>> mapped = Futures.transform(response,
              (x) -> {
                return Optional.of(CoreTransactionGetResult.createFrom(collection, docId, x));
              },
              sn.executor());

//      Futures.addCallback(mapped,
//              new FutureCallback<Optional<CoreTransactionGetResult>>() {
//                @Override
//                public void onSuccess(@NullableDecl Optional<CoreTransactionGetResult> v) {
//                  long elapsed = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
//                  if (v.isPresent()) {
//                    logger.info(attemptId, "completed get of %s in %dus", v.get(), elapsed);
//                  } else {
//                    logger.info(attemptId, "completed get of %s, could not find, in %dus",
//                            DebugUtil.docId(collection, docId), elapsed);
//                  }
//                }
//
//                @Override
//                public void onFailure(Throwable t) {
//                }
//              }, sn.executor());

      return mapped;
    }
}
