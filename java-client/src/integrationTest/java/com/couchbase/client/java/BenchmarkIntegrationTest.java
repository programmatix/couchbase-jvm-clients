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

package com.couchbase.client.java;

import com.couchbase.client.core.deps.org.HdrHistogram.Histogram;
import com.couchbase.client.core.deps.org.LatencyUtils.LatencyStats;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A very basic benchmarking test.
 * todo snremove remove this once we're done
 */
@Disabled // Disabled as this is for manual testing only
class BenchmarkIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;

  @BeforeAll
  static void beforeAll() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }


  @Test
  void insertAndGetHighThroughput() {
//    Cluster direct = Cluster.connect("couchbase://localhost", "Administrator", "password");
//    run(direct.bucket(config().bucketname()).defaultCollection());
//    direct.disconnect();

    Cluster ps = Cluster.connect("protostellar://localhost", "Administrator", "password");
    run(ps.bucket(config().bucketname()).defaultCollection());
  }

  private void run(Collection collection) {
    int opsTotalBase = 1000;
    int runForSecs = 10;

    // Just for warmup, throw away
    run(collection, 1, opsTotalBase, runForSecs);

    run(collection, 1, opsTotalBase, runForSecs);
    run(collection, 2, opsTotalBase, runForSecs);
    run(collection, 3, opsTotalBase, runForSecs);
    run(collection, 5, opsTotalBase, runForSecs);
    run(collection, 10, opsTotalBase, runForSecs);
    run(collection, 20, opsTotalBase, runForSecs);
    run(collection, 50, opsTotalBase, runForSecs);
    run(collection, 100, opsTotalBase, runForSecs);
    run(collection, 200, opsTotalBase, runForSecs);
    //run(collection, 500, opsTotalBase, runForSecs);
  }

  private LatencyStats run(Collection collection, int opsDesiredInFlight, int opsTotalBase, int runForSecs) {
    int opsTotal = opsDesiredInFlight * opsTotalBase;

    AtomicInteger opsActuallyInFlight = new AtomicInteger();
    AtomicInteger opsActuallyInFlightMax = new AtomicInteger();
    Set<Integer> concurrentlyOutgoingMessagesSeen = new ConcurrentSkipListSet<>();
    LatencyStats stats = new LatencyStats();
    AtomicInteger errorCount = new AtomicInteger();
    AtomicInteger errorCountTimeouts = new AtomicInteger();

    AtomicInteger count = new AtomicInteger();

    long realStart = System.nanoTime();

    Flux.generate(() -> null,
      (state, sink) -> {
      String next = UUID.randomUUID().toString();
      sink.next(next);
        return null;
      })
      .takeWhile(v -> TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - realStart) <= runForSecs)
      .parallel(opsDesiredInFlight)
      // Defaults to 10*numProcessors threads
      .runOn(Schedulers.boundedElastic())
      .concatMap(id -> {
        concurrentlyOutgoingMessagesSeen.add(opsActuallyInFlight.incrementAndGet());
//        int newCountReal = opsActuallyInFlight.incrementAndGet();
//
//        if (newCountReal > opsActuallyInFlightMax.get()) {
//          opsActuallyInFlightMax.set(newCountReal);
//          System.out.println("New max " + newCountReal);
//        }
        long start = System.nanoTime();
        return collection.reactive().insert(id.toString(), "Hello, world")
          .onErrorResume(err -> {
            opsActuallyInFlight.decrementAndGet();
            if (err instanceof TimeoutException) {
              errorCountTimeouts.incrementAndGet();
            }
            else {
              errorCount.incrementAndGet();
            }
            //System.out.println("Error: " + err.toString());
            return Mono.empty();
          })
          .doOnNext(v -> {
            opsActuallyInFlight.decrementAndGet();
            stats.recordLatency(System.nanoTime() - start);
//            opsActuallyInFlight.decrementAndGet();

            int newCount = count.getAndIncrement();
//            if (newCount % 20000 == 0) {
//            }
          });
      })
      .sequential()
      .blockLast();

//    System.out.println("Ran " + opsTotal + " with " + opsDesiredInFlight + " parallel");
    System.out.println("Ran with " + opsDesiredInFlight + " parallel");
    Histogram histogram = stats.getIntervalHistogram();
//    System.out.println("   Min: " + TimeUnit.NANOSECONDS.toMicros(histogram.getMinValue()));
//    System.out.println("   Max: " + TimeUnit.NANOSECONDS.toMicros(histogram.getMaxValue()));
    System.out.println("   Mean: " + TimeUnit.NANOSECONDS.toMicros((long) histogram.getMean()) + "µs");
//    System.out.println("   p50: " + TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(50)));
//    System.out.println("   p95: " + TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(95)));
//    System.out.println("   p99: " + TimeUnit.NANOSECONDS.toMicros(histogram.getValueAtPercentile(99)));
    System.out.println("   Max in-flight: " + concurrentlyOutgoingMessagesSeen.stream().max(Comparator.comparingInt(v -> v)).get());
    System.out.println("   Ops/sec: " + (int)((double) count.get() / runForSecs));
    System.out.println("   Run for: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - realStart) + "ms");
    if (errorCountTimeouts.get() != 0) {
      System.out.println("   Timeouts: " + errorCountTimeouts.get());
    }
    if (errorCount.get() != 0) {
      System.out.println("   Other errors: " + errorCount.get());
    }

    return stats;
  }
}
