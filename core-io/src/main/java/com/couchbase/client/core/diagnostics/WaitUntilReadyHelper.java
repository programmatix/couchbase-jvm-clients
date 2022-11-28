/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.core.WaitUntilReadyCompletedEvent;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;

/**
 * Helper class to perform the "wait until ready" logic.
 */
@Stability.Internal
public class WaitUntilReadyHelper {
  private static final Logger logger = LoggerFactory.getLogger(WaitUntilReadyHelper.class);

  @Stability.Internal
  public static CompletableFuture<Void> waitUntilReady(final Core core, final Set<ServiceType> serviceTypes,
                                                       final Duration timeout, final ClusterState desiredState,
                                                       final Optional<String> bucketName) {
    final WaitUntilReadyState state = new WaitUntilReadyState();

    state.transition(WaitUntilReadyStage.CONFIG_LOAD);
    return Flux
      .interval(Duration.ofMillis(10), core.context().environment().scheduler())
      // There is a good chance that downstream demand is lower (taking longer than
      // the 10ms of the interval signal). We can just drop the ticks that we don't need,
      // since the interval acts like a "pacemaker" here and keeps us going with new tries
      // until the wait until ready completes or times out.
      .doOnNext(v -> logger.info("waitUntilReady 1 {}", v))
      .onBackpressureDrop()
      .filter(i -> !(core.configurationProvider().bucketConfigLoadInProgress()
        || core.configurationProvider().globalConfigLoadInProgress()
        || (bucketName.isPresent() && core.configurationProvider().collectionRefreshInProgress())
        || (bucketName.isPresent() && core.clusterConfig().bucketConfig(bucketName.get()) == null))
      )
      .flatMap(i -> {
        logger.info("waitUntilReady 2 {} {} {} {}", i, serviceTypes, bucketName, state);
        if (bucketName.isPresent()) {
          state.transition(WaitUntilReadyStage.BUCKET_NODES_HEALTHY);
          // To avoid tmpfails on the bucket, we double check that all nodes from the nodes list are
          // in a healthy status - but for this we need to actually fetch the verbose config, since
          // the terse one doesn't have that status in it.
          GenericManagerRequest request = new GenericManagerRequest(
            core.context(),
            () -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/pools/default/buckets/" + bucketName.get()),
            true,
            null
          );
          core.send(request);
          return Reactor.wrap(request, request.response(), true)
            .filter(response -> {
              logger.info("waitUntilReady 3 got node list {}", response.status());

             if (response.status() != ResponseStatus.SUCCESS) {
               return false;
             }

              ObjectNode root = (ObjectNode) Mapper.decodeIntoTree(response.content());
              ArrayNode nodes = (ArrayNode) root.get("nodes");

              long healthy = StreamSupport
                .stream(nodes.spliterator(), false)
                .filter(node -> node.get("status").asText().equals("healthy"))
                .count();

              logger.info("waitUntilReady 3 got node list {} {}", healthy, nodes);

              return nodes.size() == healthy;
            })
            .map(ignored -> i);
        } else {
         return Flux.just(i);
        }
      })
      .take(1)
      .flatMap(aLong -> {
        logger.info("waitUntilReady 4 {}", aLong);

        // There could be a scenario where a user calls waitUntilReady on the cluster object despite
        // running a server cluster pre 6.5 (which does not support cluster-level config). Per definition,
        // we cannot make progress. So we let WaitUntilReady complete (so a user can move on to open a bucket)
        // but we'll print a warning to make sure we clarify the situation.
        //
        // Note that an explicit decision has been made to not fail fast and let it pass through because individual
        // operations will fail anyways if no further bucket is being opened and there is just nothing to "wait for"
        // to being ready at this point. Bucket level wait until ready is the way to go there.
        if (!bucketName.isPresent() && !core.clusterConfig().hasClusterOrBucketConfig()) {
          logger.info("waitUntilReady 5");
          state.transition(WaitUntilReadyStage.COMPLETE);
          WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
            servicesToCheck(core, serviceTypes, bucketName),
            timeout,
            desiredState,
            bucketName,
            core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
            state
          );
          core.context().environment().eventBus().publish(new WaitUntilReadyCompletedEvent(
            waitUntilReadyContext, WaitUntilReadyCompletedEvent.Reason.CLUSTER_LEVEL_NOT_SUPPORTED
          ));
          return Flux.empty();
        }

        logger.info("waitUntilReady 6");

        state.transition(WaitUntilReadyStage.PING);
        final Flux<ClusterState> diagnostics = Flux
          .interval(Duration.ofMillis(10), core.context().environment().scheduler())
          // Diagnostics are in-memory and should be quicker than 10ms, but just in case
          // make sure that slower downstream does not terminate the diagnostics interval
          // pacemaker.
          .doOnNext(v -> logger.info("waitUntilReady PING {} 1", v))
          .onBackpressureDrop()
          .doOnNext(v -> logger.info("waitUntilReady PING {} 2", v))
          .map(i -> diagnosticsCurrentState(core))
          .doOnNext(v -> logger.info("waitUntilReady PING {} {}", v, desiredState))
          .takeUntil(s -> s == desiredState);

        Duration timeoutForPing = Duration.ofMillis(Math.min(5000, timeout.toMillis()));

        return Flux.concat(ping(core, servicesToCheck(core, serviceTypes, bucketName), timeoutForPing, bucketName)
                        .doOnNext(v -> logger.info("waitUntilReady PING PingResult {} {}", v, servicesToCheck(core, serviceTypes, bucketName)))
                , diagnostics
                        .doOnNext(v -> logger.info("waitUntilReady PING diagnostics {}", v)));
      })
      .then()
      .doOnError(err -> logger.info("waitUntilReady hit error {}", err.toString()))
      .timeout(
        timeout,
        Mono.defer(() -> {
          WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
            servicesToCheck(core, serviceTypes, bucketName),
            timeout,
            desiredState,
            bucketName,
            core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
            state
          );
          CancellationErrorContext errorContext = new CancellationErrorContext(waitUntilReadyContext);
          return Mono.error(new UnambiguousTimeoutException("WaitUntilReady timed out", errorContext));
        }),
        core.context().environment().scheduler()
      )
      .doOnSuccess(unused -> {
        logger.info("waitUntilReady 8");

        state.transition(WaitUntilReadyStage.COMPLETE);
        WaitUntilReadyContext waitUntilReadyContext = new WaitUntilReadyContext(
          servicesToCheck(core, serviceTypes, bucketName),
          timeout,
          desiredState,
          bucketName,
          core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
          state
        );
        core.context().environment().eventBus().publish(
          new WaitUntilReadyCompletedEvent(waitUntilReadyContext, WaitUntilReadyCompletedEvent.Reason.SUCCESS));
      })
      .toFuture();
  }

  private static Set<ServiceType> servicesToCheck(final Core core, final Set<ServiceType> serviceTypes,
                                                  final Optional<String> bucketName) {
    return !isNullOrEmpty(serviceTypes)
      ? serviceTypes
      : HealthPinger
      .extractPingTargets(core.clusterConfig(), bucketName)
      .stream()
      .map(RequestTarget::serviceType)
      .collect(Collectors.toSet());
  }

  /**
   * Checks diagnostics and returns the current aggregated cluster state.
   */
  private static ClusterState diagnosticsCurrentState(final Core core) {
    return DiagnosticsResult.aggregateClusterState(core
      .diagnostics()
      .collect(Collectors.groupingBy(EndpointDiagnostics::type))
      .values());
  }

  /**
   * Performs the ping part of the wait until ready (calling into the health pinger).
   */
  private static Flux<PingResult> ping(final Core core, final Set<ServiceType> serviceTypes, final Duration timeout,
                                       final Optional<String> bucketName) {
    return HealthPinger
      .ping(core, Optional.of(timeout), core.context().environment().retryStrategy(), serviceTypes, Optional.empty(), bucketName)
      .doOnNext(v -> logger.info("ping result {}", v))
      .flux();
  }

  /**
   * Encapsulates the state of where a wait until ready flow is in.
   */
  @Stability.Internal
  public static class WaitUntilReadyState {

    private final Map<WaitUntilReadyStage, Long> timings = new ConcurrentHashMap<>();
    private final AtomicLong totalDuration = new AtomicLong();

    private volatile WaitUntilReadyStage currentStage = WaitUntilReadyStage.INITIAL;
    private volatile long currentStart = System.nanoTime();

    void transition(final WaitUntilReadyStage next) {
      long timing = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - currentStart);
      if (currentStage != WaitUntilReadyStage.INITIAL) {
        timings.put(currentStage, timing);
      }
      totalDuration.addAndGet(timing);
      currentStage = next;
      currentStart = System.nanoTime();
    }

    public Map<String, Object> export() {
      Map<String, Object> toExport = new TreeMap<>();

      toExport.put("current_stage", currentStage);
      if (currentStage != WaitUntilReadyStage.COMPLETE) {
        long currentMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - currentStart);
        toExport.put("current_stage_since_ms", currentMs);
        toExport.put("total_ms", totalDuration.get() + currentMs);
      } else {
        toExport.put("total_ms", totalDuration.get());
      }
      toExport.put("timings_ms", timings);

      return toExport;
    }

    public long totalDuration() {
      return totalDuration.get();
    }

    @Override
    public String toString() {
      return export().toString();
    }
  }

  /**
   * Describes the different stages of wait until ready.
   */
  private enum WaitUntilReadyStage {
    /**
     * Not started yet, initial stage.
     */
    INITIAL,
    /**
     * Waits until all global and bucket level configs are loaded.
     */
    CONFIG_LOAD,
    /**
     * Waits until all the nodes in a bucket config are healthy.
     */
    BUCKET_NODES_HEALTHY,
    /**
     * Performs ping operations and checks their return values.
     */
    PING,
    /**
     * Completed successfully.
     */
    COMPLETE,
  }

}
