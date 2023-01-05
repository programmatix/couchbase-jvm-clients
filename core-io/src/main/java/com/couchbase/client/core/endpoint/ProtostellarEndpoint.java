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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.events.endpoint.EndpointStateChangedEvent;
import com.couchbase.client.core.deps.io.grpc.Attributes;
import com.couchbase.client.core.deps.io.grpc.CallCredentials;
import com.couchbase.client.core.deps.io.grpc.CallOptions;
import com.couchbase.client.core.deps.io.grpc.Channel;
import com.couchbase.client.core.deps.io.grpc.ClientCall;
import com.couchbase.client.core.deps.io.grpc.ClientInterceptor;
import com.couchbase.client.core.deps.io.grpc.ClientStreamTracer;
import com.couchbase.client.core.deps.io.grpc.ConnectivityState;
import com.couchbase.client.core.deps.io.grpc.InsecureChannelCredentials;
import com.couchbase.client.core.deps.io.grpc.ManagedChannel;
import com.couchbase.client.core.deps.io.grpc.Metadata;
import com.couchbase.client.core.deps.io.grpc.MethodDescriptor;
import com.couchbase.client.core.deps.io.grpc.Status;
import com.couchbase.client.core.deps.io.grpc.netty.NettyChannelBuilder;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.protostellar.analytics.v1.AnalyticsGrpc;
import com.couchbase.client.protostellar.kv.v1.KvGrpc;
import com.couchbase.client.protostellar.query.v1.QueryGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a GRPC ManagedChannel.
 * <p>
 * Currently only have a single ProtostellarEndpoint per CoreProtostellar.
 * May eventually end up with a pool of them.
 */
public class ProtostellarEndpoint {
  private final Logger logger = LoggerFactory.getLogger(ProtostellarEndpoint.class);

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  // todo snbrett circuit breakers
  // private final CircuitBreaker circuitBreaker;

  private final ManagedChannel managedChannel;
  private final KvGrpc.KvFutureStub kvStub;
  private final KvGrpc.KvBlockingStub kvBlockingStub;
  private final AnalyticsGrpc.AnalyticsStub analyticsStub;
  private final QueryGrpc.QueryStub queryStub;
  private final String hostname;
  private final int port;
  private final CoreEnvironment environment;
  private final CoreContext coreContext;

  public ProtostellarEndpoint(final CoreContext coreContext, final CoreEnvironment environment, final String hostname, final int port) {
    logger.info("creating {} {}", hostname, port);
    this.hostname = hostname;
    this.port = port;
    this.environment = environment;
    this.coreContext = coreContext;
    this.managedChannel = channel();


    // This getState is inherently non-atomic.  However, nothing should be able to use this channel or endpoint yet, so it should be guaranteed to be IDLE.
    ConnectivityState now = this.managedChannel.getState(false);
    logger.info("channel starts in state {}/{}", now, convert(now));
    notifyOnChannelStateChange(now);

    CallCredentials creds = new CallCredentials() {
      @Override
      public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier applier) {
        executor.execute(() -> {
          try {
            Metadata headers = new Metadata();
            coreContext.authenticator().authProtostellarRequest(headers);
            applier.apply(headers);
          } catch (Throwable e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
      }

      @Override
      public void thisUsesUnstableApi() {
      }
    };

    AtomicInteger concurrentlyOutgoingMessages = new AtomicInteger();
    Set<Integer> concurrentlyOutgoingMessagesSeen = new ConcurrentSkipListSet<>();
    AtomicInteger concurrentlyIncomingMessages = new AtomicInteger();
    Set<Integer> concurrentlyIncomingMessagesSeen = new ConcurrentSkipListSet<>();

    // todo sn set idempotent flag wherever possible

    // todo sn probably remove pre-release, just trying to understand the internals
    ClientStreamTracer.Factory factory = new ClientStreamTracer.Factory() {
      public ClientStreamTracer newClientStreamTracer(ClientStreamTracer.StreamInfo info, Metadata headers) {
        return new ClientStreamTracer() {
          @Override
          public void outboundMessageSent(int seqNo, long optionalWireSize, long optionalUncompressedSize) {
            super.outboundMessageSent(seqNo, optionalWireSize, optionalUncompressedSize);
            concurrentlyOutgoingMessages.decrementAndGet();
          }

          @Override
          public void outboundMessage(int seqNo) {
            super.outboundMessage(seqNo);
            // not very useful, seqno always 0
            // logger.info("outbound {}", seqNo);

            // opsActuallyInFlight only reaches the max threads of the underlying executor.  This _might_ be the rpcs that are being concurrently
            // sent on the wire, rather than RPCs that are in-flight, which would actually be a big improvement over OOTB classic which will
            // only send one KV request at a time.
            if (concurrentlyOutgoingMessagesSeen.add(concurrentlyOutgoingMessages.incrementAndGet())) {
              System.out.println("New outgoing max " + concurrentlyOutgoingMessagesSeen.stream().max(Comparator.comparingInt(v -> v)).get());
            }
          }

          @Override
          public void inboundMessage(int seqNo) {
            super.inboundMessage(seqNo);
            // logger.info("inbound {}", seqNo);

            if (concurrentlyIncomingMessagesSeen.add(concurrentlyIncomingMessages.incrementAndGet())) {
              System.out.println("New incoming max " + concurrentlyIncomingMessagesSeen.stream().max(Comparator.comparingInt(v -> v)).get());
            }
          }

          @Override
          public void inboundMessageRead(int seqNo, long optionalWireSize, long optionalUncompressedSize) {
            super.inboundMessageRead(seqNo, optionalWireSize, optionalUncompressedSize);
            concurrentlyIncomingMessages.decrementAndGet();
          }

          @Override
          public void streamCreated(Attributes transportAttrs, Metadata headers) {
            super.streamCreated(transportAttrs, headers);
            // not very useful, doesn't give the stream id - many streams created and destroyed constantly
            // logger.info("stream created");
          }

          @Override
          public void streamClosed(Status status) {
            super.streamClosed(status);
            // logger.info("stream closed");
          }
        };
      }
    };

    ClientInterceptor ci = new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        // logger.info("{}", method.getFullMethodName());

        // todo sn an interceptor seems to be the only way of setting this option.  remove pre-release.
        return next.newCall(method, callOptions.withStreamTracerFactory(factory));
      }
    };

//    CallOptions options = CallOptions.DEFAULT
//      .withStreamTracerFactory(factory)
//      .withCallCredentials(creds);

    kvStub = KvGrpc.newFutureStub(managedChannel).withInterceptors(ci);
    kvBlockingStub = KvGrpc.newBlockingStub(managedChannel).withInterceptors(ci);
    analyticsStub = AnalyticsGrpc.newStub(managedChannel).withCallCredentials(creds);
    queryStub = QueryGrpc.newStub(managedChannel).withCallCredentials(creds);


  }

  private ManagedChannel channel() {
    logger.info("making channel {} {}", hostname, port);

    // todo sn what to do with tcpKeepAlivesEnabled

    // todo snbrett we're using unverified TLS for now - presumably we can eventually use the same Capella cert bundling approach and use TLS properly
    return NettyChannelBuilder.forAddress(hostname, port, InsecureChannelCredentials.create())
      .executor(environment.executor())
      .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) environment.timeoutConfig().connectTimeout().toMillis())
      // Retry strategies to be determined, but presumably we will need something custom rather than what GRPC provides
      .disableRetry()
      .build();
  }

  private void notifyOnChannelStateChange(ConnectivityState current) {
    this.managedChannel.notifyWhenStateChanged(current, () -> {
      ConnectivityState now = this.managedChannel.getState(false);
      logger.info("channel has changed state from {}/{} to {}/{}", current, convert(current), now, convert(now));

      EndpointContext ec = new EndpointContext(coreContext,
        new HostAndPort(hostname, port),
        null,
        null,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

      environment.eventBus().publish(new EndpointStateChangedEvent(ec, convert(current), convert(now)));

      notifyOnChannelStateChange(now);
    });
  }

  // todo sn Each gRPC channel uses 0 or more HTTP/2 connections and each connection usually has a limit on the number of concurrent streams. When the number of active RPCs on the connection reaches this limit, additional RPCs are queued in the client and must wait for active RPCs to finish before they are sent. Applications with high load or long-lived streaming RPCs might see performance issues because of this queueing.
  // https://github.com/grpc/grpc/issues/21386 indicates limit is quite low, maybe 100 concurrent rpcs
  // inspiration: https://github.com/googleapis/gax-java/blob/main/gax-grpc/src/main/java/com/google/api/gax/grpc/ChannelPool.java

  private static EndpointState convert(ConnectivityState state) {
    switch (state) {
      case IDLE:
        // Channels that haven't had any RPCs yet or in a while will be in this state.
        return EndpointState.DISCONNECTED;
      case READY:
        return EndpointState.CONNECTED;
      case SHUTDOWN:
        return EndpointState.DISCONNECTING;
      case TRANSIENT_FAILURE:
      case CONNECTING:
        return EndpointState.CONNECTING;
    }
    throw new IllegalStateException("Unknown state " + state);
  }

  // todo sn hook up diagnostics
  public EndpointDiagnostics diagnostics() {
    return new EndpointDiagnostics(null,
      convert(managedChannel.getState(false)),
      CircuitBreaker.State.CLOSED,
      null, // todo sn
      hostname,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty());
  }

  public synchronized void shutdown(Duration timeout) {
    if (shutdown.compareAndSet(false, true)) {
      logger.info("waiting for channel to shutdown");
      managedChannel.shutdown();
      try {
        managedChannel.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
      }
      logger.info("channel has shutdown");
    }
  }

  public KvGrpc.KvFutureStub kvStub() {
    return kvStub;
  }

  public KvGrpc.KvBlockingStub kvBlockingStub() {
    return kvBlockingStub;
  }

  public AnalyticsGrpc.AnalyticsStub analyticsStub() {
    return analyticsStub;
  }

  public QueryGrpc.QueryStub queryStub() {
    return queryStub;
  }

  /**
   * Note that this is synchronized against something that could block for some time - but only during shutdown.
   * <p>
   * It's synchronized to make the shutdown process atomic.
   */
  public synchronized boolean isShutdown() {
    return shutdown.get();
  }
}
